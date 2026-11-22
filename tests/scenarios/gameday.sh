#!/usr/bin/env bash
# Game day (#324): a mix of the Tester Agent practice scenarios plus a PVMBG outage, a sensor problem and a k6 load
# spike, with no manual step. It only observes during recovery: the single thing it undoes is its own injected fault
# when the fault's hold time ends, the node recovery after wipe-node, and the exit trap that puts back what it changed.
# Run against the Kubernetes cluster.
#
#   tests/scenarios/gameday.sh                   real run, SEED defaults to the epoch and is printed
#   SEED=42 tests/scenarios/gameday.sh           same order and gaps as the earlier run with SEED=42
#   DRY_RUN=1 SEED=42 tests/scenarios/gameday.sh print the plan and the commands, touches no cluster
#
# Needs: kubectl (context of the cluster), curl, jq, awk, and k6 or docker for the load spike.
#
# Accounts (all three must be different users, the access token lives only ACCESS_TOKEN_TTL_S, 60 s by default):
#   GAMEDAY_EMAIL / GAMEDAY_PASSWORD               probe user, must be Tim Lapangan (the read SLO is for Lapangan)
#   GAMEDAY_MEDIA_EMAIL / GAMEDAY_MEDIA_PASSWORD   Media user for the k6 spike, logged in right before the spike
#   GAMEDAY_FIELD_EMAIL / GAMEDAY_FIELD_PASSWORD   second Lapangan user for the k6 spike (its own rate limit bucket)
# Each probe loop logs in once and keeps its own cookie jar, so the API refreshes the expired jwt behind the request
# with the refresh_token cookie (the refresh token rotates, so jars are never shared between loops).
#
# Scenario runs: `/app run practice <name>` only submits, the steps run about 5 s later. The script polls
# `kubectl logs -n tester-agent -l app=tester-agent` for the end marker (END_PATTERN, "pemulihan akhir selesai") and
# records the real duration and an exit state per scenario (clean, ended, failed, node-down or timeout). Only then
# does the gap start. FAIL_PATTERN is a guess at the failure wording and was not checked against a live failed run.
#
# wipe-node (Tester Agent notes, #377): it wipes the local-path volumes and powers the node off, and the agent on that
# node replays the run on every boot until the ConfigMap tester-run is deleted. After the scenario the script therefore
# holds WIPE_DOWN_HOLD seconds, deletes tester-run, starts the VM, waits for the node, coordination-db, user-db,
# rabbitmq and the store DaemonSet. This is the one place the script acts during recovery, because without it the node
# powers off again. What it needs from the environment:
#   NODE_UP_CMD          command that starts the VM, run with NODE set to the Kubernetes node name. The dev VMs are
#                        VirtualBox, so for example NODE_UP_CMD='VBoxManage startvm "$NODE" --type headless'
#                        (the VM name must equal the node name, else adapt the command). Empty: the script waits and
#                        you start the VM yourself, with the same command.
#   WIPE_RECREATE_PVC=1  recreate the PVCs of coordination-db, user-db and rabbitmq when they do not become ready after
#                        the wipe (the local-path data is gone and Postgres has a broken data directory), then restart
#                        pvmbg-mock because the PVMBG token pair and the spent one time code were lost. Default 0:
#                        the script only prints the exact manual commands.
# If the wiped node is the control plane (node-a) kubectl is down meanwhile, so the script starts the VM first and
# deletes tester-run as soon as the API answers. Not undone by the script: the seeded Operator in user-db (re-run the
# user-db seed or create the account again) and messages that were in RabbitMQ.
# Output: tests/results/gameday-<stamp>.log, gameday-<stamp>-summary.md, gameday-<stamp>-probe.csv, gameday-<stamp>-alerts.csv
# shellcheck disable=SC2329  # the inject_* functions are called by name from start_injector
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 2
# shellcheck disable=SC1091
[ -f .env ] && { set -a; . ./.env; set +a; }

DRY_RUN=${DRY_RUN:-0}
SEED=${SEED:-$(date +%s)}
NS=${NS:-sistem-informasi-kebencanaan-bnpb}
TESTER_NS=${TESTER_NS:-tester-agent}
API_URL=${API_URL:-http://10.77.0.11:30081}          # Client-Facing API NodePort, see docs/DEPLOY.md
PROM_URL=${PROM_URL:-}                               # empty: kubectl port-forward svc/prometheus
PVMBG_URL=${PVMBG_URL:-}                             # empty: kubectl port-forward svc/pvmbg-mock
PROM_LOCAL_PORT=${PROM_LOCAL_PORT:-19090}
PVMBG_LOCAL_PORT=${PVMBG_LOCAL_PORT:-18082}
WARMUP=${WARMUP:-60}                                 # seconds of probing before the first fault
RECOVERY=${RECOVERY:-300}                            # seconds of observation after the last fault, nobody touches anything
GAP_MIN=${GAP_MIN:-60}
GAP_MAX=${GAP_MAX:-180}
SCENARIO_TIMEOUT=${SCENARIO_TIMEOUT:-600}
PVMBG_HOLD=${PVMBG_HOLD:-120}
SENSOR_HOLD=${SENSOR_HOLD:-90}
SENSOR_DEPLOY=${SENSOR_DEPLOY:-sensor-pantai}
SPIKE_DURATION=${SPIKE_DURATION:-45s}                    # kept under ACCESS_TOKEN_TTL_S, k6 sends only the jwt
SPIKE_MEDIA_VUS=${SPIKE_MEDIA_VUS:-60}
SPIKE_FIELD_VUS=${SPIKE_FIELD_VUS:-4}
K6_IMAGE=${K6_IMAGE:-grafana/k6:1.4.2}
READ_SLO=${READ_SLO:-99.5}                           # percent, doc/P5_SLO.md SLI 2
CRITICAL_SLO=${CRITICAL_SLO:-99.9}                   # percent, doc/P5_SLO.md SLI 1
STRICT=${STRICT:-0}                                  # 1: exit 1 when the verdict is FAIL
PROBE_EMAIL=${GAMEDAY_EMAIL:-}
PROBE_PASSWORD=${GAMEDAY_PASSWORD:-}
MEDIA_EMAIL=${GAMEDAY_MEDIA_EMAIL:-}
MEDIA_PASSWORD=${GAMEDAY_MEDIA_PASSWORD:-}
FIELD_EMAIL=${GAMEDAY_FIELD_EMAIL:-}
FIELD_PASSWORD=${GAMEDAY_FIELD_PASSWORD:-}
ACCESS_TOKEN_TTL_S=${ACCESS_TOKEN_TTL_S:-60}         # must match bnpb-config, bounds the k6 spike length
POLL_INTERVAL=${POLL_INTERVAL:-2}
END_PATTERN=${END_PATTERN:-pemulihan akhir selesai}
CLEAN_PATTERN=${CLEAN_PATTERN:-keadaan bersih}
FAIL_PATTERN=${FAIL_PATTERN:-gagal|tidak ditemukan|"level":"error"}
FAIL_GRACE=${FAIL_GRACE:-60}                         # seconds to wait for the end marker after a failure line
WIPE_DOWN_HOLD=${WIPE_DOWN_HOLD:-60}                 # seconds the node stays down, observed, before it is brought back
NODE_UP_CMD=${NODE_UP_CMD:-}
NODE_READY_TIMEOUT=${NODE_READY_TIMEOUT:-600}
STATEFUL_TIMEOUT=${STATEFUL_TIMEOUT:-180}
WIPE_RECREATE_PVC=${WIPE_RECREATE_PVC:-0}

SCENARIOS=(kill-store partition kill-leader clock-skew wipe-node schema)
INJECTORS=(pvmbg-outage sensor-down k6-spike)

STAMP=$(date +%Y%m%d-%H%M%S)
OUT=tests/results
LOG=$OUT/gameday-$STAMP.log
SUMMARY=$OUT/gameday-$STAMP-summary.md
PROBE_CSV=$OUT/gameday-$STAMP-probe.csv
ALERTS_CSV=$OUT/gameday-$STAMP-alerts.csv
WORK=$(mktemp -d)
SAVED=$WORK/replicas.saved
PIDS=()
OUTAGE_TOUCHED=0
STARTED=0
START_EPOCH=0
END_EPOCH=0
WIPE_RAN=0
SPIKE_STATE=not-run
RESULT_LINES=()

if [ "$DRY_RUN" = 1 ]; then LOG=/dev/null; else mkdir -p "$OUT"; fi

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG"; }

# run <cmd...>: the only way the script touches the cluster, so a dry run can print instead.
run() {
  if [ "$DRY_RUN" = 1 ]; then log "[dry-run] $*"; return 0; fi
  "$@"
}

# nap <seconds> <why>
nap() {
  if [ "$DRY_RUN" = 1 ]; then log "[dry-run] sleep $1 ($2)"; return 0; fi
  log "wait ${1}s ($2)"
  sleep "$1"
}

# Sets REPLY and is never called in a command substitution, or RANDOM would not advance in the parent and the seed would not repeat.
rand_between() { REPLY=$(($1 + RANDOM % ($2 - $1 + 1))); }

# ---------- exit trap: put back everything this script changed ----------

restore_all() {
  [ "$DRY_RUN" = 1 ] && return 0
  local p
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  if [ "$OUTAGE_TOUCHED" = 1 ] && [ -n "$PVMBG_URL" ]; then
    log "trap: PVMBG outage off"
    set_outage off || log "trap: could not confirm outage off, check POST /admin/outage by hand"
  fi
  if [ -s "$SAVED" ]; then
    local name want have
    while IFS='=' read -r name want; do
      have=$(kubectl get deploy "$name" -n "$NS" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "")
      if [ -n "$have" ] && [ "$have" != "$want" ]; then
        log "trap: scale deploy/$name from $have back to $want"
        kubectl scale deploy "$name" -n "$NS" --replicas="$want" >/dev/null 2>&1 || log "trap: scale of $name failed"
      fi
    done <"$SAVED"
  fi
  if [ "$WIPE_RAN" = 1 ]; then
    kubectl -n "$TESTER_NS" delete cm tester-run --ignore-not-found >/dev/null 2>&1 || log "trap: could not delete tester-run, run kubectl -n $TESTER_NS delete cm tester-run"
  fi
  rm -rf "$WORK"
}
on_exit() {
  local rc=$?
  trap - EXIT
  if [ "$STARTED" = 1 ] && [ "$DRY_RUN" != 1 ]; then log "exit trap running (rc=$rc), restoring outage and replica counts"; fi
  restore_all
  exit "$rc"
}
trap on_exit EXIT
trap 'exit 130' INT TERM

# ---------- cluster access ----------

wait_http() { # wait_http <url> <seconds>
  local i
  for ((i = 0; i < $2; i++)); do curl -sf -m 2 "$1" >/dev/null 2>&1 && return 0; sleep 1; done
  return 1
}

open_forward() { # open_forward <svc> <local port> <remote port> <health path>
  kubectl port-forward -n "$NS" "svc/$1" "$2:$3" >"$WORK/pf-$1.log" 2>&1 &
  PIDS+=("$!")
  wait_http "http://localhost:$2$4" 30 || { log "port-forward to $1 did not come up"; return 1; }
}

setup_access() {
  if [ "$DRY_RUN" = 1 ]; then
    [ -z "$PROM_URL" ] && PROM_URL="http://localhost:$PROM_LOCAL_PORT"
    [ -z "$PVMBG_URL" ] && PVMBG_URL="http://localhost:$PVMBG_LOCAL_PORT"
    log "[dry-run] kubectl port-forward -n $NS svc/prometheus $PROM_LOCAL_PORT:9090 (unless PROM_URL is set)"
    log "[dry-run] kubectl port-forward -n $NS svc/pvmbg-mock $PVMBG_LOCAL_PORT:8082 (unless PVMBG_URL is set)"
    return 0
  fi
  kubectl get ns "$NS" >/dev/null 2>&1 || { echo "kubectl cannot reach namespace $NS" >&2; exit 2; }
  if [ -z "$PROM_URL" ]; then
    open_forward prometheus "$PROM_LOCAL_PORT" 9090 /-/ready || exit 2
    PROM_URL=http://localhost:$PROM_LOCAL_PORT
  fi
  if [ -z "$PVMBG_URL" ]; then
    open_forward pvmbg-mock "$PVMBG_LOCAL_PORT" 8082 /health || exit 2
    PVMBG_URL=http://localhost:$PVMBG_LOCAL_PORT
  fi
  curl -sf -m 3 "$API_URL/health" >/dev/null || { echo "API not reachable at $API_URL (set API_URL)" >&2; exit 2; }
}

save_replicas() {
  if [ "$DRY_RUN" = 1 ]; then log "[dry-run] kubectl get deploy -n $NS -o jsonpath=... > replicas.saved (name=replicas, restored by the trap)"; return 0; fi
  kubectl get deploy -n "$NS" -o jsonpath='{range .items[*]}{.metadata.name}={.spec.replicas}{"\n"}{end}' >"$SAVED"
  log "saved replica counts: $(tr '\n' ' ' <"$SAVED")"
}

saved_replicas() { # saved_replicas <deploy>, falls back to 1
  if [ "$DRY_RUN" = 1 ]; then echo 1; return; fi
  awk -F= -v n="$1" '$1 == n {print $2; found=1} END {if (!found) print 1}' "$SAVED"
}

SAVED_NODES=$WORK/nodes.saved
save_nodes() {
  if [ "$DRY_RUN" = 1 ]; then log "[dry-run] kubectl get nodes (names saved so a wiped control plane can be restarted)"; return 0; fi
  kubectl get nodes --no-headers 2>/dev/null | awk '{print $1 "=1"}' >"$SAVED_NODES"
}

set_outage() { # set_outage on|off, POST /admin/outage toggles, so read the state first
  local want=false cur i
  [ "$1" = on ] && want=true
  for i in 1 2 3; do
    cur=$(curl -sf -m 3 "$PVMBG_URL/health" | jq -r '.outage_active' 2>/dev/null)
    [ "$cur" = "$want" ] && return 0
    [ -n "$cur" ] && curl -sf -m 3 -X POST "$PVMBG_URL/admin/outage" >/dev/null
    sleep 1
  done
  return 1
}

# login_jar <email> <password> <jar>: logs in and keeps jwt and refresh_token in the jar, 0 when the jwt cookie is there.
login_jar() {
  : >"$3"
  curl -s -m 5 -c "$3" -X POST "$API_URL/auth/login" -d "{\"email\":\"$1\",\"password\":\"$2\"}" >/dev/null
  awk '$6 == "jwt" && $7 != "" {f=1} END {exit !f}' "$3"
}

login_jwt() { # login_jwt <email> <password>, prints a fresh jwt cookie value
  local jar
  jar=$(mktemp -p "$WORK")
  login_jar "$1" "$2" "$jar" || return 1
  awk '$6 == "jwt" {print $7}' "$jar" | tail -1
}

dur_seconds() { # 45s, 90, 2m
  case $1 in
    *m) echo $((${1%m} * 60)) ;;
    *s) echo "${1%s}" ;;
    *) echo "$1" ;;
  esac
}

# ---------- the plan ----------

build_plan() {
  RANDOM=$SEED
  local i j tmp n=${#SCENARIOS[@]}
  ORDER=("${SCENARIOS[@]}")
  for ((i = n - 1; i > 0; i--)); do
    j=$((RANDOM % (i + 1)))
    tmp=${ORDER[i]}; ORDER[i]=${ORDER[j]}; ORDER[j]=$tmp
  done
  # Each injector rides along with a distinct scenario slot, so faults overlap on purpose.
  local slots=() k idx
  for ((i = 0; i < n; i++)); do slots+=("$i"); done
  for ((i = n - 1; i > 0; i--)); do
    j=$((RANDOM % (i + 1)))
    tmp=${slots[i]}; slots[i]=${slots[j]}; slots[j]=$tmp
  done
  SLOT_INJECTOR=()
  SLOT_DELAY=()
  for ((k = 0; k < ${#INJECTORS[@]}; k++)); do
    idx=${slots[k]}
    SLOT_INJECTOR[idx]=${INJECTORS[k]}
    rand_between 5 30
    SLOT_DELAY[idx]=$REPLY
  done
  GAPS=()
  for ((i = 0; i < n; i++)); do rand_between "$GAP_MIN" "$GAP_MAX"; GAPS+=("$REPLY"); done
}

print_plan() {
  log "plan for SEED=$SEED: warmup ${WARMUP}s, then ${#ORDER[@]} scenarios, recovery observation ${RECOVERY}s"
  local i
  for ((i = 0; i < ${#ORDER[@]}; i++)); do
    log "  slot $((i + 1)): scenario ${ORDER[i]}${SLOT_INJECTOR[i]:+ plus injector ${SLOT_INJECTOR[i]} after ${SLOT_DELAY[i]}s}, then gap ${GAPS[i]}s"
  done
}

# ---------- injectors, each undoes only its own fault when its hold time ends ----------

inject_pvmbg_outage() {
  log "inject: PVMBG outage on for ${PVMBG_HOLD}s"
  run set_outage on
  nap "$PVMBG_HOLD" "PVMBG outage hold"
  run set_outage off
  log "inject: PVMBG outage off (the injected fault ended, the system has to recover by itself)"
}

inject_sensor_down() {
  local want
  want=$(saved_replicas "$SENSOR_DEPLOY")
  log "inject: sensor problem, scale deploy/$SENSOR_DEPLOY to 0 for ${SENSOR_HOLD}s"
  run kubectl scale deploy "$SENSOR_DEPLOY" -n "$NS" --replicas=0
  nap "$SENSOR_HOLD" "sensor down hold"
  run kubectl scale deploy "$SENSOR_DEPLOY" -n "$NS" --replicas="$want"
  log "inject: sensor scaled back to $want (the injected fault ended)"
}

inject_k6_spike() {
  local media field secs max
  secs=$(dur_seconds "$SPIKE_DURATION")
  max=$((ACCESS_TOKEN_TTL_S - 10))
  if [ "$secs" -gt "$max" ]; then
    log "warning: SPIKE_DURATION $SPIKE_DURATION is longer than the token lifetime (${ACCESS_TOKEN_TTL_S}s), cut to ${max}s"
    SPIKE_DURATION=${max}s
  fi
  log "inject: k6 spike tests/load/priority.js for $SPIKE_DURATION, $SPIKE_MEDIA_VUS media and $SPIKE_FIELD_VUS field VUs"
  if [ "$DRY_RUN" = 1 ]; then
    log "[dry-run] login GAMEDAY_MEDIA_EMAIL and GAMEDAY_FIELD_EMAIL right before the spike (fresh jwt each, separate from the probe user)"
    log "[dry-run] k6 run tests/load/priority.js (local k6, else docker run $K6_IMAGE) BASE_URL=$API_URL DURATION=$SPIKE_DURATION"
    return 0
  fi
  media=$(login_jwt "$MEDIA_EMAIL" "$MEDIA_PASSWORD")
  field=$(login_jwt "$FIELD_EMAIL" "$FIELD_PASSWORD")
  if [ -z "$media" ] || [ -z "$field" ]; then
    log "inject: k6 spike NOT run, login failed for the media or field user (the cluster may be down at this moment)"
    SPIKE_STATE=login-failed
    return 0
  fi
  # k6 thresholds are expected to fail while the cluster is hurt, so its exit code is logged and not acted on.
  local rc
  if command -v k6 >/dev/null 2>&1; then
    BASE_URL=$API_URL MEDIA_JWT=$media FIELD_JWT=$field DURATION=$SPIKE_DURATION MEDIA_VUS=$SPIKE_MEDIA_VUS FIELD_VUS=$SPIKE_FIELD_VUS \
      k6 run --summary-export "$OUT/gameday-$STAMP-k6.json" tests/load/priority.js >>"$LOG" 2>&1
  else
    docker run --rm --network host -v "$PWD/tests/load:/load:ro" \
      -e BASE_URL="$API_URL" -e MEDIA_JWT="$media" -e FIELD_JWT="$field" -e DURATION="$SPIKE_DURATION" \
      -e MEDIA_VUS="$SPIKE_MEDIA_VUS" -e FIELD_VUS="$SPIKE_FIELD_VUS" "$K6_IMAGE" run /load/priority.js >>"$LOG" 2>&1
  fi
  rc=$?
  log "inject: k6 spike finished with exit code $rc"
}

start_injector() { # start_injector <name> <delay>, runs in the background
  local name=$1 delay=$2
  # The flag is set here, not inside the subshell, so the exit trap sees it.
  [ "$name" = pvmbg-outage ] && OUTAGE_TOUCHED=1
  (
    nap "$delay" "random delay before $name"
    "inject_${name//-/_}"
  ) &
  PIDS+=("$!")
  JOBS_STARTED+=("$!")
}

# tester_logs <since RFC3339 UTC>: the agent log lines of every node since then. --tail=-1 because a selector defaults
# to 10 lines, --ignore-errors because the pod of a powered off node cannot answer.
tester_logs() {
  timeout 30 kubectl logs -n "$TESTER_NS" -l app=tester-agent --since-time="$1" --tail=-1 --ignore-errors 2>/dev/null
}

down_nodes() { kubectl get nodes --no-headers 2>/dev/null | awk '$2 !~ /^Ready/ {print $1}'; }

# wait_scenario <name> <t0 epoch> <since RFC3339>: sets STATE and returns when the run has ended in the agent log.
wait_scenario() {
  local name=$1 t0=$2 since=$3 logs fail_at=0 now
  STATE=timeout
  while :; do
    now=$(date +%s)
    if [ $((now - t0)) -ge "$SCENARIO_TIMEOUT" ]; then STATE=timeout; return 0; fi
    logs=$(tester_logs "$since")
    if printf '%s\n' "$logs" | grep -qiE "$END_PATTERN"; then
      if printf '%s\n' "$logs" | grep -qiE "$FAIL_PATTERN"; then STATE=failed
      elif printf '%s\n' "$logs" | grep -qiE "$CLEAN_PATTERN"; then STATE=clean
      else STATE=ended; fi
      return 0
    fi
    if printf '%s\n' "$logs" | grep -qiE "$FAIL_PATTERN"; then
      [ "$fail_at" = 0 ] && fail_at=$now
      if [ $((now - fail_at)) -ge "$FAIL_GRACE" ]; then STATE=failed; return 0; fi
    fi
    if [ "$name" = wipe-node ] && [ -n "$(down_nodes)" ]; then STATE=node-down; return 0; fi
    sleep "$POLL_INTERVAL"
  done
}

# ---------- wipe-node recovery: the node is down and replays the run on every boot ----------

delete_tester_run() { kubectl -n "$TESTER_NS" delete cm tester-run --ignore-not-found >/dev/null 2>&1; }

start_node() { # start_node <node>
  if [ -n "$NODE_UP_CMD" ]; then
    log "start node $1 with NODE_UP_CMD"
    NODE=$1 bash -c "$NODE_UP_CMD" >>"$LOG" 2>&1 || log "NODE_UP_CMD failed for $1, start the VM by hand"
  else
    log "NODE_UP_CMD is empty: start the VM of node $1 now (VirtualBox: VBoxManage startvm \"$1\" --type headless), waiting up to ${NODE_READY_TIMEOUT}s"
  fi
}

wait_nodes_ready() { # until no node is NotReady and the API answers
  local i
  for ((i = 0; i < NODE_READY_TIMEOUT; i += 5)); do
    if kubectl get nodes >/dev/null 2>&1 && [ -z "$(down_nodes)" ]; then return 0; fi
    sleep 5
  done
  return 1
}

workload_ready() { # workload_ready <kind/name>, for example statefulset/user-db or ds/store
  kubectl rollout status "$1" -n "$NS" --timeout="${STATEFUL_TIMEOUT}s" >>"$LOG" 2>&1
}

recreate_pvc() { # recreate_pvc <statefulset>
  log "recreate PVC data-$1-0 and pod $1-0 (local-path data was wiped)"
  kubectl delete pvc "data-$1-0" -n "$NS" --wait=false >>"$LOG" 2>&1
  kubectl delete pod "$1-0" -n "$NS" >>"$LOG" 2>&1
}

recover_after_wipe() {
  local nodes n
  WIPE_RAN=1
  if [ "$DRY_RUN" = 1 ]; then
    log "[dry-run] hold ${WIPE_DOWN_HOLD}s with the node down, then: kubectl -n $TESTER_NS delete cm tester-run"
    log "[dry-run] start the node with NODE_UP_CMD (NODE=<node>), for example VBoxManage startvm <node> --type headless; empty means you start it"
    log "[dry-run] wait for nodes Ready, rollout status of statefulset/coordination-db, user-db, rabbitmq and ds/store"
    log "[dry-run] WIPE_RECREATE_PVC=$WIPE_RECREATE_PVC: if set, delete pvc data-<name>-0 and pod <name>-0 of a stateful pod that stays not ready, then restart deploy/pvmbg-mock"
    return 0
  fi
  nodes=$(down_nodes)
  [ -n "$nodes" ] && log "node(s) down after wipe-node: $(echo "$nodes" | tr '\n' ' ')"
  nap "$WIPE_DOWN_HOLD" "node stays down, observing the outage"
  if kubectl -n "$TESTER_NS" get cm tester-run >/dev/null 2>&1; then
    if delete_tester_run; then log "deleted ConfigMap tester-run (the agent would power the node off again at the next boot)"
    else log "could not delete tester-run now (the API is probably down), will retry once the node is back"; fi
  fi
  # With the API down no node can be named, so every saved node gets the start command, starting a running VM is harmless.
  [ -z "$nodes" ] && nodes=$(awk -F= '{print $1}' "$SAVED_NODES")
  for n in $nodes; do start_node "$n"; done
  if wait_nodes_ready; then log "all nodes Ready"; else log "nodes not Ready after ${NODE_READY_TIMEOUT}s, check the VM and k3s by hand"; return 0; fi
  delete_tester_run && log "tester-run deleted (no replay on later boots)"
  local w bad=()
  for w in statefulset/coordination-db statefulset/user-db statefulset/rabbitmq ds/store; do
    if workload_ready "$w"; then log "ready: $w"; else log "not ready after ${STATEFUL_TIMEOUT}s: $w"; bad+=("$w"); fi
  done
  [ "${#bad[@]}" -eq 0 ] && return 0
  if [ "$WIPE_RECREATE_PVC" = 1 ]; then
    for w in "${bad[@]}"; do
      case $w in statefulset/*) recreate_pvc "${w#statefulset/}" ;; esac
    done
    for w in "${bad[@]}"; do
      if workload_ready "$w"; then log "ready after PVC recreation: $w"; else log "still not ready: $w"; fi
    done
    kubectl rollout restart deploy/pvmbg-mock -n "$NS" >>"$LOG" 2>&1 && log "restarted pvmbg-mock (token pair and one time code were lost with the wipe)"
    log "user-db and RabbitMQ were reset: the seeded Operator and queued messages are gone, re-seed the Operator by hand"
  else
    log "WIPE_RECREATE_PVC is 0, manual steps for each statefulset above (NAME = coordination-db, user-db or rabbitmq):"
    log "  kubectl delete pvc data-NAME-0 -n $NS --wait=false && kubectl delete pod NAME-0 -n $NS"
    log "  kubectl rollout restart deploy/pvmbg-mock -n $NS   (PVMBG token pair lost with coordination-db)"
  fi
}

run_scenario() {
  local name=$1 t0 since out rc dur
  t0=$(date +%s)
  since=$(date -u -d "@$t0" +%Y-%m-%dT%H:%M:%SZ)
  log "scenario $name: start"
  if [ "$DRY_RUN" = 1 ]; then
    log "[dry-run] timeout 60 kubectl exec -n $TESTER_NS daemonset/tester-agent -- /app run practice $name   (only submits, prints Permintaan <id> dikirim)"
    log "[dry-run] poll every ${POLL_INTERVAL}s: kubectl logs -n $TESTER_NS -l app=tester-agent --since-time=<start> --tail=-1 --ignore-errors, until /$END_PATTERN/ (timeout ${SCENARIO_TIMEOUT}s)"
    [ "$name" = wipe-node ] && recover_after_wipe
    return 0
  fi
  out=$(timeout 60 kubectl exec -n "$TESTER_NS" daemonset/tester-agent -- /app run practice "$name" 2>&1)
  rc=$?
  printf '%s\n' "$out" >>"$LOG"
  if [ "$rc" != 0 ]; then
    STATE="submit-failed"
  else
    log "scenario $name: submitted ($(printf '%s' "$out" | tr '\n' ' ')), waiting for the end marker"
    wait_scenario "$name" "$t0" "$since"
  fi
  dur=$(($(date +%s) - t0))
  log "scenario $name: $STATE after ${dur}s (submit rc=$rc)"
  RESULT_LINES+=("| $name | $STATE | $rc | $(date -d "@$t0" +%H:%M:%S) | ${dur}s |")
  [ "$name" = wipe-node ] && recover_after_wipe
  return 0
}

# ---------- probe: availability of the main endpoints, about one sample per second each ----------

# probe_loop <name> <path>: its own cookie jar. An expired jwt is refreshed behind the request through refresh_token, and
# the jar takes the rotated pair. After 3 failures in a row with 401 (a refresh whose answer was lost) it logs in again.
probe_loop() {
  local code jar fails=0
  jar=$(mktemp -p "$WORK")
  login_jar "$PROBE_EMAIL" "$PROBE_PASSWORD" "$jar"
  while :; do
    code=$(curl -s -o /dev/null -m 2 -b "$jar" -c "$jar" -w '%{http_code}' "$API_URL$2")
    echo "$(date +%s),$1,$code" >>"$PROBE_CSV"
    if [ "$code" = 401 ]; then
      fails=$((fails + 1))
      [ "$fails" -ge 3 ] && { login_jar "$PROBE_EMAIL" "$PROBE_PASSWORD" "$jar"; fails=0; }
    else fails=0; fi
    sleep 1
  done
}

prom_scalar() { # prom_scalar <expr> [epoch], prints the first value or n/a
  local extra=()
  [ -n "${2:-}" ] && extra=(--data-urlencode "time=$2")
  curl -sG -m 10 "$PROM_URL/api/v1/query" --data-urlencode "query=$1" "${extra[@]}" 2>/dev/null |
    jq -r '.data.result[0].value[1] // "n/a"' 2>/dev/null || echo n/a
}

alerts_loop() { # every 10 s: critical alert delivery as seen by Prometheus
  while :; do
    echo "$(date +%s),$(prom_scalar 'sum(critical_alerts_delivered_total)'),$(prom_scalar 'sum(aggregator_critical_staged_total)'),$(prom_scalar 'slo:critical_alert_undelivered:count10m')" >>"$ALERTS_CSV"
    sleep 10
  done
}

start_probes() {
  if [ "$DRY_RUN" = 1 ]; then
    log "[dry-run] login $API_URL/auth/login as the probe user (Tim Lapangan), one cookie jar per loop, one background loop per endpoint writing to $PROBE_CSV:"
    log "[dry-run]   GET /health, GET /hazards?type=SEISMIC, GET /hazards?type=VOLCANIC, GET /hazards?type=COASTAL"
    log "[dry-run] alert loop every 10s to $ALERTS_CSV: sum(critical_alerts_delivered_total), sum(aggregator_critical_staged_total), slo:critical_alert_undelivered:count10m"
    return 0
  fi
  local probe_jar=$WORK/probe-check.jar need
  for need in GAMEDAY_EMAIL GAMEDAY_PASSWORD GAMEDAY_MEDIA_EMAIL GAMEDAY_MEDIA_PASSWORD GAMEDAY_FIELD_EMAIL GAMEDAY_FIELD_PASSWORD; do
    [ -n "${!need:-}" ] || { echo "Set $need (probe user is Lapangan, media and field users are separate accounts, see the script header)" >&2; exit 2; }
  done
  if [ "$PROBE_EMAIL" = "$FIELD_EMAIL" ] || [ "$PROBE_EMAIL" = "$MEDIA_EMAIL" ] || [ "$FIELD_EMAIL" = "$MEDIA_EMAIL" ]; then
    echo "The probe, media and field users must be three different accounts, the spike would eat the probe's rate limit bucket" >&2; exit 2
  fi
  login_jar "$PROBE_EMAIL" "$PROBE_PASSWORD" "$probe_jar" || { echo "Login failed for the probe user" >&2; exit 2; }
  login_jar "$MEDIA_EMAIL" "$MEDIA_PASSWORD" "$probe_jar" || { echo "Login failed for the media user" >&2; exit 2; }
  login_jar "$FIELD_EMAIL" "$FIELD_PASSWORD" "$probe_jar" || { echo "Login failed for the field user" >&2; exit 2; }
  : >"$PROBE_CSV"
  : >"$ALERTS_CSV"
  local pair
  for pair in "health:/health" "seismic:/hazards?type=SEISMIC" "volcanic:/hazards?type=VOLCANIC"; do
    probe_loop "${pair%%:*}" "${pair#*:}" &
    PIDS+=("$!")
    sleep 1
  done
  alerts_loop &
  PIDS+=("$!")
}

# ---------- summary against the SLO and the error budget ----------

# availability <endpoint> prints: samples good percent longest_failing_streak_seconds
availability() {
  awk -F, -v n="$1" '
    $2 == n { t++; if ($3 == 200) { g++; run = 0 } else { run++; if (run > worst) worst = run } }
    END { printf "%d %d %.3f %d\n", t, g, (t ? 100 * g / t : 0), worst }' "$PROBE_CSV"
}

verdict_line() { # verdict_line <PASS|FAIL|NO DATA> <text>
  printf -- '- %s: %s\n' "$1" "$2"
}

num_ge() { awk -v a="$1" -v b="$2" 'BEGIN {exit !(a + 0 >= b + 0)}'; }
isnum() { [[ $1 =~ ^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$ ]]; }

write_summary() {
  local run_s=$((END_EPOCH - START_EPOCH)) at=$END_EPOCH
  local fail=0 ep t g pct worst
  {
    echo "# Game day summary $STAMP"
    echo
    echo "Seed $SEED, run of ${run_s}s from $(date -d "@$START_EPOCH" '+%F %T') to $(date -d "@$END_EPOCH" '+%T'). Log: $LOG."
    echo
    echo "## Plan and scenario results"
    echo
    echo "| Scenario | End state in the agent log | Submit exit code | Started | Duration until the end marker |"
    echo "|---|---|---|---|---|"
    printf '%s\n' "${RESULT_LINES[@]}"
    echo
    echo "End state: clean (end marker and clean state seen), ended (end marker only), failed (failure line seen), node-down (wipe-node powered a node off), timeout (SCENARIO_TIMEOUT hit), submit-failed. k6 spike: $SPIKE_STATE."
    echo
    echo "Injected faults, run alongside the scenarios:"
    local i
    for ((i = 0; i < ${#ORDER[@]}; i++)); do
      [ -n "${SLOT_INJECTOR[i]:-}" ] && echo "- slot $((i + 1)) ${ORDER[i]}: ${SLOT_INJECTOR[i]}"
    done
    echo
    echo "## Probe: availability of the main endpoints (about 1 sample per second, good = HTTP 200)"
    echo
    echo "| Endpoint | Samples | Good | Availability | Longest failing streak |"
    echo "|---|---|---|---|---|"
    for ep in health seismic volcanic; do
      read -r t g pct worst < <(availability "$ep")
      echo "| $ep | $t | $g | $pct percent | ${worst}s |"
    done
    echo
    echo "## Result against the SLO (doc/P5_SLO.md)"
    echo
  } >"$SUMMARY"

  local reads_t=0 reads_g=0 ep_t ep_g
  for ep in seismic volcanic; do
    read -r ep_t ep_g _ _ < <(availability "$ep")
    reads_t=$((reads_t + ep_t)); reads_g=$((reads_g + ep_g))
  done
  local read_pct=n/a
  [ "$reads_t" -gt 0 ] && read_pct=$(awk -v g="$reads_g" -v t="$reads_t" 'BEGIN {printf "%.3f", 100 * g / t}')

  local c_sli c_sli_min c_burn5 c_burn1h c_rem2h c_rem_min c_undeliv c_undeliv_max
  local r_sli_min r_burn5 r_burn1h r_rem2h r_rem_min c_range r_range firing
  c_sli=$(prom_scalar 'slo:critical_alert_sli:ratio_rate5m' "$at")
  c_sli_min=$(prom_scalar "min_over_time(slo:critical_alert_sli:ratio_rate5m[${run_s}s])" "$at")
  c_burn5=$(prom_scalar "max_over_time(slo:critical_alert_burn_rate:rate5m[${run_s}s])" "$at")
  c_burn1h=$(prom_scalar "max_over_time(slo:critical_alert_burn_rate:rate1h[${run_s}s])" "$at")
  c_rem2h=$(prom_scalar 'slo:critical_alert_error_budget_remaining:ratio2h' "$at")
  c_rem_min=$(prom_scalar "min_over_time(slo:critical_alert_error_budget_remaining:ratio2h[${run_s}s])" "$at")
  c_undeliv=$(prom_scalar 'slo:critical_alert_undelivered:count10m' "$at")
  c_undeliv_max=$(prom_scalar "max_over_time(slo:critical_alert_undelivered:count10m[${run_s}s])" "$at")
  # Same expression as dashboard panels 23 and 24, with $__range replaced by the run length.
  c_range=$(prom_scalar "(1 - (1 - sum(increase(critical_alert_latency_seconds_bucket{le=\"5\"}[${run_s}s])) / sum(increase(critical_alert_latency_seconds_count[${run_s}s]))) / 0.001)" "$at")
  r_sli_min=$(prom_scalar "min_over_time(slo:read_sli:ratio_rate5m[${run_s}s])" "$at")
  r_burn5=$(prom_scalar "max_over_time(slo:read_burn_rate:rate5m[${run_s}s])" "$at")
  r_burn1h=$(prom_scalar "max_over_time(slo:read_burn_rate:rate1h[${run_s}s])" "$at")
  r_rem2h=$(prom_scalar 'slo:read_error_budget_remaining:ratio2h' "$at")
  r_rem_min=$(prom_scalar "min_over_time(slo:read_error_budget_remaining:ratio2h[${run_s}s])" "$at")
  r_range=$(prom_scalar "(1 - (((sum(increase(traces_span_metrics_calls_total{service_name=\"client-facing-api\", span_kind=\"SPAN_KIND_SERVER\", status_code=\"STATUS_CODE_ERROR\"}[${run_s}s])) or on() vector(0)) + (sum(increase(client_api_inflight_rejected_requests_total{role=\"Lapangan\"}[${run_s}s])) or on() vector(0))) / clamp_min(sum(increase(traces_span_metrics_calls_total{service_name=\"client-facing-api\", span_kind=\"SPAN_KIND_SERVER\"}[${run_s}s])) - (sum(increase(client_api_inflight_rejected_requests_total{role!=\"Lapangan\"}[${run_s}s])) or on() vector(0)), 0)) / 0.005)" "$at")
  firing=$(curl -sG -m 10 "$PROM_URL/api/v1/query" --data-urlencode "query=max by (alertname) (max_over_time(ALERTS{alertstate=\"firing\"}[${run_s}s]))" --data-urlencode "time=$at" 2>/dev/null |
    jq -r '[.data.result[].metric.alertname] | join(", ")' 2>/dev/null)

  budget_used() { isnum "$1" && awk -v r="$1" 'BEGIN {printf "%.1f", 100 * (1 - r)}' || echo n/a; }

  {
    echo "| Measure | Critical alert timeliness (SLO $CRITICAL_SLO percent within 5 s) | Tim Lapangan read availability (SLO $READ_SLO percent, proxy) |"
    echo "|---|---|---|"
    echo "| Current SLI, last 5m (slo:critical_alert_sli:ratio_rate5m, slo:read_sli:ratio_rate5m) | $c_sli | not queried |"
    echo "| Lowest SLI during the run | $c_sli_min | $r_sli_min |"
    echo "| Peak burn rate 5m (slo:*_burn_rate:rate5m) | $c_burn5 | $r_burn5 |"
    echo "| Peak burn rate 1h (slo:*_burn_rate:rate1h) | $c_burn1h | $r_burn1h |"
    echo "| Budget remaining, trailing 2h at the end (slo:*_error_budget_remaining:ratio2h) | $c_rem2h | $r_rem2h |"
    echo "| Lowest budget remaining during the run | $c_rem_min | $r_rem_min |"
    echo "| Budget used, trailing 2h at the end | $(budget_used "$c_rem2h") percent | $(budget_used "$r_rem2h") percent |"
    echo "| Budget remaining over the run range (dashboard panels 23 and 24) | $c_range | $r_range |"
    echo "| Undelivered critical alerts, now and peak (slo:critical_alert_undelivered:count10m) | $c_undeliv and $c_undeliv_max | not applicable |"
    echo "| Probe availability of the two /hazards reads | not applicable | $read_pct percent |"
    echo
    echo "Alerts that fired during the run: ${firing:-none or Prometheus not reachable}."
    echo
    echo "Budget remaining below 0 means the budget is exhausted. The trailing 2h window also holds traffic from before the run (Prometheus keeps 2h, doc/P5_SLO.md section 5), and the run-range value is strict when few events exist."
    echo
    echo "## Verdict"
    echo
  } >>"$SUMMARY"

  if isnum "$read_pct"; then
    if num_ge "$read_pct" "$READ_SLO"; then verdict_line PASS "probe read availability $read_pct percent, target $READ_SLO" >>"$SUMMARY"
    else verdict_line FAIL "probe read availability $read_pct percent, target $READ_SLO" >>"$SUMMARY"; fail=1; fi
  else verdict_line "NO DATA" "probe read availability" >>"$SUMMARY"; fi

  if isnum "$c_sli_min"; then
    if num_ge "$c_sli_min" "$(awk -v s="$CRITICAL_SLO" 'BEGIN {print s / 100}')"; then verdict_line PASS "critical alert SLI never below target, lowest $c_sli_min" >>"$SUMMARY"
    else verdict_line FAIL "critical alert SLI dipped to $c_sli_min, target $CRITICAL_SLO percent" >>"$SUMMARY"; fail=1; fi
  else verdict_line "NO DATA" "critical alert SLI (Prometheus rule returned nothing)" >>"$SUMMARY"; fi

  if isnum "$c_undeliv_max"; then
    if awk -v v="$c_undeliv_max" 'BEGIN {exit !(v + 0 == 0)}'; then verdict_line PASS "no critical alert staged and left undelivered" >>"$SUMMARY"
    else verdict_line FAIL "critical alerts were undelivered at some point, peak $c_undeliv_max (requirement: zero lost)" >>"$SUMMARY"; fail=1; fi
  else verdict_line "NO DATA" "undelivered critical alerts" >>"$SUMMARY"; fi

  if isnum "$c_rem2h" && isnum "$r_rem2h"; then
    if num_ge "$c_rem2h" 0 && num_ge "$r_rem2h" 0; then verdict_line PASS "both error budgets still positive" >>"$SUMMARY"
    else verdict_line FAIL "an error budget is exhausted (critical $c_rem2h, read $r_rem2h)" >>"$SUMMARY"; fail=1; fi
  else verdict_line "NO DATA" "error budgets" >>"$SUMMARY"; fi

  echo >>"$SUMMARY"
  echo "Write the post-mortem from doc/POSTMORTEM_TEMPLATE.md and explain each failure with doc/EXPLAIN_FAILURE.md. Do not alter this file." >>"$SUMMARY"
  log "summary written to $SUMMARY"
  cat "$SUMMARY"
  GAMEDAY_FAIL=$fail
}

print_dry_summary() {
  log "[dry-run] at the end the summary queries Prometheus ($PROM_URL) at the end time, over the run length:"
  local q
  for q in slo:critical_alert_sli:ratio_rate5m slo:critical_alert_burn_rate:rate5m slo:critical_alert_burn_rate:rate1h \
    slo:critical_alert_error_budget_remaining:ratio2h slo:critical_alert_undelivered:count10m \
    slo:read_sli:ratio_rate5m slo:read_burn_rate:rate5m slo:read_burn_rate:rate1h slo:read_error_budget_remaining:ratio2h; do
    log "[dry-run]   $q (value at the end, min_over_time or max_over_time over the run)"
  done
  log "[dry-run]   dashboard panels 23 and 24 expressions with the run length for \$__range, and ALERTS{alertstate=\"firing\"}"
  log "[dry-run] then it writes $SUMMARY and prints PASS or FAIL against SLO $CRITICAL_SLO and $READ_SLO percent, error budget used"
}

# ---------- main ----------

GAMEDAY_FAIL=0
JOBS_STARTED=()
log "game day, SEED=$SEED, DRY_RUN=$DRY_RUN, API_URL=$API_URL, namespace $NS"
build_plan
print_plan
setup_access
save_replicas
save_nodes
STARTED=1
log "start probes"
start_probes
START_EPOCH=$(date +%s)
nap "$WARMUP" "warmup, baseline before any fault"

for ((slot = 0; slot < ${#ORDER[@]}; slot++)); do
  if [ -n "${SLOT_INJECTOR[slot]:-}" ]; then start_injector "${SLOT_INJECTOR[slot]}" "${SLOT_DELAY[slot]}"; fi
  run_scenario "${ORDER[slot]}"
  nap "${GAPS[slot]}" "gap after ${ORDER[slot]}, observing only"
done

if [ "${#JOBS_STARTED[@]}" -gt 0 ] && [ "$DRY_RUN" != 1 ]; then
  log "waiting for the injected faults to end"
  wait "${JOBS_STARTED[@]}" 2>/dev/null
fi
log "recovery window, hands off (the node recovery after wipe-node was already done above)"
nap "$RECOVERY" "recovery observation, the script changes nothing"
END_EPOCH=$(date +%s)

if [ "$DRY_RUN" = 1 ]; then
  print_dry_summary
  log "[dry-run] exit trap would: turn the PVMBG outage off if on, scale deployments back to the saved counts, stop probes and port-forwards"
  log "dry run done"
  exit 0
fi
write_summary
if [ "$GAMEDAY_FAIL" = 1 ]; then
  log "RESULT: FAIL (see the verdict above)"
  [ "$STRICT" = 1 ] && exit 1
else
  log "RESULT: PASS or NO DATA only (see the verdict above)"
fi
exit 0
