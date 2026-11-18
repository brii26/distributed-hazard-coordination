#!/usr/bin/env bash
# End-to-end orchestrator (#328): runs every practice scenario script that exists, in a fixed order, against the Kubernetes
# cluster and prints one summary table. It only runs the child scripts, it has no scenario logic of its own.
#
#   tests/scenarios/all-k8s.sh --list                       the scenarios, their scripts, timeouts and what each needs
#   DRY_RUN=1 tests/scenarios/all-k8s.sh                    the plan and the commands, touches no cluster, runs no child
#   tests/scenarios/all-k8s.sh                              every non-destructive scenario
#   tests/scenarios/all-k8s.sh --only rolling-update,autoscale
#   tests/scenarios/all-k8s.sh --skip load-ratio
#   tests/scenarios/all-k8s.sh --include-destructive        also gameday and wipe-node (prints what they destroy first)
#
# Options: --list, --only <a,b>, --skip <a,b> (both repeatable), --include-destructive, --no-audit (skip the Audit API check
# after each scenario), --fail-on-skip (a SKIP makes the exit code 1), -h.
# Env: DRY_RUN=1, NS (sistem-informasi-kebencanaan-bnpb), TESTER_NS (tester-agent), KUBECTL (kubectl), RESULTS_DIR
# (tests/results), TIMEOUT_<NAME> in seconds with the name upper case and - as _ (TIMEOUT_ROLLING_UPDATE=900), HEALTH_WAIT
# (120 s to wait for a healthy cluster before and after each scenario), HEALTH_POLL (5), CONFIRM_SECONDS (10, countdown before
# a destructive scenario, 0 for none), AUDIT_URL and AUDIT_TOKEN (default: port-forward of deployment/aggregator and the Secret
# audit-token), SCRIPT_ROOT (the repository root, only changed by the self test).
# The child scripts read their own settings (EMAIL, PASSWORD, API_URL, BASE_URL, GAMEDAY_*), each one is listed by --list.
#
# Exit code: 0 when nothing failed (PASS and SKIP only), 1 when a scenario FAILed or timed out, an Audit check failed, or a SKIP
# under --fail-on-skip, 2 for a usage error or when the cluster is not healthy at the start. A child that exits 2 means it
# could not start (missing input, no cluster) and is reported as SKIP with that reason, 124 and 137 as TIMEOUT, other codes as FAIL.
# Needs: kubectl, curl, jq, timeout. Results: tests/results/all-<stamp>.json, .txt, .log and all-<stamp>/<scenario>.log
set -uo pipefail

ROOT=${SCRIPT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
NS=${NS:-sistem-informasi-kebencanaan-bnpb}
TESTER_NS=${TESTER_NS:-tester-agent}
KUBECTL=${KUBECTL:-kubectl}
DRY_RUN=${DRY_RUN:-0}
RESULTS_DIR=${RESULTS_DIR:-$ROOT/tests/results}
HEALTH_WAIT=${HEALTH_WAIT:-120}
HEALTH_POLL=${HEALTH_POLL:-5}
CONFIRM_SECONDS=${CONFIRM_SECONDS:-10}
CM=${CM:-bnpb-config}
LEASE_SOURCES=${LEASE_SOURCES:-BMKG PVMBG SENSOR_PANTAI}
AUDIT_LOCAL_PORT=${AUDIT_LOCAL_PORT:-18091}

# name|issue|slot|destructive|timeout s|script candidates, first that exists wins|env that must be set|env given to the child|args|what it does
# slot=1: the script has not landed on main (or is in an open PR), the entry is a placeholder that starts working once a file with
# one of the candidate names exists. Order: read-only first, then the ones that change the ConfigMap, then the heavy load, then the
# destructive ones, with wipe-node last because it can need a manual recovery.
REGISTRY='
audit-checksum|#295|0|0|180|tests/scenarios/audit-checksum.sh|AUDIT_TOKEN|AUDIT_URL=@audit_url||Audit checksum against an independent sha256 of every event id (read only)
rolling-update|#286|0|0|900|tests/scenarios/rolling-update.sh|EMAIL PASSWORD|||Rolling restart of the Aggregator under k6 load (patches bnpb-config, restores it)
autoscale|#327|0|0|2700|tests/scenarios/autoscale.sh|API_URL AUDIT_TOKEN|||HPA scales the Aggregator up and down under a k6 ramp (patches bnpb-config, restores it)
p2-leases|#287|1|0|1800|tests/scenarios/problem2-k8s.sh||||P2 kill-leader, clock-skew and optional VM freeze against the source leases (PR #388)
p3-e2e|#307|1|0|1200|tests/scenarios/problem3-k8s.sh tests/scenarios/p3-e2e.sh tests/scenarios/problem3-e2e.sh||||P3 end-to-end schema policy script
p4-60x|#315|1|0|1800|tests/scenarios/problem4-k8s.sh tests/scenarios/p4-60x.sh tests/scenarios/problem4-60x.sh||||P4 replay at 60x speed with latency and lag measurements
load-p2|#288|0|0|2400|tests/load/p2-k8s.sh|BASE_URL EMAIL PASSWORD AUDIT_TOKEN|||10k VU load with the Aggregator scaled 3 to 5 to 2 (patches bnpb-config and the HPA, restores both)
load-ratio|#288|0|0|4800|tests/load/p2-ratio.sh|BASE_URL EMAIL PASSWORD|||Throughput with 1 and with 3 Aggregators, PASS at ratio 2.0 or more
gameday|#324|0|1|7200|tests/scenarios/gameday.sh|GAMEDAY_EMAIL GAMEDAY_PASSWORD GAMEDAY_MEDIA_EMAIL GAMEDAY_MEDIA_PASSWORD GAMEDAY_FIELD_EMAIL GAMEDAY_FIELD_PASSWORD|STRICT=1||Game day: kill-store, partition, kill-leader, clock-skew, wipe-node, schema plus outages and a k6 spike
p1-wipe-node|#298|1|1|3600|tests/scenarios/problem1-k8s.sh tests/scenarios/p1-wipe-node.sh tests/scenarios/problem1-wipe-node.sh||||P1 wipe-node, node join and losing a second node
'

destroys() {
  case $1 in
    gameday) echo "wipe-node deletes the local-path volumes of one node and powers its VM off (coordination-db, user-db and RabbitMQ data there is lost, the seeded Operator account goes with user-db, the PVMBG token pair is lost). kill-store stops the store, coordination-db and user-db pods of a node. kill-leader stops Aggregator pods, partition and clock-skew disturb the node network and clock. The node must be started again (NODE_UP_CMD, or by hand)." ;;
    p1-wipe-node) echo "wipes the local-path storage of a node and powers it off, then removes a second node. The data of the stores, coordination-db, user-db and RabbitMQ on those nodes is lost and has to be rebuilt by the rebalancer, a recreated PVC or a seed run." ;;
    *) echo "unknown" ;;
  esac
}

MANUAL_STEPS=(
  "VM freeze (P2 freeze check, #287): set FREEZE_NODE, NODE_FREEZE_CMD and NODE_THAW_CMD for p2-leases, otherwise that check prints SKIP"
  "Joining a node (P1 node join, #298): the join is done by hand when the P1 script asks for it"
  "Starting the VM after wipe-node (gameday, p1-wipe-node): set NODE_UP_CMD, otherwise start the VM yourself when the script waits"
)

die() { echo "all-k8s.sh: $*" >&2; exit 2; }

NAMES=()
declare -A F_ISSUE F_SLOT F_DESTR F_TIMEOUT F_PATHS F_NEEDS F_ENV F_ARGS F_WHAT
while IFS='|' read -r n issue slot destr to paths needs env args what; do
  [ -n "$n" ] || continue
  NAMES+=("$n"); F_ISSUE[$n]=$issue; F_SLOT[$n]=$slot; F_DESTR[$n]=$destr; F_TIMEOUT[$n]=$to
  F_PATHS[$n]=$paths; F_NEEDS[$n]=$needs; F_ENV[$n]=$env; F_ARGS[$n]=$args; F_WHAT[$n]=$what
done <<<"$REGISTRY"

# ---------- arguments ----------
LIST=0 INCLUDE_DESTRUCTIVE=0 AUDIT_AFTER=1 FAIL_ON_SKIP=0
ONLY=() SKIPPED=()
split_names() { local -n arr=$1; local IFS=,; local x; for x in $2; do
  [ -n "${F_ISSUE[$x]:-}" ] || die "unknown scenario '$x' (see --list)"; arr+=("$x"); done; }
while [ $# -gt 0 ]; do
  case $1 in
    --list) LIST=1 ;;
    --only) [ $# -ge 2 ] || die "--only needs a value"; split_names ONLY "$2"; shift ;;
    --skip) [ $# -ge 2 ] || die "--skip needs a value"; split_names SKIPPED "$2"; shift ;;
    --include-destructive) INCLUDE_DESTRUCTIVE=1 ;;
    --no-audit) AUDIT_AFTER=0 ;;
    --fail-on-skip) FAIL_ON_SKIP=1 ;;
    -h|--help) sed -n '2,/^set -uo/p' "$0" | sed '$d;s/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option $1 (try -h)" ;;
  esac
  shift
done

in_list() { local x=$1; shift; local y; for y in "$@"; do [ "$x" = "$y" ] && return 0; done; return 1; }
resolve_script() { local p; for p in ${F_PATHS[$1]}; do [ -f "$ROOT/$p" ] && { echo "$p"; return 0; }; done; return 1; }
timeout_for() { local v; v="TIMEOUT_$(echo "$1" | tr 'a-z-' 'A-Z_')"; echo "${!v:-${F_TIMEOUT[$1]}}"; }

# ---------- --list ----------
if [ "$LIST" = 1 ]; then
  printf '%-3s %-15s %-6s %-5s %-7s %-8s %s\n' '#' NAME ISSUE KIND TIMEOUT STATE SCRIPT
  i=0
  for n in "${NAMES[@]}"; do
    i=$((i + 1)); kind=safe; [ "${F_DESTR[$n]}" = 1 ] && kind=DESTR
    if s=$(resolve_script "$n"); then state=present; else state=missing; s=${F_PATHS[$n]%% *}; fi
    printf '%-3s %-15s %-6s %-5s %-7s %-8s %s\n' "$i" "$n" "${F_ISSUE[$n]}" "$kind" "$(timeout_for "$n")s" "$state" "$s"
    echo "    ${F_WHAT[$n]}"
    [ -n "${F_NEEDS[$n]}" ] && echo "    needs env: ${F_NEEDS[$n]}"
    [ "${F_SLOT[$n]}" = 1 ] && echo "    optional slot, candidate files: ${F_PATHS[$n]}"
    [ "${F_DESTR[$n]}" = 1 ] && echo "    opt-in with --include-destructive"
  done
  exit 0
fi

# ---------- output files ----------
STAMP=$(date +%Y%m%d-%H%M%S)
RUN_DIR=$RESULTS_DIR/all-$STAMP
JSON_OUT=$RESULTS_DIR/all-$STAMP.json
TXT_OUT=$RESULTS_DIR/all-$STAMP.txt
MAIN_LOG=$RESULTS_DIR/all-$STAMP.log
WORK=$(mktemp -d)
RECORDS=$WORK/records.jsonl
: >"$RECORDS"
if [ "$DRY_RUN" != 1 ]; then
  mkdir -p "$RUN_DIR" || die "cannot create $RUN_DIR"
  exec > >(tee -a "$MAIN_LOG") 2>&1
fi

kc() { "$KUBECTL" -n "$NS" "$@"; }
say() { printf '%s\n' "$*"; }
PFPIDS=()
stop_forwards() { local p; for p in "${PFPIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done; PFPIDS=(); }

# ---------- cluster state ----------
HEALTH_MSG=
health_check() {
  HEALTH_MSG=
  local nodes pods bad lease src
  nodes=$("$KUBECTL" get nodes -o json 2>/dev/null) || { HEALTH_MSG="kubectl cannot list nodes"; return 1; }
  bad=$(jq -r '[.items[] | select((any(.status.conditions[]?; .type == "Ready" and .status == "True")) | not) | .metadata.name] | join(" ")' <<<"$nodes")
  [ "$(jq '.items | length' <<<"$nodes")" -gt 0 ] || { HEALTH_MSG="no nodes"; return 1; }
  [ -z "$bad" ] || { HEALTH_MSG="nodes not Ready: $bad"; return 1; }
  pods=$(kc get pods -o json 2>/dev/null) || { HEALTH_MSG="kubectl cannot list pods in $NS"; return 1; }
  bad=$(jq -r '[.items[] | select(.status.phase != "Succeeded") | select(.metadata.deletionTimestamp != null or .status.phase != "Running" or (any(.status.containerStatuses[]?; .ready | not))) | .metadata.name] | join(" ")' <<<"$pods")
  [ -z "$bad" ] || { HEALTH_MSG="pods not ready: $bad"; return 1; }
  jq -e 'any(.items[]; .metadata.name == "coordination-db-0" and .status.phase == "Running")' <<<"$pods" >/dev/null ||
    { HEALTH_MSG="coordination-db-0 is not running"; return 1; }
  # shellcheck disable=SC2016  # $POSTGRES_USER is expanded inside the pod
  lease=$(kc exec coordination-db-0 -- sh -c 'psql -U "$POSTGRES_USER" -d coordination -tAF, -c "SELECT source, epoch, (holder IS NOT NULL AND expires_at > now())::int FROM source_lease ORDER BY source"' 2>/dev/null) ||
    { HEALTH_MSG="cannot read source_lease from coordination-db-0"; return 1; }
  lease=${lease//$'\r'/}
  for src in $LEASE_SOURCES; do
    grep -q "^$src,[0-9]*,1$" <<<"$lease" || { HEALTH_MSG="lease of $src is not held"; return 1; }
  done
  return 0
}
health_wait() {
  local deadline=$((SECONDS + HEALTH_WAIT))
  while ! health_check; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep "$HEALTH_POLL"
  done
}

cm_values() { kc get configmap "$CM" -o json 2>/dev/null | jq -c '.data as $d | {ACCESS_TOKEN_TTL_S: ($d.ACCESS_TOKEN_TTL_S // null), RATE_LIMIT_RPS: ($d.RATE_LIMIT_RPS // null), CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS: ($d.CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS // null)}'; }
has_obj() { "$KUBECTL" "$@" -o name >/dev/null 2>&1; }

CM_BASE='' TR_BASE=0 HPA_BASE=0
snapshot_baseline() {
  CM_BASE=$(cm_values) || true
  [ -n "$CM_BASE" ] || die "cannot read ConfigMap $CM in $NS"
  has_obj -n "$TESTER_NS" get configmap tester-run && TR_BASE=1
  has_obj -n "$NS" get hpa aggregator && HPA_BASE=1
  say "baseline: $CM $CM_BASE, tester-run present=$TR_BASE, HPA aggregator present=$HPA_BASE"
  [ "$TR_BASE" = 1 ] && say "WARNING: ConfigMap tester-run exists before the run. A wipe-node run is replayed on every node boot until it is deleted, check it with: kubectl -n $TESTER_NS get cm tester-run"
  return 0
}

# cleanup_cluster: puts back what a child may have left. Sets CLEAN_NOTE. Never fails the run, a problem is only reported.
CLEAN_NOTE=
cleanup_cluster() {
  CLEAN_NOTE=
  local now patch
  now=$(cm_values) || true
  if [ -z "$now" ]; then CLEAN_NOTE="could not read $CM;"
  elif [ "$now" != "$CM_BASE" ]; then
    patch=$(jq -nc --argjson b "$CM_BASE" '{data: $b}')
    if kc patch configmap "$CM" --type merge -p "$patch" >/dev/null &&
      kc rollout restart deployment/auth-service deployment/client-facing-api >/dev/null &&
      kc rollout status deployment/auth-service --timeout=180s >/dev/null &&
      kc rollout status deployment/client-facing-api --timeout=180s >/dev/null; then
      CLEAN_NOTE="restored $CM ($now -> $CM_BASE);"
    else CLEAN_NOTE="FAILED to restore $CM, run deploy/deploy.sh;"; fi
  fi
  if [ "$TR_BASE" = 0 ] && has_obj -n "$TESTER_NS" get configmap tester-run; then
    "$KUBECTL" -n "$TESTER_NS" delete configmap tester-run >/dev/null 2>&1 && CLEAN_NOTE="$CLEAN_NOTE deleted tester-run;" || CLEAN_NOTE="$CLEAN_NOTE FAILED to delete tester-run;"
  fi
  if [ "$HPA_BASE" = 1 ] && ! has_obj -n "$NS" get hpa aggregator; then
    "$KUBECTL" -n "$NS" apply -f "$ROOT/deploy/aggregator-hpa.yaml" >/dev/null 2>&1 && CLEAN_NOTE="$CLEAN_NOTE recreated the HPA;" || CLEAN_NOTE="$CLEAN_NOTE FAILED to recreate the HPA;"
  fi
  CLEAN_NOTE=${CLEAN_NOTE# }
  [ -n "$CLEAN_NOTE" ] && say "cleanup: $CLEAN_NOTE"
  return 0
}

# ---------- Audit API ----------
AUDIT_BASE='' AUDIT_TOK=${AUDIT_TOKEN:-}
open_audit() {
  AUDIT_BASE=
  if [ -n "${AUDIT_URL:-}" ]; then AUDIT_BASE=$AUDIT_URL; return 0; fi
  kc port-forward deployment/aggregator "$AUDIT_LOCAL_PORT:8091" >"$WORK/pf-audit.log" 2>&1 &
  PFPIDS+=("$!")
  local t=0
  until curl -sf -m 2 "http://127.0.0.1:$AUDIT_LOCAL_PORT/health" >/dev/null; do
    t=$((t + 1)); kill -0 "${PFPIDS[-1]}" 2>/dev/null && [ "$t" -lt 20 ] || { stop_forwards; return 1; }
    sleep 1
  done
  AUDIT_BASE=http://127.0.0.1:$AUDIT_LOCAL_PORT
}
# audit_get <path and query>: sets AUDIT_CODE and AUDIT_BODY, never called in a command substitution
AUDIT_CODE='' AUDIT_BODY=''
audit_get() {
  local out
  out=$(curl -s -m 20 -w '\n%{http_code}' -H "Authorization: Bearer $AUDIT_TOK" "$AUDIT_BASE$1") || out=$'\n000'
  AUDIT_CODE=${out##*$'\n'}; AUDIT_BODY=${out%$'\n'*}
}

# audit_check: events, checksum, location and unprocessed per source. Sets AUDIT_RESULT (PASS, FAIL or SKIP) and AUDIT_NOTE.
AUDIT_RESULT='' AUDIT_NOTE=''
audit_check() {
  AUDIT_RESULT=PASS AUDIT_NOTE=
  if [ "$AUDIT_AFTER" != 1 ]; then AUDIT_RESULT=SKIP AUDIT_NOTE="--no-audit"; return 0; fi
  if [ -z "$AUDIT_TOK" ]; then AUDIT_RESULT=SKIP AUDIT_NOTE="no audit token"; return 0; fi
  if ! open_audit; then AUDIT_RESULT=SKIP AUDIT_NOTE="no port-forward to the Audit API"; return 0; fi
  local src body hid
  for src in $LEASE_SOURCES; do
    audit_get "/internal/audit/events?source=$src&limit=1"; body=$AUDIT_BODY
    if [ "$AUDIT_CODE" != 200 ] || ! jq -e '.items | type == "array"' <<<"$body" >/dev/null 2>&1; then
      AUDIT_RESULT=FAIL; AUDIT_NOTE="$AUDIT_NOTE events $src HTTP $AUDIT_CODE;"; continue; fi
    hid=$(jq -r '.items[0].hazard_id // empty' <<<"$body")
    audit_get "/internal/audit/checksum?source=$src"; body=$AUDIT_BODY
    if [ "$AUDIT_CODE" != 200 ] || ! jq -e 'has("count") and has("checksum")' <<<"$body" >/dev/null 2>&1; then
      AUDIT_RESULT=FAIL; AUDIT_NOTE="$AUDIT_NOTE checksum $src HTTP $AUDIT_CODE;"; fi
    if [ -n "$hid" ]; then
      audit_get "/internal/audit/location?hazard_id=$hid" >/dev/null
      case $AUDIT_CODE in 200) ;; 404|405) AUDIT_NOTE="$AUDIT_NOTE location n/a;" ;; *) AUDIT_RESULT=FAIL; AUDIT_NOTE="$AUDIT_NOTE location $src HTTP $AUDIT_CODE;" ;; esac
    fi
    audit_get "/internal/audit/unprocessed?source=$src&limit=1" >/dev/null
    case $AUDIT_CODE in 200) ;; 404|405) AUDIT_NOTE="$AUDIT_NOTE unprocessed n/a;" ;; *) AUDIT_RESULT=FAIL; AUDIT_NOTE="$AUDIT_NOTE unprocessed $src HTTP $AUDIT_CODE;" ;; esac
  done
  stop_forwards
  AUDIT_NOTE=$(printf '%s' "$AUDIT_NOTE" | awk -v RS=';' 'NF && !seen[$0]++ {printf "%s;", $0}')
  return 0
}

# ---------- results ----------
SUMMARY_ROWS=()
CNT_PASS=0 CNT_FAIL=0 CNT_SKIP=0 CNT_TIMEOUT=0 CNT_PLAN=0 AUDIT_FAILS=0
record() { # name status reason secs exit script log audit cleanup
  local n=$1 st=$2
  jq -nc --arg name "$n" --arg issue "${F_ISSUE[$n]}" --arg status "$st" --arg reason "$3" --argjson seconds "${4:-0}" --argjson exit_code "${5:--1}" \
    --arg script "$6" --arg log "$7" --arg audit "$8" --arg cleanup "$9" --argjson destructive "${F_DESTR[$n]}" --argjson timeout_s "$(timeout_for "$n")" \
    '{name: $name, issue: $issue, status: $status, reason: $reason, seconds: $seconds, exit_code: (if $exit_code < 0 then null else $exit_code end), script: $script, log: $log, audit: $audit, cleanup: $cleanup, destructive: $destructive, timeout_s: $timeout_s}' >>"$RECORDS"
  case $st in PASS) CNT_PASS=$((CNT_PASS + 1)) ;; FAIL) CNT_FAIL=$((CNT_FAIL + 1)) ;; SKIP) CNT_SKIP=$((CNT_SKIP + 1)) ;; TIMEOUT) CNT_TIMEOUT=$((CNT_TIMEOUT + 1)) ;; PLAN) CNT_PLAN=$((CNT_PLAN + 1)) ;; esac
  [ "$8" = FAIL ] && AUDIT_FAILS=$((AUDIT_FAILS + 1))
  SUMMARY_ROWS+=("$(printf '%-3s %-15s %-8s %7s %-6s %s' "$((${#SUMMARY_ROWS[@]} + 1))" "$n" "$st" "${4:-0}s" "${8:--}" "$3")")
}

fmt_dur() { printf '%dm%02ds' $(($1 / 60)) $(($1 % 60)); }
START_EPOCH=$(date +%s) START_ISO=$(date -Iseconds)
ABORTED=0

write_results() {
  local total=$(($(date +%s) - START_EPOCH)) overall=PASS
  [ $((CNT_FAIL + CNT_TIMEOUT + AUDIT_FAILS)) -gt 0 ] && overall=FAIL
  [ "$FAIL_ON_SKIP" = 1 ] && [ "$CNT_SKIP" -gt 0 ] && overall=FAIL
  [ "$ABORTED" = 1 ] && overall=ABORTED
  [ "$DRY_RUN" = 1 ] && overall=PLAN
  OVERALL=$overall
  {
    echo
    echo "== Summary ($START_ISO)"
    printf '%-3s %-15s %-8s %7s %-6s %s\n' '#' SCENARIO RESULT TIME AUDIT DETAIL
    printf '%s\n' "${SUMMARY_ROWS[@]}"
    echo
    echo "PASS $CNT_PASS, FAIL $CNT_FAIL, TIMEOUT $CNT_TIMEOUT, SKIP $CNT_SKIP, audit failures $AUDIT_FAILS"
    echo "Overall: $overall, total time $(fmt_dur "$total") (${total}s)"
    [ "$DRY_RUN" = 1 ] || echo "Results: $JSON_OUT, $TXT_OUT, logs in $RUN_DIR/"
  } | if [ "$DRY_RUN" = 1 ]; then cat; else tee "$TXT_OUT"; fi
  if [ "$DRY_RUN" != 1 ]; then
    jq -s --arg started "$START_ISO" --arg overall "$overall" --argjson total "$total" --argjson aborted "$ABORTED" --arg dir "$RUN_DIR" \
      '{started: $started, total_seconds: $total, overall: $overall, aborted: ($aborted == 1), log_dir: $dir,
        counts: {pass: (map(select(.status == "PASS")) | length), fail: (map(select(.status == "FAIL")) | length), timeout: (map(select(.status == "TIMEOUT")) | length), skip: (map(select(.status == "SKIP")) | length), audit_fail: (map(select(.audit == "FAIL")) | length)},
        scenarios: .}' "$RECORDS" >"$JSON_OUT"
  fi
}

finish() {
  write_results
  stop_forwards
  rm -rf "$WORK"
  case $OVERALL in FAIL|ABORTED) exit 1 ;; *) exit 0 ;; esac
}
# shellcheck disable=SC2329  # called through the trap
on_signal() {
  trap - INT TERM
  say; say "interrupted, cleaning up"
  ABORTED=1
  stop_forwards
  [ "$DRY_RUN" = 1 ] || cleanup_cluster
  OVERALL=ABORTED; write_results; rm -rf "$WORK"; exit 130
}
trap on_signal INT TERM

# ---------- plan ----------
say "== All scenarios (#328) $START_ISO, namespace $NS$([ "$DRY_RUN" = 1 ] && echo ', DRY RUN: nothing is run and no cluster is touched')"
say
say "Manual steps (not automated):"
printf '  - %s\n' "${MANUAL_STEPS[@]}"
say

if [ "$DRY_RUN" != 1 ]; then
  for c in "$KUBECTL" curl jq timeout; do command -v "$c" >/dev/null || die "missing $c"; done
  if [ -z "$AUDIT_TOK" ]; then AUDIT_TOK=$(kc get secret audit-token -o jsonpath='{.data.token}' 2>/dev/null | base64 -d 2>/dev/null) || AUDIT_TOK=; fi
  [ -z "${AUDIT_TOKEN:-}" ] && [ -n "$AUDIT_TOK" ] && export AUDIT_TOKEN=$AUDIT_TOK
  say "== Cluster preconditions"
  if ! health_wait; then say "FAIL: $HEALTH_MSG"; say "The cluster is not healthy, nothing was run. Fix it and run again."; rm -rf "$WORK"; exit 2; fi
  say "PASS: nodes Ready, pods ready, coordination-db up, leases held ($LEASE_SOURCES)"
  snapshot_baseline
fi

# Decide per scenario what happens, before anything runs, so --list style information and the destructive warning come first.
declare -A DECISION
destructive_running=()
for n in "${NAMES[@]}"; do
  reason=
  if [ "${#ONLY[@]}" -gt 0 ] && ! in_list "$n" "${ONLY[@]}"; then reason="not selected by --only"
  elif [ "${#SKIPPED[@]}" -gt 0 ] && in_list "$n" "${SKIPPED[@]}"; then reason="skipped by --skip"
  elif ! resolve_script "$n" >/dev/null; then
    if [ "${F_SLOT[$n]}" = 1 ]; then reason="not on main yet (${F_ISSUE[$n]}), looked for: ${F_PATHS[$n]}"; else reason="script missing: ${F_PATHS[$n]}"; fi
  elif [ "${F_DESTR[$n]}" = 1 ] && [ "$INCLUDE_DESTRUCTIVE" != 1 ]; then reason="destructive, add --include-destructive to run it"
  else
    for v in ${F_NEEDS[$n]}; do
      [ "$v" = AUDIT_TOKEN ] && { [ "$DRY_RUN" = 1 ] || [ -n "$AUDIT_TOK" ]; } && continue
      [ -n "${!v:-}" ] || { reason="env $v is not set"; break; }
    done
  fi
  DECISION[$n]=$reason
  [ -z "$reason" ] && [ "${F_DESTR[$n]}" = 1 ] && destructive_running+=("$n")
done
if [ "${#destructive_running[@]}" -gt 0 ]; then
  say "!! DESTRUCTIVE scenarios selected: ${destructive_running[*]}"
  for n in "${destructive_running[@]}"; do say "!! $n will destroy: $(destroys "$n")"; done
  say
fi

# ---------- run ----------
i=0
for n in "${NAMES[@]}"; do
  i=$((i + 1))
  script=$(resolve_script "$n" || true)
  say; say "== [$i/${#NAMES[@]}] $n (${F_ISSUE[$n]}): ${F_WHAT[$n]}"
  if [ -n "${DECISION[$n]}" ]; then say "SKIP: ${DECISION[$n]}"; record "$n" SKIP "${DECISION[$n]}" 0 -1 "$script" "" "" ""; continue; fi
  tmo=$(timeout_for "$n")
  env_args=()
  for kv in ${F_ENV[$n]}; do
    case $kv in *=@audit_url) kv="${kv%%=*}=${AUDIT_URL:-http://127.0.0.1:$AUDIT_LOCAL_PORT}" ;; esac
    env_args+=("$kv")
  done
  cmd="env ${env_args[*]:-} bash $script ${F_ARGS[$n]}"
  if [ "$DRY_RUN" = 1 ]; then
    say "PLAN: timeout ${tmo}s ${cmd/env  /}  (log: $RUN_DIR/$n.log)"
    [ "${F_DESTR[$n]}" = 1 ] && say "PLAN: would destroy: $(destroys "$n")"
    record "$n" PLAN "would run $script, timeout ${tmo}s" 0 -1 "$script" "" "" ""; continue
  fi
  if ! health_wait; then
    say "SKIP: cluster not healthy: $HEALTH_MSG"; record "$n" SKIP "cluster not healthy: $HEALTH_MSG" 0 -1 "$script" "" "" ""; continue
  fi
  if [ "${F_DESTR[$n]}" = 1 ]; then
    say "!! DESTRUCTIVE: $n will destroy: $(destroys "$n")"
    if [ "$CONFIRM_SECONDS" -gt 0 ]; then say "!! starting in ${CONFIRM_SECONDS}s, Ctrl-C to stop"; sleep "$CONFIRM_SECONDS"; fi
  fi
  forward_pid=
  if [[ " ${F_ENV[$n]} " == *"@audit_url"* ]] && [ -z "${AUDIT_URL:-}" ]; then
    if open_audit; then forward_pid=${PFPIDS[-1]}; else
      say "SKIP: no port-forward to the Audit API"; record "$n" SKIP "no port-forward to the Audit API" 0 -1 "$script" "" "" ""; continue; fi
  fi
  log=$RUN_DIR/$n.log
  say "run: timeout ${tmo}s ${cmd/env  /}  (log: $log)"
  t0=$SECONDS
  read -ra child_args <<<"${F_ARGS[$n]}"
  (cd "$ROOT" && exec timeout -k 30 "$tmo" env "${env_args[@]}" bash "$script" "${child_args[@]}") 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  secs=$((SECONDS - t0))
  [ -n "$forward_pid" ] && stop_forwards
  case $rc in
    0) st=PASS reason="exit 0" ;;
    124|137) st=TIMEOUT reason="killed after ${tmo}s (exit $rc)" ;;
    2) st=SKIP reason="child could not start (exit 2): $(grep -v '^\s*$' "$log" | tail -1 | cut -c1-100)" ;;
    *) st=FAIL reason="exit $rc: $(grep -E 'FAIL' "$log" | head -1 | cut -c1-100)" ;;
  esac
  say "-- $n: $st after $(fmt_dur "$secs")"
  say "== cleanup and health re-check after $n"
  cleanup_cluster
  if health_wait; then say "health: ok"; hnote=; else say "health: $HEALTH_MSG"; hnote="cluster unhealthy afterwards: $HEALTH_MSG"; fi
  audit_check
  say "audit check: $AUDIT_RESULT ${AUDIT_NOTE}"
  [ -n "$hnote" ] && reason="$reason; $hnote"
  [ -n "$AUDIT_NOTE" ] && [ "$AUDIT_RESULT" = FAIL ] && reason="$reason; audit: $AUDIT_NOTE"
  record "$n" "$st" "$reason" "$secs" "$rc" "$script" "$log" "$AUDIT_RESULT" "$CLEAN_NOTE"
done

say
[ "$DRY_RUN" = 1 ] || { say "== Final cleanup"; cleanup_cluster; }
finish
