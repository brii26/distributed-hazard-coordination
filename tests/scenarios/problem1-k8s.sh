#!/usr/bin/env bash
# Problem 1 on the cluster (#298): a node loses all its data, comes back empty, and a second node is lost later.
#
#   tests/scenarios/problem1-k8s.sh                  all scenarios
#   ONLY="A B" tests/scenarios/problem1-k8s.sh       some scenarios (A read, B first loss and return, E second loss)
#   DRY_RUN=1 tests/scenarios/problem1-k8s.sh        print the plan, touches no cluster
#
# A FAIL means the implementation is wrong. NOT RUN means the script could not run, never a verdict.
# Exit code 1 when any check is FAIL, 2 when the cluster is not ready.
# Needs kubectl on the cluster, curl, jq, sha256sum and a store DaemonSet on at least two nodes (three for scenario E).
# It stops the store on one node, empties that node's hostPath with a one-shot pod and starts the store again, so run it on a
# cluster nobody else uses. The exit trap puts the DaemonSet back.
set -uo pipefail

DRY_RUN=${DRY_RUN:-0}
ONLY=${ONLY:-A B E}
NS=${NS:-sistem-informasi-kebencanaan-bnpb}
API_PORT=${API_PORT:-38095}
AUDIT_PORT=${AUDIT_PORT:-38096}
PROBE_TIMEOUT=${PROBE_TIMEOUT:-8}      # seconds a probe call may take before it counts as a hang
RECOVER_WAIT=${RECOVER_WAIT:-900}      # seconds the system gets to heal after a node returns empty
SAMPLE=${SAMPLE:-20}                   # ids whose copies are checked with the location endpoint
TICK=${TICK:-3}                        # seconds between two tries of a wait
HOSTPATH=${HOSTPATH:-/var/lib/bnpb-store}
RUN=${RUN:-p1-$(date +%s)}

PASSED=0 FAILED=0 NOTRUN=0
PIDS=()
WORK=$(mktemp -d)
API=http://127.0.0.1:$API_PORT
AUDIT=http://127.0.0.1:$AUDIT_PORT
AUDIT_TOKEN="" OP_EMAIL="" OP_PASS=""
LIMITED=0 WIPE_POD=""

say() { printf '%s\n' "$*"; }
pass() { say "PASS: $*"; PASSED=$((PASSED + 1)); }
fail() { say "FAIL: $*"; FAILED=$((FAILED + 1)); }
notrun() { say "NOT RUN: $*"; NOTRUN=$((NOTRUN + 1)); }
info() { say "INFO: $*"; }
heading() { printf '\n== %s\n' "$*"; }
kc() { kubectl -n "$NS" "$@"; }
want() { if [ "$2" = "$3" ]; then pass "$1 ($2)"; else fail "$1: got $2, want $3"; fi }

# Sleep that a TERM can interrupt, so the exit trap runs at once.
nap() { sleep "$1" & wait $!; }

# Runs the command every few seconds until it succeeds or the seconds are up.
until_true() {
  local end=$((SECONDS + $1))
  shift
  while [ "$SECONDS" -lt "$end" ]; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    nap "$TICK"
  done
  return 1
}

secret() { kc get secret "$1" -o "jsonpath={.data.$2}" | base64 -d; }

lift_limit() {
  kc patch ds store --type=json -p '[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]' >/dev/null 2>&1
  kubectl label node --all p1-store- >/dev/null 2>&1
  LIMITED=0
}

cleanup() {
  local rc=$? p
  trap - EXIT INT TERM
  if [ "$DRY_RUN" != 1 ]; then
    [ -n "$WIPE_POD" ] && kc delete pod "$WIPE_POD" --wait=false >/dev/null 2>&1
    if [ "$LIMITED" = 1 ]; then lift_limit && say "trap: the store DaemonSet is no longer limited to some nodes"; fi
  fi
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  rm -rf "$WORK"
  say ""
  say "PASS $PASSED, FAIL $FAILED, NOT RUN $NOTRUN"
  [ "$rc" = 2 ] && exit 2
  [ "$FAILED" -gt 0 ] && exit 1
  exit 0
}
trap cleanup EXIT
trap 'exit 2' INT TERM

forward() {
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] kubectl -n $NS port-forward $1 $2:$3"; return 0; fi
  kc port-forward "$1" "$2:$3" >"$WORK/pf-$2.log" 2>&1 &
  PIDS+=("$!")
  until_true 20 curl -s -o /dev/null "http://127.0.0.1:$2/" || { say "port-forward $1 $2:$3 failed" >&2; return 1; }
}

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
audit_get() { curl -s -m "$PROBE_TIMEOUT" -H "Authorization: Bearer $AUDIT_TOKEN" "$AUDIT$1"; }
op_get() { curl -s -m "$PROBE_TIMEOUT" -b "$WORK/op.jar" -c "$WORK/op.jar" "$API$1"; }

store_nodes() { kc get pods -l app=store -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort; }

prepare() {
  heading "Cluster"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] check the pods, read the audit token and the Operator login, port-forward the API and the Aggregator"; return 0; fi
  kubectl version --request-timeout=5s >/dev/null 2>&1 || { say "NOT RUN: kubectl cannot reach the cluster" >&2; exit 2; }
  local notready
  notready=$(kc get pods --no-headers 2>/dev/null | awk '{ split($2, a, "/"); if (a[1] != a[2] || ($3 != "Running" && $3 != "Completed")) n++ } END { print n + 0 }')
  [ "$notready" = 0 ] || { say "NOT RUN: $notready pods in $NS are not ready" >&2; exit 2; }
  AUDIT_TOKEN=$(secret audit-token token)
  OP_EMAIL=$(kc get configmap bnpb-config -o 'jsonpath={.data.SEED_OPERATOR_EMAIL}')
  OP_PASS=$(secret auth-secrets seed_operator_password)
  [ -n "$AUDIT_TOKEN" ] && [ -n "$OP_PASS" ] || { say "NOT RUN: a Secret is missing" >&2; exit 2; }
  forward svc/client-facing-api "$API_PORT" 8080 || exit 2
  forward deployment/aggregator "$AUDIT_PORT" 8091 || exit 2
  want "Operator login" "$(code -c "$WORK/op.jar" -X POST "$API/auth/login" -d "{\"email\":\"$OP_EMAIL\",\"password\":\"$OP_PASS\"}")" 200
  say "cluster ready, run id $RUN, store nodes: $(store_nodes | tr '\n' ' ')"
}

# Writes "source<TAB>source_ref_id<TAB>hazard_id" of every audit event (of one source if given) into the file, following the cursor.
snapshot() {
  local out=$1 src=${2:-} cursor="" page
  : >"$out"
  for _ in $(seq 1 200); do
    page=$(audit_get "/internal/audit/events?limit=1000${src:+&source=$src}${cursor:+&cursor=$cursor}")
    jq -r '.items[]? | [.source, .source_ref_id, .hazard_id] | @tsv' <<<"$page" >>"$out"
    cursor=$(jq -r '.next_cursor // empty' <<<"$page")
    [ -n "$cursor" ] || break
  done
}

# Audit checksum of one source against an independent SHA-256 over the listed ids, retried while writes move the data.
checksum_matches() {
  local src=$1 c1 c2 ids local_sum
  for _ in 1 2 3 4 5 6 7 8; do
    c1=$(audit_get "/internal/audit/checksum?source=$src" | jq -r '.checksum // empty')
    snapshot "$WORK/cs.tsv" "$src"
    c2=$(audit_get "/internal/audit/checksum?source=$src" | jq -r '.checksum // empty')
    if [ -n "$c1" ] && [ "$c1" = "$c2" ]; then
      ids=$(awk -F'\t' -v s="$src" '$1 == s { print $2 }' "$WORK/cs.tsv" | LC_ALL=C sort)
      local_sum=$(printf '%s' "$ids" | sha256sum | cut -d' ' -f1)
      [ "$local_sum" = "$c1" ] && return 0
      say "checksum of $src: audit $c1, over the listed ids $local_sum" >&2
      return 1
    fi
  done
  return 2
}

# The nodes of a hazard's copies, one per line.
copies_of() { audit_get "/internal/audit/location?hazard_id=$1" | jq -r '.locations[]?.node' | sort -u; }

# Every sampled id has copies on at least two different nodes, and the node (if given) holds some.
placement() {
  local node=${1:-} id n ok=0 bad=0 on_node=0 per=""
  for id in $(cut -f3 "$WORK/sample.tsv"); do
    n=$(copies_of "$id")
    if [ "$(wc -l <<<"$n" | tr -d ' ')" -ge 2 ]; then ok=$((ok + 1)); else bad=$((bad + 1)); fi
    [ -n "$node" ] && grep -qx "$node" <<<"$n" && on_node=$((on_node + 1))
    per="$per $n"
  done
  info "copies per node among $SAMPLE sampled ids: $(tr ' ' '\n' <<<"$per" | grep -v '^$' | sort | uniq -c | awk '{ printf "%s=%s ", $2, $1 }')"
  PLACE_BAD=$bad PLACE_ON_NODE=$on_node
}

# The probe: every second a cross-shard list page and an audit page, with status, time and the partial marker.
probe() {
  local out
  while :; do
    out=$(curl -s -m "$PROBE_TIMEOUT" -b "$WORK/probe.jar" -c "$WORK/probe.jar" -w '\n%{http_code} %{time_total}' "$API/hazards?limit=50")
    printf '%s list %s %s\n' "$(date +%s)" "${out##*$'\n'}" "$(jq -r '.partial // false' <<<"${out%$'\n'*}" 2>/dev/null || echo false)" >>"$WORK/probe.log"
    out=$(curl -s -m "$PROBE_TIMEOUT" -H "Authorization: Bearer $AUDIT_TOKEN" -w '\n%{http_code} %{time_total}' "$AUDIT/internal/audit/events?limit=50")
    printf '%s audit %s %s\n' "$(date +%s)" "${out##*$'\n'}" "$(jq -r '.partial // false' <<<"${out%$'\n'*}" 2>/dev/null || echo false)" >>"$WORK/probe.log"
    nap 1
  done
}

# Scenario A: the cross-shard list and the audit events, page by page.
sA() {
  heading "A: read across all shards, page by page"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] page through /hazards?limit=50 and /internal/audit/events, check status 200, no repeated id and the order"; return 0; fi
  local cursor="" page bad=0 ids="" n total unique
  for _ in $(seq 1 20); do
    page=$(op_get "/hazards?limit=50${cursor:+&cursor=$cursor}")
    n=$(jq -r '.data | length' <<<"$page" 2>/dev/null || echo bad)
    [ "$n" = bad ] && { bad=1; break; }
    ids="$ids"$'\n'"$(jq -r '.data[] | (.hazard_id // .id)' <<<"$page")"
    cursor=$(jq -r '.page.next_cursor // empty' <<<"$page")
    [ -n "$cursor" ] || break
  done
  want "A the hazard list answered every page with data" "$bad" 0
  total=$(grep -c . <<<"$ids")
  unique=$(grep . <<<"$ids" | sort -u | wc -l)
  want "A repeated ids across the pages of the hazard list ($total ids)" "$((total - unique))" 0
  snapshot "$WORK/a.tsv"
  want "A the audit events listed some hazards" "$([ -s "$WORK/a.tsv" ] && echo yes || echo no)" yes
  want "A repeated hazard ids in the audit events" "$(cut -f3 "$WORK/a.tsv" | sort | uniq -d | wc -l | tr -d ' ')" 0
}

# Empties the hostPath of one node: the store is stopped there, a one-shot pod removes the files, the store starts again.
lose_node() {
  local node=$1 other image
  info "$node loses its data: store stopped, $HOSTPATH emptied, store started again"
  for other in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
    [ "$other" = "$node" ] || kubectl label node "$other" p1-store=on --overwrite >/dev/null
  done
  kc patch ds store --type=merge -p '{"spec":{"template":{"spec":{"nodeSelector":{"p1-store":"on"}}}}}' >/dev/null
  LIMITED=1
  until_true 90 bash -c "! kubectl -n $NS get pods -l app=store -o jsonpath='{.items[*].spec.nodeName}' | grep -qw $node" || { notrun "the store pod on $node did not stop"; return 1; }
  image=$(kc get ds store -o jsonpath='{.spec.template.spec.containers[0].image}')
  WIPE_POD="p1-wipe-$RUN-$node"
  kc run "$WIPE_POD" --restart=Never --image="$image" --overrides="{\"spec\":{\"nodeName\":\"$node\",\"containers\":[{\"name\":\"w\",\"image\":\"$image\",\"command\":[\"sh\",\"-c\",\"rm -rf /data/* /data/.[!.]* ; echo left=\$(ls -A /data | wc -l)\"],\"volumeMounts\":[{\"name\":\"d\",\"mountPath\":\"/data\"}]}],\"volumes\":[{\"name\":\"d\",\"hostPath\":{\"path\":\"$HOSTPATH\"}}]}}" >/dev/null 2>&1
  if ! until_true 120 bash -c "[ \"\$(kubectl -n $NS get pod $WIPE_POD -o jsonpath='{.status.phase}')\" = Succeeded ]"; then notrun "the wipe pod on $node did not finish"; lift_limit; return 1; fi
  info "wipe pod: $(kc logs "$WIPE_POD" 2>/dev/null | tail -1)"
  kc delete pod "$WIPE_POD" --wait=false >/dev/null 2>&1
  WIPE_POD=""
  lift_limit
}

recovered() {
  local node=$1
  [ "$(store_nodes | grep -cw "$node")" -ge 1 ] || return 1
  [ "$(kc get pods -l app=store --no-headers | awk '{ split($2, a, "/"); if (a[1] != a[2]) n++ } END { print n + 0 }')" = 0 ] || return 1
  [ "$(audit_get "/internal/audit/events?limit=1" | jq -r 'if has("partial") then .partial else true end')" = false ] || return 1
  placement "$node" >/dev/null 2>&1
  [ "$PLACE_BAD" = 0 ] && [ "$PLACE_ON_NODE" -gt 0 ]
}

# One loss of a node: save the ids, run the probe, wipe, wait for the recovery and check what the spec asks.
sWipe() {
  local label=$1 node=$2 missing bad total partial src before_count after_count t0
  heading "$label: $node loses all its data and returns with an empty disk"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] save the audit ids, start the probe, stop the store on $node, empty $HOSTPATH there, start the store, wait for the recovery, compare the ids, the checksum, the copies and the probe log"; return 0; fi
  snapshot "$WORK/before.tsv"
  before_count=$(wc -l <"$WORK/before.tsv" | tr -d ' ')
  info "$label: $before_count audit events saved before the loss"
  shuf -n "$SAMPLE" "$WORK/before.tsv" >"$WORK/sample.tsv"
  : >"$WORK/probe.log"
  probe &
  PROBE_PID=$!
  PIDS+=("$PROBE_PID")
  lose_node "$node" || { kill "$PROBE_PID" 2>/dev/null; return; }
  t0=$SECONDS
  if until_true "$RECOVER_WAIT" recovered "$node"; then pass "$label recovery in $((SECONDS - t0))s: every store pod is ready, no partial answer, every sampled id has two copies and $node holds some"
  else fail "$label recovery did not finish in ${RECOVER_WAIT}s: $node is not back to a full share with two copies per id (#294)"; fi
  kill "$PROBE_PID" 2>/dev/null
  snapshot "$WORK/after.tsv"
  after_count=$(wc -l <"$WORK/after.tsv" | tr -d ' ')
  missing=$(comm -23 <(cut -f1,2 "$WORK/before.tsv" | sort -u) <(cut -f1,2 "$WORK/after.tsv" | sort -u) | wc -l | tr -d ' ')
  want "$label no audit event from before the loss is missing (#289, #294)" "$missing" 0
  info "$label: $before_count events before, $after_count after (the sources keep writing)"
  want "$label writes continued during the loss (more events after than before)" "$([ "$after_count" -gt "$before_count" ] && echo yes || echo no)" yes
  for src in $(cut -f1 "$WORK/after.tsv" | sort -u); do
    checksum_matches "$src"
    case $? in
      0) pass "$label audit checksum of $src equals an independent SHA-256 over its ids" ;;
      1) fail "$label audit checksum of $src does not match the listed ids (#295)" ;;
      *) notrun "$label checksum of $src: the data kept moving for 5 tries" ;;
    esac
  done
  total=$(wc -l <"$WORK/probe.log" | tr -d ' ')
  bad=$(awk -v t="$PROBE_TIMEOUT" '$3 + 0 >= 500 || $3 + 0 == 0 || $4 + 0 >= t { n++ } END { print n + 0 }' "$WORK/probe.log")
  partial=$(awk '$5 == "true" { n++ } END { print n + 0 }' "$WORK/probe.log")
  info "$label probe: $total answers, $partial partial"
  want "$label probe answers that were a 5xx or a hang (#291, #292)" "$bad" 0
  placement "$node"
  want "$label sampled ids with fewer than two copies after recovery (#294)" "$PLACE_BAD" 0
  want "$label the returned node holds copies again" "$([ "$PLACE_ON_NODE" -gt 0 ] && echo yes || echo no)" yes
}

prepare
if [ "$DRY_RUN" = 1 ]; then workers=(node-b node-c); stores=3; else
  mapfile -t workers < <(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  stores=$(store_nodes | wc -l)
fi
for s in $ONLY; do
  case $s in
    A) sA ;;
    B) if [ "${#workers[@]}" -ge 1 ] && [ "$stores" -ge 2 ]; then sWipe B "${workers[0]}"; else notrun "B needs a worker node and a store on at least two nodes"; fi ;;
    E) if [ "${#workers[@]}" -ge 2 ] && [ "$stores" -ge 3 ]; then sWipe E "${workers[1]}"; else notrun "E needs two worker nodes and a store on three nodes"; fi ;;
  esac
done
