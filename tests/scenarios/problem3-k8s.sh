#!/usr/bin/env bash
# Problem 3 on the cluster (#307): seven scenarios that play users and failures against the running system.
#
#   tests/scenarios/problem3-k8s.sh                  all scenarios
#   ONLY="1 2" tests/scenarios/problem3-k8s.sh       some scenarios
#   DRY_RUN=1 tests/scenarios/problem3-k8s.sh        print the plan and every command, touches no cluster
#
# 1 additive change accepted, 2 the three incompatible changes parked, 3 retryable failure heals, 4 who may inspect,
# 5 bad messages crash nothing, 6 replay after adjusting the system, 7 a 202 report is never lost.
# A FAIL means the implementation is wrong. Exit 2 and NOT RUN mean the script could not run, never a verdict.
# Needs: kubectl (context of the cluster), curl, jq, base64, sha256sum. The cluster is a local VM, so the script may break the PVMBG mock,
# stop the field report processor and patch bnpb-config; the exit trap puts all of it back.
# Exit code 1 when any check is FAIL, 2 when the cluster is not ready.
set -uo pipefail

DRY_RUN=${DRY_RUN:-0}
ONLY=${ONLY:-1 2 3 4 5 6 7}
NS=${NS:-sistem-informasi-kebencanaan-bnpb}
TESTER_NS=${TESTER_NS:-tester-agent}
API_PORT=${API_PORT:-38080}
AUDIT_PORT=${AUDIT_PORT:-38091}
MOCK_PORT=${MOCK_PORT:-38082}
MQ_PORT=${MQ_PORT:-35672}
N=${N:-200}                    # field reports in scenario 7
WAIT=${WAIT:-120}              # seconds the system gets to produce a result
FAST_MS=${FAST_MS:-1000}       # answer time of POST /field-reports while the downstream is dead
ADJUST=${ADJUST:-rename:eruption_count=eruption_count_24h,type:ash_column_height_m,default:volcano_id=UNKNOWN}
RUN=${RUN:-p3-$(date +%s)}

PASSED=0 FAILED=0 NOTRUN=0
PIDS=()
WORK=$(mktemp -d)
API=http://127.0.0.1:$API_PORT
AUDIT=http://127.0.0.1:$AUDIT_PORT
MOCK=http://127.0.0.1:$MOCK_PORT
MQ=http://127.0.0.1:$MQ_PORT
AUDIT_TOKEN="" ADMIN_TOKEN="" OP_EMAIL="" OP_PASS="" MQ_USER="" MQ_PASS="" AUDIT_PF="" API_PF=""
RESTORE_SCHEMA=0 RESTORE_MALFORMED=0 RESTORE_ADJUST=0 RESTORE_PROCESSOR=""

say() { printf '%s\n' "$*"; }
pass() { say "PASS: $*"; PASSED=$((PASSED + 1)); }
fail() { say "FAIL: $*"; FAILED=$((FAILED + 1)); }
notrun() { say "NOT RUN: $*"; NOTRUN=$((NOTRUN + 1)); }
info() { say "INFO: $*"; }
heading() { printf '\n== %s\n' "$*"; }

# x <cmd...>: the only way the script changes anything, so a dry run prints instead.
x() {
  if [ "$DRY_RUN" = 1 ]; then printf '[dry-run] %s\n' "$*"; return 0; fi
  "$@"
}

# nap <seconds>: sleep that a TERM can interrupt, so the exit trap runs at once.
nap() { sleep "$1" & wait $!; }

kc() { kubectl -n "$NS" "$@"; }

# want <what> <got> <expected>
want() { if [ "$2" = "$3" ]; then pass "$1 ($2)"; else fail "$1: got $2, want $3"; fi }

# until_true <seconds> <cmd...>: runs cmd every 2 s until it succeeds.
until_true() {
  local end=$((SECONDS + $1))
  shift
  while [ "$SECONDS" -lt "$end" ]; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    nap 2
  done
  return 1
}

secret() { kc get secret "$1" -o "jsonpath={.data.$2}" | base64 -d; }

cleanup() {
  trap - EXIT INT TERM
  local p
  if [ "$DRY_RUN" != 1 ]; then
    if [ "$RESTORE_SCHEMA" = 1 ]; then curl -s -X POST "$MOCK/admin/schema-version" -d '{"version":1}' >/dev/null && say "trap: PVMBG schema back to legacy"; fi
    if [ "$RESTORE_MALFORMED" = 1 ]; then curl -s -X POST "$MOCK/admin/malformed" -d '{"kind":"off"}' >/dev/null && say "trap: PVMBG malformed off"; fi
    if [ -n "$RESTORE_PROCESSOR" ]; then kc scale deployment/field-report-processor --replicas="$RESTORE_PROCESSOR" >/dev/null && say "trap: field report processor back to $RESTORE_PROCESSOR"; fi
    if [ "$RESTORE_ADJUST" = 1 ]; then
      kc patch configmap bnpb-config --type=json -p '[{"op":"remove","path":"/data/PVMBG_SCHEMA_ADJUST"}]' >/dev/null 2>&1
      kc rollout restart deployment/aggregator >/dev/null && say "trap: PVMBG_SCHEMA_ADJUST removed, Aggregator restarted"
    fi
  fi
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  rm -rf "$WORK"
  say ""
  say "PASS $PASSED, FAIL $FAILED, NOT RUN $NOTRUN"
  [ "$FAILED" -gt 0 ] && exit 1
  exit 0
}
trap cleanup EXIT
trap 'exit 2' INT TERM

# ---------- reaching the cluster ----------

forward() { # forward <target> <local port> <remote port>
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] kubectl -n $NS port-forward $1 $2:$3"; return 0; fi
  kc port-forward "$1" "$2:$3" >"$WORK/pf-$2.log" 2>&1 &
  PIDS+=("$!")
  until_true 20 curl -s -o /dev/null "http://127.0.0.1:$2/" || { say "port-forward $1 $2:$3 failed" >&2; return 1; }
}

# The tunnel of a port-forward dies with its pod, so a scenario that deletes that pod opens a new one.
reforward_api() {
  kill "$API_PF" 2>/dev/null
  forward svc/client-facing-api "$API_PORT" 8080 || return 1
  API_PF=${PIDS[${#PIDS[@]} - 1]}
}

prepare() {
  heading "Cluster"
  if [ "$DRY_RUN" = 1 ]; then
    say "[dry-run] check that every pod in $NS is Ready, read the Secrets audit-token, admin-token, auth-secrets, rabbitmq-credentials"
    say "[dry-run] port-forward client-facing-api, aggregator, pvmbg-mock, rabbitmq"
    return 0
  fi
  kubectl version --request-timeout=5s >/dev/null 2>&1 || { say "NOT RUN: kubectl cannot reach the cluster" >&2; exit 2; }
  local notready
  notready=$(kc get pods --no-headers 2>/dev/null | awk '{ split($2, a, "/"); if (a[1] != a[2] || ($3 != "Running" && $3 != "Completed")) n++ } END { print n + 0 }')
  [ "$notready" = 0 ] || { say "NOT RUN: $notready pods in $NS are not ready" >&2; exit 2; }
  AUDIT_TOKEN=$(secret audit-token token) ADMIN_TOKEN=$(secret admin-token token)
  OP_EMAIL=$(kc get configmap bnpb-config -o 'jsonpath={.data.SEED_OPERATOR_EMAIL}')
  OP_PASS=$(secret auth-secrets seed_operator_password)
  MQ_USER=$(secret rabbitmq-credentials user) MQ_PASS=$(secret rabbitmq-credentials password)
  [ -n "$AUDIT_TOKEN" ] && [ -n "$ADMIN_TOKEN" ] && [ -n "$OP_PASS" ] || { say "NOT RUN: a Secret is missing" >&2; exit 2; }
  forward svc/client-facing-api "$API_PORT" 8080 || exit 2
  API_PF=${PIDS[${#PIDS[@]} - 1]}
  forward deployment/aggregator "$AUDIT_PORT" 8091 || exit 2
  AUDIT_PF=${PIDS[${#PIDS[@]} - 1]}
  forward svc/pvmbg-mock "$MOCK_PORT" 8082 || exit 2
  forward svc/rabbitmq "$MQ_PORT" 15672 || exit 2
  say "cluster ready, run id $RUN"
}

# ---------- helpers for the scenarios ----------

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
login() { code -c "$WORK/$1.jar" -X POST "$API/auth/login" -d "{\"email\":\"$2\",\"password\":\"$3\"}"; }
ops() { curl -s -b "$WORK/op.jar" "$@"; }
audit_get() { curl -s -H "Authorization: Bearer $AUDIT_TOKEN" "$AUDIT$1"; }
unprocessed() { audit_get "/internal/audit/unprocessed?source=$1&limit=1000"; }
parked_count() { unprocessed "$1" | jq '[.items[] | select(.status == "parked")] | length'; }
restarts() { kc get pods -o json | jq -r '[.items[] | select(.metadata.name | test("aggregator|client-facing-api|field-report-processor")) | "\(.metadata.name)=\(([.status.containerStatuses[]?.restartCount] | add) // 0)"] | sort | join(" ")'; }
set_schema() { curl -s -X POST "$MOCK/admin/schema-version" -d "$1" >/dev/null; RESTORE_SCHEMA=1; }
set_malformed() { curl -s -X POST "$MOCK/admin/malformed" -d "{\"kind\":\"$1\"}"; RESTORE_MALFORMED=1; }

ensure_accounts() {
  [ -f "$WORK/accounts" ] && return 0
  want "Operator login" "$(login op "$OP_EMAIL" "$OP_PASS")" 200
  code -X POST "$API/auth/register" -d '{"email":"media@demo.example","password":"media-pass-1"}' >/dev/null
  code -b "$WORK/op.jar" -X POST "$API/accounts" -d '{"email":"field@demo.example","password":"field-pass-1","role":"Lapangan"}' >/dev/null
  login media media@demo.example media-pass-1 >/dev/null
  login field field@demo.example field-pass-1 >/dev/null
  : >"$WORK/accounts"
}

# ---------- scenario 1 ----------

s1() {
  heading "Scenario 1: the source adds a field (accepted change)"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] POST $MOCK/admin/schema-version (empty body, extended), wait for the next polls, GET /hazards?type=VOLCANIC, no new failed message, restart counts"; return 0; fi
  ensure_accounts
  local before_restarts before_parked
  before_restarts=$(restarts)
  before_parked=$(parked_count PVMBG)
  set_schema '{"version":1}'
  curl -s -X POST "$MOCK/admin/schema-version" -d '' >/dev/null
  if until_true "$WAIT" bash -c "curl -s -b '$WORK/op.jar' '$API/hazards?type=VOLCANIC&limit=20' | jq -e '[.data[] | select(.attributes.confidence_level != null)] | length > 0'"; then
    pass "S1 a volcanic record with attributes.confidence_level is served"
  else
    fail "S1 no record with confidence_level after ${WAIT}s (#278, #299)"
  fi
  want "S1 older volcanic records still served" "$(code -b "$WORK/op.jar" "$API/hazards?type=VOLCANIC&limit=20")" 200
  want "S1 no new parked PVMBG message" "$(parked_count PVMBG)" "$before_parked"
  want "S1 restart counts unchanged" "$(restarts)" "$before_restarts"
  set_schema '{"version":1}'
}

# ---------- scenario 2 ----------

# failed_row <message id>: the Operator route's row for that id, found by following the cursor, or nothing.
failed_row() {
  local cursor="" page row i
  for i in $(seq 1 30); do
    page=$(ops "$API/admin/failed-messages?status=parked&limit=100${cursor:+&cursor=$cursor}")
    row=$(jq -c --arg id "$1" '[.data[]? | select(.message_id == $id)] | first // empty' <<<"$page")
    if [ -n "$row" ]; then printf '%s' "$row"; return 0; fi
    cursor=$(jq -r '.next_cursor // empty' <<<"$page")
    [ -n "$cursor" ] || return 0
  done
}

# round <change> <field pattern in the reason> <marker that must be in the stored payload>
s2_round() {
  local change=$1 reason_re=$2 marker=$3 before after row id payload sha
  before=$(parked_count PVMBG)
  set_schema "{\"version\":2,\"change\":\"$change\"}"
  if ! until_true "$WAIT" bash -c "[ \"\$(curl -s -H 'Authorization: Bearer $AUDIT_TOKEN' '$AUDIT/internal/audit/unprocessed?source=PVMBG&limit=1000' | jq '[.items[] | select(.status == \"parked\")] | length')\" -gt $before ]"; then
    fail "S2 $change: no new parked PVMBG message after ${WAIT}s (#299, #300)"
    set_schema '{"version":1}'
    return
  fi
  set_schema '{"version":1}'
  after=$(unprocessed PVMBG)
  row=$(jq -c --arg re "$reason_re" '[.items[] | select(.status == "parked" and (.reason | test($re)))] | last // empty' <<<"$after")
  if [ -z "$row" ]; then fail "S2 $change: no parked row whose reason names $reason_re: $(jq -c '[.items[].reason] | unique' <<<"$after")"; return; fi
  if [[ "$(jq -r .reason <<<"$row")" == "schema_incompatible: field '"* ]]; then pass "S2 $change reason has the Appendix A form"; else fail "S2 $change reason is not 'schema_incompatible: field ...' as Appendix A shows: $(jq -r .reason <<<"$row") (#300, #301)"; fi
  want "S2 $change fields of the auditor item" "$(jq -c 'keys' <<<"$row")" '["attempts","message_id","reason","received_at","retryable","source","status"]'
  want "S2 $change retryable" "$(jq .retryable <<<"$row")" false
  case "$(jq -r .received_at <<<"$row")" in *Z) pass "S2 $change received_at is UTC" ;; *) fail "S2 $change received_at is not UTC: $(jq -r .received_at <<<"$row")" ;; esac
  id=$(jq -r .message_id <<<"$row")
  payload=$(failed_row "$id")
  if [ -z "$payload" ]; then fail "S2 $change: the Operator route does not list the parked message $id (#304)"; return; fi
  sha=$(jq -r .payload_sha256 <<<"$payload")
  jq -r .payload <<<"$payload" | base64 -d >"$WORK/payload.bin"
  want "S2 $change stored payload matches its sha256" "$(sha256sum <"$WORK/payload.bin" | cut -d' ' -f1)" "$sha"
  if grep -q -- "$marker" "$WORK/payload.bin"; then pass "S2 $change the stored payload still has the original field ($marker)"; else fail "S2 $change the stored payload lost the original field ($marker)"; fi
}

s2() {
  heading "Scenario 2: the source breaks the format, three ways"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] for rename, type, missing: POST $MOCK/admin/schema-version {version 2, change}, wait for a new parked PVMBG item in /internal/audit/unprocessed, check its fields, reason, payload (Operator route), restore the mock"; return 0; fi
  ensure_accounts
  local before_restarts
  before_restarts=$(restarts)
  s2_round rename "field 'eruption_count_24h' missing" '"eruption_count"'
  s2_round type "field 'ash_column_height_m' changed from number to string" '"ash_column_height_m":"'
  s2_round missing "field 'volcano_id' missing" '"report_id"'
  want "S2 hazards still served" "$(code -b "$WORK/op.jar" "$API/hazards?type=VOLCANIC&limit=5")" 200
  want "S2 restart counts unchanged" "$(restarts)" "$before_restarts"
}

# ---------- scenario 3 ----------

all_unprocessed() { audit_get "/internal/audit/unprocessed?limit=1000"; }
agent_logs() { kubectl -n "$TESTER_NS" logs -l app=tester-agent --tail=-1 --max-log-requests=10 2>/dev/null; }
agent_done_count() { local l; l=$(agent_logs); grep -c "pemulihan akhir selesai" <<<"$l"; }

# pvmbg_seqs: the sequence numbers at the end of every PVMBG source_ref_id in the audit events, sorted.
pvmbg_seqs() {
  local cursor="" page i
  for i in $(seq 1 30); do
    page=$(audit_get "/internal/audit/events?source=PVMBG&limit=1000${cursor:+&cursor=$cursor}")
    jq -r '.items[]?.source_ref_id' <<<"$page"
    cursor=$(jq -r '.next_cursor // empty' <<<"$page")
    [ -n "$cursor" ] || break
  done | sed 's/^.*-//' | sort -n | uniq
}

s3() {
  heading "Scenario 3: retryable and never-retryable are treated differently"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] kubectl exec -n $TESTER_NS daemonset/tester-agent -- /app run practice kill-store, sample /internal/audit/unprocessed every 3 s, wait for the agent end marker, check no retrying row is left, no hole in the PVMBG ids and the parked messages unchanged"; return 0; fi
  if ! kubectl -n "$TESTER_NS" get daemonset tester-agent >/dev/null 2>&1; then notrun "S3 the Tester Agent is not installed in the namespace $TESTER_NS"; return; fi
  ensure_accounts
  local before_done parked_before max_seq_before samples max_retrying max_attempts r n end seen after holes dup
  parked_before=$(all_unprocessed | jq -c '[.items[] | select(.status == "parked") | {(.message_id): .attempts}] | add // {}')
  max_seq_before=$(pvmbg_seqs | tail -1)
  before_done=$(agent_done_count)
  info "S3 parked messages before: $(jq 'length' <<<"$parked_before"), newest PVMBG sequence ${max_seq_before:-none}"
  kubectl -n "$TESTER_NS" exec daemonset/tester-agent -- /app run practice kill-store >"$WORK/kill-store.log" 2>&1 || { notrun "S3 the Tester Agent refused kill-store: $(head -c 200 "$WORK/kill-store.log")"; return; }
  max_retrying=0 max_attempts=0 seen=0
  end=$((SECONDS + 300))
  while [ "$SECONDS" -lt "$end" ]; do
    samples=$(all_unprocessed)
    r=$(jq '[.items[]? | select(.status == "retrying")] | length' <<<"$samples" 2>/dev/null || echo 0)
    [ "${r:-0}" -gt "$max_retrying" ] && max_retrying=$r
    n=$(jq '[.items[]? | select(.status == "retrying") | .attempts] | max // 0' <<<"$samples" 2>/dev/null || echo 0)
    [ "${n:-0}" -gt "$max_attempts" ] && max_attempts=$n
    if [ "$(agent_done_count)" -gt "$before_done" ]; then seen=1; break; fi
    nap 3
  done
  if [ "$seen" = 0 ]; then notrun "S3 the Tester Agent did not report the end of kill-store within 300 s"; return; fi
  info "S3 kill-store finished, waiting 60 s for the retry worker; most retrying rows seen at once: $max_retrying, highest attempts: $max_attempts"
  nap 60
  until_true "$WAIT" bash -c "[ \"\$(curl -s -H 'Authorization: Bearer $AUDIT_TOKEN' '$AUDIT/internal/audit/unprocessed?limit=1000' | jq '[.items[]? | select(.status == \"retrying\")] | length')\" = 0 ]"
  after=$(all_unprocessed)
  want "S3 no message is left in the retrying state" "$(jq '[.items[]? | select(.status == "retrying")] | length' <<<"$after")" 0
  holes=$(pvmbg_seqs | awk -v base="${max_seq_before:-0}" '$1 + 0 > base + 0 { if (prev && $1 + 0 != prev + 1) gap++; prev = $1 + 0 } END { print gap + 0 }')
  want "S3 holes in the PVMBG sequence after the outage (a lost record)" "$holes" 0
  dup=$(audit_get "/internal/audit/events?source=PVMBG&limit=1000" | jq '[.items[].source_ref_id] | length - (unique | length)')
  want "S3 repeated source_ref_id among the audit events" "$dup" 0
  want "S3 parked messages that disappeared from the auditor's list (#300, #304)" "$(jq -c --argjson b "$parked_before" '[.items[] | select(.status == "parked") | {(.message_id): .attempts}] | add // {} | . as $a | ($b | to_entries | map(select($a[.key] == null)) | length)' <<<"$after")" 0
  want "S3 parked messages whose attempts changed (a never-retryable message was retried)" "$(jq -c --argjson b "$parked_before" '[.items[] | select(.status == "parked") | {(.message_id): .attempts}] | add // {} | . as $a | ($b | to_entries | map(select($a[.key] != null and $a[.key] != .value)) | length)' <<<"$after")" 0
  if [ "$max_retrying" -gt 0 ]; then
    pass "S3 retrying rows were seen during the outage ($max_retrying at once, attempts up to $max_attempts) and all healed"
  else
    notrun "S3 kill-store produced no retrying row (the second copy of the data may have absorbed it), so the retrying path itself was not shown; the table lock fallback is not written yet"
  fi
  want "S3 restart counts do not matter here, hazards still served" "$(code -b "$WORK/op.jar" "$API/hazards?type=VOLCANIC&limit=5")" 200
}

# ---------- scenario 4 ----------

s4() {
  heading "Scenario 4: who may inspect failed messages"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] GET $API/admin/failed-messages as Operator, Media, Tim Lapangan and without a cookie; the audit route with a cookie"; return 0; fi
  ensure_accounts
  want "S4 Operator" "$(code -b "$WORK/op.jar" "$API/admin/failed-messages?limit=1")" 200
  want "S4 Media" "$(code -b "$WORK/media.jar" "$API/admin/failed-messages?limit=1")" 403
  want "S4 Tim Lapangan" "$(code -b "$WORK/field.jar" "$API/admin/failed-messages?limit=1")" 403
  want "S4 no cookie" "$(code "$API/admin/failed-messages?limit=1")" 401
  want "S4 audit route with a login cookie" "$(code -b "$WORK/op.jar" "$AUDIT/internal/audit/unprocessed")" 401
  if [ "$(unprocessed PVMBG | jq '[.items[] | has("payload")] | any')" = false ]; then pass "S4 the audit items carry no payload"; else fail "S4 the audit items carry a payload"; fi
}

queue_depth() { curl -s -u "$MQ_USER:$MQ_PASS" "$MQ/api/queues/%2F/field-reports" | jq '.messages // 0'; }

# publish <file with the body> <message id>: puts the raw bytes of the file on the field-reports queue through the broker's HTTP API.
publish() {
  base64 -w0 <"$1" >"$WORK/b64"
  printf '{"properties":{"delivery_mode":2,"message_id":"%s"},"routing_key":"field-reports","payload_encoding":"base64","payload":"%s"}' "$2" "$(cat "$WORK/b64")" >"$WORK/pub.json"
  curl -s -u "$MQ_USER:$MQ_PASS" -X POST "$MQ/api/exchanges/%2F/amq.default/publish" -H 'content-type: application/json' --data-binary "@$WORK/pub.json" | jq -r '.routed'
}

# bad_body <kind> <file>: writes one bad input of the four kinds the M2 text lists.
bad_body() {
  case $1 in
    truncated) printf '{"description":"cut in the mi' >"$2" ;;
    deep) { printf '{"description":'; head -c 100 </dev/zero | tr '\0' '['; head -c 100 </dev/zero | tr '\0' ']'; printf '}'; } >"$2" ;;
    utf8) printf '{"description":"bad \377\376 bytes"}' >"$2" ;;
    huge) { printf '{"description":"'; head -c 1100000 </dev/zero | tr '\0' 'x'; printf '"}'; } >"$2" ;;
  esac
}

s5_source() { # s5_source <kind> <reason code>
  local kind=$1 code=$2 before after newest_before
  before=$(unprocessed PVMBG | jq --arg c "$code" '[.items[] | select(.reason | startswith($c))] | length')
  newest_before=$(ops "$API/hazards?type=VOLCANIC&limit=1" | jq -r '.data[0].source_ref_id // ""')
  set_malformed "$kind" >/dev/null
  if until_true "$WAIT" bash -c "[ \"\$(curl -s -H 'Authorization: Bearer $AUDIT_TOKEN' '$AUDIT/internal/audit/unprocessed?source=PVMBG&limit=1000' | jq --arg c '$code' '[.items[] | select(.reason | startswith(\$c))] | length')\" -gt $before ]"; then
    pass "S5 source $kind: a parked PVMBG message with the code $code"
  else
    fail "S5 source $kind: nothing parked with the code $code after ${WAIT}s (#302)"
  fi
  if until_true 60 bash -c "[ \"\$(curl -s -b '$WORK/op.jar' '$API/hazards?type=VOLCANIC&limit=1' | jq -r '.data[0].source_ref_id // \"\"')\" != '$newest_before' ]"; then
    pass "S5 source $kind: a new normal record was stored while the bad one was served"
  else
    fail "S5 source $kind: no new normal record in 60 s, the good records did not keep flowing (#302)"
  fi
  after=$(unprocessed PVMBG | jq --arg c "$code" '[.items[] | select(.reason | startswith($c))] | length')
  info "S5 source $kind: $((after - before)) new parked row(s) with $code"
  set_malformed off >/dev/null
}

s5() {
  heading "Scenario 5: bad messages must not break anything"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] (a) POST $API/field-reports with truncated, deep, invalid UTF-8 and oversize bodies; (b) publish the same kinds to the field-reports queue through $MQ; (c) POST $MOCK/admin/malformed for each kind; restart counts before and after"; return 0; fi
  ensure_accounts
  local before_restarts kind code before_ff
  before_restarts=$(restarts)
  say "-- (a) the field report API"
  for kind in truncated deep utf8 huge; do
    bad_body "$kind" "$WORK/bad.json"
    want "S5 API $kind body answered 400" "$(code -b "$WORK/field.jar" -X POST "$API/field-reports" --data-binary "@$WORK/bad.json")" 400
  done
  say "-- (b) the queue"
  before_ff=$(unprocessed FIELD_REPORT | jq '[.items[] | select(.status == "parked")] | length')
  for kind in truncated deep utf8 huge; do
    bad_body "$kind" "$WORK/bad.json"
    want "S5 queue $kind published" "$(publish "$WORK/bad.json" "$RUN-bad-$kind")" true
  done
  printf '{"report_id":"%s-good","description":"a normal report in the middle of the bad ones","reporter_id":"field@demo.example","reporter_role":"Lapangan","received_at":"%s"}' "$RUN" "$(date -u +%FT%TZ)" >"$WORK/good.json"
  want "S5 queue a good report published" "$(publish "$WORK/good.json" "$RUN-good")" true
  if until_true "$WAIT" bash -c "[ \"\$(curl -s -u '$MQ_USER:$MQ_PASS' '$MQ/api/queues/%2F/field-reports' | jq '.messages // 0')\" = 0 ]"; then pass "S5 queue drained to 0"; else fail "S5 queue still holds $(queue_depth) messages after ${WAIT}s (#306)"; fi
  local parked
  parked=$(unprocessed FIELD_REPORT | jq '[.items[] | select(.status == "parked")] | length')
  if [ "$parked" -ge $((before_ff + 4)) ]; then pass "S5 queue: the four bad messages are parked as FIELD_REPORT ($((parked - before_ff)) new)"; else fail "S5 queue: only $((parked - before_ff)) of 4 bad messages parked (#306)"; fi
  say "-- (c) the data source"
  s5_source truncated malformed_json
  s5_source deep too_deep
  s5_source utf8 invalid_utf8
  s5_source huge too_large
  want "S5 restart counts unchanged" "$(restarts)" "$before_restarts"
}
schema_parked() { unprocessed PVMBG | jq '[.items[] | select(.status == "parked" and (.reason | startswith("schema_incompatible")))] | length'; }
replay() { curl -s -X POST -H "Authorization: Bearer ${1:-$ADMIN_TOKEN}" "$AUDIT/internal/admin/replay?source=PVMBG"; }

s6() {
  heading "Scenario 6: replay after the system is adjusted"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] replay once, patch bnpb-config with PVMBG_SCHEMA_ADJUST=$ADJUST, kubectl rollout restart deployment/aggregator, replay again and check recovered, duplicates, a second replay; remove the setting on exit"; return 0; fi
  ensure_accounts
  local expected parked_all rep dup logs
  expected=$(schema_parked)
  parked_all=$(parked_count PVMBG)
  if [ "$expected" -lt 3 ]; then notrun "S6 needs the parked schema_incompatible messages of scenario 2, found $expected"; return; fi
  want "S6 replay without the admin token" "$(code -X POST "$AUDIT/internal/admin/replay?source=PVMBG")" 401
  rep=$(replay)
  want "S6 replay before the adjustment recovers nothing" "$(jq .recovered <<<"$rep")" 0
  kc patch configmap bnpb-config --type=merge -p "{\"data\":{\"PVMBG_SCHEMA_ADJUST\":\"$ADJUST\"}}" >/dev/null
  RESTORE_ADJUST=1
  kc rollout restart deployment/aggregator >/dev/null
  if ! kc rollout status deployment/aggregator --timeout=240s >/dev/null 2>&1; then notrun "S6 the Aggregator did not become ready after the restart"; return; fi
  kill "$AUDIT_PF" 2>/dev/null
  forward deployment/aggregator "$AUDIT_PORT" 8091 || { notrun "S6 cannot reach the restarted Aggregator"; return; }
  AUDIT_PF=${PIDS[${#PIDS[@]} - 1]}
  logs=$(kc logs -l role=aggregator --tail=-1 --max-log-requests=10 2>/dev/null)
  if ! grep -q "PVMBG schema adjustments active" <<<"$logs"; then notrun "S6 the Aggregator on the cluster does not know PVMBG_SCHEMA_ADJUST (a build without it)"; return; fi
  rep=$(replay)
  info "S6 replay answer: $(jq -c 'del(.errors)' <<<"$rep")"
  want "S6 recovered equals the schema_incompatible messages parked before" "$(jq .recovered <<<"$rep")" "$expected"
  want "S6 schema_incompatible messages still parked" "$(schema_parked)" 0
  want "S6 other parked messages untouched" "$(parked_count PVMBG)" "$((parked_all - expected))"
  dup=$(audit_get "/internal/audit/events?source=PVMBG&limit=1000" | jq '[.items[].source_ref_id] | length - (unique | length)')
  want "S6 repeated source_ref_id among the audit events" "$dup" 0
  want "S6 a second replay recovers nothing more" "$(replay | jq .recovered)" 0
}
# send_report <index>: one report with its own key, retried with the same key until it gets a 202. Appends "key code seconds" to sent.log.
send_report() {
  local i=$1 key="$RUN-r$1" out tries=0
  while [ "$tries" -lt 40 ]; do
    out=$(curl -s -o /dev/null -w '%{http_code} %{time_total}' -b "$WORK/field.jar" -X POST "$API/field-reports" -H "Idempotency-Key: $key" \
      -d "{\"description\":\"report $i of $RUN\",\"kind\":\"p3-test\"}")
    case $out in
      202*) printf '%s %s\n' "$key" "$out" >>"$WORK/sent.log"; return 0 ;;
      000*) nap 2; reforward_api >/dev/null 2>&1 ;;
    esac
    tries=$((tries + 1))
    nap 1
  done
  printf '%s %s\n' "$key" "$out" >>"$WORK/failed.log"
}

# The report id is derived from the Idempotency-Key, so a stored report is matched by its description "report <i> of <run>".
stored_indexes() { # the index i of every report of this run on every store node, one line per row
  local pod user
  user=$(secret store-credentials user)
  for pod in $(kc get pods -l app=store -o name); do
    kc exec "$pod" -- psql -U "$user" -d canonical_store -tAc "SELECT (regexp_match(description, '^report ([0-9]+) of $RUN\$'))[1] FROM field_reports WHERE description LIKE 'report % of $RUN'" 2>/dev/null
  done
}

s7() {
  heading "Scenario 7: a field report answered 202 is never lost"
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] scale field-report-processor to 0, send 50 reports and time them, scale back, send the rest while deleting the processor, an Aggregator and a Client-Facing API pod, then compare the 202 keys with the stored report ids"; return 0; fi
  ensure_accounts
  : >"$WORK/sent.log"; : >"$WORK/failed.log"
  local before_restarts replicas i third p95 slow stored accepted
  before_restarts=$(restarts)
  replicas=$(kc get deployment field-report-processor -o 'jsonpath={.spec.replicas}')
  RESTORE_PROCESSOR=$replicas
  kc scale deployment/field-report-processor --replicas=0 >/dev/null
  kc rollout status deployment/field-report-processor --timeout=60s >/dev/null 2>&1
  say "-- (a) downstream dead: processor scaled to 0"
  for i in $(seq 1 50); do send_report "$i"; done
  slow=$(awk -v ms="$FAST_MS" '$2 == 202 && $3 * 1000 > ms { n++ } END { print n + 0 }' "$WORK/sent.log")
  p95=$(awk '{ print $3 * 1000 }' "$WORK/sent.log" | sort -n | awk '{ a[NR] = $1 } END { print int(a[int(NR * 0.95) + (NR * 0.95 > int(NR * 0.95))]) }')
  want "S7 all 50 answers while the downstream is dead are 202" "$(wc -l <"$WORK/sent.log" | tr -d ' ')" 50
  want "S7 answers slower than ${FAST_MS} ms (p95 is ${p95} ms)" "$slow" 0
  say "-- (b) processor back, the rest is sent while pods die"
  kc scale deployment/field-report-processor --replicas="$replicas" >/dev/null
  RESTORE_PROCESSOR=""
  third=$(((N - 50) / 3))
  for i in $(seq 51 "$N"); do
    send_report "$i"
    case $((i - 50)) in
      "$third") kc delete pod -l app=field-report-processor --wait=false >/dev/null; info "S7 deleted the processor pods after report $i" ;;
      $((third * 2))) kc delete pod -l role=aggregator --wait=false --field-selector=status.phase=Running >/dev/null 2>&1; info "S7 deleted the Aggregator pods after report $i" ;;
      $((third * 3 - 1))) kc delete pod -l app=client-facing-api --wait=false >/dev/null; info "S7 deleted the Client-Facing API pods after report $i" ;;
    esac
  done
  until_true "$WAIT" bash -c "[ \"\$(curl -s -u '$MQ_USER:$MQ_PASS' '$MQ/api/queues/%2F/field-reports' | jq '.messages // 0')\" = 0 ]" || info "S7 the queue still holds $(queue_depth) messages"
  nap 10
  sed 's/^.*-r\([0-9]*\) .*/\1/' "$WORK/sent.log" | sort -u >"$WORK/accepted.idx"
  stored_indexes >"$WORK/stored.rows"
  sort -u "$WORK/stored.rows" >"$WORK/stored.idx"
  accepted=$(wc -l <"$WORK/accepted.idx" | tr -d ' ')
  want "S7 reports never acknowledged with 202 after retries" "$(wc -l <"$WORK/failed.log" | tr -d ' ')" 0
  want "S7 distinct stored reports equal the keys that got a 202 ($accepted)" "$(wc -l <"$WORK/stored.idx" | tr -d ' ')" "$accepted"
  want "S7 acknowledged reports missing from the store" "$(comm -23 "$WORK/accepted.idx" "$WORK/stored.idx" | wc -l | tr -d ' ')" 0
  want "S7 reports stored that no 202 acknowledged" "$(comm -13 "$WORK/accepted.idx" "$WORK/stored.idx" | wc -l | tr -d ' ')" 0
  info "S7 duplicates absorbed: $(curl -s "$AUDIT/metrics" | grep '^aggregator_field_reports_total' | tr '\n' ' ')"
  info "S7 restart counts before: $before_restarts"
  info "S7 restart counts after:  $(restarts)"
}

prepare
for s in $ONLY; do "s$s"; done
