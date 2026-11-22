#!/usr/bin/env bash
# Offline self test for problem3-k8s.sh (#307). A fake kubectl and curl on PATH play a cluster that keeps its state in files:
# first a healthy one, then one that is broken in exactly one way. The script must print FAIL for that check only, NOT RUN
# when it cannot run, and exit 2 when the cluster cannot be reached. Covers all seven scenarios. Needs bash, jq, base64 and sha256sum.
#
#   tests/scenarios/problem3-k8s_test.sh
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
FAILS=0
ok() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1 (got: $2, want: $3)"; FAILS=$((FAILS + 1)); fi; }
has() { if grep -qE -- "$3" <<<"$2"; then echo "ok   $1"; else echo "FAIL $1 (no match for: $3)"; FAILS=$((FAILS + 1)); fi; }
hasnt() { if grep -qE -- "$3" <<<"$2"; then echo "FAIL $1 (unexpected: $3)"; FAILS=$((FAILS + 1)); else echo "ok   $1"; fi; }

mkdir -p "$T/bin" "$T/state"

cat >"$T/bin/curl" <<'EOF'
#!/usr/bin/env bash
# Fake curl: answers by method and path from the files in $FAKE. $FAKE_BREAK names how the cluster is broken.
brk=${FAKE_BREAK:-}
url="" method=GET data="" out="" fmt="" jarr="" jarw="" bearer="" idem=""
while [ $# -gt 0 ]; do
  case $1 in
    -X) method=$2; shift ;;
    -d|--data-binary) data=$2; [ "$method" = GET ] && method=POST; shift ;;
    -o) out=$2; shift ;;
    -w) fmt=$2; shift ;;
    -b) jarr=$2; shift ;;
    -c) jarw=$2; shift ;;
    -H) case $2 in Authorization:*) bearer=${2#Authorization: Bearer } ;; Idempotency-Key:*) idem=${2#Idempotency-Key: } ;; esac; shift ;;
    -u) shift ;;
    -*) ;;
    *) url=$1 ;;
  esac
  shift
done
case $data in @*) data=$(cat "${data#@}") ;; esac
path=${url#http://127.0.0.1:}
path=/${path#*/}
role=""
[ -n "$jarr" ] && [ -f "$jarr" ] && role=$(cat "$jarr")
now=$(date +%s)
code=200 body='{}' tm=0.010
[ "$brk" = slow ] && tm=2.500

query() { sed -n "s/.*[?&]$1=\([^&]*\).*/\1/p" <<<"$path"; }

# Parked schema message of the current mode: one line in parked.jsonl.
add_parked() {
  local ch=$1 n reason payload sha b64
  n=$(($(wc -l <"$FAKE/parked.jsonl") + 1))
  case $ch in
    rename) reason="field 'eruption_count_24h' missing"; payload='{"report_id":"R'$n'","eruption_count":3}' ;;
    type) reason="field 'ash_column_height_m' changed from number to string"; payload='{"report_id":"R'$n'","ash_column_height_m":"1200.5"}' ;;
    *) reason="field 'volcano_id' missing"; payload='{"report_id":"R'$n'"}' ;;
  esac
  [ "$brk" = nomarker ] && payload='{"x":1}'
  sha=$(printf '%s' "$payload" | sha256sum | cut -d' ' -f1)
  [ "$brk" = badsha ] && sha=$(printf 'other' | sha256sum | cut -d' ' -f1)
  b64=$(printf '%s' "$payload" | base64 -w0)
  reason="schema_incompatible: $reason"
  [ "$brk" = doubleprefix ] && reason="schema_incompatible: $reason"
  jq -cn --arg id "PVMBG:$ch$n" --arg r "$reason" --arg p "$b64" --arg s "$sha" --argjson rt "$([ "$brk" = retryable ] && echo true || echo false)" \
    '{message_id:$id,source:"PVMBG",received_at:"2026-11-22T05:00:00Z",reason:$r,retryable:$rt,attempts:1,status:"parked",payload:$p,payload_sha256:$s}' >>"$FAKE/parked.jsonl"
}

add_other() { jq -cn --arg id "$1" --arg s "$2" --arg r "$3" '{message_id:$id,source:$s,received_at:"2026-11-22T05:00:00Z",reason:$r,retryable:false,attempts:1,status:"parked"}' >>"$FAKE/other.jsonl"; }

audit_view() { jq -c 'del(.payload,.payload_sha256)' "$FAKE/parked.jsonl" "$FAKE/other.jsonl" 2>/dev/null; }

retrying_rows() {
  local until
  until=$(cat "$FAKE/retrying_until" 2>/dev/null || echo 0)
  if [ "$now" -lt "$until" ] || { [ "$brk" = stuck ] && [ -f "$FAKE/retrying_until" ]; }; then
    jq -cn '{message_id:"PVMBG:retry1",source:"PVMBG",received_at:"2026-11-22T05:00:00Z",reason:"timeout: store",retryable:true,attempts:2,status:"retrying"}'
  fi
}

case "$method $path" in
  "POST /auth/login"*)
    case $(jq -r .email <<<"$data") in operator*) r=op ;; media*) r=media ;; field*) r=field ;; *) r=none ;; esac
    [ -n "$jarw" ] && printf '%s' "$r" >"$jarw" ;;
  "POST /auth/register"*|"POST /accounts"*) code=201 ;;
  "GET /admin/failed-messages"*)
    case $role in
      op) body=$(jq -cs '{data: ., next_cursor: null}' "$FAKE/parked.jsonl") ;;
      media) if [ "$brk" = media200 ]; then body='{"data":[]}'; else code=403; fi ;;
      field) if [ "$brk" = field200 ]; then body='{"data":[]}'; else code=403; fi ;;
      *) if [ "$brk" = anon200 ]; then body='{"data":[]}'; else code=401; fi ;;
    esac ;;
  "GET /internal/audit/unprocessed"*)
    if [ -n "$bearer" ]; then
      src=$(query source)
      { audit_view; retrying_rows; } | jq -cs --arg s "$src" '{items: [.[] | select($s == "" or .source == $s)], next_cursor: null}' >"$FAKE/out.json"
      body=$(cat "$FAKE/out.json")
      [ "$brk" = payload ] && body='{"items":[{"message_id":"m","source":"PVMBG","status":"parked","payload":"x"}],"next_cursor":null}'
    elif [ "$brk" = auditcookie ]; then body='{"items":[]}'
    else code=401; fi ;;
  "GET /internal/audit/events"*)
    if [ -n "$bearer" ]; then body=$(jq -cs '{items: ., next_cursor: null}' "$FAKE/events.jsonl"); else code=401; fi ;;
  "GET /metrics"*) ;;
  "GET /hazards"*limit=1*)
    n=$(($(cat "$FAKE/seq" 2>/dev/null || echo 500) + 1))
    if [ "$brk" = stuck ] && [ -f "$FAKE/malformed" ] && [ "$(cat "$FAKE/malformed")" != off ]; then n=$(cat "$FAKE/seq"); else echo "$n" >"$FAKE/seq"; fi
    body='{"data":[{"source_ref_id":"PVMBG-T-'$n'","attributes":{}}]}' ;;
  "GET /hazards"*)
    if [ -f "$FAKE/extended" ]; then body='{"data":[{"source_ref_id":"R","attributes":{"confidence_level":0.8}}]}'
    else body='{"data":[{"source_ref_id":"R","attributes":{}}]}'; fi ;;
  "POST /admin/schema-version"*)
    if [ -z "$data" ]; then [ "$brk" = noextend ] || : >"$FAKE/extended"
    else
      rm -f "$FAKE/extended"
      if [ "$(jq -r '.version // 0' <<<"$data")" = 2 ]; then [ "$brk" = nonew ] || add_parked "$(jq -r '.change // "all"' <<<"$data")"; fi
    fi ;;
  "POST /admin/malformed"*)
    k=$(jq -r '.kind // "off"' <<<"$data")
    echo "$k" >"$FAKE/malformed"
    case $k in
      truncated) add_other "PVMBG:m1" PVMBG "malformed_json: unexpected end of JSON input" ;;
      deep) add_other "PVMBG:m2" PVMBG "too_deep: nesting too deep" ;;
      utf8) add_other "PVMBG:m3" PVMBG "invalid_utf8: invalid utf-8" ;;
      huge) [ "$brk" = nohuge ] || add_other "PVMBG:m4" PVMBG "too_large: record too large" ;;
    esac ;;
  "POST /field-reports"*)
    if [ -z "$idem" ]; then
      if ! jq -e '.description | type == "string"' <<<"$data" >/dev/null 2>&1 || [ "${#data}" -gt 8192 ] || ! printf '%s' "$data" | iconv -f UTF-8 -t UTF-8 >/dev/null 2>&1; then
        code=400; [ "$brk" = api202 ] && grep -q "bad " <<<"$data" && code=202
      fi
    elif [ "$brk" = never202 ] && [[ "$idem" == *-r9 ]]; then code=503
    else
      code=202
      i=$(jq -r '.description' <<<"$data" | sed -n 's/^report \([0-9]*\) of .*/\1/p')
      if [ -n "$i" ] && ! { [ "$brk" = lost ] && [ "$i" = 7 ]; }; then echo "$i" >>"$FAKE/stored"; fi
    fi ;;
  "POST /api/exchanges/"*)
    id=$(jq -r '.properties.message_id' <<<"$data")
    case $id in
      *-good) ;;
      *-huge) [ "$brk" = parkedfew ] || add_other "FR:$id" FIELD_REPORT "malformed: body" ;;
      *) add_other "FR:$id" FIELD_REPORT "malformed: body" ;;
    esac
    body='{"routed":true}' ;;
  "GET /api/queues/"*)
    if [ "$brk" = queuestuck ]; then body='{"messages":1}'; else body='{"messages":0}'; fi ;;
  "POST /internal/admin/replay"*)
    if [ -z "$bearer" ]; then code=401
    else
      rec=0
      if [ -f "$FAKE/adjusted" ] && [ -f "$FAKE/fixtures" ] && [ "$brk" != noadjust ]; then
        c=$(($(cat "$FAKE/replays" 2>/dev/null || echo 0) + 1)); echo "$c" >"$FAKE/replays"
        run=$(grep -o 'p3-[0-9]*' "$FAKE/fixtures" | head -1)
        mk() { grep -q "\"$1\"" "$FAKE/events.jsonl" && [ -z "${4:-}" ] && return 0; jq -cn --arg r "$1" --arg o "$2" --argjson a "$3" '{source_ref_id:$r,occurred_at:$o,attributes:{schema_adjusted:$a}}' >>"$FAKE/events.jsonl"; }
        nm='["rename:eruption_count"]'; [ "$brk" = nomark ] && nm='[]'
        ord=2026-10-01T10:00:00Z; [ "$brk" = wrongorder ] && ord=2026-10-01T09:00:00Z
        mk "P3R-$run" 2026-10-01T08:00:00Z "$nm"
        mk "P3T-$run" 2026-10-01T08:10:00Z '["type:ash_column_height_m"]'
        if ! { [ "$brk" = twoplay ] && [ "$c" = 1 ]; }; then
          mk "P3M-$run" 2026-10-01T08:20:00Z '["default:volcano_id"]'
          mk "P3O-$run" "$ord" '["rename:eruption_count"]'
          [ "$brk" = dupes ] && mk "P3R-$run" 2026-10-01T08:00:00Z "$nm" again
        fi
        rec=4
      fi
      body='{"source":"PVMBG","scanned":8,"recovered":'$rec',"superseded":0,"still_parked":0,"skipped_oversize":0,"errors":[]}'
    fi ;;
esac
[ -n "$fmt" ] && { f=${fmt//%\{http_code\}/$code}; printf '%s' "${f//%\{time_total\}/$tm}"; }
[ -z "$out" ] && printf '%s' "$body"
exit 0
EOF

cat >"$T/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
# Fake kubectl: the same state files as the fake curl.
brk=${FAKE_BREAK:-}
args="$*"
now=$(date +%s)
case "$args" in
  version*) [ "$brk" = nocluster ] && exit 1; exit 0 ;;
  *"get pods --no-headers"*)
    if [ "$brk" = notready ]; then echo "aggregator-1 0/1 Pending 0 1m"; else echo "aggregator-1 1/1 Running 0 1m"; fi ;;
  *"get pods -o json"*) echo '{"items":[{"metadata":{"name":"aggregator-1"},"status":{"containerStatuses":[{"restartCount":0}]}}]}' ;;
  *"get pods -l role=aggregator -o name"*) printf 'pod/aggregator-1\npod/aggregator-2\n' ;;
  *"get pods -l app=store -o name"*) printf 'pod/store-1\npod/store-2\npod/store-3\n' ;;
  *"get pods -o name"*) printf 'pod/aggregator-1\npod/audit-logger-1\npod/notifier-1\npod/dashboard-updater-1\n' ;;
  *"get secret"*) printf 'dG9r' ;;
  *"get configmap"*) printf 'operator@bnpb.example' ;;
  *"get deployment field-report-processor"*) printf '2' ;;
  *"get daemonset tester-agent"*) [ "$brk" = noagent ] && exit 1; exit 0 ;;
  *"exec daemonset/tester-agent"*)
    echo "$(($(cat "$FAKE/agent_runs" 2>/dev/null || echo 0) + 1))" >"$FAKE/agent_runs"
    last=$(grep -o '[0-9]*$' "$FAKE/events.jsonl" 2>/dev/null | tail -1)
    jq -r '.source_ref_id' "$FAKE/events.jsonl" | tail -1 >/dev/null
    base=$(jq -r 'select(.source_ref_id | startswith("PVMBG-S")) | .source_ref_id' "$FAKE/events.jsonl" | sed 's/^.*-//' | sort -n | tail -1)
    nxt=$((10#$base + 1)); [ "$brk" = hole ] && nxt=$((nxt + 1))
    for n in "$nxt" $((nxt + 1)); do jq -cn --arg r "PVMBG-S-$(printf '%06d' "$n")" '{source_ref_id:$r,occurred_at:"2026-11-22T05:00:00Z",attributes:{}}' >>"$FAKE/events.jsonl"; done
    [ "$brk" = dup ] && jq -cn --arg r "PVMBG-S-$(printf '%06d' "$nxt")" '{source_ref_id:$r,occurred_at:"2026-11-22T05:00:00Z",attributes:{}}' >>"$FAKE/events.jsonl"
    [ "$brk" = vanish ] && : >"$FAKE/parked.jsonl"
    [ "$brk" = stuck ] && echo "$((now + 1))" >"$FAKE/retrying_until"
    exit 0 ;;
  *"-n tester-agent logs"*) n=$(cat "$FAKE/agent_runs" 2>/dev/null || echo 0); for _ in $(seq 1 "$n"); do echo "[L] pemulihan akhir selesai; keadaan bersih"; done ;;
  *"LOCK TABLE hazard_events"*) [ "$brk" = noretrying ] || echo "$((now + 4))" >"$FAKE/retrying_until"; exit 0 ;;
  *"exec -i "*) cat >"$FAKE/sql.in"; grep -o 'P3[RTMO]-p3-[0-9]*' "$FAKE/sql.in" | sort -u >"$FAKE/fixtures"; exit 0 ;;
  *"SELECT (regexp_match"*) case "$args" in *store-1*) cat "$FAKE/stored" 2>/dev/null ;; esac ;;
  *"DELETE FROM failed_messages"*) exit 0 ;;
  *"patch configmap"*) : >"$FAKE/patched"; exit 0 ;;
  *"rollout restart"*) [ -f "$FAKE/patched" ] && : >"$FAKE/adjusted"; exit 0 ;;
  *"rollout status"*) exit 0 ;;
  *"logs -l role=aggregator"*)
    if [ -f "$FAKE/adjusted" ] && [ "$brk" != noknow ]; then echo "PVMBG schema adjustments active: 1 rename, 1 type, 1 default"; fi
    echo '{"correlation_id":"replay-pvmbg-1700000001","message":"replay"}' ;;
  *"logs "*"--all-containers"*)
    case "$args" in
      *aggregator-1*) echo 'replay-pvmbg-1700000001' ;;
      *audit-logger-1*|*notifier-1*) [ "$brk" = onepod ] || echo 'replay-pvmbg-1700000001' ;;
    esac ;;
  *"get --raw"*) echo 'aggregator_field_reports_total{result="new"} 3' ;;
  *port-forward*) exec sleep 600 ;;
  *) exit 0 ;;
esac
EOF
chmod +x "$T/bin/curl" "$T/bin/kubectl"

# Sets OUT and RC. The first argument names how the fake cluster is broken, the rest is extra environment for the script.
run() {
  local brk=$1
  shift
  rm -rf "$T/state"
  mkdir -p "$T/state"
  : >"$T/state/other.jsonl"
  : >"$T/state/parked.jsonl"
  jq -cn '{message_id:"PVMBG:base1",source:"PVMBG",received_at:"2026-11-22T05:00:00Z",reason:"schema_incompatible: field ''x'' missing",retryable:false,attempts:1,status:"parked",payload:"e30=",payload_sha256:"x"}' >>"$T/state/parked.jsonl"
  jq -cn '{message_id:"PVMBG:base2",source:"PVMBG",received_at:"2026-11-22T05:00:00Z",reason:"schema_incompatible: field ''y'' missing",retryable:false,attempts:1,status:"parked",payload:"e30=",payload_sha256:"x"}' >>"$T/state/parked.jsonl"
  for n in 100 101 102 103; do jq -cn --arg r "PVMBG-S-000$n" '{source_ref_id:$r,occurred_at:"2026-11-22T04:00:00Z",attributes:{}}' >>"$T/state/events.jsonl"; done
  OUT=$(env PATH="$T/bin:$PATH" FAKE="$T/state" FAKE_BREAK="$brk" WAIT=2 TICK=1 SAMPLE=1 SETTLE=1 SETTLE_SHORT=1 WAIT_FLOW=3 RETRIES=2 LOCK_SECONDS=3 "$@" bash "$here/problem3-k8s.sh" 2>&1)
  RC=$?
  if [ -n "${SHOW:-}" ]; then grep -E '^(FAIL|NOT RUN):' <<<"$OUT" | cut -c1-170 | sed 's/^/     | /'; fi
}

# Runs one scenario on a cluster broken one way: the script exits 1 and prints COUNT FAIL lines (default 1), one with the text.
one_break() {
  local scen=$1 brk=$2 text=$3
  shift 3
  run "$brk" ONLY="$scen" "$@"
  ok "$brk exits 1" "$RC" 1
  has "$brk is a FAIL on: $text" "$OUT" "FAIL: $text"
  ok "$brk fails ${COUNT:-1} check(s), no more" "$(grep -c '^FAIL:' <<<"$OUT")" "${COUNT:-1}"
}

echo "== healthy cluster, every scenario"
run none ONLY="1 2 3 4 5 6 7" N=60
ok "exit code 0" "$RC" 0
hasnt "no FAIL line" "$OUT" '^FAIL:'
hasnt "nothing was not run" "$OUT" '^NOT RUN:'
for s in "S1 a volcanic record" "S2 rename reason has the Appendix A form" "S2 missing stored payload matches" "S3 retrying rows were seen" "S4 Media is" "S5 queue drained" "S5 source huge: a parked" "S6 order" "S6 correlation id" "S7 distinct stored reports"; do
  has "healthy: $s" "$OUT" "PASS: $s|PASS: ${s% *}"
done
has "the exit trap puts the schema back" "$OUT" 'trap: PVMBG schema back to legacy'
has "the exit trap removes the scenario 6 test messages" "$OUT" 'trap: the parked test messages of scenario 6 removed'
ok "the mock is left on the legacy schema" "$([ -f "$T/state/extended" ] && echo extended || echo legacy)" legacy

echo "== scenario 1"
one_break 1 noextend 'S1 no record with confidence_level'

echo "== scenario 2"
COUNT=3 one_break 2 doubleprefix 'S2 rename reason is not'
COUNT=3 one_break 2 retryable 'S2 rename retryable: got true, want false'
COUNT=3 one_break 2 badsha 'S2 rename stored payload matches its sha256'
COUNT=3 one_break 2 nomarker 'S2 rename the stored payload lost the original field'
COUNT=3 one_break 2 nonew 'S2 rename: no new parked PVMBG message'

echo "== scenario 3"
one_break 3 stuck 'S3 no message is left in the retrying state'
one_break 3 hole 'S3 holes in the PVMBG sequence'
one_break 3 dup 'S3 repeated source_ref_id'
one_break 3 vanish "S3 parked messages that disappeared"
one_break 3 noretrying 'S3 neither kill-store nor a'
run noagent ONLY=3
ok "no Tester Agent exits 0" "$RC" 0
has "no Tester Agent says NOT RUN" "$OUT" 'NOT RUN: S3 the Tester Agent is not installed'
hasnt "no Tester Agent prints no FAIL" "$OUT" '^FAIL:'

echo "== scenario 4"
for pair in "media200:S4 Media" "field200:S4 Tim Lapangan" "anon200:S4 no cookie" "auditcookie:S4 audit route with a login cookie" "payload:S4 the audit items carry a payload"; do
  one_break 4 "${pair%%:*}" "${pair#*:}"
done

echo "== scenario 5"
one_break 5 api202 'S5 API utf8 body answered 400'
one_break 5 queuestuck 'S5 queue still holds'
one_break 5 parkedfew 'S5 queue: only 3 of 4 bad messages parked'
one_break 5 nohuge 'S5 source huge: nothing parked with the code too_large'
COUNT=4 one_break 5 stuck 'S5 source (truncated|deep|utf8|huge): no new normal record'

echo "== scenario 6"
COUNT=6 one_break 6 noadjust 'S6 test reports stored after the replays'
one_break 6 twoplay 'S6 replays needed to recover them all'
one_break 6 wrongorder 'S6 order: the stored version has occurred_at 2026-10-01T09:00:00Z'
run nomark ONLY=6
has "nomark is a FAIL on the mark" "$OUT" 'FAIL: S6 P3R-p3-[0-9]+ was recovered without the mark rename:eruption_count'
one_break 6 dupes 'S6 repeated source_ref_id among the audit events'
one_break 6 onepod 'S6 correlation id .* it stops in one pod'
run noknow ONLY=6
ok "an Aggregator without the setting exits 0" "$RC" 0
has "an Aggregator without the setting says NOT RUN" "$OUT" 'NOT RUN: S6 the Aggregator on the cluster does not know PVMBG_SCHEMA_ADJUST'
hasnt "an Aggregator without the setting prints no FAIL" "$OUT" '^FAIL:'

echo "== scenario 7"
one_break 7 slow 'S7 answers slower than' N=60
COUNT=2 one_break 7 lost 'S7 acknowledged reports missing from the store' N=60
COUNT=2 one_break 7 never202 'S7 reports never acknowledged with 202 after retries' N=60

echo "== the script cannot run: never a verdict"
run nocluster ONLY=4
ok "no cluster exits 2" "$RC" 2
has "no cluster says NOT RUN" "$OUT" 'NOT RUN'
hasnt "no cluster prints no FAIL" "$OUT" '^FAIL:'
run notready ONLY=4
ok "pods not ready exits 2" "$RC" 2
hasnt "pods not ready prints no FAIL" "$OUT" '^FAIL:'

echo "== dry run"
run none DRY_RUN=1
ok "dry run exits 0" "$RC" 0
has "dry run prints commands" "$OUT" '\[dry-run\]'
hasnt "dry run checks nothing" "$OUT" '^(PASS|FAIL):'

echo
if [ "$FAILS" -eq 0 ]; then echo "all self tests passed"; else echo "$FAILS self tests failed"; fi
[ "$FAILS" -eq 0 ]
