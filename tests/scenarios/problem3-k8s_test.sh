#!/usr/bin/env bash
# Offline self test for problem3-k8s.sh (#307). A fake kubectl and curl on PATH play a healthy cluster, then a cluster that is
# broken in exactly one way. The script must print FAIL for that check only, and exit 2 when the cluster cannot be reached.
# Covers scenarios 1 and 4, the dry run, the exit codes and the exit trap. Needs bash and jq.
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
url="" method=GET data="" out="" fmt="" jarr="" jarw="" bearer=""
while [ $# -gt 0 ]; do
  case $1 in
    -X) method=$2; shift ;;
    -d|--data-binary) data=$2; [ "$method" = GET ] && method=POST; shift ;;
    -o) out=$2; shift ;;
    -w) fmt=$2; shift ;;
    -b) jarr=$2; shift ;;
    -c) jarw=$2; shift ;;
    -H) case $2 in Authorization:*) bearer=${2#Authorization: Bearer } ;; esac; shift ;;
    -u) shift ;;
    -*) ;;
    *) url=$1 ;;
  esac
  shift
done
path=${url#http://127.0.0.1:}
path=/${path#*/}
role=""
[ -n "$jarr" ] && [ -f "$jarr" ] && role=$(cat "$jarr")
code=200 body='{}'
case "$method $path" in
  "POST /auth/login"*)
    case $(jq -r .email <<<"$data") in operator*) r=op ;; media*) r=media ;; field*) r=field ;; *) r=none ;; esac
    [ -n "$jarw" ] && printf '%s' "$r" >"$jarw" ;;
  "POST /auth/register"*|"POST /accounts"*) code=201 ;;
  "GET /admin/failed-messages"*)
    case $role in
      op) body='{"data":[],"next_cursor":null}' ;;
      media) if [ "${FAKE_BREAK:-}" = media200 ]; then body='{"data":[]}'; else code=403; fi ;;
      field) if [ "${FAKE_BREAK:-}" = field200 ]; then body='{"data":[]}'; else code=403; fi ;;
      *) if [ "${FAKE_BREAK:-}" = anon200 ]; then body='{"data":[]}'; else code=401; fi ;;
    esac ;;
  "GET /internal/audit/unprocessed"*)
    if [ -n "$bearer" ]; then
      body='{"items":[],"next_cursor":null}'
      [ "${FAKE_BREAK:-}" = payload ] && body='{"items":[{"message_id":"m","source":"PVMBG","status":"parked","payload":"x"}],"next_cursor":null}'
    elif [ "${FAKE_BREAK:-}" = auditcookie ]; then body='{"items":[]}'
    else code=401; fi ;;
  "GET /hazards"*)
    if [ -f "$FAKE/extended" ]; then body='{"data":[{"source_ref_id":"R","attributes":{"confidence_level":0.8}}]}'
    else body='{"data":[{"source_ref_id":"R","attributes":{}}]}'; fi ;;
  "POST /admin/schema-version"*)
    if [ -z "$data" ]; then [ "${FAKE_BREAK:-}" = noextend ] || : >"$FAKE/extended"; else rm -f "$FAKE/extended"; fi ;;
esac
[ -n "$fmt" ] && printf '%s' "${fmt//%\{http_code\}/$code}"
[ -z "$out" ] && printf '%s' "$body"
exit 0
EOF

cat >"$T/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  version*) [ "${FAKE_BREAK:-}" = nocluster ] && exit 1; exit 0 ;;
  *"get pods --no-headers"*)
    if [ "${FAKE_BREAK:-}" = notready ]; then echo "aggregator-1 0/1 Pending 0 1m"; else echo "aggregator-1 1/1 Running 0 1m"; fi ;;
  *"get pods -o json"*) echo '{"items":[{"metadata":{"name":"aggregator-1"},"status":{"containerStatuses":[{"restartCount":0}]}}]}' ;;
  *"get secret"*) printf 'dG9r' ;;
  *"get configmap"*) printf 'operator@bnpb.example' ;;
  *port-forward*) exec sleep 600 ;;
  *) exit 0 ;;
esac
EOF
chmod +x "$T/bin/curl" "$T/bin/kubectl"

# Sets OUT and RC. The first argument names how the fake cluster is broken, the rest is extra environment for the script.
run() {
  local brk=$1
  shift
  rm -f "$T/state/extended"
  OUT=$(env PATH="$T/bin:$PATH" FAKE="$T/state" FAKE_BREAK="$brk" WAIT=2 "$@" bash "$here/problem3-k8s.sh" 2>&1)
  RC=$?
}

echo "== healthy cluster"
run none ONLY="1 4"
ok "exit code 0" "$RC" 0
hasnt "no FAIL line" "$OUT" '^FAIL:'
has "S1 sees the extra field" "$OUT" 'PASS: S1 a volcanic record with attributes.confidence_level'
has "S4 Media is 403" "$OUT" 'PASS: S4 Media \(403\)'
has "S4 Operator is 200" "$OUT" 'PASS: S4 Operator \(200\)'
has "the exit trap puts the schema back" "$OUT" 'trap: PVMBG schema back to legacy'
ok "the mock is left on the legacy schema" "$([ -f "$T/state/extended" ] && echo extended || echo legacy)" legacy

echo "== broken in one way, scenario 4"
for pair in "media200:S4 Media" "field200:S4 Tim Lapangan" "anon200:S4 no cookie" "auditcookie:S4 audit route with a login cookie" "payload:S4 the audit items carry a payload"; do
  brk=${pair%%:*} what=${pair#*:}
  run "$brk" ONLY=4
  ok "$brk exits 1" "$RC" 1
  has "$brk is a FAIL on $what" "$OUT" "FAIL: $what"
  ok "$brk fails exactly one check" "$(grep -c '^FAIL:' <<<"$OUT")" 1
done

echo "== broken in one way, scenario 1"
run noextend ONLY=1
ok "noextend exits 1" "$RC" 1
has "noextend is a FAIL on the extra field" "$OUT" 'FAIL: S1 no record with confidence_level'
ok "noextend fails exactly one check" "$(grep -c '^FAIL:' <<<"$OUT")" 1

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
