#!/usr/bin/env bash
# Offline self test for problem1-k8s.sh (#298). A fake kubectl and curl on PATH play a cluster that keeps its state in files:
# first a healthy one, then one that is broken in exactly one way. The script must print FAIL for that check only, NOT RUN
# when it cannot run, and exit 2 when the cluster cannot be reached. Needs bash, jq and sha256sum.
#
#   tests/scenarios/problem1-k8s_test.sh
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
# Fake curl: answers from the files in $FAKE. $FAKE_BREAK names how the cluster is broken.
brk=${FAKE_BREAK:-}
url="" method=GET data="" out="" fmt="" jarw="" bearer=""
while [ $# -gt 0 ]; do
  case $1 in
    -X) method=$2; shift ;;
    -d) data=$2; [ "$method" = GET ] && method=POST; shift ;;
    -o) out=$2; shift ;;
    -w) fmt=$2; shift ;;
    -b) shift ;;
    -c) jarw=$2; shift ;;
    -m) shift ;;
    -H) case $2 in Authorization:*) bearer=${2#Authorization: Bearer } ;; esac; shift ;;
    -*) ;;
    *) url=$1 ;;
  esac
  shift
done
path=${url#http://127.0.0.1:}
path=/${path#*/}
code=200 body='{}'
query() { sed -n "s/.*[?&]$1=\([^&]*\).*/\1/p" <<<"$path"; }
events_json() { jq -Rcs --arg s "$1" '{items: [split("\n")[] | select(length > 0) | split("\t") | {source: .[0], source_ref_id: .[1], hazard_id: .[2]} | select($s == "" or .source == $s)], next_cursor: null, partial: false}' "$FAKE/events.tsv"; }
case "$method $path" in
  "POST /auth/login"*) [ -n "$jarw" ] && : >"$jarw" ;;
  "GET /hazards"*)
    if [ "$brk" = fivexx ] && [ "$jarw" != "${jarw%probe.jar}" ]; then code=500; body='{}'
    else
      n=$(wc -l <"$FAKE/events.tsv")
      body=$(jq -cn --argjson n "$n" '{data: [range(0; 5) | {hazard_id: ("h" + (. | tostring))}], page: {next_cursor: null, has_more: false}, partial: false}')
      [ "$brk" = dupids ] && body='{"data":[{"hazard_id":"h1"},{"hazard_id":"h1"}],"page":{"next_cursor":null,"has_more":false},"partial":false}'
      case $path in *limit=50*) [ "$brk" = nowrites ] || printf 'BMKG\tW-%s\tW%s\n' "$(date +%s%N)" "$(date +%s%N)" >>"$FAKE/events.tsv" ;; esac
    fi ;;
  "GET /internal/audit/events"*)
    [ -n "$bearer" ] || code=401
    src=$(query source)
    body=$(events_json "$src")
    case $path in *limit=50*) [ "$brk" = nowrites ] || printf 'PVMBG\tW-%s\tW%s\n' "$(date +%s%N)" "$(date +%s%N)" >>"$FAKE/events.tsv" ;; esac ;;
  "GET /internal/audit/checksum"*)
    src=$(query source)
    sum=$(awk -F'\t' -v s="$src" '$1 == s { print $2 }' "$FAKE/events.tsv" | LC_ALL=C sort | { printf '%s' "$(cat)"; } | sha256sum | cut -d' ' -f1)
    [ "$brk" = badchecksum ] && sum=0000
    body=$(jq -cn --arg s "$src" --arg c "$sum" '{source: $s, checksum: $c, partial: false}') ;;
  "GET /internal/audit/location"*)
    id=$(query hazard_id)
    case $brk in
      onecopy) body='{"locations":[{"node":"node-a","role":"primary"}]}' ;;
      emptynode) body='{"locations":[{"node":"node-a","role":"primary"},{"node":"node-c","role":"replica"}]}' ;;
      *) h=$(printf '%s' "$id" | cksum | cut -d' ' -f1)
         if [ $((h % 2)) = 0 ]; then body='{"locations":[{"node":"node-a","role":"primary"},{"node":"node-b","role":"replica"}]}'
         else body='{"locations":[{"node":"node-b","role":"primary"},{"node":"node-c","role":"replica"}]}'; fi ;;
    esac ;;
esac
[ -n "$fmt" ] && { f=${fmt//%\{http_code\}/$code}; printf '%b' "${f//%\{time_total\}/0.010}"; }
[ -z "$out" ] && printf '%s' "$body"
exit 0
EOF

cat >"$T/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
# Fake kubectl: the store DaemonSet limit decides which nodes have a store pod.
brk=${FAKE_BREAK:-}
args="$*"
case "$args" in
  version*) [ "$brk" = nocluster ] && exit 1; exit 0 ;;
  *"get pods --no-headers"*) if [ "$brk" = notready ]; then echo "aggregator-1 0/1 Pending 0 1m"; else echo "aggregator-1 1/1 Running 0 1m"; fi ;;
  *"get pods -l app=store --no-headers"*) for n in $(cat "$FAKE/stores"); do echo "store-$n 1/1 Running 0 1m"; done ;;
  *"get pods -l app=store -o jsonpath"*) cat "$FAKE/stores" | tr ' ' '\n' ;;
  *"get nodes -l"*) printf 'node-b\nnode-c\n' ;;
  *"get nodes -o jsonpath"*) printf 'node-a node-b node-c' ;;
  *"get secret"*) printf 'dG9r' ;;
  *"get configmap"*) printf 'operator@bnpb.example' ;;
  *"get ds store -o jsonpath"*) printf 'postgres:16-alpine' ;;
  *"get pod "*"jsonpath"*) printf 'Succeeded' ;;
  *"patch ds store --type=merge"*) printf 'node-a' >"$FAKE/stores"; exit 0 ;;
  *"patch ds store --type=json"*)
    printf 'node-a node-b node-c' >"$FAKE/stores"
    [ "$brk" = nowrites ] || for n in 1 2 3 4 5; do printf 'BMKG\tN-%s-%s\tn%s%s\n' "$n" "$(date +%s%N)" "$n" "$(date +%s%N)" >>"$FAKE/events.tsv"; done
    [ "$brk" = lost ] && { grep -v -e '^BMKG.B-1.b1$' -e '^PVMBG.P-1.p1$' "$FAKE/events.tsv" >"$FAKE/e.tmp"; mv "$FAKE/e.tmp" "$FAKE/events.tsv"; }
    exit 0 ;;
  *"logs p1-wipe"*) echo "left=0" ;;
  *port-forward*) exec sleep 600 ;;
  *) exit 0 ;;
esac
EOF
chmod +x "$T/bin/curl" "$T/bin/kubectl"

# Sets OUT and RC. The first argument names how the fake cluster is broken, the rest is extra environment for the script.
run() {
  local brk=$1 n
  shift
  rm -rf "$T/state"
  mkdir -p "$T/state"
  printf 'node-a node-b node-c' >"$T/state/stores"
  for n in 1 2 3 4 5 6; do printf 'BMKG\tB-%s\tb%s\n' "$n" "$n" >>"$T/state/events.tsv"; printf 'PVMBG\tP-%s\tp%s\n' "$n" "$n" >>"$T/state/events.tsv"; done
  OUT=$(env PATH="$T/bin:$PATH" FAKE="$T/state" FAKE_BREAK="$brk" TICK=1 RECOVER_WAIT=4 SAMPLE=6 "$@" bash "$here/problem1-k8s.sh" 2>&1)
  RC=$?
  if [ -n "${SHOW:-}" ]; then grep -E '^(FAIL|NOT RUN):' <<<"$OUT" | cut -c1-170 | sed 's/^/     | /'; fi
}

# Runs the scenarios on a cluster broken one way: the script exits 1 and prints COUNT FAIL lines (default 1), one with the text.
one_break() {
  local scen=$1 brk=$2 text=$3
  run "$brk" ONLY="$scen"
  ok "$brk exits 1" "$RC" 1
  has "$brk is a FAIL on: $text" "$OUT" "FAIL: $text"
  ok "$brk fails ${COUNT:-1} check(s), no more" "$(grep -c '^FAIL:' <<<"$OUT")" "${COUNT:-1}"
}

echo "== healthy cluster, every scenario"
run none ONLY="A B E"
if [ -n "${SHOWALL:-}" ]; then echo "$OUT"; exit 0; fi
ok "exit code 0" "$RC" 0
hasnt "no FAIL line" "$OUT" '^FAIL:'
has "A passes" "$OUT" 'PASS: A repeated hazard ids in the audit events'
has "B finds no event missing" "$OUT" 'PASS: B no audit event from before the loss is missing'
has "E finds no event missing" "$OUT" 'PASS: E no audit event from before the loss is missing'
has "B recovers" "$OUT" 'PASS: B recovery in'
has "the probe saw no 5xx" "$OUT" 'PASS: B probe answers that were a 5xx or a hang'
has "the returned node holds copies" "$OUT" 'PASS: B the returned node holds copies again'
has "the exit trap restored the DaemonSet" "$OUT" 'PASS [0-9]+, FAIL 0'
ok "all three nodes have a store again" "$(cat "$T/state/stores")" "node-a node-b node-c"

echo "== scenario A"
one_break A dupids 'A repeated ids across the pages of the hazard list'

echo "== the first loss, B"
one_break B lost 'B no audit event from before the loss is missing'
one_break B fivexx 'B probe answers that were a 5xx or a hang'
one_break B nowrites 'B writes continued'
COUNT=2 one_break B badchecksum 'B audit checksum of BMKG does not match the listed ids'
COUNT=3 one_break B onecopy 'B recovery did not finish'
COUNT=2 one_break B emptynode 'B recovery did not finish'

echo "== the second loss, E"
one_break E lost 'E no audit event from before the loss is missing'

echo "== the script cannot run: never a verdict"
run nocluster ONLY=A
ok "no cluster exits 2" "$RC" 2
has "no cluster says NOT RUN" "$OUT" 'NOT RUN'
hasnt "no cluster prints no FAIL" "$OUT" '^FAIL:'
run notready ONLY=A
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
