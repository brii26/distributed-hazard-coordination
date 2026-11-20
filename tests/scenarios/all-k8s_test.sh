#!/usr/bin/env bash
# Offline self test for all-k8s.sh (#328). It builds a fake repository with fake child scripts (PASS, FAIL, hang, exit 2, missing),
# a fake kubectl that keeps its state in files, and a fake Audit API. It touches no cluster and no VM.
#
#   tests/scenarios/all-k8s_test.sh
# Needs bash, jq, curl and python3.
# shellcheck disable=SC2016,SC2012  # fake scripts are written with single quotes on purpose, ls output is plain names here
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
T=$(mktemp -d)
trap 'kill "${AUDIT_PID:-0}" 2>/dev/null; rm -rf "$T"' EXIT
FAILS=0
ok() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1 (got: $2, want: $3)"; FAILS=$((FAILS + 1)); fi; }
has() { if grep -qE -- "$3" <<<"$2"; then echo "ok   $1"; else echo "FAIL $1 (no match for: $3)"; FAILS=$((FAILS + 1)); fi; }
hasnt() { if grep -qE -- "$3" <<<"$2"; then echo "FAIL $1 (unexpected: $3)"; FAILS=$((FAILS + 1)); else echo "ok   $1"; fi; }

# ---- fake repository ----
R=$T/repo; S=$T/state
mkdir -p "$R/tests/scenarios" "$R/tests/load" "$R/deploy" "$S"
: >"$R/deploy/aggregator-hpa.yaml"
child() { printf '#!/usr/bin/env bash\n%s\n' "$2" >"$R/$1"; }
child tests/scenarios/audit-checksum.sh 'echo "audit url $AUDIT_URL"; echo PASS'
# leaves a patched ConfigMap and a tester-run behind, like a run that was killed half way
child tests/scenarios/rolling-update.sh 'echo "RESULT: PASS"; echo "{\"ACCESS_TOKEN_TTL_S\":\"3600\",\"RATE_LIMIT_RPS\":\"1000\"}" >"$FAKE_STATE/cm.json"; touch "$FAKE_STATE/tester_run"'
# fails, and leaves a lost lease behind when asked
child tests/scenarios/autoscale.sh 'echo "FAIL: replicas did not return"; [ -f "$FAKE_STATE/autoscale_breaks" ] && touch "$FAKE_STATE/leaselost"; exit 1'
child tests/scenarios/problem2-k8s.sh 'echo hanging; sleep 60'
child tests/load/p2-k8s.sh 'echo "BASE_URL missing"; exit 2'
child tests/load/p2-ratio.sh 'echo PASS; rm -f "$FAKE_STATE/hpa"'
child tests/scenarios/gameday.sh 'echo "gameday STRICT=$STRICT"; echo PASS'

# ---- fake kubectl ----
cat >"$T/kubectl" <<'KEOF'
#!/usr/bin/env bash
S=$FAKE_STATE
args=("$@"); [ "${args[0]:-}" = -n ] && args=("${args[@]:2}")
set -- "${args[@]}"
echo "kubectl $*" >>"$S/calls.log"; [ -n "${FAKE_TRACE:-}" ] && echo "kubectl $*" >&2
case "$*" in
  "get nodes -o json") echo '{"items":[{"metadata":{"name":"node-a"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}' ;;
  "get pods -o json")
    ready=true; [ -f "$S/podsdown" ] && ready=false
    echo "{\"items\":[{\"metadata\":{\"name\":\"coordination-db-0\"},\"status\":{\"phase\":\"Running\",\"containerStatuses\":[{\"ready\":$ready}]}},{\"metadata\":{\"name\":\"aggregator-1\"},\"status\":{\"phase\":\"Running\",\"containerStatuses\":[{\"ready\":true}]}}]}" ;;
  exec\ coordination-db-0*) if [ -f "$S/leaselost" ]; then printf 'BMKG,3,1\nPVMBG,3,0\nSENSOR_PANTAI,3,1\n'; else printf 'BMKG,3,1\nPVMBG,3,1\nSENSOR_PANTAI,3,1\n'; fi ;;
  "get configmap bnpb-config -o json") jq -c '{data: .}' "$S/cm.json" ;;
  "patch configmap bnpb-config --type merge -p "*) jq -s '.[0] * .[1].data | with_entries(select(.value != null))' "$S/cm.json" <(printf "%s" "${@: -1}") >"$S/cm.new" && mv "$S/cm.new" "$S/cm.json" ;;
  rollout*) echo "$*" >>"$S/rollouts.log" ;;
  "get configmap tester-run -o name") [ -f "$S/tester_run" ] && echo configmap/tester-run || exit 1 ;;
  "delete configmap tester-run") rm -f "$S/tester_run" ;;
  "get hpa aggregator -o name") [ -f "$S/hpa" ] && echo hpa/aggregator || exit 1 ;;
  apply\ -f*) touch "$S/hpa"; echo "$*" >>"$S/applies.log" ;;
  "get secret audit-token -o jsonpath={.data.token}") printf secret-token | base64 ;;
  *) echo "fake kubectl: unhandled $*" >&2; exit 1 ;;
esac
KEOF
chmod +x "$T/kubectl"

# ---- fake Audit API ----
cat >"$T/audit.py" <<'PEOF'
import http.server, json, os, sys
S = os.environ["FAKE_STATE"]
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, obj):
        b = json.dumps(obj).encode(); self.send_response(code); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        if self.headers.get("Authorization") != "Bearer secret-token": return self.send(401, {})
        p = self.path
        if p.startswith("/internal/audit/events"): return self.send(200, {"items": [{"hazard_id": "11111111-1111-1111-1111-111111111111"}]})
        if p.startswith("/internal/audit/checksum"):
            if os.path.exists(S + "/checksum_broken"): return self.send(500, {})
            return self.send(200, {"count": 1, "checksum": "x"})
        return self.send(404, {})
srv = http.server.HTTPServer(("127.0.0.1", 0), H)
open(S + "/audit.port", "w").write(str(srv.server_port)); srv.serve_forever()
PEOF
export FAKE_STATE=$S
python3 "$T/audit.py" & AUDIT_PID=$!
for _ in $(seq 50); do [ -s "$S/audit.port" ] && break; sleep 0.1; done

reset() {
  rm -f "$S"/leaselost "$S"/podsdown "$S"/checksum_broken "$S"/autoscale_breaks "$S"/calls.log "$S"/rollouts.log "$S"/applies.log
  echo '{"ACCESS_TOKEN_TTL_S":"60","RATE_LIMIT_RPS":"50"}' >"$S/cm.json"; rm -f "$S/tester_run"; touch "$S/hpa"; rm -rf "$T/results"
}
run() { # args...; sets OUT and RC
  OUT=$(SCRIPT_ROOT=$R KUBECTL=$T/kubectl RESULTS_DIR=$T/results AUDIT_URL=http://127.0.0.1:$(cat "$S/audit.port") AUDIT_TOKEN='' \
    HEALTH_WAIT=0 HEALTH_POLL=0 CONFIRM_SECONDS=0 TIMEOUT_P2_LEASES=2 \
    EMAIL=e PASSWORD=p API_URL=u BASE_URL=b GAMEDAY_EMAIL=a GAMEDAY_PASSWORD=a GAMEDAY_MEDIA_EMAIL=b GAMEDAY_MEDIA_PASSWORD=b GAMEDAY_FIELD_EMAIL=c GAMEDAY_FIELD_PASSWORD=c \
    bash "$here/all-k8s.sh" "$@" 2>&1); RC=$?
}
row() { grep -E "^[0-9]+ +$1 " <<<"$OUT"; }

echo "== syntax and --list"
bash -n "$here/all-k8s.sh"; ok "bash -n" $? 0
SCRIPT_ROOT=$R bash "$here/all-k8s.sh" --list >"$T/list.txt"; ok "--list exit" $? 0
has "--list shows 10 scenarios in order" "$(grep -cE '^[0-9]+ ' "$T/list.txt")" '^10$'
has "--list marks a missing slot" "$(cat "$T/list.txt")" 'p3-e2e +#307 +safe +[0-9]+s +missing'

echo "== DRY_RUN=1 runs nothing and writes nothing"
reset
OUT=$(DRY_RUN=1 SCRIPT_ROOT=$R KUBECTL=/nonexistent RESULTS_DIR=$T/results EMAIL=e PASSWORD=p AUDIT_TOKEN=x GAMEDAY_EMAIL=a GAMEDAY_PASSWORD=a GAMEDAY_MEDIA_EMAIL=b GAMEDAY_MEDIA_PASSWORD=b GAMEDAY_FIELD_EMAIL=c GAMEDAY_FIELD_PASSWORD=c bash "$here/all-k8s.sh" --include-destructive 2>&1); RC=$?
ok "dry run exit" $RC 0
has "dry run prints the plan" "$OUT" 'PLAN: timeout 180s'
has "dry run prints what gameday destroys" "$OUT" 'would destroy: wipe-node deletes'
has "dry run overall" "$OUT" 'Overall: PLAN'
ok "dry run created no results" "$([ -e "$T/results" ] && echo yes || echo no)" no

echo "== full run: PASS, FAIL, TIMEOUT, SKIP (exit 2 child, missing, destructive)"
reset
run
ok "exit code 1 when something failed" $RC 1
has "manual steps at the top" "$(head -8 <<<"$OUT")" 'Manual steps'
has "audit-checksum PASS and audit PASS" "$(row audit-checksum)" 'PASS .* PASS'
has "child got AUDIT_URL" "$OUT" 'audit url http://127.0.0.1:'
has "rolling-update PASS" "$(row rolling-update)" ' PASS '
has "autoscale FAIL with its first FAIL line" "$(row autoscale)" 'FAIL +[0-9]+s .*exit 1: FAIL: replicas'
has "hanging child is TIMEOUT" "$(row p2-leases)" ' TIMEOUT '
has "timeout reason" "$(row p2-leases)" 'killed after 2s'
has "p3 slot SKIP not on main" "$(row p3-e2e)" 'SKIP .*not on main yet \(#307\)'
has "p4 slot SKIP" "$(row p4-60x)" 'SKIP .*#315'
has "child exit 2 is SKIP" "$(row load-p2)" 'SKIP .*child could not start \(exit 2\): BASE_URL missing'
has "load-ratio PASS" "$(row load-ratio)" ' PASS '
has "gameday SKIP without the flag" "$(row gameday)" 'SKIP .*--include-destructive'
has "p1 slot SKIP" "$(row p1-wipe-node)" 'SKIP .*#298'
has "counts" "$OUT" 'PASS 3, FAIL 1, TIMEOUT 1, SKIP 5'
has "total time printed" "$OUT" 'total time [0-9]+m[0-9]+s'
echo "-- cleanup between scenarios"
ok "ConfigMap restored" "$(jq -cS . "$S/cm.json")" '{"ACCESS_TOKEN_TTL_S":"60","RATE_LIMIT_RPS":"50"}'
ok "tester-run deleted" "$([ -e "$S/tester_run" ] && echo left || echo gone)" gone
ok "HPA recreated after load-ratio removed it" "$([ -e "$S/hpa" ] && echo back || echo gone)" back
has "cleanup note in the log" "$OUT" 'cleanup: restored bnpb-config'
has "auth-service and client-facing-api restarted" "$(cat "$S/rollouts.log")" 'rollout restart deployment/auth-service deployment/client-facing-api'
has "cleanup before the next scenario" "$(grep -nE 'cleanup and health|== \[' <<<"$OUT" | sed -n '1,12p')" 'cleanup and health re-check after rolling-update'
echo "-- result files"
j=$(ls "$T"/results/all-*.json); ok "JSON is valid" "$(jq -e . "$j" >/dev/null 2>&1; echo $?)" 0
ok "JSON overall" "$(jq -r .overall "$j")" FAIL
ok "JSON statuses" "$(jq -r '[.scenarios[].status] | join(",")' "$j")" "PASS,PASS,FAIL,TIMEOUT,SKIP,SKIP,SKIP,PASS,SKIP,SKIP"
ok "JSON rolling-update cleanup recorded" "$(jq -r '.scenarios[] | select(.name == "rolling-update") | .cleanup' "$j" | cut -c1-22)" "restored bnpb-config ("
ok "text summary saved" "$(ls "$T"/results/all-*.txt | wc -l | tr -d ' ')" 1
ok "per scenario log saved" "$(ls "$T"/results/all-*/ | sort | tr '\n' ' ')" "audit-checksum.log autoscale.log load-p2.log load-ratio.log p2-leases.log rolling-update.log "

echo "== --only and --skip, exit 0"
reset
run --only audit-checksum,load-ratio
ok "only: exit 0" $RC 0
has "only: others SKIP" "$(row rolling-update)" 'SKIP .*not selected by --only'
has "only: counts" "$OUT" 'PASS 2, FAIL 0, TIMEOUT 0, SKIP 8'
reset
run --only audit-checksum --skip audit-checksum
has "skip wins" "$(row audit-checksum)" 'SKIP .*skipped by --skip'
run --only nope; ok "unknown name exit 2" $RC 2
run --bogus; ok "unknown option exit 2" $RC 2
reset
run --only audit-checksum --fail-on-skip
ok "--fail-on-skip turns SKIP into exit 1" $RC 1

echo "== --include-destructive prints what it destroys, then runs"
reset
run --only gameday --include-destructive
ok "gameday run exit" $RC 0
has "warning before the run" "$OUT" '!! gameday will destroy: wipe-node deletes'
has "gameday PASS" "$(row gameday)" ' PASS '
has "child got STRICT=1" "$OUT" 'gameday STRICT=1'

echo "== a failure does not poison the next scenario, an unhealthy cluster is not entered"
reset; touch "$S/autoscale_breaks"
run --only autoscale,load-ratio
has "autoscale FAIL" "$(row autoscale)" ' FAIL '
has "cluster unhealthy afterwards is recorded" "$(row autoscale)" 'cluster unhealthy afterwards: lease of PVMBG is not held'
has "next scenario SKIP because of health" "$(row load-ratio)" 'SKIP .*cluster not healthy: lease of PVMBG is not held'
ok "exit 1" $RC 1
reset; touch "$S/podsdown"
run --only audit-checksum
ok "unhealthy at the start: exit 2" $RC 2
has "names the failing check" "$OUT" 'pods not ready: coordination-db-0$'
ok "nothing was run" "$(find "$T/results" -name '*.json' 2>/dev/null | wc -l | tr -d ' ')" 0

echo "== Audit API check fails after a passing scenario"
reset; touch "$S/checksum_broken"
run --only audit-checksum
has "scenario PASS, audit FAIL" "$(row audit-checksum)" 'PASS .* FAIL'
ok "audit failure gives exit 1" $RC 1

echo
if [ "$FAILS" = 0 ]; then echo "all-k8s.sh self test: PASS"; else echo "all-k8s.sh self test: $FAILS FAILED"; exit 1; fi
