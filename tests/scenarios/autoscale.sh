#!/usr/bin/env bash
# Bonus #327: k6 ramp against the deployed API while the HPA of the Aggregator reacts.
# PASS needs replicas held at the minimum while idle, above it during the load, back at it after the load,
# error rate under 1% (429 excluded) and no duplicate id in the Audit API with a matching checksum.
# The script raises ACCESS_TOKEN_TTL_S and RATE_LIMIT_RPS in bnpb-config for the run (the rate limit is per account,
# so one login cannot reach the CPU target at 50 rps) and restores both on exit. Run it with the HPA in place and not paused.
#
#   API_URL=http://10.77.0.11:30081 AUDIT_TOKEN=... tests/scenarios/autoscale.sh
# Needs kubectl on the cluster, k6 (or docker for grafana/k6), curl and jq. Env: LOGIN_EMAIL, LOGIN_PASSWORD,
# PEAK_VUS (default 150), HOLD (default 4m), IDLE_PERIOD seconds (default 180), SETTLE_TIMEOUT seconds (default 900),
# LOAD_TTL_S (default 7200), LOAD_RATE_RPS (default 100000).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
NS=${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}
API_URL=${API_URL:?set API_URL, for example http://10.77.0.11:30081}
AUDIT_TOKEN=${AUDIT_TOKEN:?set AUDIT_TOKEN}
LOGIN_EMAIL=${LOGIN_EMAIL:-field@demo.example}
LOGIN_PASSWORD=${LOGIN_PASSWORD:-field-pass-1}
PEAK_VUS=${PEAK_VUS:-150}
HOLD=${HOLD:-4m}
SETTLE_TIMEOUT=${SETTLE_TIMEOUT:-900}
IDLE_PERIOD=${IDLE_PERIOD:-180}
LOAD_TTL_S=${LOAD_TTL_S:-7200}
LOAD_RATE_RPS=${LOAD_RATE_RPS:-100000}
AUDIT_LOCAL_PORT=${AUDIT_LOCAL_PORT:-18091}

work=$(mktemp -d)
pids=()
orig_ttl=""
orig_rps=""
patched=0

cfg_get() { kubectl -n "$NS" get configmap bnpb-config -o "jsonpath={.data.$1}"; }
cfg_set() {
  kubectl -n "$NS" patch configmap bnpb-config --type merge -p "{\"data\":{\"ACCESS_TOKEN_TTL_S\":\"$1\",\"RATE_LIMIT_RPS\":\"$2\"}}" >/dev/null
  kubectl -n "$NS" rollout restart deployment/auth-service deployment/client-facing-api >/dev/null
  kubectl -n "$NS" rollout status deployment/auth-service --timeout=180s >/dev/null
  kubectl -n "$NS" rollout status deployment/client-facing-api --timeout=180s >/dev/null
}

cleanup() {
  trap - EXIT INT TERM
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  if [ "$patched" = 1 ]; then
    echo "== restoring ACCESS_TOKEN_TTL_S=$orig_ttl and RATE_LIMIT_RPS=$orig_rps"
    cfg_set "$orig_ttl" "$orig_rps" || echo "restore failed, set both keys in bnpb-config by hand" >&2
  fi
  rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

hpa() { kubectl -n "$NS" get hpa aggregator -o "jsonpath=$1"; }
min=$(hpa '{.spec.minReplicas}') || { echo "no HPA aggregator, apply deploy/aggregator-hpa.yaml first" >&2; exit 2; }
max=$(hpa '{.spec.maxReplicas}')
echo "HPA aggregator: min $min, max $max"
[ "$max" -gt "$min" ] || { echo "HPA is pinned (min = max), restore it before this test" >&2; exit 2; }

orig_ttl=$(cfg_get ACCESS_TOKEN_TTL_S)
orig_rps=$(cfg_get RATE_LIMIT_RPS)
echo "== raising ACCESS_TOKEN_TTL_S $orig_ttl -> $LOAD_TTL_S and RATE_LIMIT_RPS $orig_rps -> $LOAD_RATE_RPS"
patched=1
cfg_set "$LOAD_TTL_S" "$LOAD_RATE_RPS"

jwt=$(curl -sf --retry 8 --retry-delay 2 --retry-all-errors -c - -X POST "$API_URL/auth/login" -d "{\"email\":\"$LOGIN_EMAIL\",\"password\":\"$LOGIN_PASSWORD\"}" -o /dev/null | awk '$6 == "jwt" {print $7}')
[ -n "$jwt" ] || { echo "login failed, no jwt cookie" >&2; exit 2; }

cat > "$work/ramp.js" <<JS
import http from 'k6/http';
import { sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errors = new Rate('errors_excluding_429');
const peak = Number(__ENV.PEAK_VUS);

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-vus',
      stages: [
        { duration: '1m', target: Math.ceil(peak / 3) },
        { duration: '2m', target: peak },
        { duration: __ENV.HOLD, target: peak },
        { duration: '1m', target: 0 },
      ],
    },
  },
  thresholds: { errors_excluding_429: ['rate<0.01'] },
};

export default function () {
  const res = http.get(\`\${__ENV.API_URL}/hazards?type=SEISMIC\`, {
    headers: { Cookie: \`jwt=\${__ENV.JWT}\` },
    responseCallback: http.expectedStatuses(200, 429),
  });
  errors.add(res.status !== 200 && res.status !== 429);
  sleep(0.01);
}
JS

if command -v k6 >/dev/null; then
  k6cmd=(k6 run)
  script="$work/ramp.js"
else
  k6cmd=(docker run --rm --network host -u "$(id -u):$(id -g)" -v "$work:/w:ro" -e API_URL -e JWT -e PEAK_VUS -e HOLD grafana/k6:1.4.2 run)
  script=/w/ramp.js
fi

echo "== idle for $IDLE_PERIOD s, the count must stay at $min"
idle_ok=1
idle_end=$((SECONDS + IDLE_PERIOD))
while [ "$SECONDS" -lt "$idle_end" ]; do
  cur=$(hpa '{.status.currentReplicas}')
  cpu=$(kubectl -n "$NS" get hpa aggregator --no-headers | awk '{print $3, $4}')
  printf '%s replicas=%s cpu=%s\n' "$(date +%T)" "${cur:-0}" "$cpu"
  [ "${cur:-0}" -eq "$min" ] || idle_ok=0
  sleep 15
done

echo "== ramp up to $PEAK_VUS VUs, replicas every 15 s"
export API_URL JWT=$jwt PEAK_VUS HOLD
"${k6cmd[@]}" "$script" >"$work/k6.log" 2>&1 &
k6pid=$!
pids+=("$k6pid")

peak_seen=$min
while kill -0 "$k6pid" 2>/dev/null; do
  cur=$(hpa '{.status.currentReplicas}')
  cpu=$(kubectl -n "$NS" get hpa aggregator --no-headers | awk '{print $3, $4}')
  printf '%s replicas=%s cpu=%s\n' "$(date +%T)" "${cur:-0}" "$cpu"
  [ "${cur:-0}" -gt "$peak_seen" ] && peak_seen=${cur:-0}
  sleep 15
done
k6rc=0
wait "$k6pid" || k6rc=$?
tail -n 25 "$work/k6.log"

echo "== load stopped, waiting for $min replicas (scaleDown window 300 s, one pod per 120 s)"
settled=0
deadline=$((SECONDS + SETTLE_TIMEOUT))
while [ "$SECONDS" -lt "$deadline" ]; do
  cur=$(hpa '{.status.currentReplicas}')
  printf '%s replicas=%s\n' "$(date +%T)" "${cur:-0}"
  if [ "${cur:-0}" -eq "$min" ]; then settled=1; break; fi
  sleep 15
done

fail=0
check() { if [ "$2" = 1 ]; then echo "PASS: $1"; else echo "FAIL: $1"; fail=1; fi; }

check "replicas stayed at $min for $IDLE_PERIOD s without traffic" "$idle_ok"
check "replicas rose above $min (peak $peak_seen, max $max)" "$([ "$peak_seen" -gt "$min" ] && echo 1 || echo 0)"
check "replicas back at $min after the load" "$settled"
check "k6 error rate under 1% excluding 429" "$([ "$k6rc" -eq 0 ] && echo 1 || echo 0)"

echo "== Audit API"
kubectl -n "$NS" port-forward svc/aggregator "$AUDIT_LOCAL_PORT:8091" >/dev/null 2>&1 &
pids+=("$!")
export AUDIT_URL="http://localhost:$AUDIT_LOCAL_PORT"
export AUDIT_TOKEN
wait_pf=$((SECONDS + 20))
until curl -s -o /dev/null "$AUDIT_URL/health"; do
  [ "$SECONDS" -lt "$wait_pf" ] || { echo "port-forward to the Aggregator failed" >&2; exit 2; }
  sleep 1
done

for src in BMKG PVMBG SENSOR_PANTAI; do
  : >"$work/ids"
  cursor=""
  while :; do
    page=$(curl -sf -H "Authorization: Bearer $AUDIT_TOKEN" -G "$AUDIT_URL/internal/audit/events" \
      --data-urlencode "source=$src" --data-urlencode "limit=1000" --data-urlencode "cursor=$cursor")
    jq -r '.items[].source_ref_id' <<<"$page" >>"$work/ids"
    cursor=$(jq -r '.next_cursor // empty' <<<"$page")
    [ -n "$cursor" ] || break
  done
  dups=$(LC_ALL=C sort "$work/ids" | uniq -d | wc -l | tr -d ' ')
  check "$src: no duplicate event id ($dups duplicated of $(wc -l <"$work/ids" | tr -d ' '))" "$([ "$dups" -eq 0 ] && echo 1 || echo 0)"
  if bash tests/scenarios/audit-checksum.sh "$src" >"$work/sum.log" 2>&1; then
    check "$src: checksum matches an independent sha256" 1
  else
    cat "$work/sum.log"
    check "$src: checksum matches an independent sha256" 0
  fi
done

echo
if [ "$fail" = 0 ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; exit 1; fi
