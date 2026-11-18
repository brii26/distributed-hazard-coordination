#!/usr/bin/env bash
# Problem 2 load test on the cluster: >= 10000 VUs for >= 120 s from outside the cluster, Aggregator scaled 3 -> 5 -> 2 -> 3
# during the hold, then the Audit checksum and the upstream mock logs are checked.
#
#   BASE_URL=http://10.77.0.11:30081 EMAIL=operator@bnpb.example PASSWORD=... AUDIT_TOKEN=... tests/load/p2-k8s.sh
#
# The script sets ACCESS_TOKEN_TTL_S, RATE_LIMIT_RPS and CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS in ConfigMap bnpb-config for the run, restarts
# auth-service and client-facing-api, deletes the Aggregator HPA if there is one, and restores all of it on exit (also after Ctrl-C).
# The run FAILS when less than MIN_200_SHARE (0.95) of the answers are 200.
#
# Env: VUS (10000), HOLD (150s), RAMP (30s), THINK_S (1), FORCE=1 to skip the machine refusal, SKIP_CHECKS=1 to skip the audit and mock checks,
#      SCALE_UP_AT / SCALE_DOWN_AT = seconds into the hold for 5 and 2 replicas (default 25% and 60% of the hold).
# Needs kubectl pointed at the cluster, curl, jq, and k6 or docker.
set -euo pipefail

cd "$(dirname "$0")/../.."
# shellcheck source=tests/load/k8s-lib.sh
. tests/load/k8s-lib.sh

BASE_URL=${BASE_URL:?set BASE_URL, the public entry point such as http://10.77.0.11:30081}
EMAIL=${EMAIL:?set EMAIL}
PASSWORD=${PASSWORD:?set PASSWORD}
VUS=${VUS:-10000}; HOLD=${HOLD:-150s}; RAMP=${RAMP:-30s}; THINK_S=${THINK_S:-1}
export BASE_URL EMAIL PASSWORD VUS HOLD RAMP THINK_S

hold_s=${HOLD%s}
case $HOLD in *s) ;; *) echo "HOLD must be in seconds, like 150s" >&2; exit 2 ;; esac
SCALE_UP_AT=${SCALE_UP_AT:-$((hold_s / 4))}
SCALE_DOWN_AT=${SCALE_DOWN_AT:-$((hold_s * 6 / 10))}
ramp_s=${RAMP%s}

check_machine "$VUS" || exit 2

out=tests/load/results
mkdir -p "$out"
stamp=$(date +%Y%m%d-%H%M%S)
prefix="$out/p2-k8s-$stamp"

original=$(kc get deployment/aggregator -o jsonpath='{.spec.replicas}')
pf= ; k6pid= ; scaler=
cleanup() {
  [ -n "$scaler" ] && kill "$scaler" 2>/dev/null || true
  [ -n "$k6pid" ] && kill "$k6pid" 2>/dev/null || true
  [ -n "$pf" ] && kill "$pf" 2>/dev/null || true
  kc scale deployment/aggregator --replicas=3 >/dev/null 2>&1 || true
  load_restore
}
trap cleanup EXIT INT TERM

load_prepare "$out" "$stamp"
echo "aggregator replicas before: $original, setting 3"
kc scale deployment/aggregator --replicas=3
kc rollout status deployment/aggregator --timeout=180s

start_iso=$(date -u +%Y-%m-%dT%H:%M:%SZ)
cat >"$prefix.config.json" <<JSON
{"base_url":"$BASE_URL","vus":$VUS,"hold":"$HOLD","ramp":"$RAMP","think_s":$THINK_S,"scale_up_at_s":$SCALE_UP_AT,"scale_down_at_s":$SCALE_DOWN_AT,
 "started":"$start_iso","host":"$(hostname)","cpus":$(nproc),"mem_available_mb":$(($(awk '/MemAvailable/ {print $2}' /proc/meminfo) / 1024)),"ulimit_n":"$(ulimit -n)",
 "k6":"$(command -v k6 >/dev/null 2>&1 && k6 version | head -1 || echo "$K6_IMAGE")"}
JSON

# Scaler: waits for the ramp, then 3 -> 5 -> 2 inside the hold. Times are written to the config log.
(
  sleep $((ramp_s + SCALE_UP_AT))
  echo "$(date -u +%FT%TZ) scale 5" >>"$prefix.scale.log"; kc scale deployment/aggregator --replicas=5
  sleep $((SCALE_DOWN_AT - SCALE_UP_AT))
  echo "$(date -u +%FT%TZ) scale 2" >>"$prefix.scale.log"; kc scale deployment/aggregator --replicas=2
) &
scaler=$!

set +e
run_k6 "$PWD/$prefix.json" > >(tee "$prefix.txt") 2>&1 &
k6pid=$!
wait "$k6pid"; k6rc=$?
k6pid=
wait "$scaler" 2>/dev/null; scaler=
set -e
echo "$(date -u +%FT%TZ) scale 3" >>"$prefix.scale.log"
kc scale deployment/aggregator --replicas=3
kc rollout status deployment/aggregator --timeout=180s

fail=0
[ -f "$prefix.compact.json" ] || { echo "FAIL: no k6 summary written"; exit 1; }
echo "k6 summary: $(jq -c . "$prefix.compact.json")"
rate=$(jqnum "$prefix.compact.json" .error_rate_excluding_429)
n429=$(jqnum "$prefix.compact.json" .limited_429)
vmax=$(jqnum "$prefix.compact.json" .vus_max)
echo "429 (controlled): $n429"
share_check "$prefix.compact.json" || fail=1
if awk -v r="$rate" 'BEGIN{exit !(r < 0.01)}'; then echo "PASS: error rate excluding 429 is $rate (< 0.01)"; else echo "FAIL: error rate $rate"; fail=1; fi
if [ "$vmax" -ge 10000 ] && [ "$hold_s" -ge 120 ]; then echo "PASS: peak $vmax VUs held $HOLD"; else echo "FAIL: peak $vmax VUs held $HOLD, spec wants >= 10000 for >= 120 s"; fail=1; fi
[ "$k6rc" = 0 ] || { echo "FAIL: k6 exited $k6rc (a threshold failed or the run broke)"; fail=1; }

if [ "${SKIP_CHECKS:-0}" != 1 ]; then
  # Audit checksum, over a port-forward to the Aggregator Service.
  : "${AUDIT_TOKEN:?set AUDIT_TOKEN for the Audit API, or SKIP_CHECKS=1}"
  kc port-forward svc/aggregator 18091:8091 >"$prefix.pf.log" 2>&1 &
  pf=$!
  for _ in $(seq 20); do curl -sf http://127.0.0.1:18091/health >/dev/null && break; sleep 1; done
  for src in BMKG PVMBG SENSOR_PANTAI; do
    if AUDIT_URL=http://127.0.0.1:18091 AUDIT_TOKEN=$AUDIT_TOKEN bash tests/scenarios/audit-checksum.sh "$src" | tee -a "$prefix.audit.txt"; then
      echo "PASS: audit $src checksum matches the independent sha256 over unique ids"
    else echo "FAIL: audit $src checksum"; fail=1; fi
    # Every id of the source, all pages, must be unique.
    if audit_ids http://127.0.0.1:18091 "$AUDIT_TOKEN" "$src" "$prefix.$src.ids" ; then
      dups=$(sort "$prefix.$src.ids" | uniq -d | wc -l)
      echo "$src: $(wc -l <"$prefix.$src.ids") ids over all pages, $dups repeated"
      if [ "$dups" = 0 ]; then echo "PASS: no duplicate $src ids"; else echo "FAIL: $src duplicate count $dups"; fail=1; fi
    else echo "FAIL: could not page the $src events"; fail=1; fi
  done
  kill "$pf" 2>/dev/null || true; pf=

  # Single fetch: the mocks log the caller (remote_addr, the Aggregator pod IP). For each source, two different callers whose requests overlap in time
  # mean two instances fetched the same source at once. A handover (the old caller stops, a new one takes over, for example after the scale-in) is accepted.
  tol_ms=${SINGLE_FETCH_TOLERANCE_MS:-100}
  for mock in bmkg-mock pvmbg-mock; do
    kc logs "deploy/$mock" --since-time="$start_iso" >"$prefix.$mock.log" 2>&1 || true
    res=$(single_fetch "$prefix.$mock.log" "$tol_ms")
    echo "$mock: $res (tolerance ${tol_ms} ms)"
    if [ "$(jq .total <<<"$res")" -eq 0 ]; then echo "FAIL: $mock logged no data requests since $start_iso"; fail=1
    elif [ "$(jq .without_caller <<<"$res")" -gt 0 ]; then echo "FAIL: $mock log lines have no remote_addr, rebuild and redeploy the mock image"; fail=1
    elif [ "$(jq '.overlapping_pairs' <<<"$res")" = 0 ]; then echo "PASS: $mock single fetch, no two callers at the same time"
    else echo "FAIL: $mock was fetched by two callers at the same time, look at $prefix.$mock.log"; fail=1; fi
  done
fi

echo "saved: $prefix.{config.json,scale.log,txt,json,compact.json}"
if [ "$fail" = 0 ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; exit 1; fi
