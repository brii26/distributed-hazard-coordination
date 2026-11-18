#!/usr/bin/env bash
# Throughput ratio: the same k6 test (p2-k8s.js) against 1 Aggregator and against 3, prints 3-instance / 1-instance and PASS at >= 2.0.
#
#   BASE_URL=http://10.77.0.11:30081 EMAIL=operator@bnpb.example PASSWORD=... tests/load/p2-ratio.sh
#
# Env: VUS (10000), HOLD (150s), RAMP (30s), THINK_S (1), FORCE=1, MIN_200_SHARE (0.95).
# Throughput is the 200 answers per second inside the hold. A 429 is never throughput.
# The script raises ACCESS_TOKEN_TTL_S, RATE_LIMIT_RPS and CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS in ConfigMap bnpb-config for the run (restarts
# auth-service and client-facing-api), deletes the Aggregator HPA if there is one, and restores all of it on exit.
# The result is FAIL when either run has less than MIN_200_SHARE of 200 answers, because then the ratio measures the limiter.
set -euo pipefail

cd "$(dirname "$0")/../.."
# shellcheck source=tests/load/k8s-lib.sh
. tests/load/k8s-lib.sh

BASE_URL=${BASE_URL:?set BASE_URL}
EMAIL=${EMAIL:?set EMAIL}
PASSWORD=${PASSWORD:?set PASSWORD}
VUS=${VUS:-10000}; HOLD=${HOLD:-150s}; RAMP=${RAMP:-30s}; THINK_S=${THINK_S:-1}
export BASE_URL EMAIL PASSWORD VUS HOLD RAMP THINK_S

check_machine "$VUS" || exit 2

out=tests/load/results
mkdir -p "$out"
stamp=$(date +%Y%m%d-%H%M%S)

cleanup() {
  kc scale deployment/aggregator --replicas=3 >/dev/null 2>&1 || true
  load_restore
}
trap cleanup EXIT INT TERM
load_prepare "$out" "$stamp"

fail=0
# run_with <n>: scales to n, runs k6, leaves the 200 throughput in $tput. Not a subshell, so $fail is kept.
run_with() {
  local n=$1 rc=0
  local prefix="$out/p2-ratio-$stamp-$n"
  kc scale deployment/aggregator --replicas="$n" >&2
  kc rollout status deployment/aggregator --timeout=180s >&2
  sleep 10
  run_k6 "$PWD/$prefix.json" >"$prefix.txt" 2>&1 || rc=$?
  cat "$prefix.txt" >&2
  if [ "$rc" != 0 ]; then echo "FAIL: k6 exited $rc for $n replicas, see $prefix.txt" >&2; fail=1; fi
  [ -f "$prefix.compact.json" ] || { echo "FAIL: no k6 summary for $n replicas" >&2; exit 1; }
  echo "replicas $n:" >&2
  share_check "$prefix.compact.json" >&2 || fail=1
  tput=$(jqnum "$prefix.compact.json" .throughput_200_per_s)
}

run_with 1; t1=$tput
run_with 3; t3=$tput
ratio=$(awk -v a="$t1" -v b="$t3" 'BEGIN{ if (a > 0) printf "%.2f", b / a; else print "nan" }')
{
  echo "{\"replicas_1_per_s\":$t1,\"replicas_3_per_s\":$t3,\"ratio\":\"$ratio\",\"vus\":$VUS,\"hold\":\"$HOLD\"}"
} | tee "$out/p2-ratio-$stamp.json"
echo "throughput 1 instance: $t1 req/s, 3 instances: $t3 req/s, ratio: $ratio"
if ! awk -v r="$ratio" 'BEGIN{exit !(r >= 2.0)}'; then echo "ratio $ratio < 2.0"; fail=1; fi
if [ "$fail" = 0 ]; then echo "RESULT: PASS (ratio $ratio >= 2.0 on 200 throughput, 200 share ok in both runs)"; else echo "RESULT: FAIL"; exit 1; fi
