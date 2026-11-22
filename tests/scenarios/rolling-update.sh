#!/usr/bin/env bash
# Rolling update of the Aggregator under load, against a running k3s cluster. Does not use lib.sh, which needs the compose stack.
#
#   EMAIL=operator@example.com PASSWORD=... tests/scenarios/rolling-update.sh
#
# k6 sends constant load for DURATION (60s) through the Client-Facing API while `kubectl rollout restart deployment/aggregator` runs.
# PASS needs zero failed requests (a 429 is counted, not failed, but at least MIN_OK_PCT percent must be 200), every lease epoch up
# (it changed hands) and never down, no lease gap longer than GAP_MAX seconds, and no repeated source_ref_id in the Audit API.
# The Audit API token is read from the Secret audit-token, nothing else is needed from outside the cluster.
# For the run the script patches ACCESS_TOKEN_TTL_S and RATE_LIMIT_RPS in the ConfigMap bnpb-config and restarts auth-service and
# client-facing-api, so k6 can use one login (60 s tokens would expire mid-run and the refresh token rotates, so VUs can't share it,
# and one account has one rate bucket). The original values are put back and both Deployments restarted again on exit, even on Ctrl-C.
# Needs kubectl, curl, jq and k6 (or docker, then grafana/k6 runs from its image).
set -uo pipefail

NS=${NS:-sistem-informasi-kebencanaan-bnpb}
DURATION=${DURATION:-60s}
VUS=${VUS:-20}
RESTART_AT=${RESTART_AT:-10}
# A released lease is taken again at the next renew try of another pod (LEASE_RENEW_INTERVAL, 3 s) plus a sample or two.
# Above LEASE_TTL (15 s) the check would pass even if the release on SIGTERM did not work.
GAP_MAX=${GAP_MAX:-5}
MIN_OK_PCT=${MIN_OK_PCT:-90}
RUN_TOKEN_TTL_S=${RUN_TOKEN_TTL_S:-3600}
RUN_RATE_LIMIT_RPS=${RUN_RATE_LIMIT_RPS:-1000}
API_PORT=${API_PORT:-38090}
AUDIT_PORT=${AUDIT_PORT:-38091}
BASE_URL=${BASE_URL:-http://127.0.0.1:$API_PORT}
EMAIL=${EMAIL:?set EMAIL, the Operator login}
PASSWORD=${PASSWORD:?set PASSWORD}
root=$(cd "$(dirname "$0")/../.." && pwd)

for c in kubectl curl jq; do command -v "$c" >/dev/null || { echo "missing $c" >&2; exit 2; }; done

work=$(mktemp -d)
pids=()
patched=0
orig_ttl=
orig_rps=
cleanup() {
  trap - EXIT INT TERM
  local p
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done
  if [ "$patched" = 1 ]; then
    echo "== Restoring ACCESS_TOKEN_TTL_S and RATE_LIMIT_RPS"
    set_config "$orig_ttl" "$orig_rps"
    restart_api_and_auth
  fi
  rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

FAILED=0
expect_true() {
  local what=$1
  shift
  if "$@"; then echo "PASS: $what"; else echo "FAIL: $what"; FAILED=1; fi
}

kc() { kubectl -n "$NS" "$@"; }

# Sets both keys in bnpb-config, an empty value removes the key.
set_config() {
  local patch
  patch=$(jq -nc --arg t "$1" --arg r "$2" '{data: {ACCESS_TOKEN_TTL_S: (if $t == "" then null else $t end), RATE_LIMIT_RPS: (if $r == "" then null else $r end)}}')
  kc patch configmap bnpb-config --type merge -p "$patch" >/dev/null
}

restart_api_and_auth() {
  kc rollout restart deployment/auth-service deployment/client-facing-api >/dev/null
  kc rollout status deployment/auth-service --timeout=180s
  kc rollout status deployment/client-facing-api --timeout=180s
}

lease_rows() {
  # shellcheck disable=SC2016 # $POSTGRES_USER expands inside the pod
  kc exec coordination-db-0 -- sh -c 'psql -U "$POSTGRES_USER" -d coordination -tAF, -c "SELECT source, epoch, (holder IS NOT NULL AND expires_at > now())::int FROM source_lease ORDER BY source"'
}

k6_run() {
  local env=(-e "BASE_URL=$BASE_URL" -e "EMAIL=$EMAIL" -e "PASSWORD=$PASSWORD" -e "VUS=$VUS" -e "DURATION=$DURATION")
  if command -v k6 >/dev/null; then
    k6 run "${env[@]}" --summary-export "$work/summary.json" "$root/tests/load/rolling.js"
  else
    docker run --rm --network host -u "$(id -u):$(id -g)" -v "$root/tests/load:/load" -v "$work:/out" "${env[@]}" \
      "${K6_IMAGE:-grafana/k6:latest}" run --summary-export /out/summary.json /load/rolling.js
  fi
}

command -v k6 >/dev/null || command -v docker >/dev/null || { echo "need k6 or docker" >&2; exit 2; }

echo "== Setup"
kc get deployment aggregator >/dev/null || { echo "no cluster or no aggregator deployment" >&2; exit 2; }
orig_ttl=$(kc get configmap bnpb-config -o jsonpath='{.data.ACCESS_TOKEN_TTL_S}')
orig_rps=$(kc get configmap bnpb-config -o jsonpath='{.data.RATE_LIMIT_RPS}')
echo "ACCESS_TOKEN_TTL_S ${orig_ttl:-unset} to $RUN_TOKEN_TTL_S, RATE_LIMIT_RPS ${orig_rps:-unset} to $RUN_RATE_LIMIT_RPS for this run"
patched=1
set_config "$RUN_TOKEN_TTL_S" "$RUN_RATE_LIMIT_RPS" || { echo "cannot patch bnpb-config" >&2; exit 2; }
restart_api_and_auth || { echo "auth-service or client-facing-api did not roll out" >&2; exit 2; }
kc port-forward svc/client-facing-api "$API_PORT:8080" >"$work/pf-api.log" 2>&1 &
pids+=($!)
until curl -sf "$BASE_URL/health" >/dev/null; do
  kill -0 "${pids[0]}" 2>/dev/null || { echo "port-forward to the API failed" >&2; exit 2; }
  sleep 1
done

audit_token=$(kc get secret audit-token -o jsonpath='{.data.token}' | base64 -d)
start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
lease_rows >"$work/lease-before.csv" || { echo "cannot read source_lease" >&2; exit 2; }
cat "$work/lease-before.csv"

(
  while :; do
    { printf '%s;' "$(date +%s)"; lease_rows | paste -sd';' -; echo; } >>"$work/lease-samples.csv" 2>/dev/null
    sleep 1
  done
) &
pids+=($!)

echo "== Load for $DURATION, restart after ${RESTART_AT}s"
k6_run >"$work/k6.log" 2>&1 &
k6pid=$!
sleep "$RESTART_AT"
kc rollout restart deployment/aggregator
kc rollout status deployment/aggregator --timeout=300s || FAILED=1
wait "$k6pid"
k6rc=$?
tail -n 25 "$work/k6.log"

echo "== Requests"
total=$(jq -r '.metrics.rolling_total.count // 0' "$work/summary.json" 2>/dev/null || echo 0)
bad=$(jq -r '.metrics.rolling_failed.count // 0' "$work/summary.json" 2>/dev/null || echo 0)
throttled=$(jq -r '.metrics.rolling_throttled_429.count // 0' "$work/summary.json" 2>/dev/null || echo 0)
echo "total=$total failed=$bad throttled_429=$throttled (not counted as failed) k6_exit=$k6rc"
expect_true "k6 sent requests" test "$total" -gt 0
ok=$(jq -r '.metrics.rolling_ok.count // 0' "$work/summary.json" 2>/dev/null || echo 0)
pct=$((total > 0 ? ok * 100 / total : 0))
echo "200 share: $ok of $total ($pct%), at least $MIN_OK_PCT% needed"
expect_true "zero failed requests" test "$bad" -eq 0
expect_true "most requests were 200, not 429" test "$pct" -ge "$MIN_OK_PCT"

echo "== Lease"
lease_rows >"$work/lease-after.csv"
cat "$work/lease-after.csv"
kill "${pids[1]}" 2>/dev/null

while IFS=, read -r src before held; do
  if [ "$held" != 1 ]; then
    echo "SKIP: $src lease was not held before the run (source off), nothing to hand over"
    continue
  fi

  after=$(awk -F, -v s="$src" '$1 == s { print $2 }' "$work/lease-after.csv")
  expect_true "$src lease changed hands (epoch $before to $after)" test "${after:-0}" -gt "$before"

  # Epoch never goes down and the longest stretch without a valid holder stays within GAP_MAX seconds.
  stats=$(awk -F';' -v s="$src" '
    { for (i = 2; i <= NF; i++) {
        split($i, c, ",")
        if (c[1] != s) continue
        if (c[2] + 0 < last) back++
        last = c[2] + 0
        if (c[3] == 0) { run++; if (run > worst) worst = run } else run = 0
      } }
    END { printf "%d %d", back + 0, worst + 0 }' "$work/lease-samples.csv")
  read -r back worst <<<"$stats"
  expect_true "$src epoch never went back" test "$back" -eq 0
  expect_true "$src had no lease gap above ${GAP_MAX}s (longest ${worst}s)" test "$worst" -le "$GAP_MAX"
done <"$work/lease-before.csv"

echo "== Audit API"
kc port-forward deployment/aggregator "$AUDIT_PORT:8091" >"$work/pf-audit.log" 2>&1 &
pids+=($!)
until curl -sf "http://127.0.0.1:$AUDIT_PORT/health" >/dev/null; do
  kill -0 "${pids[-1]}" 2>/dev/null || { echo "port-forward to the Aggregator failed" >&2; exit 2; }
  sleep 1
done

for src in BMKG PVMBG SENSOR_PANTAI; do
  : >"$work/ids-$src.txt"
  cursor=""
  while :; do
    page=$(curl -sf -H "Authorization: Bearer $audit_token" -G "http://127.0.0.1:$AUDIT_PORT/internal/audit/events" \
      --data-urlencode "source=$src" --data-urlencode "limit=1000" --data-urlencode "cursor=$cursor") || { echo "audit request failed" >&2; FAILED=1; break; }
    jq -r '.items[].source_ref_id' <<<"$page" >>"$work/ids-$src.txt"
    cursor=$(jq -r '.next_cursor // empty' <<<"$page")
    [ -n "$cursor" ] || break
  done
  n=$(wc -l <"$work/ids-$src.txt" | tr -d ' ')
  u=$(sort -u "$work/ids-$src.txt" | wc -l | tr -d ' ')
  echo "$src: $n events since the store began, $u distinct source_ref_id (run started $start)"
  expect_true "$src has no duplicate event in the Audit API" test "$n" -eq "$u"
done

echo
if [ "$FAILED" = 0 ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; exit 1; fi
