#!/usr/bin/env bash
# Trace check: one request's trace in Jaeger and its log lines carry the same ID, across services.
# Needs jq. Sends no X-Correlation-ID on purpose: only then the trace ID is the correlation ID.
#
#   tests/scenarios/trace-check.sh                 # compose stack that is already up
#   TARGET=k8s tests/scenarios/trace-check.sh      # needs port-forwards: svc/client-facing-api to :8080, svc/jaeger to :16686
. "$(dirname "$0")/lib.sh"

TARGET=${TARGET:-compose}
NS=${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}
JAEGER=http://localhost:${JAEGER_UI_PORT:-16686}
command -v jq >/dev/null || { echo "jq is required (devenv has it once the Grafana PR is merged)" >&2; exit 2; }
curl -sf -o /dev/null "$JAEGER/" || { echo "Jaeger is not reachable at $JAEGER" >&2; exit 2; }

# svc_logs <service>
svc_logs() { if [ "$TARGET" = k8s ]; then kubectl -n "$NS" logs "deploy/$1" --since=10m; else docker compose logs "$1"; fi; }
jaeger_stop() { if [ "$TARGET" = k8s ]; then kubectl -n "$NS" delete pod -l app=jaeger --wait=false; else docker compose stop jaeger; fi; }
jaeger_start() { if [ "$TARGET" = k8s ]; then kubectl -n "$NS" rollout status deploy/jaeger; else docker compose start jaeger; fi; }

# corr_id <header file>, the response's X-Correlation-ID
corr_id() { tr -d '\r' <"$1" | awk -F': ' 'tolower($1) == "x-correlation-id" { print $2 }'; }

# trace_services <trace id>, distinct service names in the trace
trace_services() {
  curl -sf "$JAEGER/api/traces/$1" |
    jq -r '.data[0] | .processes as $p | [.spans[].processID | $p[.].serviceName] | unique[]' 2>/dev/null
}

# is_trace_id <id>
is_trace_id() { [[ $1 =~ ^[0-9a-f]{32}$ ]]; }

# logged <service> <trace id>
logged() { local out; out=$(svc_logs "$1" 2>&1); grep -q "\"correlation_id\":\"$2\"" <<<"$out"; }

# aggregator_timed <trace id>, the aggregator has a span that took time
aggregator_timed() {
  curl -sf "$JAEGER/api/traces/$1" |
    jq -e '.data[0] as $t | [$t.spans[] | select($t.processes[.processID].serviceName == "aggregator" and .duration > 0)] | length > 0' >/dev/null 2>&1
}

# check_trace <label> <trace id> <service>..., every listed service must have spans and log lines for the ID
check_trace() {
  local label=$1 id=$2 svc seen; shift 2
  step "$label: trace $id"
  expect_true "the ID is 32 hex characters, so it is the trace ID" is_trace_id "$id"
  is_trace_id "$id" || return
  has_all() { seen=$(trace_services "$id"); for svc in "$@"; do grep -qx "$svc" <<<"$seen" || return 1; done; }
  wait_until 10 has_all "$@"
  echo "services in the trace: $(echo $seen)"
  for svc in "$@"; do
    expect_true "$svc has spans in Jaeger" grep -qx "$svc" <<<"$seen"
    expect_true "$svc logged the same ID" wait_until 10 logged "$svc" "$id"
  done
}

step "Login, then GET /hazards, both without X-Correlation-ID"
show "curl -D - -X POST \$API/auth/login"
curl -s -o /dev/null -D "$JARS/login.h" -c "$JARS/op.jar" -X POST "$API/auth/login" -d "{\"email\":\"$SEED_OPERATOR_EMAIL\",\"password\":\"$SEED_OPERATOR_PASSWORD\"}"
login_id=$(corr_id "$JARS/login.h")
show "curl -D - -b op.jar \"\$API/hazards?type=VOLCANIC&limit=3\""
curl -s -o /dev/null -D "$JARS/hazards.h" -b "$JARS/op.jar" "$API/hazards?type=VOLCANIC&limit=3"
hazards_id=$(corr_id "$JARS/hazards.h")

check_trace "Login" "$login_id" client-facing-api auth-service
check_trace "GET /hazards" "$hazards_id" client-facing-api aggregator
expect_true "the aggregator has a span with a non-zero duration" aggregator_timed "$hazards_id"


step "Jaeger down: requests must still succeed"
cleanup_hooks+=("jaeger_start >/dev/null 2>&1")
jaeger_stop >/dev/null 2>&1
start=$SECONDS
status=$(code -b "$JARS/op.jar" "$API/hazards?type=VOLCANIC&limit=3")
expect "GET /hazards while Jaeger is down" "$status" 200
expect_true "it answered within 5 s" test $((SECONDS - start)) -le 5
jaeger_start >/dev/null 2>&1
expect_true "Jaeger is back" wait_until 30 curl -sf -o /dev/null "$JAEGER/"

finish
