#!/usr/bin/env bash
# Service-to-service integration tests in an isolated compose project, torn down after.
#
#   tests/integration/run.sh cfa-auth    Client-Facing API + Auth Service
#   tests/integration/run.sh agg-bmkg    Aggregator + BMKG mock
#   tests/integration/run.sh agg-pvmbg   Aggregator + PVMBG mock
#   tests/integration/run.sh all         the three, one after the other
#   tests/integration/run.sh abuse       the whole stack under abuse, about 8 minutes, not in "all"
#
# Needs docker compose and Go. Values come from .env.example, so no .env is needed.
set -euo pipefail

cd "$(dirname "$0")/../.."
root=$PWD

project=hazard-it
compose="docker compose -p $project --env-file $root/.env.example -f $root/docker-compose.yml -f $root/tests/integration/compose.override.yml"

export CLIENT_FACING_API_PORT=18080 BMKG_PORT=18081 PVMBG_PORT=18082

export POLL_INTERVAL_MS=500 POLL_TIMEOUT_MS=2000 CB_FAILURE_THRESHOLD=2 CB_OPEN_DURATION_MS=2000
export BMKG_DELAY_MIN_MS=10 BMKG_DELAY_MAX_MS=50 PVMBG_DELAY_MIN_MS=10 PVMBG_DELAY_MAX_MS=50

# shellcheck disable=SC1091
set -a; . ./.env.example; set +a
export IT_COMPOSE="$compose"

cleanup() { $compose down -v --remove-orphans >/dev/null 2>&1 || true; }

up() {
  cleanup
  trap cleanup EXIT
  $compose up -d --build --wait --no-deps "$@"
}

run_go() {
  local module=$1 pattern=$2
  (cd "services/$module" && go test -count=1 -v -timeout "${GO_TEST_TIMEOUT:-10m}" -run "$pattern" ./...)
}

suite_cfa_auth() {
  export ACCESS_TOKEN_TTL_S=2
  up user-db auth-service client-facing-api
  IT_CFA_URL="http://127.0.0.1:$CLIENT_FACING_API_PORT" \
    IT_OPERATOR_EMAIL="$SEED_OPERATOR_EMAIL" IT_OPERATOR_PASSWORD="$SEED_OPERATOR_PASSWORD" \
    IT_TOKEN_TTL_S=$ACCESS_TOKEN_TTL_S \
    run_go client-facing-api '^TestIntegration'
}

aggregator_env() {
  IT_COMPOSE="$compose" \
    CANONICAL_DB_TEST_URL="postgres://$CANONICAL_DB_USER:$CANONICAL_DB_PASSWORD@127.0.0.1:15432/$CANONICAL_DB_NAME?sslmode=disable" \
    IT_AGG_GRPC_ADDR=127.0.0.1:19091 \
    IT_BMKG_URL="http://127.0.0.1:$BMKG_PORT" IT_BMKG_KEY="$BMKG_API_KEY" \
    IT_PVMBG_URL="http://127.0.0.1:$PVMBG_PORT" IT_PVMBG_ADMIN_KEY="$PVMBG_ADMIN_KEY" \
    "$@"
}

suite_agg_bmkg() {
  export BMKG_EVENT_INTERVAL_S=3 # new events keep arriving, so the incremental poll shows
  up canonical-store rabbitmq bmkg-mock aggregator
  aggregator_env run_go aggregator '^TestIntegrationBMKG'
}

suite_agg_pvmbg() {
  export PVMBG_EVENT_INTERVAL_S=1 # new reports keep arriving, so a schema change shows quickly
  export BMKG_EVENT_INTERVAL_S=3600
  up canonical-store rabbitmq bmkg-mock pvmbg-mock aggregator
  aggregator_env run_go aggregator '^TestIntegrationPVMBG'
}

suite_abuse() {
  export PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 POLL_TIMEOUT_MS=5000
  export BMKG_EVENT_INTERVAL_S=2 PVMBG_EVENT_INTERVAL_S=2
  export GO_TEST_TIMEOUT=45m
  up bmkg-mock pvmbg-mock canonical-store user-db rabbitmq aggregator auth-service client-facing-api \
    dashboard-updater notifier audit-logger
  IT_ABUSE_URL="http://127.0.0.1:$CLIENT_FACING_API_PORT" \
    IT_OPERATOR_EMAIL="$SEED_OPERATOR_EMAIL" IT_OPERATOR_PASSWORD="$SEED_OPERATOR_PASSWORD" \
    IT_BMKG_URL="http://127.0.0.1:$BMKG_PORT" IT_BMKG_KEY="$BMKG_API_KEY" \
    IT_PVMBG_URL="http://127.0.0.1:$PVMBG_PORT" IT_PVMBG_ADMIN_KEY="$PVMBG_ADMIN_KEY" \
    IT_CANONICAL_USER="$CANONICAL_DB_USER" IT_CANONICAL_DB="$CANONICAL_DB_NAME" \
    IT_LOAD_SECONDS="${IT_LOAD_SECONDS:-15}" \
    run_go client-facing-api "${ABUSE_RUN:-^TestAbuse}"
}

case "${1:-}" in
  cfa-auth) suite_cfa_auth ;;
  agg-bmkg) suite_agg_bmkg ;;
  agg-pvmbg) suite_agg_pvmbg ;;
  abuse) suite_abuse ;;
  all) (suite_cfa_auth) && (suite_agg_bmkg) && (suite_agg_pvmbg) ;;
  *) echo "usage: $0 cfa-auth|agg-bmkg|agg-pvmbg|abuse|all" >&2; exit 2 ;;
esac
