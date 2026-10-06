#!/usr/bin/env bash
# Full-stack smoke test: brings up the whole docker-compose stack in an isolated
# project, checks every service's /health, runs the golden path (seeded Operator
# logs in, polls /hazards until data from the mocks has flowed through the
# Aggregator), then restarts one consumer and checks nothing else moved
# (doc/TRD_M1.md 0.0, 12: independent restart, P4).
#
#   tests/smoke/run.sh
#
# Needs docker compose and curl. Values come from .env.example, so no .env is needed.
set -euo pipefail

cd "$(dirname "$0")/../.."
root=$PWD

compose="docker compose -p hazard-smoke --env-file $root/.env.example -f $root/docker-compose.yml"

# shellcheck disable=SC1091
set -a; . ./.env.example; set +a

# Host ports clear of a normal `docker compose up` running next to this.
export CLIENT_FACING_API_PORT=28080 BMKG_PORT=28081 PVMBG_PORT=28082 RABBITMQ_MANAGEMENT_PORT=25672 WEB_FRONTEND_PORT=23000
# The cookie jar below talks plain HTTP.
export COOKIE_SECURE=false
# Fast polling and event arrival, so data shows within seconds.
export POLL_INTERVAL_MS=500 BMKG_EVENT_INTERVAL_S=3 PVMBG_EVENT_INTERVAL_S=3

jar=$(mktemp)
cleanup() { rm -f "$jar"; $compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; $compose ps >&2 || true; exit 1; }

# service:port for every container's /health (all listen on their own port inside).
services="bmkg-mock:8081 pvmbg-mock:8082 aggregator:8091 auth-service:8092 client-facing-api:8080 dashboard-updater:8093 notifier:8094 audit-logger:8095"

# The consumers publish no host port, so every /health is asked from inside its container.
healthy() { $compose exec -T "${1%%:*}" wget -qO- "http://localhost:${1##*:}/health" >/dev/null 2>&1; }

wait_until() { # wait_until <seconds> <description> <command...>
  local deadline=$((SECONDS + $1)) what=$2
  shift 2
  until "$@"; do
    ((SECONDS < deadline)) || fail "timed out waiting for $what"
    sleep 1
  done
}

all_healthy() { for s in $services; do healthy "$s" || return 1; done; }

started_at() { docker inspect -f '{{.State.StartedAt}}' "$($compose ps -q "$1")"; }

hazards_flow() { # the mocks' data has reached /hazards through the Aggregator
  curl -fsS -b "$jar" "http://127.0.0.1:$CLIENT_FACING_API_PORT/hazards?type=${1:-SEISMIC}" | grep -q '"hazard_id"'
}

echo "== up"
$compose down -v --remove-orphans >/dev/null 2>&1 || true
$compose up -d --build --wait --wait-timeout 300 || fail "docker compose up"
wait_until 120 "every service's /health" all_healthy
echo "ok: every /health answers"

echo "== golden path"
curl -fsS -c "$jar" -X POST "http://127.0.0.1:$CLIENT_FACING_API_PORT/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"$SEED_OPERATOR_EMAIL\",\"password\":\"$SEED_OPERATOR_PASSWORD\"}" >/dev/null || fail "login as the seeded Operator"
wait_until 60 "seismic hazards on /hazards" hazards_flow SEISMIC
wait_until 60 "volcanic hazards on /hazards" hazards_flow VOLCANIC
echo "ok: login, then /hazards returns data from both mocks"

echo "== independent restart"
others=$(for s in $services; do [ "${s%%:*}" = notifier ] || echo "${s%%:*}"; done)
declare -A before
for s in $others; do before[$s]=$(started_at "$s"); done

$compose restart notifier >/dev/null
wait_until 60 "notifier to come back" healthy notifier:8094
wait_until 30 "every /health after the restart" all_healthy
for s in $others; do
  [ "${before[$s]}" = "$(started_at "$s")" ] || fail "$s was restarted along with notifier"
done
hazards_flow SEISMIC || fail "/hazards stopped answering during the notifier restart"
echo "ok: restarting notifier left every other service up and untouched"

echo "PASS"
