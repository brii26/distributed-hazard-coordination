#!/usr/bin/env bash
# Problem 2 load test: full stack in an isolated compose project with PVMBG answering after 3 s, then k6 runs tests/load/p2.js. The summary is saved under tests/load/results/.
#
#   tests/load/run.sh
#
# Needs docker compose. Values come from .env.example, so no .env is needed.
# MIXED_VUS and MIXED_DURATION override the 50 connections / 60 s of the mixed run.
set -euo pipefail

cd "$(dirname "$0")/../.."
root=$PWD

compose="docker compose -p hazard-load --env-file $root/.env.example -f $root/docker-compose.yml"

# shellcheck disable=SC1091
set -a; . ./.env.example; set +a

export CLIENT_FACING_API_PORT=38080 BMKG_PORT=38081 PVMBG_PORT=38082 RABBITMQ_MANAGEMENT_PORT=35672 WEB_FRONTEND_PORT=33000 JAEGER_UI_PORT=36686
export COOKIE_SECURE=false
export ACCESS_TOKEN_TTL_S=3600
export PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000

cleanup() { $compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

cleanup
$compose up -d --build --wait --wait-timeout 300

sleep 10

out=tests/load/results
mkdir -p "$out"
stamp=$(date +%Y%m%d-%H%M%S)

docker run --rm --network host -u "$(id -u):$(id -g)" \
  -v "$root/tests/load:/load" \
  -e BASE_URL="http://127.0.0.1:$CLIENT_FACING_API_PORT" \
  -e EMAIL="$SEED_OPERATOR_EMAIL" -e PASSWORD="$SEED_OPERATOR_PASSWORD" \
  -e MIXED_VUS="${MIXED_VUS:-50}" -e MIXED_DURATION="${MIXED_DURATION:-60s}" \
  "${K6_IMAGE:-grafana/k6:latest}" run --summary-export "/load/results/p2-$stamp.json" /load/p2.js \
  2>&1 | tee "$out/p2-$stamp.txt"

echo "saved: $out/p2-$stamp.txt and $out/p2-$stamp.json"
