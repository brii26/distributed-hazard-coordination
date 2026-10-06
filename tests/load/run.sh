#!/usr/bin/env bash
# Problem 2 load test: brings up the full stack in an isolated compose project with
# PVMBG answering after 3 s, then runs tests/load/p2.js with k6 (from its Docker
# image, nothing to install). The summary is saved under tests/load/results/ for
# the report (doc/TRD_M1.md 12).
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

# Host ports clear of a normal `docker compose up` running next to this.
export CLIENT_FACING_API_PORT=38080 BMKG_PORT=38081 PVMBG_PORT=38082 RABBITMQ_MANAGEMENT_PORT=35672
export COOKIE_SECURE=false
# The token must outlive the run, so no refresh is needed mid-test.
export ACCESS_TOKEN_TTL_S=3600
# The slow source of Problem 2: PVMBG answers after 3 s.
export PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000

cleanup() { $compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

cleanup
$compose up -d --build --wait --wait-timeout 300

# Let the first polls land, so /hazards has data when the test starts.
sleep 10

out=tests/load/results
mkdir -p "$out"
stamp=$(date +%Y%m%d-%H%M%S)

# Host networking: k6 reaches the published Client-Facing API port directly.
docker run --rm --network host -u "$(id -u):$(id -g)" \
  -v "$root/tests/load:/load" \
  -e BASE_URL="http://127.0.0.1:$CLIENT_FACING_API_PORT" \
  -e EMAIL="$SEED_OPERATOR_EMAIL" -e PASSWORD="$SEED_OPERATOR_PASSWORD" \
  -e MIXED_VUS="${MIXED_VUS:-50}" -e MIXED_DURATION="${MIXED_DURATION:-60s}" \
  "${K6_IMAGE:-grafana/k6:latest}" run --summary-export "/load/results/p2-$stamp.json" /load/p2.js \
  2>&1 | tee "$out/p2-$stamp.txt"

echo "saved: $out/p2-$stamp.txt and $out/p2-$stamp.json"
