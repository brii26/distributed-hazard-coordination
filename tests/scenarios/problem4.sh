#!/usr/bin/env bash
# Problem 4: each service is its own container, and each database has exactly one owner.
#
#   tests/scenarios/problem4.sh
. "$(dirname "$0")/lib.sh"

login op "$SEED_OPERATOR_EMAIL" "$SEED_OPERATOR_PASSWORD" >/dev/null
running() { [ "$(docker compose ps --status running --format '{{.Service}}' | grep -cx "$1")" = 1 ]; }
cleanup_hooks+=("docker compose up -d --no-deps --wait notifier >/dev/null 2>&1")

step "Stop one container; the rest keep running"
aggregator_before=$(started aggregator) api_before=$(started client-facing-api)
show "docker compose stop notifier"
docker compose stop notifier 2>&1 | tail -1
show "docker compose ps --format '{{.Service}} {{.Status}}'"
docker compose ps --format '{{.Service}} {{.Status}}'
expect_true "notifier is not running" bash -c '! docker compose ps --status running --format "{{.Service}}" | grep -qx notifier'
expect_true "aggregator is running" running aggregator
expect_true "client-facing-api is running" running client-facing-api
show "curl -s -b op.jar \"\$API/hazards?type=SEISMIC&limit=1\" | jq '.meta.source_status'"
hazards op "type=SEISMIC&limit=1" | jq '.meta.source_status'
expect "API keeps serving while the Notifier is down" "$(hazards op 'type=SEISMIC&limit=1' | jq -r '.meta.source_status')" UP

step "Rebuild and restart only the Notifier"
show "docker compose up -d --build --no-deps --wait notifier"
docker compose up -d --build --no-deps --wait notifier 2>&1 | tail -2
expect_true "notifier is running again" running notifier
expect "Aggregator was not restarted" "$(started aggregator)" "$aggregator_before"
expect "Client-Facing API was not restarted" "$(started client-facing-api)" "$api_before"

step "Storage isolation: who can reach each database"
reach() { docker compose exec -T "$1" sh -c "nc -zw2 $2 5432 >/dev/null 2>&1 && echo reachable || echo unreachable"; }
canonical_ok=1 userdb_ok=1
for s in aggregator auth-service bmkg-mock pvmbg-mock rabbitmq client-facing-api web-frontend dashboard-updater notifier audit-logger; do
  c=$(reach "$s" canonical-store) u=$(reach "$s" user-db)
  echo "$s  canonical-store: $c  user-db: $u"
  [ "$c" = reachable ] && [ "$s" != aggregator ] && canonical_ok=0
  [ "$c" = unreachable ] && [ "$s" = aggregator ] && canonical_ok=0
  [ "$u" = reachable ] && [ "$s" != auth-service ] && userdb_ok=0
  [ "$u" = unreachable ] && [ "$s" = auth-service ] && userdb_ok=0
done
expect "only the Aggregator reaches the Canonical Store" "$canonical_ok" 1
expect "only the Auth Service reaches the User DB" "$userdb_ok" 1

finish
