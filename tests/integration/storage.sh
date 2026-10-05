#!/usr/bin/env bash
# Integration tests for one service plus its own storage or broker: starts a real
# User DB, Canonical Store (both with the project's migrations) and RabbitMQ,
# runs the suites that need them, then removes the containers.
#
#   tests/integration/storage.sh
#
# Suites: the Auth Service store against the User DB, the Aggregator store against
# the Canonical Store, and the Aggregator publisher against RabbitMQ. Each one skips
# when its env var is unset, so a plain `go test ./...` stays offline.
#
# Needs Docker and Go. Values come from .env.example, so no .env is needed.
set -euo pipefail

cd "$(dirname "$0")/../.."
root=$PWD

# shellcheck disable=SC1091
set -a; . ./.env.example; set +a

prefix=hazard-storage-it
userdb_port=25433 canonical_port=25432 rabbit_port=25673

cleanup() { docker rm -f $prefix-userdb $prefix-canonical $prefix-rabbitmq >/dev/null 2>&1 || true; }
cleanup
trap cleanup EXIT

docker run -d --name $prefix-userdb -p 127.0.0.1:$userdb_port:5432 \
  -e POSTGRES_USER="$USER_DB_USER" -e POSTGRES_PASSWORD="$USER_DB_PASSWORD" -e POSTGRES_DB="$USER_DB_NAME" \
  -e SEED_OPERATOR_EMAIL -e SEED_OPERATOR_PASSWORD -e SECRET_SALT \
  -v "$root/services/auth-service/migrations:/docker-entrypoint-initdb.d:ro" postgres:16-alpine >/dev/null

docker run -d --name $prefix-canonical -p 127.0.0.1:$canonical_port:5432 \
  -e POSTGRES_USER="$CANONICAL_DB_USER" -e POSTGRES_PASSWORD="$CANONICAL_DB_PASSWORD" -e POSTGRES_DB="$CANONICAL_DB_NAME" \
  -v "$root/services/aggregator/migrations:/docker-entrypoint-initdb.d:ro" postgres:16-alpine >/dev/null

docker run -d --name $prefix-rabbitmq -p 127.0.0.1:$rabbit_port:5672 rabbitmq:3.13-alpine >/dev/null

# Postgres only listens on TCP once its init scripts (the migrations) are done.
wait_for() {
  local what=$1
  shift
  for _ in $(seq 90); do
    "$@" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "$what did not become ready" >&2
  exit 1
}

wait_for "User DB" docker exec $prefix-userdb pg_isready -h 127.0.0.1 -U "$USER_DB_USER"
wait_for "Canonical Store" docker exec $prefix-canonical pg_isready -h 127.0.0.1 -U "$CANONICAL_DB_USER"
wait_for "RabbitMQ" docker exec $prefix-rabbitmq rabbitmq-diagnostics -q check_running

status=0

echo "== Auth Service store against the User DB"
(cd services/auth-service &&
  USER_DB_TEST_URL="postgres://$USER_DB_USER:$USER_DB_PASSWORD@127.0.0.1:$userdb_port/$USER_DB_NAME?sslmode=disable" \
    go test -count=1 -race -v -run '^TestStore' ./...) || status=1

echo "== Aggregator store against the Canonical Store, publisher against RabbitMQ"
(cd services/aggregator &&
  CANONICAL_DB_TEST_URL="postgres://$CANONICAL_DB_USER:$CANONICAL_DB_PASSWORD@127.0.0.1:$canonical_port/$CANONICAL_DB_NAME?sslmode=disable" \
    RABBITMQ_TEST_URL="amqp://guest:guest@127.0.0.1:$rabbit_port/" \
    go test -count=1 -race -v ./...) || status=1

exit $status
