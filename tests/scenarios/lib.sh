#!/usr/bin/env bash
# Shared helpers for the problem scripts. Source it, don't run it.
# Each script runs against a stack that is already up (docker compose up -d --build --wait).
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
[ -f .env ] || { echo "No .env here. Run: cp .env.example .env" >&2; exit 2; }
set -a; . ./.env; set +a

API=http://localhost:${CLIENT_FACING_API_PORT:-8080}
BMKG=http://localhost:${BMKG_PORT:-8081}
PVMBG=http://localhost:${PVMBG_PORT:-8082}
JARS=$(mktemp -d)
FAILED=0

cleanup_hooks=()
trap 'for h in "${cleanup_hooks[@]}"; do eval "$h"; done; rm -rf "$JARS"' EXIT

curl -sf "$API/health" >/dev/null || { echo "Stack is not up at $API. Run: docker compose up -d --build --wait" >&2; exit 2; }

step() { printf '\n== %s\n' "$*"; }
show() { printf '$ %s\n' "$*"; }

# expect <description> <actual> <expected>
expect() {
  if [ "$2" = "$3" ]; then echo "PASS: $1 (got: $2)"; else echo "FAIL: $1 (expected: $3, got: $2)"; FAILED=1; fi
}

# expect_true <description> <command...>
expect_true() {
  local what=$1; shift
  if "$@"; then echo "PASS: $what"; else echo "FAIL: $what"; FAILED=1; fi
}

# wait_until <seconds> <command...>, returns non-zero on timeout
wait_until() {
  local deadline=$((SECONDS + $1)); shift
  until "$@"; do ((SECONDS < deadline)) || return 1; sleep 1; done
}

# login <name> <email> <password>, cookies go to $JARS/<name>.jar
login() { curl -s -c "$JARS/$1.jar" -X POST "$API/auth/login" -d "{\"email\":\"$2\",\"password\":\"$3\"}"; echo; }

# code <curl args...> prints only the HTTP status
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

# hazards <jar> <query>, prints the JSON body
hazards() { curl -s -b "$JARS/$1.jar" "$API/hazards?$2"; }

# started <service>, container start time; unchanged means it was never restarted
started() { docker inspect -f '{{.State.StartedAt}}' "$(docker compose ps -q "$1")"; }

# psql <sql> in the Canonical Store
psql_canonical() { docker compose exec -T canonical-store psql -U "$CANONICAL_DB_USER" -d "$CANONICAL_DB_NAME" -tAc "$1"; }

finish() {
  echo
  if [ "$FAILED" = 0 ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; exit 1; fi
}
