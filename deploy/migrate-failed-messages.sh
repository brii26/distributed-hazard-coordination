#!/usr/bin/env bash
# One-off for a cluster deployed before #409: failed_messages moves from the store nodes to coordination-db.
# Creates the table in coordination-db (its init scripts only run on an empty volume), then copies the rows of every
# store pod. A message_id already in coordination-db is kept as it is. Safe to run again. Run it before the new
# Aggregator image takes traffic, or again right after, to pick up rows parked on the nodes in between.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS="$HERE/../services/aggregator/migrations"
NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
COORD=coordination-db-0

coord_psql() {
  kubectl -n "$NS" exec -i "$COORD" -- sh -c 'psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"' sh "$@"
}

for f in 003_failed_messages.sql 016_failed_messages_coordination.sql; do
  coord_psql < "$MIGRATIONS/$f"
done

for pod in $(kubectl -n "$NS" get pods -l app=store -o name); do
  rows=$(kubectl -n "$NS" exec "$pod" -- sh -c 'psql -tA -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM failed_messages"' 2>/dev/null || echo 0)
  if [ "${rows:-0}" = 0 ]; then
    echo "$pod: 0 rows"
    continue
  fi

  kubectl -n "$NS" exec "$pod" -- sh -c 'psql -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "COPY failed_messages TO STDOUT"' |
    coord_psql -c "CREATE TEMP TABLE incoming (LIKE failed_messages)" -c "COPY incoming FROM STDIN" \
      -c "INSERT INTO failed_messages SELECT * FROM incoming ON CONFLICT (message_id) DO NOTHING"
  echo "$pod: $rows rows copied"
done

coord_psql -tA -c "SELECT 'coordination-db: ' || count(*) || ' rows' FROM failed_messages"
