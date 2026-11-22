#!/usr/bin/env bash
# Copies the failed_messages rows that sit on the Canonical Store nodes into the coordination database.
# Run it once after deploying the Aggregator that reads failed_messages from the coordination database. Safe to run again:
# a row that is already there is left as it is.
set -euo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
DBUSER="$(kubectl -n "$NS" get secret store-credentials -o jsonpath='{.data.user}' | base64 -d)"
COLS="message_id, source, received_at, payload, payload_sha256, payload_size, oversize, reason, retryable, attempts, status, source_ref_id, last_error_at, mapped_event, next_attempt_at, correlation_id"
dump="$(mktemp)"
trap 'rm -f "$dump"' EXIT

{
  echo "CREATE TEMP TABLE m (LIKE failed_messages);"
  for pod in $(kubectl -n "$NS" get pods -l app=store,role=store -o name); do
    echo "COPY m ($COLS) FROM STDIN WITH (FORMAT csv);"
    kubectl -n "$NS" exec "$pod" -- psql -U "$DBUSER" -d "${STORE_DB_NAME:-canonical_store}" -qc "\\copy (SELECT $COLS FROM failed_messages) TO STDOUT WITH (FORMAT csv)"
    echo '\.'
  done
  echo "INSERT INTO failed_messages ($COLS) SELECT $COLS FROM m ON CONFLICT (message_id) DO NOTHING;"
  echo "SELECT status, count(*) FROM failed_messages GROUP BY 1 ORDER BY 1;"
} >"$dump"

kubectl -n "$NS" exec -i coordination-db-0 -- psql -U "$DBUSER" -d "${COORD_DB_NAME:-coordination}" -q <"$dump"
