#!/usr/bin/env bash
# Creates the ConfigMap coordination-db-init from the coordination migrations only. Safe to run again.
# The files are taken from services/aggregator/migrations, so the tables have one definition.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
MIGRATIONS="$ROOT/services/aggregator/migrations"
FILES=(003_failed_messages.sql 004_source_lease.sql 005_source_lease_sources.sql 006_source_cursors.sql 011_shard_state.sql 012_source_token.sql 016_failed_messages_coordination.sql)

args=()
for f in "${FILES[@]}"; do
  [ -f "$MIGRATIONS/$f" ] || { echo "missing migration $f" >&2; exit 1; }
  args+=("--from-file=$MIGRATIONS/$f")
done

kubectl apply -f "$HERE/namespace.yaml" >/dev/null
kubectl create configmap coordination-db-init --namespace "$NAMESPACE" \
  "${args[@]}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "configmap/coordination-db-init (${#FILES[@]} files)"
