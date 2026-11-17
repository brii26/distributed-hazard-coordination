#!/usr/bin/env bash
# Creates the ConfigMap store-schema from the migrations. Safe to run again.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"

kubectl apply -f "$HERE/namespace.yaml" >/dev/null
kubectl create configmap store-schema --namespace "$NAMESPACE" \
  --from-file="$ROOT/services/aggregator/migrations" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "configmap/store-schema ($(ls "$ROOT/services/aggregator/migrations" | wc -l) files)"
kubectl create configmap user-db-init --namespace "$NAMESPACE" \
  --from-file="$ROOT/services/auth-service/migrations" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "configmap/user-db-init ($(ls "$ROOT/services/auth-service/migrations" | wc -l) files)"
