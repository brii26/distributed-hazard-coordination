#!/usr/bin/env bash
# Creates the ConfigMap rabbitmq-definitions from infra/rabbitmq. Safe to run again.
# Restart the broker afterwards (kubectl rollout restart statefulset/rabbitmq) to load a change.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
SRC="$ROOT/infra/rabbitmq"

kubectl apply -f "$HERE/namespace.yaml" >/dev/null
kubectl create configmap rabbitmq-definitions --namespace "$NAMESPACE" \
  --from-file="$SRC/definitions.json.tpl" --from-file="$SRC/render-definitions.sh" --from-file="$SRC/rabbitmq.conf" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "configmap/rabbitmq-definitions (3 files)"
