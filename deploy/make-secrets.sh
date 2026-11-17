#!/usr/bin/env bash
# Creates the Kubernetes Secrets from a .env file. Safe to run again.
# Usage: deploy/make-secrets.sh [env-file]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
ENV_FILE="${1:-$ROOT/.env}"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"

[ -f "$ENV_FILE" ] || { echo "no env file at $ENV_FILE (cp .env.example .env first)" >&2; exit 1; }

declare -A V
while IFS= read -r line || [ -n "$line" ]; do
  line="${line%$'\r'}"
  case "$line" in '' | '#'*) continue ;; esac
  key="${line%%=*}"
  val="${line#*=}"
  [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
  if [[ "$val" =~ ^\"(.*)\"$ || "$val" =~ ^\'(.*)\'$ ]]; then val="${BASH_REMATCH[1]}"; fi
  V[$key]="$val"
done < "$ENV_FILE"

need() { [ -n "${V[$1]:-}" ] || { echo "$1 is missing or empty in $ENV_FILE" >&2; exit 1; }; }

apply() { kubectl apply --server-side --force-conflicts --field-manager=make-secrets -f - >/dev/null; }

secret() {
  local name="$1" pair body=""
  shift
  for pair in "$@"; do
    need "${pair#*=}"
    body+="${pair%%=*}=${V[${pair#*=}]}"$'\n'
  done
  kubectl create secret generic "$name" --namespace "$NAMESPACE" \
    --from-env-file=<(printf '%s' "$body") --dry-run=client -o yaml | apply
  echo "secret/$name"
}

kubectl apply -f "$HERE/namespace.yaml" >/dev/null

secret bmkg-credentials api_key=BMKG_API_KEY
secret pvmbg-credentials client_id=PVMBG_OAUTH_CLIENT_ID client_secret=PVMBG_OAUTH_CLIENT_SECRET one_time_code=PVMBG_ONE_TIME_CODE
secret store-credentials user=CANONICAL_DB_USER password=CANONICAL_DB_PASSWORD
secret user-db-credentials user=USER_DB_USER password=USER_DB_PASSWORD
secret rabbitmq-credentials user=RABBITMQ_USER password=RABBITMQ_PASSWORD
secret auth-secrets jwt_signing_secret=JWT_SIGNING_SECRET secret_salt=SECRET_SALT seed_operator_password=SEED_OPERATOR_PASSWORD

secret audit-token token=AUDIT_TOKEN
secret pantai-token token=PANTAI_AUTH_TOKEN
secret satellite-token token=SATELLITE_GROUP_TOKEN

if [ -n "${V[PANTAI_KEY_FILE]:-}" ] && [ -f "${V[PANTAI_KEY_FILE]}" ]; then
  kubectl create secret generic pantai-key --namespace "$NAMESPACE" \
    --from-file=key="${V[PANTAI_KEY_FILE]}" --dry-run=client -o yaml | apply
else
  kubectl create secret generic pantai-key --namespace "$NAMESPACE" \
    --from-file=key=<(printf '%s' "change-me") --dry-run=client -o yaml | apply
fi
echo "secret/pantai-key"
