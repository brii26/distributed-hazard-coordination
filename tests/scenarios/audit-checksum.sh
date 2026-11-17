#!/usr/bin/env bash
# Checks the Audit checksum against an independent sort | sha256sum over every id the events endpoint returns.
#
#   AUDIT_URL=http://localhost:8091 AUDIT_TOKEN=change-me tests/scenarios/audit-checksum.sh [BMKG|PVMBG]
set -euo pipefail

url=${AUDIT_URL:-http://localhost:8091}
token=${AUDIT_TOKEN:?set AUDIT_TOKEN}
source=${1:-BMKG}
auth=(-H "Authorization: Bearer $token")

ids=$(mktemp)
trap 'rm -f "$ids"' EXIT

cursor=""
while :; do
  page=$(curl -sf "${auth[@]}" -G "$url/internal/audit/events" \
    --data-urlencode "source=$source" --data-urlencode "limit=1000" --data-urlencode "cursor=$cursor")
  jq -r '.items[].source_ref_id' <<<"$page" >>"$ids"
  cursor=$(jq -r '.next_cursor // empty' <<<"$page")
  [ -n "$cursor" ] || break
done

want_count=$(wc -l <"$ids" | tr -d ' ')
want_sum=$(printf '%s' "$(LC_ALL=C sort "$ids" | paste -sd'\n')" | sha256sum | cut -d' ' -f1)

got=$(curl -sf "${auth[@]}" "$url/internal/audit/checksum?source=$source")
got_count=$(jq -r .count <<<"$got")
got_sum=$(jq -r .checksum <<<"$got")

echo "events: $want_count ids, independent sha256 $want_sum"
echo "checksum endpoint: $got_count ids, $got_sum"

if [ "$want_count" = "$got_count" ] && [ "$want_sum" = "$got_sum" ]; then echo "PASS"; else echo "FAIL"; exit 1; fi
