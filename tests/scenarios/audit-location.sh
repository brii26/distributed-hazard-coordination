#!/usr/bin/env bash
# Checks that every node named by the Audit location route is a real Kubernetes node and that the two copies sit on different nodes.
#
#   AUDIT_URL=http://localhost:8091 AUDIT_TOKEN=change-me tests/scenarios/audit-location.sh [HAZARD_ID]
# Without an id it takes the first one from the events route.
set -euo pipefail

url=${AUDIT_URL:-http://localhost:8091}
token=${AUDIT_TOKEN:?set AUDIT_TOKEN}
auth=(-H "Authorization: Bearer $token")

id=${1:-$(curl -sf "${auth[@]}" "$url/internal/audit/events?limit=1" | jq -r '.items[0].hazard_id')}
[ -n "$id" ] && [ "$id" != null ] || { echo "no hazard to look up"; exit 1; }

got=$(curl -sf "${auth[@]}" -G "$url/internal/audit/location" --data-urlencode "hazard_id=$id")
echo "$got"

nodes=$(kubectl get nodes -o name | sed 's|^node/||')
distinct=$(jq -r '.locations[].node' <<<"$got" | sort -u | wc -l | tr -d ' ')
count=$(jq '.locations | length' <<<"$got")
ok=1

while read -r n; do
  grep -qx "$n" <<<"$nodes" || { echo "node $n is not in kubectl get nodes"; ok=0; }
done < <(jq -r '.locations[].node' <<<"$got")

[ "$(jq -r .partial <<<"$got")" = false ] && [ "$count" = 2 ] && [ "$distinct" = 2 ] || { echo "want 2 copies on 2 nodes, not partial"; ok=0; }

if [ "$ok" = 1 ]; then echo "PASS"; else echo "FAIL"; exit 1; fi
