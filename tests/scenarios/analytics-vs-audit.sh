#!/usr/bin/env bash
# Checks that /analytics totals equal the totals counted from GET /internal/audit/events.
# Events are counted the same way on both sides: since <= ingested_at <= as_of.
#
#   API_URL=http://localhost:8080 AUDIT_URL=http://localhost:8091 AUDIT_TOKEN=change-me \
#   SEED_OPERATOR_EMAIL=operator@bnpb.example SEED_OPERATOR_PASSWORD=change-me tests/scenarios/analytics-vs-audit.sh [1h|6h|24h|7d]
set -euo pipefail

api=${API_URL:-http://localhost:8080}
audit=${AUDIT_URL:-http://localhost:8091}
token=${AUDIT_TOKEN:?set AUDIT_TOKEN}
email=${SEED_OPERATOR_EMAIL:?set SEED_OPERATOR_EMAIL}
password=${SEED_OPERATOR_PASSWORD:?set SEED_OPERATOR_PASSWORD}
range=${1:-7d}

jar=$(mktemp)
items=$(mktemp)
trap 'rm -f "$jar" "$items"' EXIT

curl -sf -c "$jar" -H 'Content-Type: application/json' \
  -d "$(jq -n --arg e "$email" --arg p "$password" '{email:$e,password:$p}')" "$api/auth/login" >/dev/null
a=$(curl -sf -b "$jar" "$api/analytics?range=$range")

as_of=$(jq -r .as_of <<<"$a")
case $range in 1h) secs=3600 ;; 6h) secs=21600 ;; 24h) secs=86400 ;; 7d) secs=604800 ;; *) echo "bad range"; exit 2 ;; esac
end=$(date -u -d "$as_of" +%s)
since=$(date -u -d "@$((end - secs))" +%Y-%m-%dT%H:%M:%SZ)

cursor=""
while :; do
  page=$(curl -sf -H "Authorization: Bearer $token" -G "$audit/internal/audit/events" \
    --data-urlencode "since=$since" --data-urlencode "limit=1000" --data-urlencode "cursor=$cursor")
  jq -c --arg end "$as_of" '.items[] | select((.ingested_at | rtrimstr("Z")) <= ($end | rtrimstr("Z"))) | {source}' <<<"$page" >>"$items"
  cursor=$(jq -r '.next_cursor // empty' <<<"$page")
  [ -n "$cursor" ] || break
done

ok=1
want=$(wc -l <"$items" | tr -d ' ')
got=$(jq '[.series[].count] | add // 0' <<<"$a")
echo "total: analytics $got, audit $want (range $range, as_of $as_of)"
[ "$got" = "$want" ] || ok=0

for s in $( (jq -r '.by_source[].key' <<<"$a"; jq -r .source <"$items") | sort -u); do
  g=$(jq --arg s "$s" '[.by_source[] | select(.key == $s) | .count] | add // 0' <<<"$a")
  w=$(jq -r --arg s "$s" 'select(.source == $s) | 1' <"$items" | wc -l | tr -d ' ')
  echo "$s: analytics $g, audit $w"
  [ "$g" = "$w" ] || ok=0
done

[ "$(jq .partial <<<"$a")" = "false" ] || echo "note: analytics says partial, numbers can differ"

if [ "$ok" = 1 ]; then echo PASS; else echo FAIL; exit 1; fi
