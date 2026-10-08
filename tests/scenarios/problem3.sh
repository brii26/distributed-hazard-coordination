#!/usr/bin/env bash
# Problem 3: separate credentials per source, per-role field access, and token refresh for Tim Lapangan.
#
#   tests/scenarios/problem3.sh      takes about 80 s (it waits for a token to expire)
. "$(dirname "$0")/lib.sh"

login op "$SEED_OPERATOR_EMAIL" "$SEED_OPERATOR_PASSWORD" >/dev/null

step "Each source accepts only its own credentials"
show "curl \$BMKG/seismic-events (no key)"
expect "BMKG without a key" "$(code "$BMKG/seismic-events")" 401
show "curl -H \"X-BMKG-Key: \$PVMBG_ADMIN_KEY\" \$BMKG/seismic-events"
expect "BMKG with the PVMBG key" "$(code -H "X-BMKG-Key: $PVMBG_ADMIN_KEY" "$BMKG/seismic-events")" 403
show "curl -H \"Authorization: Bearer \$BMKG_API_KEY\" \$PVMBG/volcanic-reports"
expect "PVMBG with the BMKG key as token" "$(code -H "Authorization: Bearer $BMKG_API_KEY" "$PVMBG/volcanic-reports")" 401

step "Media sees the summary only"
reg=$(code -X POST "$API/auth/register" -d '{"email":"media@demo.example","password":"media-pass-1"}')
echo "register Media account: $reg (201 first time, 409 on a rerun)"
login media media@demo.example media-pass-1
show "curl -s -b media.jar \"\$API/hazards?type=SEISMIC&limit=1\" | jq '.data[0] | keys'"
hazards media "type=SEISMIC&limit=1" | jq '.data[0] | keys'
expect "raw fields are absent for Media" \
  "$(hazards media 'type=SEISMIC&limit=1' | jq '.data[0] | has("latitude") or has("longitude") or has("attributes")')" false
show "curl -b media.jar \"\$API/hazards?type=SEISMIC&fields=latitude\""
expect "Media asking for latitude" "$(code -b "$JARS/media.jar" "$API/hazards?type=SEISMIC&fields=latitude")" 403
step "Operator sees the raw fields"
expect "raw fields are present for the Operator" \
  "$(hazards op 'type=SEISMIC&limit=1' | jq '.data[0] | has("latitude") and has("longitude") and has("attributes")')" true

step "Tim Lapangan: the token expires and is refreshed behind the next request"
create=$(code -b "$JARS/op.jar" -X POST "$API/accounts" -d '{"email":"field@demo.example","password":"field-pass-1","role":"Lapangan"}')
echo "create Tim Lapangan account: $create (201 first time, 409 on a rerun)"
login field field@demo.example field-pass-1
cp "$JARS/field.jar" "$JARS/field-old.jar"
wait=$((${ACCESS_TOKEN_TTL_S:-60} + 5))
echo "waiting ${wait}s for the access token to expire..."
sleep "$wait"
show "curl -s -i -b field.jar -c field.jar \"\$API/hazards?type=SEISMIC&limit=1\" | grep -iE '^HTTP|^set-cookie: (jwt|refresh_token)'"
resp=$(curl -s -i -b "$JARS/field.jar" -c "$JARS/field.jar" "$API/hazards?type=SEISMIC&limit=1")
echo "$resp" | grep -iE '^HTTP|^set-cookie: (jwt|refresh_token)' | sed -E 's/(=[^;]{12})[^;]*/\1.../'
expect "expired session answered" "$(echo "$resp" | head -1 | awk '{print $2}')" 200
expect_true "new jwt cookie was set" grep -qi '^set-cookie: jwt=' <<<"$resp"
expect_true "rotated refresh_token cookie was set" grep -qi '^set-cookie: refresh_token=' <<<"$resp"
show "replay the old cookies"
expect "old cookies are rejected" "$(code -b "$JARS/field-old.jar" "$API/hazards?type=SEISMIC&limit=1")" 401

finish
