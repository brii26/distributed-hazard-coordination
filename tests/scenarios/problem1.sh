#!/usr/bin/env bash
# Problem 1: PVMBG adds a field while the system runs; the Aggregator must keep producing HazardEvent without a restart.
#
#   tests/scenarios/problem1.sh
. "$(dirname "$0")/lib.sh"

login op "$SEED_OPERATOR_EMAIL" "$SEED_OPERATOR_PASSWORD" >/dev/null
confidence='[.data[] | {area_name, severity, confidence: .attributes.confidence_level}]'

step "Switch PVMBG back to the legacy schema"
for _ in 1 2; do
  v=$(curl -s -X POST "$PVMBG/admin/schema-version" -H "X-Admin-Key: $PVMBG_ADMIN_KEY" | jq -r .schema_version)
  [ "$v" = legacy ] && break
done
expect "PVMBG is on the legacy schema" "$v" legacy
sleep 15

step "Before the change: newest volcanic reports have no confidence_level"
show "curl -s -b op.jar \"\$API/hazards?type=VOLCANIC&limit=3\" | jq '$confidence'"
hazards op "type=VOLCANIC&limit=3" | jq "$confidence"
aggregator_before=$(started aggregator)

step "PVMBG adds confidence_level"
show "curl -s -X POST \$PVMBG/admin/schema-version -H \"X-Admin-Key: \$PVMBG_ADMIN_KEY\""
v=$(curl -s -X POST "$PVMBG/admin/schema-version" -H "X-Admin-Key: $PVMBG_ADMIN_KEY" | jq -r .schema_version)
expect "PVMBG is on the extended schema" "$v" extended
cleanup_hooks+=("curl -s -o /dev/null -X POST $PVMBG/admin/schema-version -H 'X-Admin-Key: $PVMBG_ADMIN_KEY'")

step "After the change: new reports carry confidence_level"
has_confidence() { hazards op "type=VOLCANIC&limit=3" | jq -e '[.data[] | select(.attributes.confidence_level != null)] | length > 0' >/dev/null; }
wait_until 60 has_confidence
show "curl -s -b op.jar \"\$API/hazards?type=VOLCANIC&limit=3\" | jq '$confidence'"
hazards op "type=VOLCANIC&limit=3" | jq "$confidence"
expect_true "a volcanic report has confidence_level in attributes" has_confidence
expect "Aggregator was not restarted" "$(started aggregator)" "$aggregator_before"

step "Canonical Store: records before and after the change in the same table"
show "SELECT source_ref_id, severity, attributes ? 'confidence_level' AS has_confidence, ingested_at FROM hazard_events WHERE hazard_type = 'VOLCANIC' ORDER BY ingested_at DESC LIMIT 6"
docker compose exec -T canonical-store psql -U "$CANONICAL_DB_USER" -d "$CANONICAL_DB_NAME" -c \
  "SELECT source_ref_id, severity, attributes ? 'confidence_level' AS has_confidence, ingested_at FROM hazard_events WHERE hazard_type = 'VOLCANIC' ORDER BY ingested_at DESC LIMIT 6"
expect_true "older records without the field are still stored" \
  test "$(psql_canonical "SELECT count(*) FROM hazard_events WHERE hazard_type='VOLCANIC' AND NOT attributes ? 'confidence_level'")" -gt 0

finish
