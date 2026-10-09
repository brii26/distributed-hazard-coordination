#!/usr/bin/env bash
# Problem 2: a slow or dead source must not hold up requests for the other one.
#
#   tests/scenarios/problem2.sh               latency and outage checks (about 3 minutes)
#   tests/scenarios/problem2.sh --load        also run the k6 load test (adds about 3 minutes, brings up its own stack)
#   tests/scenarios/problem2.sh --destructive also show the 503 with nothing stored (deletes stored volcanic reports)
. "$(dirname "$0")/lib.sh"

load=0 destructive=0
for a in "$@"; do
  case $a in --load) load=1 ;; --destructive) destructive=1 ;; *) echo "unknown option $a" >&2; exit 2 ;; esac
done

login op "$SEED_OPERATOR_EMAIL" "$SEED_OPERATOR_PASSWORD" >/dev/null
admin=(-H "X-Admin-Key: $PVMBG_ADMIN_KEY")
# The endpoint toggles, so only switch the outage off when it is on. The exit hook and the recovery step both call this.
outage_off() { [ "$(curl -s "$PVMBG/health" | jq -r .outage_active)" = true ] && curl -s -o /dev/null -X POST "$PVMBG/admin/outage" "${admin[@]}"; return 0; }
status() { hazards op "type=$1&limit=1" | jq -r '.meta.source_status'; }
volcanic_up() { [ "$(status VOLCANIC)" = UP ]; }
cleanup_hooks+=("docker compose up -d --wait pvmbg-mock >/dev/null 2>&1")

step "Make sure PVMBG is not in outage"
state=$(curl -s -X POST "$PVMBG/admin/outage" "${admin[@]}" | jq -r .outage_active)
[ "$state" = true ] && outage_off
wait_until 120 volcanic_up

step "PVMBG answers after 3 s; seismic requests must stay fast"
show "PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 docker compose up -d --wait pvmbg-mock"
PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 docker compose up -d --wait pvmbg-mock 2>&1 | tail -3
for i in 1 2 3 4 5; do
  t=$(curl -s -o /dev/null -w '%{time_total}' -b "$JARS/op.jar" "$API/hazards?type=SEISMIC")
  echo "seismic request $i: ${t}s"
  expect_true "seismic request $i under 0.3 s" awk "BEGIN{exit !($t < 0.3)}"
done
docker compose up -d --wait pvmbg-mock >/dev/null 2>&1
wait_until 120 volcanic_up

if [ "$load" = 1 ]; then
  step "k6 load test"
  show "tests/load/run.sh"
  expect_true "load test thresholds hold" tests/load/run.sh
fi

step "Outage: PVMBG goes down"
show "curl -s -X POST \$PVMBG/admin/outage -H \"X-Admin-Key: \$PVMBG_ADMIN_KEY\""
cleanup_hooks+=("outage_off")
expect "outage is active" "$(curl -s -X POST "$PVMBG/admin/outage" "${admin[@]}" | jq -r .outage_active)" true
is_down() { [ "$(status VOLCANIC)" = DOWN ]; }
wait_until 60 is_down
show "curl -s -b op.jar \"\$API/hazards?type=VOLCANIC&limit=1\" | jq .meta"
hazards op "type=VOLCANIC&limit=1" | jq .meta
show "curl -s -b op.jar \"\$API/hazards?type=SEISMIC&limit=1\" | jq .meta"
hazards op "type=SEISMIC&limit=1" | jq .meta
expect "volcanic source is DOWN" "$(status VOLCANIC)" DOWN
expect_true "volcanic data is still served with stale_since set" \
  bash -c "curl -s -b '$JARS/op.jar' '$API/hazards?type=VOLCANIC&limit=1' | jq -e '(.data | length > 0) and .meta.stale_since != null' >/dev/null"
expect "seismic source stays UP" "$(status SEISMIC)" UP

if [ "$destructive" = 1 ]; then
  step "Nothing stored for the source: explicit 503"
  show "DELETE FROM hazard_events WHERE hazard_type = 'VOLCANIC'"
  psql_canonical "DELETE FROM hazard_events WHERE hazard_type = 'VOLCANIC'"
  expect "volcanic request without stored data" "$(code -b "$JARS/op.jar" "$API/hazards?type=VOLCANIC")" 503
fi

step "Recovery: PVMBG comes back, nothing restarted"
aggregator_before=$(started aggregator)
outage_off
if wait_until 90 volcanic_up; then echo "PVMBG UP again"; fi
show "curl -s -b op.jar \"\$API/hazards?type=VOLCANIC&limit=1\" | jq .meta"
hazards op "type=VOLCANIC&limit=1" | jq .meta
expect "volcanic source is UP again" "$(status VOLCANIC)" UP
expect "stale_since is cleared" "$(hazards op 'type=VOLCANIC&limit=1' | jq -r '.meta.stale_since')" null
expect "Aggregator was not restarted" "$(started aggregator)" "$aggregator_before"

finish
