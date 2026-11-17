#!/usr/bin/env bash
# Critical alerts reach the Notifier within 5 seconds and none is lost while RabbitMQ and the Notifier are killed.
#
#   docker compose up -d --build --wait canonical-store rabbitmq aggregator notifier
#   tests/scenarios/critical-alerts.sh        N=90 by default, takes about 3 minutes
#
# It plays the part of the Aggregator's staging step: rows go into critical_outbox exactly as a critical Sensor Pantai
# observation would be staged, and the real relay, broker and Notifier do the rest.
set -uo pipefail

cd "$(dirname "$0")/../.."
[ -f .env ] || { echo "No .env here. Run: cp .env.example .env" >&2; exit 2; }
set -a; . ./.env; set +a

N=${N:-90}
PART=$((N / 3))
RUN=$(date +%s)
FAILED=0
DB_USER=${CANONICAL_DB_USER:-aggregator}
DB_NAME=${CANONICAL_DB_NAME:-canonical_store}

dc() { docker compose "$@"; }
metrics() { dc exec -T notifier wget -qO- http://localhost:8094/metrics; }
metric() { metrics | awk -v k="$1" '$1 == k {print $2}'; }
sql() { dc exec -T canonical-store psql -q -U "$DB_USER" -d "$DB_NAME" -c "$1" >/dev/null; }
step() { printf '\n== %s\n' "$*"; }
check() { if [ "$2" = "$3" ]; then echo "PASS: $1 (got: $2)"; else echo "FAIL: $1 (expected: $3, got: $2)"; FAILED=1; fi; }

# stage <label> <count>: stages count critical observations ingested right now.
stage() {
  sql "INSERT INTO critical_outbox (observation_id, correlation_id, body)
       SELECT 'crit-$RUN-$1-' || g, 'cycle-$RUN-$1',
              convert_to(jsonb_build_object('hazard_id', 'h-$RUN-$1-' || g, 'source', 'SENSOR_PANTAI',
                'source_ref_id', 'crit-$RUN-$1-' || g, 'hazard_type', 'COASTAL', 'severity', 'AWAS',
                'occurred_at', now(), 'ingested_at', clock_timestamp(), 'attributes', jsonb_build_object('critical', true))::text, 'UTF8')
       FROM generate_series(1, $2) g"
}

# The counters reset when the Notifier restarts, so progress is read from its log, one "critical alert sent" line per alert.
alerted() { dc logs --no-log-prefix notifier 2>/dev/null | grep '"critical alert sent"' | grep -o "crit-$RUN-[a-z0-9-]*" | sort -u | grep -c .; }
wait_alerted() { # <target> <seconds>
  local deadline=$((SECONDS + $2))
  until [ "$(alerted)" -ge "$1" ]; do ((SECONDS < deadline)) || return 1; sleep 1; done
}
latencies() { # sorted latency_ms of one phase, from the notifier log
  dc logs --no-log-prefix notifier 2>/dev/null | grep '"critical message processed"' | grep "crit-$RUN-$1-" | grep -o '"latency_ms":[0-9]*' | cut -d: -f2 | sort -n
}

metrics >/dev/null || { echo "Notifier not reachable. Start the stack first." >&2; exit 2; }

step "Normal operation: $PART alerts staged one by one"
for i in $(seq "$PART"); do stage "a$i" 1; sleep 0.2; done
wait_alerted "$PART" 30 || echo "(timed out)"
step_a=$(for i in $(seq "$PART"); do latencies "a$i"; done | sort -n)
count=$(echo "$step_a" | grep -c .)
p95=$(echo "$step_a" | awk -v n="$count" 'NR == int(n * 0.95 + 0.999) {print $1}')
echo "latency_ms p95=$p95 max=$(echo "$step_a" | tail -1) over $count alerts"
check "all normal-operation alerts delivered" "$count" "$PART"
[ "${p95:-99999}" -le 5000 ] && echo "PASS: p95 latency within 5000 ms" || { echo "FAIL: p95 latency above 5000 ms"; FAILED=1; }

step "RabbitMQ and the Notifier both down while $PART alerts are staged"
dc stop notifier rabbitmq >/dev/null 2>&1
stage b "$PART"
sleep 5
dc up -d --wait rabbitmq >/dev/null 2>&1
dc up -d notifier >/dev/null 2>&1
wait_alerted $((2 * PART)) 90 || echo "(timed out)"
check "alerts staged during the outage delivered" "$(alerted)" "$((2 * PART))"

step "RabbitMQ restarted and the Notifier killed while $((N - 2 * PART)) alerts arrive"
stage c "$((N - 2 * PART))"
dc restart rabbitmq >/dev/null 2>&1 &
dc kill notifier >/dev/null 2>&1
wait
dc up -d --wait rabbitmq >/dev/null 2>&1
dc up -d notifier >/dev/null 2>&1
wait_alerted "$N" 120 || echo "(timed out)"

step "Verdict"
sent=$(dc logs --no-log-prefix notifier 2>/dev/null | grep '"critical alert sent"' | grep -o "\"observation_id\":\"crit-$RUN-[a-z0-9-]*\"" | sort)
check "every staged observation was alerted" "$(echo "$sent" | sort -u | grep -c .)" "$N"
check "no observation was alerted twice" "$(echo "$sent" | uniq -d | grep -c .)" "0"
unsent=$(dc exec -T canonical-store psql -tA -U "$DB_USER" -d "$DB_NAME" -c "SELECT count(*) FROM critical_outbox WHERE observation_id LIKE 'crit-$RUN-%' AND sent_at IS NULL")
check "the outbox holds nothing unsent" "$unsent" "0"
echo "notifier counters since its last start: seen=$(metric critical_events_seen_total) delivered=$(metric critical_alerts_delivered_total)"
dc exec -T canonical-store psql -q -U "$DB_USER" -d "$DB_NAME" -c "DELETE FROM critical_outbox WHERE observation_id LIKE 'crit-$RUN-%'" >/dev/null

exit "$FAILED"
