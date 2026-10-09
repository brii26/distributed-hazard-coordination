#!/usr/bin/env bash
# Problem 5: one HazardEvent fans out to independent consumers; a dead consumer does not disturb the others.
#
#   tests/scenarios/problem5.sh      takes about 2 minutes
. "$(dirname "$0")/lib.sh"

backlog() { docker compose exec -T rabbitmq rabbitmqctl list_queues name messages | awk '/notifier/ {print $2}'; }
queues() { docker compose exec -T rabbitmq rabbitmqctl list_queues name messages; }
cleanup_hooks+=("docker compose up -d --wait notifier audit-logger >/dev/null 2>&1")

step "Stop the Notifier while publishing continues"
aggregator_before=$(started aggregator)
show "docker compose stop notifier"
docker compose stop notifier 2>&1 | tail -1
echo "waiting 30s for events to pile up..."
sleep 30
show "rabbitmqctl list_queues name messages"
queues
piled=$(backlog)
expect_true "the Notifier queue holds a backlog" test "${piled:-0}" -gt 0

step "Bring the Notifier back; it catches up"
show "docker compose start notifier"
docker compose start notifier 2>&1 | tail -1
sleep 15
show "rabbitmqctl list_queues name messages"
queues
caught_up=$(backlog)
expect_true "the backlog shrank (was $piled, now ${caught_up:-?})" test "${caught_up:-$piled}" -lt "$piled"
show "docker compose logs --since 15s notifier | grep -c '\"message processed\"'"
processed=$(docker compose logs --since 15s notifier | grep -c '"message processed"')
echo "$processed"
expect_true "the Notifier processed messages after returning" test "$processed" -gt 0

step "Add a consumer without touching the producer: the Audit Logger"
show "docker compose rm -sf audit-logger"
docker compose rm -sf audit-logger 2>&1 | tail -1
show "docker compose up -d --wait audit-logger"
docker compose up -d --wait audit-logger 2>&1 | tail -1
lines() { docker compose exec -T audit-logger sh -c 'wc -l < /var/lib/audit-logger/audit.log' | tr -d '[:space:]'; }
first=$(lines)
grew() { [ "$(lines)" -gt "$first" ]; }
wait_until 40 grew; second=$(lines)
show "wc -l /var/lib/audit-logger/audit.log"
echo "$first -> $second"
expect_true "the Audit Logger records new events right away" test "$second" -gt "$first"
expect "Aggregator was not restarted" "$(started aggregator)" "$aggregator_before"

finish
