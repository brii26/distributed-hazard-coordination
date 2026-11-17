# Test scenarios (Problems 1 to 5)

Copy-paste walkthroughs for each problem, run against a stack started as described in [RUNNING.md](RUNNING.md). Back to the [README](../README.md).


Each problem also has a script that runs these steps and prints PASS or FAIL per check: `tests/scenarios/problem1.sh` to `problem5.sh`, or `tests/scenarios/all.sh`.

Run from the repository root with the stack up (`docker compose up -d --build --wait`). Needs `curl` and `jq`. Each block is copy-paste; the expected outcome follows it. Load the values the stack runs with and log in as the seeded Operator once:

Load the environment and the helper variables, then log in as the seeded Operator. The answer is `{"expires_in":...}` and the cookies are saved in `op.jar`.

```bash
set -a; . ./.env; set +a
API=http://localhost:${CLIENT_FACING_API_PORT:-8080}
BMKG=http://localhost:${BMKG_PORT:-8081}
PVMBG=http://localhost:${PVMBG_PORT:-8082}
login() { curl -s -c "$1.jar" -X POST $API/auth/login -d "{\"email\":\"$2\",\"password\":\"$3\"}"; echo; }
login op "$SEED_OPERATOR_EMAIL" "$SEED_OPERATOR_PASSWORD"
```

## Problem 1: schema change at the source

Read the newest volcanic reports, then switch PVMBG to the extended schema. The switch answers `{"schema_version":"extended"}`.

```bash
curl -s -b op.jar "$API/hazards?type=VOLCANIC&limit=3" | jq '[.data[] | {area_name, severity, confidence: .attributes.confidence_level}]'
curl -s -X POST $PVMBG/admin/schema-version; echo
```

```bash
sleep 15
curl -s -b op.jar "$API/hazards?type=VOLCANIC&limit=3" | jq '[.data[] | {area_name, severity, confidence: .attributes.confidence_level}]'
docker compose ps aggregator --format '{{.Status}}'
```

Before the switch `confidence` is `null`; after it the newest reports carry a value, the older ones are still served unchanged, and the Aggregator's uptime shows it was never restarted. Switch back with the same `POST`.

The same records side by side in the Canonical Store, newest first (also the Problem 4 evidence of records before and after the change):

```bash
docker compose exec -T canonical-store psql -U "$CANONICAL_DB_USER" -d "$CANONICAL_DB_NAME" -c \
  "SELECT source_ref_id, severity, attributes ? 'confidence_level' AS has_confidence, ingested_at FROM hazard_events WHERE hazard_type = 'VOLCANIC' ORDER BY ingested_at DESC LIMIT 6"
```

`has_confidence` is `t` for the reports ingested after the switch and `f` for the ones before, all in one table with no migration.

---

## Problem 2: slow and failing source

PVMBG latency at 3 s while BMKG stays fast, then the load test:

Set the PVMBG delay, time five seismic requests, then run the k6 load test. The load test checks `bmkg_only` p95 under 300 ms, and `mixed` with 50 connections for 60 s with under 1% errors (429s not counted).

```bash
PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 docker compose up -d --wait pvmbg-mock
for i in 1 2 3 4 5; do curl -s -o /dev/null -w '%{time_total}s\n' -b op.jar "$API/hazards?type=SEISMIC"; done
tests/load/run.sh
```

The seismic requests stay well under 300 ms (they never wait on PVMBG). Setting the delay recreates the PVMBG container, which forgets the tokens it issued; the Aggregator notices on its next token rotation and re-authenticates by itself (`token bootstrap fallback` in its log), so PVMBG data can take up to about a minute to come back after this step. `tests/load/run.sh` brings up its own copy of the stack, prints throughput, p50/p95/p99 and counts per status code, and fails if a threshold fails.

Outage on, stale data, recovery:

Turn the PVMBG outage on. The answer is `{"outage_active":true}`.

```bash
curl -s -X POST $PVMBG/admin/outage; echo
```

Wait, then ask for volcanic data. `source_status` is DOWN, `stale_since` is set and the data is still served.

```bash
sleep 20
curl -s -b op.jar "$API/hazards?type=VOLCANIC&limit=1" | jq .meta
```

Ask for seismic data. BMKG is still UP.

```bash
curl -s -b op.jar "$API/hazards?type=SEISMIC&limit=1" | jq .meta
```

Turn the outage off. The answer is `{"outage_active":false}`.

```bash
curl -s -X POST $PVMBG/admin/outage; echo
```

Wait for PVMBG to come back, then read its status. It is UP, `stale_since` is null and nothing was restarted.

```bash
for i in $(seq 90); do curl -s -b op.jar "$API/hazards?type=VOLCANIC&limit=1" | jq -e '.meta.source_status == "UP"' >/dev/null && { echo "PVMBG UP again after ${i}s"; break; }; sleep 1; done
curl -s -b op.jar "$API/hazards?type=VOLCANIC&limit=1" | jq .meta
```

With nothing stored for the source the answer is an explicit 503 instead of stale data. To see it, empty the volcanic rows while the outage is on. This deletes the stored volcanic reports (only new ones come back), so run it after the Problem 1 comparison:

Turn the outage on, wait, delete the volcanic rows and ask again. The answer is HTTP 503 with an error body.

```bash
curl -s -X POST $PVMBG/admin/outage; echo
sleep 20
docker compose exec -T canonical-store psql -U "$CANONICAL_DB_USER" -d "$CANONICAL_DB_NAME" -c "DELETE FROM hazard_events WHERE hazard_type = 'VOLCANIC'"
curl -s -w '\nHTTP %{http_code}\n' -b op.jar "$API/hazards?type=VOLCANIC"
```

Turn the outage off again. The data comes back.

```bash
curl -s -X POST $PVMBG/admin/outage; echo
```

Restore the normal PVMBG latency with `docker compose up -d --wait pvmbg-mock`.

---

## Problem 3: authentication and per-field access

Each source only accepts its own credentials:

No key: the answer is 401.

```bash
curl -s -o /dev/null -w 'BMKG, no key: %{http_code}\n' $BMKG/seismic-events
```

PVMBG's OAuth client secret sent to BMKG: the answer is 403.

```bash
curl -s -o /dev/null -w 'BMKG, PVMBG client secret: %{http_code}\n' -H "X-BMKG-Key: $PVMBG_OAUTH_CLIENT_SECRET" $BMKG/seismic-events
```

BMKG key sent as a PVMBG token: the answer is 401.

```bash
curl -s -o /dev/null -w 'PVMBG, BMKG key as token: %{http_code}\n' -H "Authorization: Bearer $BMKG_API_KEY" $PVMBG/volcanic-reports
```

A Media account sees the summary only and is refused raw fields:

Register an account. The answer is 201, or 409 on a rerun.

```bash
curl -s -X POST $API/auth/register -d '{"email":"media@demo.example","password":"media-pass-1"}' -w 'register: %{http_code}\n'
```

Log in as Media and read one record. The record has no latitude, longitude or attributes.

```bash
login media media@demo.example media-pass-1
curl -s -b media.jar "$API/hazards?type=SEISMIC&limit=1" | jq '.data[0] | keys'
```

Media asks for the latitude field: the answer is 403.

```bash
curl -s -o /dev/null -w 'Media asks for latitude: %{http_code}\n' -b media.jar "$API/hazards?type=SEISMIC&fields=latitude"
```

An access token, for any role, expires after `ACCESS_TOKEN_TTL_S` (60 s) and is refreshed behind the next request. Shown here with a Tim Lapangan account:

Create a Tim Lapangan account as the Operator. The answer is 201, or 409 on a rerun.

```bash
curl -s -b op.jar -X POST $API/accounts -d '{"email":"field@demo.example","password":"field-pass-1","role":"Lapangan"}' -w 'create: %{http_code}\n'
```

```bash
login field field@demo.example field-pass-1
sleep 65
curl -s -i -b field.jar -c field.jar "$API/hazards?type=SEISMIC&limit=1" | grep -iE '^HTTP|^set-cookie: (jwt|refresh_token)'
```

The last call answers `200` and sets a new `jwt` and a rotated `refresh_token`: the expired session was refreshed transparently. Sending the old cookies again after this fails with `401`.

---

## Problem 4: independent services and storage ownership

Stop, rebuild and restart one container while the rest keep running:

Stop the Notifier and list the services. Only the Notifier is stopped.

```bash
docker compose stop notifier && docker compose ps --format '{{.Service}} {{.Status}}'
```

Rebuild and start the Notifier alone. It is back and nothing else restarted.

```bash
docker compose up -d --build --no-deps --wait notifier
```

Ask for seismic data. The API kept serving throughout.

```bash
curl -s -b op.jar "$API/hazards?type=SEISMIC&limit=1" | jq '.meta.source_status'
```

Records before and after the schema change, side by side in the Canonical Store: see the query at the end of Problem 1.

Storage isolation (only the owner reaches each database): see "Checking that the databases are isolated" in [RUNNING.md](RUNNING.md).

---

## Problem 5: event fan-out to independent consumers

Stop one consumer while publishing continues; its queue keeps the backlog and it catches up when it returns:

Stop the Notifier, wait, and list the queues. The Notifier's count grows and the others stay near 0.

```bash
docker compose stop notifier
sleep 30
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages
```

Start the Notifier, wait, and list the queues again. The Notifier is back near 0.

```bash
docker compose start notifier
sleep 10
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages
```

```bash
docker compose logs --since 15s notifier | grep -c '"message processed"'
```

Add a consumer without touching the producer. The Audit Logger is that consumer: remove it from the running stack, then add it back. The Aggregator is neither changed nor restarted; it only knows the exchange (`services/aggregator` has no reference to any consumer):

Remove the Audit Logger and list the queues. Its queue still exists from earlier runs, or not at all on a fresh stack.

```bash
docker compose rm -sf audit-logger
docker compose exec -T rabbitmq rabbitmqctl list_queues name
```

Start the Audit Logger again, wait, and count its log lines. New events are logged straight away.

```bash
docker compose up -d --wait audit-logger
sleep 10
docker compose exec -T audit-logger sh -c 'wc -l /var/lib/audit-logger/audit.log'
```

Check the Aggregator. Its uptime shows the producer was not restarted.

```bash
docker compose ps aggregator --format '{{.Status}}'
```


## Problem 5: critical alerts in 5 seconds without loss

Needs only the store, the broker, the Aggregator and the Notifier. The script stages critical observations in the Aggregator's outbox (as a critical Sensor Pantai observation would be), kills RabbitMQ and the Notifier at different moments, and checks that every observation is alerted exactly once and that p95 latency in normal operation is 5000 ms or less.

```bash
docker compose up -d --build --wait canonical-store rabbitmq aggregator notifier
N=90 tests/scenarios/critical-alerts.sh
```

How it works: a critical event is written to the `critical_outbox` table before its upsert, a relay publishes it persistent with a publisher confirm to the durable queue `critical-alerts`, and the Notifier alerts (log line plus push on `GET /alerts/stream`, relayed by the Client-Facing API) before it acks. Metrics on the Notifier's `/metrics`: `critical_alert_latency_seconds` (from ingest to delivery), `critical_events_seen_total`, `critical_alerts_delivered_total`.
