# Explain a failure from the dashboard, metrics and traces

For the game day (#324) anyone on the team must be able to explain a failure from evidence and not from guessing. This checklist says, for each kind of failure, what to open and what to look for. Names come from the code and from `deploy/observability/grafana/dashboard.json`.

## Where to look

Grafana dashboard `hazard-overview` (title "Hazard Coordination Overview"). Panel ids and titles:

| Id | Panel title |
|---|---|
| 1 | Request rate (req/s) |
| 2 | Error rate (share of requests) |
| 3 | Latency p50 (ms) |
| 4 | Latency p95 (ms) |
| 5 | Service alive (spans in the last minute) |
| 6 | Pod restarts |
| 7 | Pod ready |
| 12 | Deployments short of desired replicas (red = scaled down or failing) |
| 21 | Critical alert SLI: delivered within 5 s (target 99.9%, last 5m) |
| 22 | Read availability SLI: Tim Lapangan (target 99.5%, last 5m) |
| 23 | Critical alert error budget remaining over the selected time range (below 0 = exhausted) |
| 24 | Read availability error budget remaining over the selected time range (below 0 = exhausted) |
| 25 | Burn rate (1 = spending the budget exactly over the SLO window, lines at 6 and 14.4 are the alert thresholds) |
| 26 | Error budget remaining (trailing 2h, below 0 = exhausted) |
| 27 | Critical alerts in the selected time range: delivered, late (over 5 s), staged but undelivered (last 10m) |

The dashboard also has empty rows named Lag (8), Breaker (9), Shards (10) and Replicas (11) and the row "SLO and error budget" (20). The rows 8 to 11 hold no panel yet, so the breaker, lease, shard and failed message metrics below have no panel. Read them in the Prometheus UI (Graph tab) or Grafana Explore with the Prometheus data source. Panels 1 to 5 are built from the span metrics Jaeger derives (`traces_span_metrics_calls_total`, `traces_span_metrics_duration_milliseconds_bucket`, label `service_name`), panels 6, 7 and 12 from kube-state-metrics.

Set the Grafana time range to the run before reading anything. Panels 23, 24 and 27 use the selected range.

Jaeger UI (service `jaeger`, port 16686, for example `kubectl port-forward -n sistem-informasi-kebencanaan-bnpb svc/jaeger 16686:16686`). Service names in the search box: `client-facing-api`, `aggregator`, `notifier`, `audit-logger`, `dashboard-updater`, `auth-service`, `bmkg-mock`, `pvmbg-mock`. Span names to search (Operation field):

| Service | Operation | Meaning |
|---|---|---|
| client-facing-api, aggregator, auth-service, bmkg-mock, pvmbg-mock | `http` | server span of one HTTP request (health and metrics are filtered out) |
| aggregator | `poll BMKG`, `poll PVMBG`, `poll SENSOR_PANTAI` | one polling cycle of a source, ends in error when the cycle failed |
| aggregator | `GET <base url><path>` | client span of the outbound call to a source |
| aggregator | `ingest BMKG`, `ingest PVMBG`, `ingest SENSOR_PANTAI` | validation and storing of one batch |
| aggregator | `canonical-store upsert`, `canonical-store list <TYPE>` | database calls to the store |
| aggregator | `rabbitmq publish exchange=<name>` | publishing an event |
| notifier, audit-logger, dashboard-updater | `consume notifier`, `consume audit-logger`, `consume dashboard-updater` | one message handled by a consumer, joined to the producer trace through the AMQP headers |

Useful Jaeger filters: tag `error=true`, `correlation_id=<id>` (set on spans and in logs), and a min duration to find slow traces. Inside a trace, a red span is the failing step and its parent chain says which request or poll it belonged to.

## The method for any failure

1. Set the time range to the run and find the first panel that moved. Write its id, the time and the service or pod.
2. Panel 12 and panel 7 say whether pods are missing or not ready, panel 6 whether they restarted, panel 5 whether a service stopped producing spans.
3. Panels 2 to 4 say which service errors or slows down. Then open Jaeger for that `service_name`, filter `error=true` in the same minute and open one trace.
4. Name the failing span, its error message, and the downstream it called. That is the explanation. Add the metric from the table below that proves it.
5. Check the cost: panels 21 to 27, then the probe CSV of the run.

## Checklist per kind of failure

Practice scenario names are the ones run by `kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <name>`. What each scenario does exactly is defined by the Tester Agent, not by this repo, so confirm the effect in the evidence and do not assume it.

### Store node down or wiped (kill-store, wipe-node)

- Panels: 7 (Pod ready, a store pod turns not ready), 12 (short of desired replicas), 6 (Pod restarts), 2 (Error rate, aggregator).
- Jaeger: service `aggregator`, operation `canonical-store upsert` or `canonical-store list <TYPE>`, `error=true`. A `poll ...` span that ends in error with a store error below it, and `ingest ...` spans failing in the same cycle.
- Metrics (Prometheus, aggregator job): `store_shards_underreplicated`, `store_shards_misplaced`, `store_repair_rows_total`, `store_shard_rows{node,shard}`. After a wipe the under-replicated count rises, then `store_repair_rows_total` grows while the node is refilled, and `store_shard_rows` for that node returns to its peers' value. The shard design is in `doc/P1_STORAGE_DESIGN.md`.
- Not only the store: `coordination-db` and `user-db` carry `role=store` too, so kill-store and wipe-node take them down together with the store pod on that node (measured in the Tester Agent notes, doc/TESTER_AGENT_NOTES.md on #377: `store-cn9gw`, `coordination-db-0` and `user-db-0` were all stopped and back after about 70 s). Expect these symptoms in the same minute:
  - Polling stops. No replica can take or renew a lease (`failed to connect ... database=coordination` in the aggregator logs, so `lease_is_leader` should show no holder, and `lease_takeovers_total` rises when the database returns with the epoch plus 1), so panel 5 shows the aggregator quiet while its pods stay ready.
  - Login fails. The Auth Service cannot reach `user-db`, so `POST /auth/login` returns an error and refresh fails too. Sessions with a still valid jwt keep reading until the 60 s token ends, then every request is 401 or 5xx. The game day probe availability drops for this reason and not because of the store.
  - The probe CSV shows a failing streak of about the pod restart time (70 s measured) for kill-store.
- Extra effects of wipe-node (measured in the same notes): the node powers off and the agent on it replays the run on every boot until `kubectl -n tester-agent delete cm tester-run`, and only the k3s local-path volumes are wiped. The PVC data of `coordination-db`, `user-db` and RabbitMQ is lost, Postgres then starts with a broken data directory, and the PVC has to be deleted and the pod recreated. Lease rows rebuild themselves. The PVMBG token pair is lost and its one time code was already spent, so PVMBG polling stays down until the mock is restarted. `user-db` loses accounts and the seeded Operator, and RabbitMQ loses queued messages, which shows as `aggregator_critical_staged_total` rising without `critical_alerts_delivered_total`. The store hostPath `/var/lib/bnpb-store` was not wiped in that run.
- What `tests/scenarios/gameday.sh` does about it: after wipe-node it deletes `tester-run`, starts the node with `NODE_UP_CMD` (for example `VBoxManage startvm "$NODE" --type headless`), waits for the node and the stateful pods, and recreates the PVCs only with `WIPE_RECREATE_PVC=1`. Without that flag it prints the manual step: `kubectl delete pvc data-<name>-0 -n sistem-informasi-kebencanaan-bnpb --wait=false && kubectl delete pod <name>-0 -n sistem-informasi-kebencanaan-bnpb` for `coordination-db`, `user-db` and `rabbitmq`, then `kubectl rollout restart deploy/pvmbg-mock`. The seeded Operator is not restored by the script.
- The store data on that node is wiped too. The store hostPath is aligned with the path wipe-node erases (#289, and #298 moves it under `/var/lib/rancher/k3s/storage/`), so the shards held by that node are gone as well as `coordination-db`, `user-db` and RabbitMQ. With RF 2 the other owner still has each shard, and the repair loop rebuilds the copy once the node is back. Reads of those shards come from the other owner meanwhile.
- Explain: which node, when the first failing upsert happened, whether reads kept working (panel 22), when polling and login stopped and returned, and when the repair finished.

### Aggregator leader killed (kill-leader)

- Panels: 6 (Pod restarts or a new pod name), 7 (Pod ready), 12, 5 (the aggregator keeps or briefly loses spans).
- Jaeger: service `aggregator`, operations `poll BMKG`, `poll PVMBG`, `poll SENSOR_PANTAI`. A gap in the poll spans for one source, then polls resume from another pod.
- Metrics: `lease_is_leader` and `lease_token` (per source, one pod shows 1 and then another), `lease_takeovers_total` (increases by one), `fenced_writes_total` (a write from the old holder was refused when it is above 0). Design in `doc/P2_LEASE_DESIGN.md`.
- Explain: when the lease changed hands, how long no source was polled, and whether the SLO paid for it (panel 21, panel 27 staged against delivered).

### Network partition (partition)

- Panels: 2 and 4 (Error rate and Latency p95 for the service on the cut side), 5 (spans stop), 1 (traffic drops). Pods stay ready in panel 7 when only the network is cut, which is the clue that it is not a crash.
- Jaeger: the client span with the error and a long duration (for example `canonical-store upsert`, `rabbitmq publish exchange=<name>`, `GET <base url><path>`) under a `poll ...` or `http` span, with a timeout or connection error in the span event. Compare the duration with the neighbours.
- Metrics: `aggregator_critical_relay_failures_total` rises if RabbitMQ is on the cut side, `aggregator_critical_staged_total` keeps rising while `critical_alerts_delivered_total` stalls, so `slo:critical_alert_undelivered:count10m` goes above 0. They catch up after healing.
- Explain: which two components lost each other (the failing client span names the target), that pods were healthy, and that staged alerts were delivered after the heal.

### Clock skew (clock-skew)

- Panels: usually not visible in panels 1 to 7. Check 27 (late alerts) and 21.
- Metrics: `aggregator_sensor_rejected_total{reason}` rises when a sensor observation time is outside what the validator accepts (`checkObservedAt` in `services/aggregator/sensor_validate.go`, compared with the sensor's own `generated_at`). `critical_alert_latency_seconds_bucket` shifts because the latency is measured from `IngestedAt` to delivery on two clocks.
- Jaeger: `ingest SENSOR_PANTAI` spans and their logs, timestamps that do not line up between services in the same trace.
- Explain: which clock moved, which check rejected data, and how many records were set aside against how many were delivered late.

### Schema change at a source (schema, PVMBG schema switch)

- Panels: 2 (Error rate for aggregator), 27 (counts).
- Jaeger: service `aggregator`, operation `ingest PVMBG` or `ingest SENSOR_PANTAI` with errors, below a `poll ...` span that itself succeeded, which says the call worked and the content did not fit.
- Metrics: `aggregator_failed_messages{status,reason}` (records set aside with a reason, retried by the retry worker), `aggregator_sensor_rejected_total{reason}` for the sensor, `critical_alerts_malformed_total` for a malformed alert message. Policy in `doc/P3_SCHEMA_POLICY.md`.
- Explain: the field or version that changed, that data was set aside and not dropped, and whether it was replayed.

### PVMBG outage (`POST /admin/outage`)

- Panels: 2 (aggregator or pvmbg-mock errors), 22 (read availability must stay up, because reads serve stale data and do not fail).
- Jaeger: service `aggregator`, `poll PVMBG` ends in error, with the child client span `GET <base url>/volcanic-reports` showing a 503. No error on `http` spans of `client-facing-api` for `/hazards?type=VOLCANIC`.
- Metrics and data: the circuit breaker of the source opens (breaker per source is in `services/aggregator/breaker.go`, the sensor one is exported as `aggregator_sensor_breaker_state`). The read response carries `source_status` and `stale_since` for the source. `curl <pvmbg>/health` answers 200 with `outage_active` true, only the simulated upstream is down.
- Explain: the failing source, since when the data is stale, that reads did not fail, and the time to fresh data after the outage ended.

### Sensor problem (sensor pod down or sensor errors)

- Panels: 12 and 7 if the pod is scaled down or not ready, 6 if it restarted, 5 for the sensor's own service.
- Jaeger: `poll SENSOR_PANTAI` ending in error with the client span `GET <sensor url>/...` failing or slow.
- Metrics: `aggregator_sensor_requests_total`, `aggregator_sensor_slow_responses_total`, `aggregator_sensor_breaker_state` (0 closed, 1 open, 2 half open), `aggregator_sensor_rejected_by_breaker_total`, `aggregator_sensor_rejected_by_bulkhead_total`, `aggregator_sensor_in_flight`, `aggregator_sensor_backoff_seconds`, `aggregator_sensor_stale`, and `aggregator_sensor_rejected_total{reason}` for bad observations.
- Explain: whether the sensor was unreachable, slow or sending bad data, which protection engaged (breaker, bulkhead, backoff) and when coastal data became stale.

### Load spike (k6)

- Panels: 1 (Request rate), 3 and 4 (Latency), 2 (Error rate), 22 and 24 (read availability and its budget).
- Metrics: `client_api_inflight_requests` and `client_api_inflight_rejected_requests_total{role}`. Media and anonymous requests are shed first and Tim Lapangan last (`doc/P5_SLO.md` section 3). Shed requests are 429 with `Retry-After`, they are not server errors.
- Jaeger: service `client-facing-api`, operation `http`, sorted by duration. Slow `http` spans with a short child `canonical-store list <TYPE>` mean the queue is in front of the aggregator.
- Explain: which role was shed, whether Tim Lapangan reads stayed available, and that SLI 2 is a proxy over all routes (`doc/P5_SLO.md` section 4).

### Critical alert late or not delivered

- Panels: 21, 23, 25, 26 and 27 (delivered, late over 5 s, staged but undelivered).
- Metrics, in the order of the chain: `aggregator_critical_staged_total` (written to the outbox), `aggregator_critical_published_total` (relayed to RabbitMQ), `aggregator_critical_relay_failures_total`, `critical_events_seen_total` and `critical_alerts_delivered_total` (Notifier), `critical_alerts_duplicates_total`, `critical_alerts_malformed_total`, `critical_alert_latency_seconds_bucket`. The first counter that stops following the one before it marks the broken hop. Rules: `slo:critical_alert_undelivered:count10m`, `slo:critical_alert_burn_rate:rate5m`.
- Jaeger: follow one alert by `correlation_id` from `ingest SENSOR_PANTAI` to `rabbitmq publish exchange=<name>` to `consume notifier`. A missing consumer span is a loss, a long gap between them is lateness.
- Explain: the hop where the alerts waited, and when they were finally delivered.

## When the checklist does not explain it

Write down which step failed to give an answer, the panel or trace that was missing, and open an issue for it. That belongs in the Detection section of the post-mortem (`doc/POSTMORTEM_TEMPLATE.md`).
