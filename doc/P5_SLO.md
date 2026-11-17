# P5: SLO for the alert path and the error budget

Date: 2026-11-18. Owner: Brian. Issue: #323. Depends on #321 (critical alert path, metrics) and #280 (observability stack). Status: draft for review. The rules are in `deploy/observability/prometheus/slo-rules.yml` and the panels are the group "SLO and error budget" at the bottom of `deploy/observability/grafana/dashboard.json`.

## 1. Scope

The SLO covers what a Tim Lapangan user depends on during a disaster: a critical event from the sensor (Sensor Pantai) must reach them quickly and must not be lost, and the read side they use must be up. It does not cover the Media portal, whose traffic is shed on purpose by the priority load shedding (#322), nor BMKG and PVMBG polling freshness.

## 2. SLIs and targets

| # | SLI | Target | Window | Status |
|---|---|---|---|---|
| 1 | Critical alert timeliness: share of delivered critical alerts delivered to Tim Lapangan within 5 s of the ingest | 99.9 percent | 30 days rolling | Implemented |
| 2 | Tim Lapangan read availability: share of read requests that are not server errors or sheds of Lapangan | 99.5 percent | 30 days rolling | Implemented as a proxy (section 4) |
| 3 | Tim Lapangan read latency: share of read requests under 1 s | 99 percent | 30 days rolling | Defined, not implemented: needs a metric that does not exist (section 4) |

Why these numbers:

- 99.9 percent and 5 s. The project requirement is p95 within 5 s and zero lost critical events. A p95 allows 5 percent of alerts to be late, which is too weak for the one path that warns people. The SLO is stricter than the requirement on purpose: 1 late alert in 1000. We stop at three nines because the producing chain (sensor, Aggregator, outbox, RabbitMQ, Notifier) has several hops and a vendor we do not trust, and a fourth nine would leave a budget smaller than one failure of the game day. The 5 s limit matches the requirement and a histogram bucket (`le="5"`) already exists in the Notifier.
- 99.5 percent for reads. The read side has a client retry path and an event stream (server-sent events) that also reconnects with `Last-Event-ID` and replays missed alerts, so a short read failure delays a user but does not lose an alert. That justifies a looser target than the alert itself. 99.5 percent is about 3.6 hours of full outage per 30 days, a budget the cluster (3 VMs, single replica per service) can plausibly meet.
- 99 percent under 1 s. A read is one gRPC call to the Aggregator. Over 1 s is where the UI visibly stalls. Looser than availability because tail latency is the first thing that degrades under shedding.
- 30 days. The common window for operational SLOs: long enough that one bad day does not erase the budget, short enough to be acted on. The monitoring stack cannot hold 30 days (section 5), so the game day reports the budget over its own time range.

## 3. Bad events

SLI 1 (critical alert timeliness). Good event: a critical alert delivered with ingest-to-delivery latency of 5 s or less. Bad event: a delivered alert slower than 5 s. A critical event that is never delivered is also a bad event, but the Notifier cannot count what it never saw, so the undelivered count is tracked beside the SLI by comparing the Aggregator and Notifier counters (`slo:critical_alert_undelivered:count10m`) and pages on its own (`CriticalAlertsUndelivered`), because the project requirement for loss is zero, not 99.9 percent. A late alert is counted bad once it finally arrives. Duplicates (`critical_alerts_duplicates_total`) and malformed messages (`critical_alerts_malformed_total`) are not events of this SLI: a duplicate sends no second alert, and a malformed message is a contract problem, visible but not charged here.

SLI 2 (read availability). Good event: a server span of the Client-Facing API that did not end in a server error. Bad event: a span with `status_code="STATUS_CODE_ERROR"` (5xx), plus every request shed from a Tim Lapangan caller (`client_api_inflight_rejected_requests_total{role="Lapangan"}`). Not counted: 4xx caused by the caller, and requests shed from any other role. Media shedding is the designed behaviour of #322 that protects Lapangan, so the shed Media and anonymous requests are removed from the denominator instead of being charged. Planned downtime does not exist and is not excluded.

SLI 3 (read latency). Bad event: a read that takes more than 1 s.

## 4. How each SLI is measured

Only metrics that exist in the code are used.

SLI 1, from the Notifier (`services/notifier/critical.go`, `GET /metrics`):

- good = `critical_alert_latency_seconds_bucket{le="5"}`, total = `critical_alert_latency_seconds_count`. The latency is measured from the ingest time of the observation (`IngestedAt`, the sensor response) to delivery, so the 5 s covers the whole chain from the sensor response, not only the Notifier.
- error ratio over a window W = `1 - sum(rate(bucket{le="5"}[W])) / sum(rate(count[W]))`.
- Undelivered diagnostic: `aggregator_critical_staged_total` (Aggregator, events written to the outbox) against `critical_alerts_delivered_total` (Notifier) over 10 minutes. Other related counters used for diagnosis, not for the SLI: `aggregator_critical_published_total`, `aggregator_critical_relay_failures_total`, `critical_events_seen_total`, and the sensor path counters `aggregator_sensor_*`.

SLI 2, from the span metrics Jaeger derives and Prometheus scrapes, plus the admission counter of the Client-Facing API:

- total = `traces_span_metrics_calls_total{service_name="client-facing-api", span_kind="SPAN_KIND_SERVER"}` minus `client_api_inflight_rejected_requests_total{role!="Lapangan"}`.
- bad = the same span metric with `status_code="STATUS_CODE_ERROR"` plus `client_api_inflight_rejected_requests_total{role="Lapangan"}`.

Missing metric (read this before trusting SLI 2 and 3). The Client-Facing API does not expose a per-route, per-role request counter or a latency histogram. Its server spans are all named `http` (the OpenTelemetry handler wraps the mux, so the route is not known when the span starts), and the span metrics have no route or role dimension. As a result SLI 2 is a proxy: it covers every route of the Client-Facing API (login, refresh, hazards, `/me`) and every role, not only the Tim Lapangan reads. The two long lived streams (`/hazards/stream`, `/alerts/stream`) also end as spans with a very long duration, which makes the span duration histogram unusable for a latency SLI, so SLI 3 has no rules and no panel. Proposed metric, not added here because this issue changes no Go code: `client_api_requests_total{route, role, code}` and `client_api_request_duration_seconds{route, role}` (histogram with a bucket at 1 s), recorded in `inflight.wrap` and excluding the two stream routes. With them, SLI 2 and 3 become the same ratio shape as SLI 1 and the proxy filters above are replaced. Until then the doc and the panels say "proxy".

Scrape targets added by this change (neither stack scraped them): `notifier:8094`, `aggregator:8091` and, on Kubernetes, `client-facing-api:8080` (Compose already scraped it as `client-api-admission`). The Aggregator runs 3 replicas, so on Kubernetes it is scraped through DNS service discovery: the headless Service `aggregator` returns one A record per pod, every replica is a target and the rules add them up with `sum`. The other targets are Service or Compose names and each has one replica, so a scrape reaches that one pod. If one of them is scaled, it needs the same per pod discovery or its counters will flap.

## 5. Window, error budget and burn rate

Error budget = `1 - target`: 0.001 for SLI 1, 0.005 for SLI 2. Over 30 days at one critical alert per minute (43,200 alerts) that is 43 late alerts for SLI 1.

Burn rate = observed error ratio divided by the error budget. A burn rate of 1 spends exactly the whole budget over the SLO window, 14.4 spends 2 percent of a 30 day budget in 1 hour, and 6 spends 5 percent in 6 hours.

Limit of the stack. Prometheus on Kubernetes keeps data for 2 hours (`--storage.tsdb.retention.time=2h`) and the Compose one has no volume, so a real 30 day window cannot be evaluated here. We therefore do two things:

- The panels "error budget remaining over the selected time range" compute the budget over the Grafana time range, so for the game day the range is the test run. This answers the acceptance criterion directly: remaining = `1 - error ratio over the range / budget`, below 0 means the budget of that range is exhausted. Read it as a strict test: with few events, one late alert can use more than 100 percent of a range budget, because the range holds far fewer than 1000 events. The tile "Critical alerts in the selected time range" shows the delivered, late and undelivered counts beside it, so the reader sees the counts behind the percentage.
- Recording rules use windows of 5 m, 30 m, 1 h and 2 h. The trailing 2 h budget panel is the longest history Prometheus holds.

Alert thresholds (rules `*FastBurn`, `*SlowBurn`), multi-window so a short blip does not page and a recovered incident stops alerting:

| Alert | Condition | Meaning | Severity label |
|---|---|---|---|
| FastBurn | burn rate over 14.4 on both 5 m and 1 h | 2 percent of the 30 day budget per hour, page now | page |
| SlowBurn | burn rate over 6 on both 30 m and 2 h | 5 percent per 6 hours, open a ticket | ticket |
| CriticalAlertsUndelivered | staged minus delivered over 10 m above 0, for 10 m | possible loss | page |
| NotifierMetricsDown | `up{job="notifier"} == 0` for 2 m | SLI 1 has no data | warning |

The textbook windows for these thresholds are 1 h/5 m and 6 h/30 m. The slow pair is shortened from 6 h to 2 h to fit the retention above. No Alertmanager exists in either stack, so the alerts are visible in the Prometheus UI and this doc does not claim notification delivery.

When there is no traffic the error ratio is defined as 0, so an idle window spends no budget. The cost is that a dead scrape also looks healthy, which is why `NotifierMetricsDown` exists.

## 6. Recording rules and panels

Recording rules (`slo:<sli>:<what>:<window>`): `slo:critical_alert_error_ratio:rate{5m,30m,1h,2h}`, `slo:critical_alert_burn_rate:rate{...}`, `slo:critical_alert_sli:ratio_rate5m`, `slo:critical_alert_error_budget_remaining:ratio2h`, `slo:critical_alert_undelivered:count10m`, and the read equivalents `slo:read_requests`, `slo:read_bad_requests`, `slo:read_error_ratio`, `slo:read_burn_rate` on the same four windows, `slo:read_sli:ratio_rate5m`, `slo:read_error_budget_remaining:ratio2h`. The budget constants (0.001 and 0.005) are in the expressions: change the target there and in this doc together.

Panels (dashboard `hazard-overview`, row "SLO and error budget"): current SLI for both, error budget remaining over the selected range for both, burn rate (5 m and 1 h) with the 6 and 14.4 lines, trailing 2 h budget remaining, and the counts tile.

Loading. Kubernetes: the rules are a ConfigMap generated by `deploy/kustomization.yaml` (`prometheus-slo-rules`) and mounted at `/etc/prometheus-slo`. Compose: the same file is attached as a config and loaded through `rule_files`. Compose has no Grafana, so the panels exist only on Kubernetes and Compose gets the rules and alerts.

## 7. How to read it on the game day

1. Set the Grafana range to the run (for example from the first injected failure to recovery).
2. Read "error budget remaining over the selected range" for both SLIs, the counts tile, and the burn rate chart for when it crossed 6 and 14.4.
3. Check `CriticalAlertsUndelivered`: any firing is a requirement failure (zero loss) whatever the percentage says.
4. Report in the post-mortem (#324) the numbers, the proxy caveat for SLI 2, and the absence of SLI 3 if the metric was not added.

## 8. Known gaps

- SLI 2 is a proxy and SLI 3 is not measured until the request metric in section 4 exists.
- No 30 day evaluation: retention is 2 hours on Kubernetes and nothing persists on Compose.
- No Alertmanager, so no notification delivery.
- The undelivered diagnostic subtracts counters from two processes and may show a small positive value for events in flight at a window edge, which is why it needs `for: 10m`. A restart of the Aggregator or Notifier resets only that process counter, and `increase` handles the reset.
- The Notifier and the Client-Facing API are scraped through their Service name, which is correct for one replica only. The Aggregator is scraped per pod through DNS. On Compose the Aggregator is a single container.
- Not verified on a live cluster: the numbers were checked with `promtool test rules` on synthetic series only.
