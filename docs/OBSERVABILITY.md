# Observability

Jaeger v2 turns every span into RED metrics (spanmetrics) and exposes them on `:8889`.
Prometheus scrapes that every 15 s (retention 2h on k8s). The services need no `/metrics` endpoint.

## Metrics

- `traces_span_metrics_calls_total` (counter): one per span.
- `traces_span_metrics_duration_milliseconds_{bucket,sum,count}` (histogram).
- Labels: `service_name`, `span_name`, `span_kind`, `status_code` (`STATUS_CODE_UNSET` or `STATUS_CODE_ERROR`),
  plus `http_response_status_code` / `http_status_code`, `http_request_method` / `http_method` and
  `rpc_grpc_status_code` when the span carries them.
- HTTP and gRPC handlers are `SPAN_KIND_SERVER`. audit-logger, notifier and dashboard-updater emit
  `SPAN_KIND_CONSUMER` spans, one per queue message.
- Errors mean span status ERROR: 5xx for HTTP, non-OK for gRPC. Sampling must stay at 100%.

## PromQL

```
# Rate (requests/s) per service
sum by (service_name) (rate(traces_span_metrics_calls_total{span_kind="SPAN_KIND_SERVER"}[5m]))

# Error ratio per service
sum by (service_name) (rate(traces_span_metrics_calls_total{span_kind="SPAN_KIND_SERVER",status_code="STATUS_CODE_ERROR"}[5m]))
  / sum by (service_name) (rate(traces_span_metrics_calls_total{span_kind="SPAN_KIND_SERVER"}[5m]))

# p95 latency (ms) per service
histogram_quantile(0.95, sum by (le, service_name) (rate(traces_span_metrics_duration_milliseconds_bucket{span_kind="SPAN_KIND_SERVER"}[5m])))
```

Use `SPAN_KIND_CONSUMER` for the queue services.

## Checking it

```
# compose (Jaeger port 8889 is internal, so go through the network)
docker compose exec prometheus wget -qO- http://jaeger:8889/metrics | grep traces_span_metrics_calls_total
# k8s
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/jaeger 8889 &
curl localhost:8889/metrics | grep traces_span_metrics_calls_total
```

Prometheus UI: `/targets` should show `aggregated-trace-metrics` as up. Manifests are in `deploy/observability/`.

## Checking traces

`tests/scenarios/trace-check.sh` proves that one request leaves the same ID in Jaeger and in the logs. It needs `jq`
(devenv has it once the Grafana PR is merged) and a stack that is already up.

```
tests/scenarios/trace-check.sh                # compose
TARGET=k8s tests/scenarios/trace-check.sh     # k8s, with port-forwards for client-facing-api (:8080) and jaeger (:16686)
```

It makes two requests, a login (client-facing-api and auth-service) and `GET /hazards` (client-facing-api and
aggregator), then reads each trace from Jaeger (`/api/traces/<id>`) and greps each service's log for the ID.
`GET /hazards` never calls auth-service, the JWT is verified inside client-facing-api. The store query is a client
span of the aggregator, the store itself emits no spans. Last step: Jaeger is stopped, `GET /hazards` must still
answer 200 within 5 s, then Jaeger is started again.

The script sends no `X-Correlation-ID`. Without it the trace ID becomes the correlation ID; with a custom one the two
differ by design and the script fails on purpose. After Jaeger restarts, services can take a while to reconnect and
spans sent meanwhile are lost, so wait a minute before running the script again.

## Deviation from "every service up in /targets"

#279 asks for Prometheus `/targets` to show every service up. It cannot: the services expose no
`/metrics` endpoint (by design, see top), so the only scrape target is `jaeger:8889`, and `up` only
says Jaeger is alive. The honest per-service substitute is traffic-based: a service counts as alive if
its spans reach Jaeger. This is weaker than `up`: it cannot tell a crashed service from an idle one,
and an idle service looks the same as a dead one.

Rules in `rules.yml` (compose config `prometheus_rules`; k8s ConfigMap `prometheus-config`) raise
`NoSpansFrom_<service>` (warning) when `traces_span_metrics_calls_total{service_name="<service>"}` has
had no series for 10 min (plus `for: 5m`). Eight services are listed; a new service must be added by hand.
Check them in Prometheus under Alerts, or with:

```
# 1 = no spans seen from that service in 10m (listed services only: this returns nothing when all report)
absent_over_time(traces_span_metrics_calls_total{service_name="aggregator"}[10m])

# services that reported in the last 10m
count by (service_name) (increase(traces_span_metrics_calls_total[10m]) > 0)
```

Notes: the error-ratio query returns no data until at least one ERROR span exists. After a Jaeger or
Prometheus restart the series are empty until the next span arrives, so alerts can fire on an idle stack.
Resource limits on k8s: Jaeger 128Mi/256Mi with `max_traces: 15000` (in-memory), Prometheus 128Mi/256Mi.
