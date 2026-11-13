# Testing

Back to the [README](../README.md).

# Service-to-service integration tests

Each suite starts only the two services of one pair (plus the storage or broker they need) in a separate compose project, `hazard-it`, drives the real network path, and tears everything down afterwards. It uses `.env.example`, so no `.env` is needed, and its host ports (18080-18082, 15432, 19091) stay clear of a normal `docker compose up`.

Client-Facing API with the Auth Service: login, logout, JWT check, transparent refresh, register and `/accounts`.

```bash
tests/integration/run.sh cfa-auth
```

Aggregator with the BMKG mock: API key, `since` cursor, events and tsunami warnings mapped and stored.

```bash
tests/integration/run.sh agg-bmkg
```

Aggregator and the PVMBG mock: OAuth bootstrap and rotation, outage and schema change (Problems 1 to 3).

```bash
tests/integration/run.sh agg-pvmbg
```

Run all three suites, one after the other.

```bash
tests/integration/run.sh all
```

The whole stack through restarts, outages, crashes, load and malformed input. It takes about 10 minutes and is not part of `all`.

```bash
tests/integration/run.sh abuse
```

Only the tests that need no stack reset (bad input, credentials, limits). About a minute.

```bash
tests/integration/run.sh abuse input
```

`abuse` is the failure-mode suite (`abuse_integration_test.go` in `services/client-facing-api`). Every test starts from a fresh `down -v` stack, so one failure never explains the next, and each carries a comment saying what it does and what it expects. A red test is a real defect: the comment names the issue when there is one. The sets are `input`, `resilience` (restarts, outages, crashes, poison messages), `load` and `all` (the default). `IT_LOAD_SECONDS=60` makes the load test run for a full minute, and `ABUSE_RUN=TestAbuseDownUp tests/integration/run.sh abuse` runs a single test.

Needs Docker Compose and Go. The tests live next to the code they exercise (`integration_test.go` in `services/client-facing-api` and `services/aggregator`) and skip unless `run.sh` sets their environment, so a plain `go test ./...` stays offline.

# Full-stack smoke test

Brings up the whole `docker-compose.yml` stack in a separate project, `hazard-smoke`, and checks that it works as one system: every service's `/health` answers, the seeded Operator can log in and `/hazards` returns data from both mocks through the Aggregator, and restarting one consumer (the Notifier) leaves every other service running and untouched.

```bash
tests/smoke/run.sh
```

Needs Docker Compose and curl. It uses `.env.example`, so no `.env` is needed, builds the images, tears everything down afterwards, and publishes its host ports on 28080-28082, 25672 and 23000, clear of a normal `docker compose up`. Takes about a minute.

# Load test (Problem 2)

Runs [k6](https://k6.io) (from its Docker image, nothing to install) against the full stack in a separate compose project, `hazard-load`, with PVMBG answering after 3 s. Two scenarios, both against `/hazards` as the seeded Operator:

- `bmkg_only`: 20 requests/s for 30 s on `/hazards?type=SEISMIC`; threshold p95 < 300 ms while PVMBG is slow.
- `mixed`: 50 concurrent connections for 60 s over seismic, volcanic, field-filtered and `/health` requests; threshold error rate < 1%, where a 429 (the controlled load shedding) does not count as an error.

Run the load test, then a bigger one.

```bash
tests/load/run.sh
MIXED_VUS=100 MIXED_DURATION=120s tests/load/run.sh
```

k6 prints throughput, avg/p50/p90/p95/p99 latency, and request counts per status code (200, 401, 429, 500, 502, 503, 504, so 429s are visible apart from real failures). The summary is saved as `tests/load/results/p2-<timestamp>.txt` and `.json` (git-ignored). The script exits non-zero if a threshold fails. Needs Docker Compose; it uses `.env.example`, so no `.env` is needed, and publishes its host ports on 38080-38082, 35672 and 33000. Results depend on the machine it runs on.

# Go workspace notes

- Adding a new service: create `services/<new-name>/`, add its `go.mod`, then `go work use ./services/<new-name>` from the repo root so it joins the workspace.
- Cross-module builds (e.g. checking everything compiles at once) use the module path, not a directory glob:
  ```bash
  go build github.com/brii26/distributed-hazard-coordination/services/...
  ```

# Storage integration tests

Tests for one service against its own real storage or broker: the Auth Service store against the User DB (login lookup, the seeded Operator, refresh rotation including the old token being rejected and concurrent reuse of one token, logout, duplicate email), the Aggregator store against the Canonical Store, and the Aggregator publisher against RabbitMQ.

```bash
tests/integration/storage.sh
```

It starts throwaway Postgres (with each service's `migrations/`) and RabbitMQ containers on ports 25432, 25433 and 25673, runs the suites and removes the containers. It uses `.env.example`, so no `.env` is needed. Needs Docker and Go. Each suite skips when its env var (`USER_DB_TEST_URL`, `CANONICAL_DB_TEST_URL`, `RABBITMQ_TEST_URL`) is unset, so a plain `go test ./...` stays offline.


## Before you push

There is no CI, so run these locally. Each directory under `services/` is its own Go module, so loop over them from the repo root:

```bash
for d in services/*/; do
  [ -f "$d/go.mod" ] || continue
  (cd "$d" && gofmt -l . && go vet ./... && go build ./... && go test -race ./... && golangci-lint run) || echo "FAILED: $d"
done
```

`gofmt -l` must print nothing. Also run `docker build` for a service when its Dockerfile or dependencies change.
