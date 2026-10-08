# Platform Koordinasi Kebencanaan

Distributed system for IF4031 Milestone 1: BMKG/PVMBG mock sources, an Aggregator that maps their data into a canonical `HazardEvent` format, a Client-Facing API + Auth Service for downstream clients (Media/Tim Lapangan/BNPB Ops), and a RabbitMQ fan-out to independent consumers.

Full technical spec: [`doc/TRD_M1.md`](doc/TRD_M1.md). Assignment spec: [`doc/IF4031 - Tugas Besar - M1.md`](<doc/IF4031 - Tugas Besar - M1.md>). Contributor workflow (branches, PRs, reviews): [`CONTRIBUTING.md`](CONTRIBUTING.md). What to work on next: [`doc/BACKLOG_DEPENDENCIES.md`](doc/BACKLOG_DEPENDENCIES.md).

## Prerequisites

- Go 1.26+
- Docker + Docker Compose (only needed for `docker compose up` — not required to build/test an individual service)
- `git`
- `curl` and `jq`, for the copy-paste scenario commands below

How you get these on your machine is up to you — a system package manager, `asdf`, whatever. If you'd rather not manage them yourself, this repo happens to include a [devenv](https://devenv.sh/) config (`devenv.nix`) that pins Go, `golangci-lint`, and `gh` for you: `devenv shell` drops you into a shell with all of it on `PATH`. That's just how the maintainers set their own machines up, not something the project requires — every command below is a plain command that works the same whether it's typed directly or inside a devenv shell.

## Run it from a clean clone

```bash
git clone https://github.com/brii26/distributed-hazard-coordination.git
cd distributed-hazard-coordination
cp .env.example .env                 # the example values work as is for a local run; .env is gitignored
docker compose up -d --build --wait  # builds every image, returns once every health check passes
docker compose ps                    # every service "running", the ones with a health check "(healthy)"
```

If one of the host ports below is already taken on your machine, change it in `.env` (`CLIENT_FACING_API_PORT`, `WEB_FRONTEND_PORT`, `BMKG_PORT`, `PVMBG_PORT`, `RABBITMQ_MANAGEMENT_PORT`); the scenario commands further down read the ports from there. The first build takes a few minutes. `--wait` returns once every container with a health check reports healthy, so when it exits the stack is ready. Stop it with `docker compose down`; add `-v` to also wipe the databases, the PVMBG token and the consumers' dedup files and start from zero.

| Port | What |
|---|---|
| <http://localhost:3000> | Web frontend (log in as `SEED_OPERATOR_EMAIL` / `SEED_OPERATOR_PASSWORD` from `.env`) |
| <http://localhost:8080> | Client-Facing API, the only public API (`/auth/*`, `/accounts`, `/me`, `/hazards`, `/health`) |
| <http://localhost:8081> | BMKG mock, to poke it directly (needs `X-BMKG-Key`) |
| <http://localhost:8082> | PVMBG mock, including the demo switches `/admin/outage` and `/admin/schema-version` (need `X-Admin-Key`) |
| <http://localhost:15672> | RabbitMQ management UI (`RABBITMQ_USER` / `RABBITMQ_PASSWORD` from `.env`) |

The Aggregator, Auth Service, both databases and the consumers publish no host port (doc/TRD_M1.md 0). Their health is reachable from inside the stack, for example `docker compose exec aggregator wget -qO- localhost:8091/health`; every service's `/health` answers `{"status":"ok"|"degraded"|"down",...}`.

## Repository layout

```
services/<name>/     one Go module per service (see doc/TRD_M1.md for what each does)
  go.mod
  main.go
  main_test.go
  Dockerfile
infra/<name>/        config for third-party components (no Go code), e.g. infra/rabbitmq
frontend/             CSR web frontend (not Go — scaffolded separately, see issue #33)
doc/                  spec, assignment docs, backlog dependency graph
.claude/skills/       vendored Go skills (also symlinked at .agents/skills for Codex)
docker-compose.yml    full local stack
go.work               ties all service modules together for cross-module tooling
```

Each service directory is its **own Go module** (`go.work` lists them all). This means `go build ./...`/`go test ./...` from the repo root won't work as a single command — the repo root isn't itself a module. Run per-service commands from inside `services/<name>/`, or use the loop shown below.

Every directory under `services/` is a Go module that the local checks below loop over. Config-only components such as the broker go in `infra/` so they aren't treated as one.

## Running the full stack

`docker compose up --build` (as above) waits on health checks before dependent services start: the Aggregator waits for both Postgres instances, RabbitMQ and both mocks, the Client-Facing API for the Aggregator and the Auth Service, the web frontend for the Client-Facing API.

### Checking that the databases are isolated

Each database sits on an internal network shared only with its owner: `canonical-net` (Aggregator and `canonical-store`) and `userdb-net` (Auth Service and `user-db`). Neither database publishes a host port; to look inside one, use `docker compose exec canonical-store psql -U aggregator`.

With the stack running, probe both databases from every service:

```bash
for s in aggregator auth-service bmkg-mock pvmbg-mock rabbitmq client-facing-api web-frontend dashboard-updater notifier audit-logger; do
  echo "$s  canonical-store: $(docker compose exec -T $s sh -c 'nc -zw2 canonical-store 5432 >/dev/null 2>&1 && echo reachable || echo unreachable')" \
          " user-db: $(docker compose exec -T $s sh -c 'nc -zw2 user-db 5432 >/dev/null 2>&1 && echo reachable || echo unreachable')"
done
```

Only `aggregator` reaches `canonical-store` and only `auth-service` reaches `user-db`; everything else says `unreachable`.

## Configuration reference

Every variable in `.env.example`. Compose reads `.env` and passes each one to the services listed; the default applies when the variable is unset. A variable with no default must be set (the example values are fine locally). The spec's per-service list is doc/TRD_M1.md 10.

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `BMKG_PORT` | bmkg-mock | 8081 | Host port of the BMKG mock |
| `BMKG_API_KEY` | bmkg-mock, aggregator | none | Static key BMKG checks in `X-BMKG-Key` |
| `BMKG_SEED_COUNT` | bmkg-mock | 20 | Seismic events generated at start |
| `BMKG_EVENT_INTERVAL_S` | bmkg-mock | 10 | A new seismic event every N seconds |
| `BMKG_DELAY_MIN_MS` / `BMKG_DELAY_MAX_MS` | bmkg-mock | 50 / 150 | Injected latency per request |
| `TRACING_ENABLED` | all Go services | true | Export OpenTelemetry spans to Jaeger. A poll cycle's or request's trace ID is its `correlation_id` in the logs |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | all Go services | http://jaeger:4317 | OTLP gRPC endpoint spans are sent to; Jaeger being down never affects a service |
| `JAEGER_UI_PORT` | jaeger | 16686 | Host port of the Jaeger UI, `http://localhost:16686`. Its Monitor tab shows request rate, errors and latency per service, from a Prometheus container Jaeger reads (traces stay in Jaeger's memory, metrics in Prometheus; both reset on restart) |
| `PVMBG_PORT` | pvmbg-mock | 8082 | Host port of the PVMBG mock |
| `PVMBG_OAUTH_CLIENT_ID` / `PVMBG_OAUTH_CLIENT_SECRET` | pvmbg-mock, aggregator | none | OAuth client credentials (Basic auth on `/oauth/token`) |
| `PVMBG_ONE_TIME_CODE` | pvmbg-mock, aggregator | none | Authorization code for the first token, accepted once per PVMBG lifetime |
| `PVMBG_ADMIN_KEY` | pvmbg-mock | none | `X-Admin-Key` for `/admin/outage` and `/admin/schema-version` |
| `PVMBG_DELAY_MIN_MS` / `PVMBG_DELAY_MAX_MS` | pvmbg-mock | 500 / 3000 | Injected latency per request (Problem 2) |
| `PVMBG_SEED_COUNT` | pvmbg-mock | 20 | Volcanic reports generated at start |
| `PVMBG_EVENT_INTERVAL_S` | pvmbg-mock | 10 | A new volcanic report every N seconds |
| `CANONICAL_DB_USER` / `CANONICAL_DB_PASSWORD` / `CANONICAL_DB_NAME` | canonical-store, aggregator | aggregator / none / canonical_store | Canonical Store credentials and database |
| `USER_DB_USER` / `USER_DB_PASSWORD` / `USER_DB_NAME` | user-db, auth-service | auth / none / user_db | User DB credentials and database |
| `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | rabbitmq, aggregator, consumers | hazard, change-me | Broker login. The user is created at container start from these, change the password in `.env` and recreate with `docker compose down -v` |
| `RABBITMQ_MANAGEMENT_PORT` | rabbitmq | 15672 | Host port of the RabbitMQ management UI |
| `RABBITMQ_EXCHANGE` | aggregator, consumers | hazard.events | Fanout exchange name; must match `infra/rabbitmq/definitions.json` |
| `POLL_INTERVAL_MS` | aggregator | 3000 | Poll interval per source |
| `POLL_TIMEOUT_MS` | aggregator | 4000 | Timeout of one poll call |
| `CB_FAILURE_THRESHOLD` | aggregator | 3 | Failed polls in a row before the circuit breaker opens |
| `CB_OPEN_DURATION_MS` | aggregator | 10000 | How long the breaker stays open before a trial poll |
| `AGGREGATOR_MAX_INFLIGHT_REQUESTS` | aggregator | 100 | Concurrency cap of the gRPC read API (`RESOURCE_EXHAUSTED` beyond it) |
| `DB_POOL_SIZE` | aggregator | 20 | Connection pool to the Canonical Store |
| `JWT_SIGNING_SECRET` | auth-service, client-facing-api | none | Shared HMAC secret for JWTs |
| `SECRET_SALT` | user-db, auth-service | none | Salt in `sha256(password + salt)`; the User DB seed uses it too |
| `ACCESS_TOKEN_TTL_S` | auth-service, client-facing-api | 60 | Tim Lapangan access token lifetime (seconds) |
| `SEED_OPERATOR_EMAIL` / `SEED_OPERATOR_PASSWORD` | user-db, auth-service | none | The Operator account created on a fresh User DB |
| `CLIENT_FACING_API_PORT` | client-facing-api | 8080 | Host port of the public API |
| `CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS` | client-facing-api | 100 | Global concurrency cap (429 + `Retry-After` beyond it) |
| `RATE_LIMIT_RPS` | client-facing-api | 50 | Token bucket per client (account) |
| `AUTH_RATE_LIMIT_RPS` | client-facing-api | 10 | Token bucket per client IP on `/auth/login` and `/auth/register` |
| `UPSTREAM_TIMEOUT_MS` | client-facing-api | 1000 | Timeout of each call to the Aggregator and the Auth Service |
| `COOKIE_SECURE` | client-facing-api | true | Mark the `jwt`/`refresh_token` cookies Secure (fine on `http://localhost`) |
| `WEB_FRONTEND_PORT` | web-frontend | 3000 | Host port of the web frontend |
| `CLIENT_FACING_API_BASE_URL` | web-frontend (build) | /api | API base the static app calls; `/api` goes through the frontend's own nginx |
| `FRONTEND_POLL_INTERVAL_MS` | web-frontend (build) | 5000 | How often the hazards page refreshes |

Fixed in `docker-compose.yml` rather than `.env`, because they only wire the containers together: internal ports (`PORT`, `GRPC_PORT` 9091/9092, `HEALTH_PORT` 8091-8095), the connection strings (`DATABASE_URL`, `USER_DB_URL`, `RABBITMQ_URL`, `BMKG_BASE_URL`, `PVMBG_BASE_URL`, `AGGREGATOR_GRPC_ADDR`, `AUTH_SERVICE_GRPC_ADDR`), each consumer's `QUEUE_NAME`, and the file paths on volumes (`PVMBG_TOKEN_VOLUME_PATH`, `DEDUP_STORE_PATH`; the Audit Logger also writes `/var/lib/audit-logger/audit.log`, its `AUDIT_LOG_PATH` default).

## Test scenarios (Problems 1 to 5)

Each problem also has a script that runs these steps and prints PASS or FAIL per check: `tests/scenarios/problem1.sh` to `problem5.sh`, or `tests/scenarios/all.sh`.

Run from the repository root with the stack up (`docker compose up -d --build --wait`). Needs `curl` and `jq`. Each block is copy-paste; the expected outcome follows it. Load the values the stack runs with and log in as the seeded Operator once:

```bash
set -a; . ./.env; set +a
API=http://localhost:${CLIENT_FACING_API_PORT:-8080}
BMKG=http://localhost:${BMKG_PORT:-8081}
PVMBG=http://localhost:${PVMBG_PORT:-8082}
login() { curl -s -c "$1.jar" -X POST $API/auth/login -d "{\"email\":\"$2\",\"password\":\"$3\"}"; echo; }
login op "$SEED_OPERATOR_EMAIL" "$SEED_OPERATOR_PASSWORD"   # {"expires_in":...}, cookies saved in op.jar
```

### Problem 1: schema change at the source

```bash
curl -s -b op.jar "$API/hazards?type=VOLCANIC&limit=3" | jq '[.data[] | {area_name, severity, confidence: .attributes.confidence_level}]'
curl -s -X POST $PVMBG/admin/schema-version -H "X-Admin-Key: $PVMBG_ADMIN_KEY"; echo   # {"schema_version":"extended"}
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

### Problem 2: slow and failing source

PVMBG latency at 3 s while BMKG stays fast, then the load test:

```bash
PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 docker compose up -d --wait pvmbg-mock
for i in 1 2 3 4 5; do curl -s -o /dev/null -w '%{time_total}s\n' -b op.jar "$API/hazards?type=SEISMIC"; done
tests/load/run.sh   # k6: bmkg_only p95 < 300 ms, mixed 50 connections for 60 s with < 1% errors (429s not counted)
```

The seismic requests stay well under 300 ms (they never wait on PVMBG). Setting the delay recreates the PVMBG container, which forgets the tokens it issued; the Aggregator notices on its next token rotation and re-authenticates by itself (`token bootstrap fallback` in its log, doc/TRD_M1.md 3.0), so PVMBG data can take up to about a minute to come back after this step. `tests/load/run.sh` brings up its own copy of the stack, prints throughput, p50/p95/p99 and counts per status code, and fails if a threshold fails.

Outage on, stale data, recovery:

```bash
curl -s -X POST $PVMBG/admin/outage -H "X-Admin-Key: $PVMBG_ADMIN_KEY"; echo   # {"outage_active":true}
sleep 20
curl -s -b op.jar "$API/hazards?type=VOLCANIC&limit=1" | jq .meta   # source_status DOWN, stale_since set, data still served
curl -s -b op.jar "$API/hazards?type=SEISMIC&limit=1" | jq .meta    # BMKG still UP
curl -s -X POST $PVMBG/admin/outage -H "X-Admin-Key: $PVMBG_ADMIN_KEY"; echo   # {"outage_active":false}
for i in $(seq 90); do curl -s -b op.jar "$API/hazards?type=VOLCANIC&limit=1" | jq -e '.meta.source_status == "UP"' >/dev/null && { echo "PVMBG UP again after ${i}s"; break; }; sleep 1; done
curl -s -b op.jar "$API/hazards?type=VOLCANIC&limit=1" | jq .meta   # UP, stale_since null, nothing restarted
```

With nothing stored for the source the answer is an explicit 503 instead of stale data. To see it, empty the volcanic rows while the outage is on. This deletes the stored volcanic reports (only new ones come back), so run it after the Problem 1 comparison:

```bash
curl -s -X POST $PVMBG/admin/outage -H "X-Admin-Key: $PVMBG_ADMIN_KEY"; echo
sleep 20
docker compose exec -T canonical-store psql -U "$CANONICAL_DB_USER" -d "$CANONICAL_DB_NAME" -c "DELETE FROM hazard_events WHERE hazard_type = 'VOLCANIC'"
curl -s -w '\nHTTP %{http_code}\n' -b op.jar "$API/hazards?type=VOLCANIC"   # HTTP 503 with an error body
curl -s -X POST $PVMBG/admin/outage -H "X-Admin-Key: $PVMBG_ADMIN_KEY"; echo   # off again; the data comes back
```

Restore the normal PVMBG latency with `docker compose up -d --wait pvmbg-mock`.

### Problem 3: authentication and per-field access

Each source only accepts its own credentials:

```bash
curl -s -o /dev/null -w 'BMKG, no key: %{http_code}\n' $BMKG/seismic-events                                    # 401
curl -s -o /dev/null -w 'BMKG, PVMBG admin key: %{http_code}\n' -H "X-BMKG-Key: $PVMBG_ADMIN_KEY" $BMKG/seismic-events   # 403
curl -s -o /dev/null -w 'PVMBG, BMKG key as token: %{http_code}\n' -H "Authorization: Bearer $BMKG_API_KEY" $PVMBG/volcanic-reports   # 401
```

A Media account sees the summary only and is refused raw fields:

```bash
curl -s -X POST $API/auth/register -d '{"email":"media@demo.example","password":"media-pass-1"}' -w 'register: %{http_code}\n'   # 201 (409 on a rerun)
login media media@demo.example media-pass-1
curl -s -b media.jar "$API/hazards?type=SEISMIC&limit=1" | jq '.data[0] | keys'                   # no latitude, longitude, attributes
curl -s -o /dev/null -w 'Media asks for latitude: %{http_code}\n' -b media.jar "$API/hazards?type=SEISMIC&fields=latitude"   # 403
```

A Tim Lapangan token expires after `ACCESS_TOKEN_TTL_S` (60 s) and is refreshed behind the next request:

```bash
curl -s -b op.jar -X POST $API/accounts -d '{"email":"field@demo.example","password":"field-pass-1","role":"Lapangan"}' -w 'create: %{http_code}\n'   # 201 (409 on a rerun)
login field field@demo.example field-pass-1
sleep 65
curl -s -i -b field.jar -c field.jar "$API/hazards?type=SEISMIC&limit=1" | grep -iE '^HTTP|^set-cookie: (jwt|refresh_token)'
```

The last call answers `200` and sets a new `jwt` and a rotated `refresh_token`: the expired session was refreshed transparently. Sending the old cookies again after this fails with `401`.

### Problem 4: independent services and storage ownership

Stop, rebuild and restart one container while the rest keep running:

```bash
docker compose stop notifier && docker compose ps --format '{{.Service}} {{.Status}}'   # only the Notifier is stopped
docker compose up -d --build --no-deps --wait notifier                                  # rebuilt and back, nothing else restarted
curl -s -b op.jar "$API/hazards?type=SEISMIC&limit=1" | jq '.meta.source_status'      # the API kept serving throughout
```

Records before and after the schema change, side by side in the Canonical Store: see the query at the end of Problem 1.

Storage isolation (only the owner reaches each database): see "Checking that the databases are isolated" above.

### Problem 5: event fan-out to independent consumers

Stop one consumer while publishing continues; its queue keeps the backlog and it catches up when it returns:

```bash
docker compose stop notifier
sleep 30
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages   # notifier's count grows, the others stay near 0
docker compose start notifier
sleep 10
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages   # notifier back near 0
docker compose logs --since 15s notifier | grep -c '"message processed"'
```

Add a consumer without touching the producer. The Audit Logger is that consumer: remove it from the running stack, then add it back. The Aggregator is neither changed nor restarted; it only knows the exchange (`services/aggregator` has no reference to any consumer):

```bash
docker compose rm -sf audit-logger
docker compose exec -T rabbitmq rabbitmqctl list_queues name   # its queue still exists from earlier runs, or not at all on a fresh stack
docker compose up -d --wait audit-logger
sleep 10
docker compose exec -T audit-logger sh -c 'wc -l /var/lib/audit-logger/audit.log'   # new events logged straight away
docker compose ps aggregator --format '{{.Status}}'                                 # its uptime shows the producer was not restarted
```

## Running a single service locally (without Docker)

```bash
cd services/bmkg-mock
go run .
```

Reads config from env vars (see that service's section in `doc/TRD_M1.md` for the full list, or `.env.example` at the repo root for every variable across all services).

## Before you push — the checks to run locally

There is no CI (the GitHub Actions workflow was removed), so these checks are the gate. Run them for every service you changed before pushing:

```bash
for d in services/*/; do
  echo "== $d =="
  (cd "$d" &&
    gofmt -l . &&                    # must print nothing — unformatted files fail the check
    go vet ./... &&
    go build ./... &&
    go test -race -v ./... &&
    golangci-lint run ./...)
done
```

`golangci-lint` isn't part of a plain Go install — grab it from [golangci-lint.run/welcome/install](https://golangci-lint.run/welcome/install/) if you're not using the devenv shell, which already has it on `PATH`.

Any non-empty `gofmt -l` output means run `gofmt -w .` in that service directory before committing. `golangci-lint` uses the repo-root `.golangci.yml` (vendored from the `golang-lint` skill) — it's strict on purpose (unchecked errors, missing timeouts on HTTP servers, etc.); fix the underlying issue rather than reaching for a `//nolint` comment unless you can justify it in the comment itself.

If you've changed a `Dockerfile` or added new dependencies, also run `docker build services/<name>` before pushing.

## Web frontend

A static single-page app built with SvelteKit (`adapter-static`, no server-side rendering) in `services/web-frontend`, served by nginx. After `docker compose up` open <http://localhost:3000>. Routes:

- `/login`, `/register` (always a Media account), and Log out in the header.
- `/hazards`: Seismic and Volcanic tabs, a severity filter, and a search box that runs on its own once typing pauses for 350 ms. The browser only ever holds one page: each request asks the API for `limit` (10, 25, 50 or 100 per page) and `offset`, and the page refreshes by polling every `FRONTEND_POLL_INTERVAL_MS` (default 5000). The raw fields (source ref, coordinates, attributes) are drawn only when the API sent them, so a Media session never shows them.
- `/accounts`: create an account with a chosen role. The header link and the page are for Operators only; the page learns the role from `GET /me` (the cookies are HttpOnly), and the API still refuses anyone else with a 403.

**How the cookies reach the API:** the page and the API share one origin. nginx serves the app and proxies `/api/*` to the Client-Facing API (stripping the prefix), so the browser attaches the HttpOnly `jwt` and `refresh_token` cookies itself (`credentials: "include"`) and no CORS is needed. `CLIENT_FACING_API_BASE_URL` (default `/api`) and `FRONTEND_POLL_INTERVAL_MS` are baked into the static files at image build time; a full URL for the first one calls an API on another origin, which would also need CORS with credentials on the Client-Facing API. Changing either needs `docker compose build web-frontend`. The cookies are `Secure` by default (`COOKIE_SECURE=true`); browsers still accept them on `http://localhost`, but set `COOKIE_SECURE=false` to use another host over plain HTTP.

The page follows the light or dark setting of the browser. Paging, the severity filter and the search are done by the API and the Canonical Store, not the browser. The API gives no total: it asks the store for one extra row to know whether another page follows. `OFFSET` and the substring search get slower on very large tables; the code notes the upgrade (keyset paging and a trigram index).

## Service-to-service integration tests

Each suite starts only the two services of one pair (plus the storage or broker they need) in a separate compose project, `hazard-it`, drives the real network path, and tears everything down afterwards. It uses `.env.example`, so no `.env` is needed, and its host ports (18080-18082, 15432, 19091) stay clear of a normal `docker compose up`.

```bash
tests/integration/run.sh cfa-auth    # Client-Facing API + Auth Service (login, logout, local JWT check, transparent refresh, register, /accounts)
tests/integration/run.sh agg-bmkg    # Aggregator + BMKG mock (API key, since cursor, events and tsunami warnings mapped and stored)
tests/integration/run.sh agg-pvmbg   # Aggregator + PVMBG mock (OAuth bootstrap and rotation, outage, schema change: Problems 1-3)
tests/integration/run.sh all         # all three, one after the other
tests/integration/run.sh abuse       # the whole stack through restarts, outages, crashes, load and malformed input (about 10 minutes; not part of "all")
tests/integration/run.sh abuse input # only the tests that need no stack reset: bad input, credentials, limits (about a minute)
```

`abuse` is the failure-mode suite (`abuse_integration_test.go` in `services/client-facing-api`). Every test starts from a fresh `down -v` stack, so one failure never explains the next, and each carries a comment saying what it does and what it expects. A red test is a real defect: the comment names the issue when there is one. The sets are `input`, `resilience` (restarts, outages, crashes, poison messages), `load` and `all` (the default). `IT_LOAD_SECONDS=60` makes the load test run for the spec's full minute, and `ABUSE_RUN=TestAbuseDownUp tests/integration/run.sh abuse` runs a single test.

Needs Docker Compose and Go. The tests live next to the code they exercise (`integration_test.go` in `services/client-facing-api` and `services/aggregator`) and skip unless `run.sh` sets their environment, so a plain `go test ./...` stays offline.

## Full-stack smoke test

Brings up the whole `docker-compose.yml` stack in a separate project, `hazard-smoke`, and checks that it works as one system: every service's `/health` answers, the seeded Operator can log in and `/hazards` returns data from both mocks through the Aggregator, and restarting one consumer (the Notifier) leaves every other service running and untouched.

```bash
tests/smoke/run.sh
```

Needs Docker Compose and curl. It uses `.env.example`, so no `.env` is needed, builds the images, tears everything down afterwards, and publishes its host ports on 28080-28082, 25672 and 23000, clear of a normal `docker compose up`. Takes about a minute.

## Load test (Problem 2)

Runs [k6](https://k6.io) (from its Docker image, nothing to install) against the full stack in a separate compose project, `hazard-load`, with PVMBG answering after 3 s. Two scenarios, both against `/hazards` as the seeded Operator:

- `bmkg_only`: 20 requests/s for 30 s on `/hazards?type=SEISMIC`; threshold p95 < 300 ms while PVMBG is slow.
- `mixed`: 50 concurrent connections for 60 s over seismic, volcanic, field-filtered and `/health` requests; threshold error rate < 1%, where a 429 (the controlled load shedding) does not count as an error.

```bash
tests/load/run.sh
MIXED_VUS=100 MIXED_DURATION=120s tests/load/run.sh   # a bigger run
```

k6 prints throughput, avg/p50/p90/p95/p99 latency, and request counts per status code (200, 401, 429, 500, 502, 503, 504, so 429s are visible apart from real failures). The summary is saved as `tests/load/results/p2-<timestamp>.txt` and `.json` (git-ignored) to paste into the report. The script exits non-zero if a threshold fails. Needs Docker Compose; it uses `.env.example`, so no `.env` is needed, and publishes its host ports on 38080-38082, 35672 and 33000. Results depend on the machine it runs on (doc/TRD_M1.md 12).

## Go workspace notes

- Adding a new service: create `services/<new-name>/`, add its `go.mod`, then `go work use ./services/<new-name>` from the repo root so it joins the workspace.
- Cross-module builds (e.g. checking everything compiles at once) use the module path, not a directory glob:
  ```bash
  go build github.com/brii26/distributed-hazard-coordination/services/...
  ```

## Storage integration tests

Tests for one service against its own real storage or broker: the Auth Service store against the User DB (login lookup, the seeded Operator, refresh rotation including the old token being rejected and concurrent reuse of one token, logout, duplicate email), the Aggregator store against the Canonical Store, and the Aggregator publisher against RabbitMQ.

```bash
tests/integration/storage.sh
```

It starts throwaway Postgres (with each service's `migrations/`) and RabbitMQ containers on ports 25432, 25433 and 25673, runs the suites and removes the containers. It uses `.env.example`, so no `.env` is needed. Needs Docker and Go. Each suite skips when its env var (`USER_DB_TEST_URL`, `CANONICAL_DB_TEST_URL`, `RABBITMQ_TEST_URL`) is unset, so a plain `go test ./...` stays offline.

## Where things are tracked

Feature/task backlog lives in [GitHub Issues](https://github.com/brii26/distributed-hazard-coordination/issues), labeled by `service:*` and `problem:P1`–`P5` (traceable back to `doc/TRD_M1.md` §11). Dependencies between issues are set as native GitHub relationships (`blocked-by`/`blocking`) — see `doc/BACKLOG_DEPENDENCIES.md` for the whole-graph view and what's unblocked right now.

`gh` (GitHub CLI) is entirely optional for a human contributor — the web UI does everything `gh` does. It's called out here because it's genuinely convenient for an AI coding agent working in this repo: creating issues/PRs and reading issue bodies from the terminal without a browser round-trip. See `AGENTS.md` for that recommendation; nothing in this repo requires it.
