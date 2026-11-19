# Configuration reference

Back to the [README](../README.md).


## The `.env` file

`.env.example` is the committed template. `.env` is your own copy with the real values, it is gitignored, and Compose reads it from the repository root.

- **Create it:** `cp .env.example .env`. The example values work as is for a local run.
- **Anywhere else:** replace every `change-me*` value with a long random string: `BMKG_API_KEY`, `PVMBG_OAUTH_CLIENT_ID`, `PVMBG_OAUTH_CLIENT_SECRET`, `PVMBG_ONE_TIME_CODE`, the three database and broker passwords, `JWT_SIGNING_SECRET`, `SECRET_SALT` and `SEED_OPERATOR_PASSWORD`. Never commit `.env`.
- **Apply a change:** `docker compose up -d` recreates the containers whose variables changed. Database and RabbitMQ credentials are only read on the first start of an empty volume, so changing them needs `docker compose down -v` first. The two frontend variables are baked in at build time and need `docker compose build web-frontend`.
- **Who reads which file:** the scenario scripts and the commands in [TEST_SCENARIOS.md](TEST_SCENARIOS.md) read `.env`. The integration, smoke and load tests use `.env.example` in their own compose projects, so they work without a `.env`.

## Variables

Every variable in `.env.example`. Compose reads `.env` and passes each one to the services listed; the default applies when the variable is unset. A variable with no default must be set (the example values are fine locally).

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
| `PVMBG_DELAY_MIN_MS` / `PVMBG_DELAY_MAX_MS` | pvmbg-mock | 500 / 3000 | Injected latency per request (Problem 2) |
| `PVMBG_SEED_COUNT` | pvmbg-mock | 20 | Volcanic reports generated at start |
| `PVMBG_EVENT_INTERVAL_S` | pvmbg-mock | 10 | A new volcanic report every N seconds |
| `CANONICAL_DB_USER` / `CANONICAL_DB_PASSWORD` / `CANONICAL_DB_NAME` | canonical-store, aggregator | aggregator / none / canonical_store | Canonical Store credentials and database |
| `USER_DB_USER` / `USER_DB_PASSWORD` / `USER_DB_NAME` | user-db, auth-service | auth / none / user_db | User DB credentials and database |
| `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | rabbitmq, aggregator, consumers | hazard, change-me | Broker login. The user is created at container start from these, change the password in `.env` and recreate with `docker compose down -v` |
| `RABBITMQ_MANAGEMENT_PORT` | rabbitmq | 15672 | Host port of the RabbitMQ management UI |
| `RABBITMQ_EXCHANGE` | aggregator, consumers | hazard.events | Fanout exchange name; must match `infra/rabbitmq/definitions.json` |
| `CRITICAL_QUEUE_NAME` | aggregator, notifier | critical-alerts | Durable queue for critical alerts; must match `infra/rabbitmq/definitions.json.tpl` |
| `NOTIFIER_URL` | client-facing-api | empty | Base URL of the Notifier. When set, `GET /alerts/stream` relays its alert stream to Lapangan and Operator, when empty the route is off. Compose and `deploy/client-facing-api.yaml` set `http://notifier:8094` |
| `POLL_INTERVAL_MS` | aggregator | 3000 | Poll interval per source |
| `POLL_TIMEOUT_MS` | aggregator | 4000 | Timeout of one poll call |
| `CB_FAILURE_THRESHOLD` | aggregator | 3 | Failed polls in a row before the circuit breaker opens |
| `CB_OPEN_DURATION_MS` | aggregator | 10000 | How long the breaker stays open before a trial poll |
| `AGGREGATOR_MAX_INFLIGHT_REQUESTS` | aggregator | 100 | Concurrency cap of the gRPC read API (`RESOURCE_EXHAUSTED` beyond it) |
| `DB_POOL_SIZE` | aggregator | 20 | Connection pool to the Canonical Store |
| `COORDINATION_DATABASE_URL` | aggregator | value of `DATABASE_URL` | Database for lease, source cursors and the PVMBG token pair. Set to the Service `coordination-db` in k3s, unset in docker-compose (one pool for both). Credentials are `store-credentials`, so there is nothing new in `.env` |
| `RETRY_BASE` / `RETRY_CAP` | aggregator | 10s / 10m | First wait before a failed record is retried, and the longest wait. The wait doubles per failure, with jitter |
| `RETRY_MAX_ATTEMPTS` | aggregator | 8 | The failure that reaches this number parks the record (no more automatic retries) |
| `RETRY_POLL` | aggregator | 5s | How often the retry worker looks for due records |
| `LEASE_TTL` / `LEASE_RENEW_INTERVAL` | aggregator (Kubernetes) | 15s / 3s | How long the polling lease lasts and how often the holder renews it. Read by the Aggregator on Kubernetes, the lease rows live in `coordination-db` |
| `STORE_EXPECTED_NODES` | aggregator (compose cluster overlay sets 3; Kubernetes: set it to the node count if wanted) | unset | How many store nodes the cluster should have. With fewer live nodes, `GET /analytics` sets `degraded` even after membership has dropped the dead node |
| `STORE_REBALANCE_INTERVAL` | aggregator (Kubernetes) | 30s | Time between rebalance rounds, which copy missing rows to the shard owners |
| `REBALANCE_PAGE_PAUSE` | aggregator (Kubernetes) | 100ms | Pause between copy pages, so a round does not starve live traffic |
| `REBALANCE_DELETE` | aggregator (Kubernetes) | false | Also delete shards from nodes that no longer own them. The rebalance lease lives in `coordination-db`, so one replica runs a round at a time |
| `JWT_SIGNING_SECRET` | auth-service, client-facing-api | none | Shared HMAC secret for JWTs |
| `SECRET_SALT` | user-db, auth-service | none | Salt in `sha256(password + salt)`; the User DB seed uses it too |
| `ACCESS_TOKEN_TTL_S` | auth-service, client-facing-api | 60 | Access token lifetime for every role (seconds) |
| `SEED_OPERATOR_EMAIL` / `SEED_OPERATOR_PASSWORD` | user-db, auth-service | none | The Operator account created on a fresh User DB |
| `CLIENT_FACING_API_PORT` | client-facing-api | 8080 | Host port of the public API |
| `CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS` | client-facing-api | 100 | Global concurrency cap, at least 2 (429 + `Retry-After` beyond it) |
| `MEDIA_MAX_INFLIGHT` | client-facing-api | 80% of global cap | Shared Media and anonymous concurrency budget. Must be positive and less than the global cap. Lapangan and Operator can use all global slots |
| `RATE_LIMIT_RPS` | client-facing-api | 50 | Token bucket per client (account) |
| `AUTH_RATE_LIMIT_RPS` | client-facing-api | 10 | Token bucket per client IP on `/auth/login` and `/auth/register` |
| `UPSTREAM_TIMEOUT_MS` | client-facing-api | 1000 | Timeout of each call to the Aggregator and the Auth Service, except the `GET /hazards` list and `GET /hazards/{id}` reads (see `READ_TIMEOUT_MS`) |
| `READ_TIMEOUT_MS` | client-facing-api | 3000 | Timeout in ms of `GET /hazards` (list) and `GET /hazards/{id}`; must outlast the primary try plus the replica try |
| `COOKIE_SECURE` | client-facing-api | true | Mark the `jwt`/`refresh_token` cookies Secure (fine on `http://localhost`) |
| `WEB_FRONTEND_PORT` | web-frontend | 3000 | Host port of the web frontend |
| `CLIENT_FACING_API_BASE_URL` | web-frontend (build) | /api | API base the static app calls; `/api` goes through the frontend's own nginx |
| `FRONTEND_POLL_INTERVAL_MS` | web-frontend (build) | 5000 | How often the hazards page refreshes |
| `AUDIT_TOKEN` | Kubernetes Secret `audit-token` | change-me | Token for the Internal Audit API |
| `ADMIN_TOKEN` | Kubernetes Secret `admin-token`, aggregator | change-me | Bearer token for `POST /internal/admin/replay`. Separate from `AUDIT_TOKEN`; unset means the endpoint refuses every call |
| `REPLAY_BATCH` | aggregator | 500 | Parked rows read per page when replaying |
| `SATELLITE_GROUP_TOKEN` | Kubernetes Secret `satellite-token` | change-me | Group token for the assistants' satellite feed |
| `PANTAI_AUTH_TOKEN` | Kubernetes Secret `pantai-token`, aggregator | change-me | Bearer token the Sensor Pantai checks. The Aggregator sends it as `Authorization: Bearer` and needs it together with `PANTAI_BASE_URL` |
| `PANTAI_BASE_URL` | aggregator | unset | Base URL of the Sensor Pantai, for example `http://sensor-pantai:8080`. Unset turns the Pantai poller off; set only one of the two and the Aggregator refuses to start |
| `SENSOR_TIMEOUT_MS` | aggregator | 1000 | Timeout of one call to the sensor |
| `SENSOR_SLOW_MS` | aggregator | 800 | A call that answers 200 but takes longer than this counts as a failure. Must not be above `SENSOR_TIMEOUT_MS` |
| `SENSOR_CB_FAILURE_THRESHOLD` | aggregator | 3 | Failed or slow polls in a row before the sensor breaker opens |
| `SENSOR_CB_OPEN_MS` | aggregator | 10000 | How long the sensor breaker stays open before one trial poll |
| `SENSOR_BACKOFF_BASE_MS` | aggregator | 3000 | First delay after a failed sensor poll, doubled for every failure in a row, with jitter. The wait is never shorter than `POLL_INTERVAL_MS` |
| `SENSOR_BACKOFF_MAX_MS` | aggregator | 30000 | Cap of that delay. Must not be below `SENSOR_BACKOFF_BASE_MS` |
| `SENSOR_MAX_CONCURRENT` | aggregator | 2 | Most calls in flight to the sensor, also the size of its own connection pool. A call over the limit fails at once |
| `SENSOR_MAX_PAGES` | aggregator | 20 | Most pages read in one sensor poll, the rest comes in the next poll |
| `SENSOR_STALE_AFTER_MS` | aggregator | 60000 | The sensor answers but its clock and newest observation have not moved for this long: the source is marked `STALE` |
| `PANTAI_PROFILE` | Sensor Pantai (ConfigMap) | practice | `practice` or `exam`. `exam` needs the key and `deploy/overlays/exam`, see [DEPLOY.md](DEPLOY.md) |
| `PANTAI_SEED` | Sensor Pantai (ConfigMap) | 42 | Seed of the generated data |
| `PANTAI_SPEED` | Sensor Pantai (ConfigMap) | 60 | Simulation speed, 1 to 100000 |
| `PANTAI_KEY_FILE` | `deploy/make-secrets.sh` | empty | Path of the exam key file given by the assistants. Empty uses a placeholder for Secret `pantai-key` |

Fixed in `docker-compose.yml` rather than `.env`, because they only wire the containers together: internal ports (`PORT`, `GRPC_PORT` 9091/9092, `HEALTH_PORT` 8091-8095), the connection strings (`DATABASE_URL`, `USER_DB_URL`, `RABBITMQ_URL`, `BMKG_BASE_URL`, `PVMBG_BASE_URL`, `AGGREGATOR_GRPC_ADDR`, `AUTH_SERVICE_GRPC_ADDR`), each consumer's `QUEUE_NAME`, and the file paths on volumes (`PVMBG_TOKEN_VOLUME_PATH`, `DEDUP_STORE_PATH`; the Audit Logger also writes `/var/lib/audit-logger/audit.log`, its `AUDIT_LOG_PATH` default). On Kubernetes the same wiring sits in the manifests of `deploy/`, where the Aggregator also gets `COORDINATION_DATABASE_URL` (the lease database, `coordination-db`), `STORE_SERVICE` (`store`) and `STORE_DSN_TEMPLATE` (one DSN per store node), see [DEPLOY.md](DEPLOY.md).

