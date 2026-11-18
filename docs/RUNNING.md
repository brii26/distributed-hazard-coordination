# Running the project

How to build, run and inspect the stack. Back to the [README](../README.md).

# Prerequisites

- Go 1.26+
- Docker + Docker Compose (only needed for `docker compose up`, not required to build/test an individual service)
- `git`
- `curl` and `jq`, for the copy-paste scenario commands in [TEST_SCENARIOS.md](TEST_SCENARIOS.md)

How you get these on your machine is up to you, a system package manager or `asdf` both work. If you'd rather not manage them yourself, this repo happens to include a [devenv](https://devenv.sh/) config (`devenv.nix`) that pins Go, `golangci-lint`, and `gh` for you: `devenv shell` drops you into a shell with all of it on `PATH`. That's just how the maintainers set their own machines up, not something the project requires. Every command in these docs is a plain command that works the same whether it's typed directly or inside a devenv shell.

# Run it from a clean clone

Copy the example environment. Its values work as is for a local run, and `.env` is gitignored.

```bash
git clone https://github.com/brii26/distributed-hazard-coordination.git
cd distributed-hazard-coordination
cp .env.example .env
```

Build every image. The command returns once every health check passes.

```bash
docker compose up -d --build --wait
```

Check the stack. Every service is "running", and the ones with a health check show "(healthy)".

```bash
docker compose ps
```

If one of the host ports below is already taken on your machine, change it in `.env` (`CLIENT_FACING_API_PORT`, `WEB_FRONTEND_PORT`, `BMKG_PORT`, `PVMBG_PORT`, `RABBITMQ_MANAGEMENT_PORT`); the scenario commands read the ports from there. The first build takes a few minutes. `--wait` returns once every container with a health check reports healthy, so when it exits the stack is ready. Stop it with `docker compose down`; add `-v` to also wipe the databases, the PVMBG token and the consumers' dedup files and start from zero.

| Port | What |
|---|---|
| <http://localhost:3000> | Web frontend (log in as `SEED_OPERATOR_EMAIL` / `SEED_OPERATOR_PASSWORD` from `.env`) |
| <http://localhost:8080> | Client-Facing API, the only public API (`/auth/*`, `/accounts`, `/me`, `/hazards`, `/health`) |
| <http://localhost:8081> | BMKG mock, to poke it directly (needs `X-BMKG-Key`) |
| <http://localhost:8082> | PVMBG mock, including the demo switches `/admin/outage` and `/admin/schema-version` (no header needed; an empty body toggles legacy and extended, which adds `confidence_level`, and from the incompatible state it goes to legacy, so after a P3 demo send an empty body twice (or `{"version":1}` then an empty body) to get extended back; `{"version":1}` is legacy; `{"version":2}` (3 or more also) breaks the contract, all three ways or one with `"change":"rename"`, `"type"` or `"missing"`, case and spaces ignored, an unknown value means all three with a warning in the log) |
| <http://localhost:15672> | RabbitMQ management UI (`RABBITMQ_USER` / `RABBITMQ_PASSWORD` from `.env`) |

The Aggregator, Auth Service, both databases and the consumers publish no host port. Their health is reachable from inside the stack, for example `docker compose exec aggregator wget -qO- localhost:8091/health`; every service's `/health` answers `{"status":"ok"|"degraded"|"down",...}`.

# Repository layout

```
services/<name>/     one Go module per service (the web frontend is SvelteKit)
  go.mod
  main.go
  main_test.go
  Dockerfile
infra/<name>/        config for third-party components (no Go code), e.g. infra/rabbitmq
doc/                  design docs and the backlog dependency graph
tests/               scenarios (P1-P5), integration, smoke and load tests
docker-compose.yml    full local stack
go.work               ties all service modules together for cross-module tooling
```

Each service directory is its **own Go module** (`go.work` lists them all). This means `go build ./...`/`go test ./...` from the repo root won't work as a single command, the repo root isn't itself a module. Run per-service commands from inside `services/<name>/`, or use the loop in [TESTING.md](TESTING.md#before-you-push).

Every directory under `services/` is a Go module that the local checks in [TESTING.md](TESTING.md#before-you-push) loop over. Config-only components such as the broker go in `infra/` so they aren't treated as one.

# Running the full stack

`docker compose up --build` (as above) waits on health checks before dependent services start: the Aggregator waits for both Postgres instances, RabbitMQ and both mocks, the Client-Facing API for the Aggregator and the Auth Service, the web frontend for the Client-Facing API.

## Checking that the databases are isolated

Each database sits on an internal network shared only with its owner: `canonical-net` (Aggregator and `canonical-store`) and `userdb-net` (Auth Service and `user-db`). Neither database publishes a host port; to look inside one, use `docker compose exec canonical-store psql -U aggregator`.

With the stack running, probe both databases from every service:

```bash
for s in aggregator auth-service bmkg-mock pvmbg-mock rabbitmq client-facing-api web-frontend dashboard-updater notifier audit-logger; do
  echo "$s  canonical-store: $(docker compose exec -T $s sh -c 'nc -zw2 canonical-store 5432 >/dev/null 2>&1 && echo reachable || echo unreachable')" \
          " user-db: $(docker compose exec -T $s sh -c 'nc -zw2 user-db 5432 >/dev/null 2>&1 && echo reachable || echo unreachable')"
done
```

Only `aggregator` reaches `canonical-store` and only `auth-service` reaches `user-db`; everything else says `unreachable`.

# Running a single service locally (without Docker)

```bash
cd services/bmkg-mock
go run .
```

Reads config from env vars (`.env.example` at the repo root lists every variable across all services).

# Web frontend

A static single-page app built with SvelteKit (`adapter-static`, no server-side rendering) in `services/web-frontend`, served by nginx. After `docker compose up` open <http://localhost:3000>. Routes:

- `/login`, `/register` (always a Media account), and Log out in the header.
- `/hazards`: Seismic and Volcanic tabs, a severity filter, and a search box that runs on its own once typing pauses for 350 ms. The browser only ever holds one page: each request asks the API for `limit` (10, 25, 50 or 100 per page) and `offset`, and the page refreshes by polling every `FRONTEND_POLL_INTERVAL_MS` (default 5000). The raw fields (source ref, coordinates, attributes) are drawn only when the API sent them, so a Media session never shows them.
- `/accounts`: create an account with a chosen role. The header link and the page are for Operators only; the page learns the role from `GET /me` (the cookies are HttpOnly), and the API still refuses anyone else with a 403.

**How the cookies reach the API:** the page and the API share one origin. nginx serves the app and proxies `/api/*` to the Client-Facing API (stripping the prefix), so the browser attaches the HttpOnly `jwt` and `refresh_token` cookies itself (`credentials: "include"`) and no CORS is needed. `CLIENT_FACING_API_BASE_URL` (default `/api`) and `FRONTEND_POLL_INTERVAL_MS` are baked into the static files at image build time; a full URL for the first one calls an API on another origin, which would also need CORS with credentials on the Client-Facing API. Changing either needs `docker compose build web-frontend`. The cookies are `Secure` by default (`COOKIE_SECURE=true`); browsers still accept them on `http://localhost`, but set `COOKIE_SECURE=false` to use another host over plain HTTP.

The page follows the light or dark setting of the browser. Paging, the severity filter and the search are done by the API and the Canonical Store, not the browser. The API gives no total: it asks the store for one extra row to know whether another page follows. `OFFSET` and the substring search get slower on very large tables; the code notes the upgrade (keyset paging and a trigram index).

