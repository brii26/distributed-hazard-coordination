# Platform Koordinasi Kebencanaan

Distributed system for IF4031 Milestone 1: BMKG/PVMBG mock sources, an Aggregator that maps their data into a canonical `HazardEvent` format, a Client-Facing API + Auth Service for downstream clients (Media/Tim Lapangan/BNPB Ops), and a RabbitMQ fan-out to independent consumers.

Full technical spec: [`doc/TRD_M1.md`](doc/TRD_M1.md). Assignment spec: [`doc/IF4031 - Tugas Besar - M1.md`](<doc/IF4031 - Tugas Besar - M1.md>). Contributor workflow (branches, PRs, reviews): [`CONTRIBUTING.md`](CONTRIBUTING.md). What to work on next: [`doc/BACKLOG_DEPENDENCIES.md`](doc/BACKLOG_DEPENDENCIES.md).

## Prerequisites

- Go 1.26+
- Docker + Docker Compose (only needed for `docker compose up` — not required to build/test an individual service)
- `git`

How you get these on your machine is up to you — a system package manager, `asdf`, whatever. If you'd rather not manage them yourself, this repo happens to include a [devenv](https://devenv.sh/) config (`devenv.nix`) that pins Go, `golangci-lint`, and `gh` for you: `devenv shell` drops you into a shell with all of it on `PATH`. That's just how the maintainers set their own machines up, not something the project requires — every command below is a plain command that works the same whether it's typed directly or inside a devenv shell.

## Setup

```bash
git clone https://github.com/brii26/distributed-hazard-coordination.git
cd distributed-hazard-coordination
cp .env.example .env  # fill in real values — .env is gitignored, never commit it
```

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

```bash
docker compose up --build
```

Waits on health checks before dependent services start (e.g. Aggregator waits on Postgres + RabbitMQ + both mocks). Client-Facing API is exposed on `:8080`, the web frontend on `:3000` (see "Web frontend" below); BMKG/PVMBG mocks on `:8081`/`:8082` if you want to poke them directly.

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

A static client-side-rendered dashboard (plain HTML/JS/CSS, no build step, no backend of its own) in `services/web-frontend`, served by nginx. After `docker compose up` open <http://localhost:3000>:

- **Log in / Register** (Register always creates a Media account), **Logout**.
- **Hazards**: Seismic and Volcanic tabs, refreshed by polling every `FRONTEND_POLL_INTERVAL_MS` (default 5000). The raw columns (latitude, longitude, attributes) are drawn only when the API sent them, so a Media session never shows them.
- **Create an account** (Operator only): the form is always shown because the HttpOnly cookies hide the role from the page; a non-Operator gets a 403 message.

**How the cookies reach the API:** the page and the API share one origin. The frontend's nginx proxies `/auth/*`, `/accounts` and `/hazards` to the Client-Facing API, so the browser attaches the HttpOnly `jwt` and `refresh_token` cookies itself (`credentials: "include"`) and no CORS is needed. `CLIENT_FACING_API_BASE_URL` (baked into `config.js` at image build time, empty by default) only needs setting to call an API on a different origin, which would also need CORS with credentials added to the Client-Facing API. The cookies are `Secure` by default (`COOKIE_SECURE=true`); browsers still accept them on `http://localhost`, but set `COOKIE_SECURE=false` to use another host over plain HTTP. Changing either frontend setting needs `docker compose build web-frontend`.

## Service-to-service integration tests

Each suite starts only the two services of one pair (plus the storage or broker they need) in a separate compose project, `hazard-it`, drives the real network path, and tears everything down afterwards. It uses `.env.example`, so no `.env` is needed, and its host ports (18080-18082, 15432, 19091) stay clear of a normal `docker compose up`.

```bash
tests/integration/run.sh cfa-auth    # Client-Facing API + Auth Service (login, logout, local JWT check, transparent refresh, register, /accounts)
tests/integration/run.sh agg-bmkg    # Aggregator + BMKG mock (API key, since cursor, events and tsunami warnings mapped and stored)
tests/integration/run.sh agg-pvmbg   # Aggregator + PVMBG mock (OAuth bootstrap and rotation, outage, schema change: Problems 1-3)
tests/integration/run.sh all         # all three, one after the other
```

Needs Docker Compose and Go. The tests live next to the code they exercise (`integration_test.go` in `services/client-facing-api` and `services/aggregator`) and skip unless `run.sh` sets their environment, so a plain `go test ./...` stays offline.

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
