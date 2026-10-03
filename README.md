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
.github/workflows/    CI
docker-compose.yml    full local stack
go.work               ties all service modules together for cross-module tooling
```

Each service directory is its **own Go module** (`go.work` lists them all). This means `go build ./...`/`go test ./...` from the repo root won't work as a single command — the repo root isn't itself a module. Run per-service commands from inside `services/<name>/`, or use the loop shown below.

CI treats every directory under `services/` as a Go module (`gofmt`, `go vet`, `golangci-lint`, `docker build`). Config-only components such as the broker go in `infra/` so they aren't picked up.

## Running the full stack

```bash
docker compose up --build
```

Waits on health checks before dependent services start (e.g. Aggregator waits on Postgres + RabbitMQ + both mocks). Client-Facing API is exposed on `:8080`; BMKG/PVMBG mocks on `:8081`/`:8082` if you want to poke them directly.

## Running a single service locally (without Docker)

```bash
cd services/bmkg-mock
go run .
```

Reads config from env vars (see that service's section in `doc/TRD_M1.md` for the full list, or `.env.example` at the repo root for every variable across all services).

## Before you push — the exact checks CI runs

CI (`.github/workflows/ci.yml`) runs these per service on every push/PR. Run them yourself first so you're not waiting on a red CI run to find out:

```bash
for d in services/*/; do
  echo "== $d =="
  (cd "$d" &&
    gofmt -l . &&                    # must print nothing — unformatted files fail CI
    go vet ./... &&
    go build ./... &&
    go test -race -v ./... &&
    golangci-lint run ./...)
done
```

`golangci-lint` isn't part of a plain Go install — grab it from [golangci-lint.run/welcome/install](https://golangci-lint.run/welcome/install/) if you're not using the devenv shell, which already has it on `PATH`.

Any non-empty `gofmt -l` output means run `gofmt -w .` in that service directory before committing. `golangci-lint` uses the repo-root `.golangci.yml` (vendored from the `golang-lint` skill) — it's strict on purpose (unchecked errors, missing timeouts on HTTP servers, etc.); fix the underlying issue rather than reaching for a `//nolint` comment unless you can justify it in the comment itself.

CI also does a `docker build` per service — if you've changed a `Dockerfile` or added new dependencies, worth a local `docker build services/<name>` before pushing too.

## Go workspace notes

- Adding a new service: create `services/<new-name>/`, add its `go.mod`, then `go work use ./services/<new-name>` from the repo root so it joins the workspace.
- Cross-module builds (e.g. checking everything compiles at once) use the module path, not a directory glob:
  ```bash
  go build github.com/brii26/distributed-hazard-coordination/services/...
  ```

## Where things are tracked

Feature/task backlog lives in [GitHub Issues](https://github.com/brii26/distributed-hazard-coordination/issues), labeled by `service:*` and `problem:P1`–`P5` (traceable back to `doc/TRD_M1.md` §11). Dependencies between issues are set as native GitHub relationships (`blocked-by`/`blocking`) — see `doc/BACKLOG_DEPENDENCIES.md` for the whole-graph view and what's unblocked right now.

`gh` (GitHub CLI) is entirely optional for a human contributor — the web UI does everything `gh` does. It's called out here because it's genuinely convenient for an AI coding agent working in this repo: creating issues/PRs, checking CI status, and reading issue bodies from the terminal without a browser round-trip. See `AGENTS.md` for that recommendation; nothing in this repo requires it.
