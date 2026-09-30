# Platform Koordinasi Kebencanaan

Distributed system for IF4031 Milestone 1: BMKG/PVMBG mock sources, an Aggregator that maps their data into a canonical `HazardEvent` format, a Client-Facing API + Auth Service for downstream clients (Media/Tim Lapangan/BNPB Ops), and a RabbitMQ fan-out to independent consumers.

Full technical spec: [`doc/TRD_M1.md`](doc/TRD_M1.md). Assignment spec: [`doc/IF4031 - Tugas Besar - M1.md`](<doc/IF4031 - Tugas Besar - M1.md>). Contributor workflow (branches, PRs, reviews): [`CONTRIBUTING.md`](CONTRIBUTING.md). What to work on next: [`doc/BACKLOG_DEPENDENCIES.md`](doc/BACKLOG_DEPENDENCIES.md).

## Prerequisites

- [Nix](https://nixos.org/download) with flakes enabled
- [devenv](https://devenv.sh/getting-started/)
- Docker (for `docker compose up` — not required just to build/test individual services)

Everything else (Go, `gh`, `golangci-lint`) is pinned in `devenv.nix` and installed automatically when you enter the shell.

## Setup

```bash
git clone https://github.com/brii26/distributed-hazard-coordination.git
cd distributed-hazard-coordination
devenv shell          # first entry downloads/builds the pinned toolchain, be patient
cp .env.example .env  # fill in real values — .env is gitignored, never commit it
```

Authenticate `gh` once per machine (needed for creating PRs, viewing issues, etc.):

```bash
gh auth login --hostname github.com --git-protocol https --web
```

(The default interactive prompt can break on some terminals with an "unexpected escape sequence" error — the flags above skip the broken prompt entirely.)

## Repository layout

```
services/<name>/     one Go module per service (see doc/TRD_M1.md for what each does)
  go.mod
  main.go
  main_test.go
  Dockerfile
frontend/             CSR web frontend (not Go — scaffolded separately, see issue #33)
doc/                  spec, assignment docs, backlog dependency graph
.claude/skills/       vendored Go skills (also symlinked at .agents/skills for Codex)
.github/workflows/    CI
docker-compose.yml    full local stack
go.work               ties all service modules together for cross-module tooling
```

Each service directory is its **own Go module** (`go.work` lists them all). This means `go build ./...`/`go test ./...` from the repo root won't work as a single command — the repo root isn't itself a module. Run per-service commands from inside `services/<name>/`, or use the loop shown below.

## Running the full stack

```bash
devenv shell -- docker compose up --build
```

Waits on health checks before dependent services start (e.g. Aggregator waits on Postgres + RabbitMQ + both mocks). Client-Facing API is exposed on `:8080`; BMKG/PVMBG mocks on `:8081`/`:8082` if you want to poke them directly.

## Running a single service locally (without Docker)

```bash
cd services/bmkg-mock
devenv shell -- go run .
```

Reads config from env vars (see that service's section in `doc/TRD_M1.md` for the full list, or `.env.example` at the repo root for every variable across all services).

## Before you push — the exact checks CI runs

CI (`.github/workflows/ci.yml`) runs these per service on every push/PR. Run them yourself first so you're not waiting on a red CI run to find out:

```bash
devenv shell -- bash -c '
for d in services/*/; do
  echo "== $d =="
  (cd "$d" &&
    gofmt -l . &&                    # must print nothing — unformatted files fail CI
    go vet ./... &&
    go build ./... &&
    go test -race -v ./... &&
    golangci-lint run ./...)
done
'
```

Any non-empty `gofmt -l` output means run `gofmt -w .` in that service directory before committing. `golangci-lint` uses the repo-root `.golangci.yml` (vendored from the `golang-lint` skill) — it's strict on purpose (unchecked errors, missing timeouts on HTTP servers, etc.); fix the underlying issue rather than reaching for a `//nolint` comment unless you can justify it in the comment itself.

CI also does a `docker build` per service — if you've changed a `Dockerfile` or added new dependencies, worth a local `docker build services/<name>` before pushing too.

## Go workspace notes

- Adding a new service: create `services/<new-name>/`, add its `go.mod`, then `go work use ./services/<new-name>` from the repo root so it joins the workspace.
- Cross-module builds (e.g. checking everything compiles at once) use the module path, not a directory glob:
  ```bash
  devenv shell -- go build github.com/brii26/distributed-hazard-coordination/services/...
  ```

## Where things are tracked

Feature/task backlog lives in [GitHub Issues](https://github.com/brii26/distributed-hazard-coordination/issues), labeled by `service:*` and `problem:P1`–`P5` (traceable back to `doc/TRD_M1.md` §11). Dependencies between issues are set as native GitHub relationships (`blocked-by`/`blocking`) — see `doc/BACKLOG_DEPENDENCIES.md` for the whole-graph view and what's unblocked right now.
