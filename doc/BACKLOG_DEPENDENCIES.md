# Backlog Dependency Graph

Not a schedule — an Activity-on-Node map of the M1 backlog (GitHub issues #1–#37), so anyone can answer "what can I start right now?" without reading every issue's `blocked-by` field individually. Dependencies are set as native GitHub issue relationships (`gh issue edit --add-blocked-by`), visible on each issue itself; this doc is the whole-graph view GitHub doesn't render on its own.

Regenerate after adding/closing issues: `gh issue list --repo brii26/distributed-hazard-coordination --json number,title,state`.

## Ready now (no open blockers)

These have zero dependencies and can be picked up immediately, by anyone, in any order:

- #19 Aggregator: unknown/changed-field policy enforcement
- #30 Dashboard Updater: consume HazardEvent and maintain in-memory state
- #33 Web Frontend: CSR scaffold (login, register, hazards views)
- #35 Cross-cutting: health check depth per service
- #37 Cross-cutting: P2 load-test harness
- #61 Cross-cutting: integration tests for a service and its own database or broker
- #94 Cross-cutting: integration tests between our services (service to service)
- #72 Cross-cutting: structured JSON logging and outbound call latency for every service
- #73 Infra: Docker network isolation for Canonical Store and User DB

## Full graph

Green = merged. Merged issues stay in the graph (edges kept) so progress is visible; only the "Ready now" list drops them.

```mermaid
graph TD
  1["#1 bmkg seed+generator"] --> 2["#2 bmkg endpoints"]
  5["#5 pvmbg seed+generator"] -.->|informs, not blocking| 7

  6["#6 pvmbg oauth issuance"] --> 15["#15 agg pvmbg token bootstrap/rotation"]
  6 --> 7["#7 pvmbg endpoint+delay"]
  7 --> 69["#69 pvmbg token expiry"]

  2 --> 16["#16 agg pollers"]
  7 --> 16
  16 --> 17["#17 agg map seismic"]
  16 --> 18["#18 agg map volcanic"]
  16 --> 20["#20 agg circuit breaker"]
  17 --> 19["#19 agg unknown-field policy"]
  18 --> 19

  10["#10 canonical store schema"] --> 21["#21 agg upsert"]
  19 --> 21
  21 --> 22["#22 agg grpc read api"]
  20 --> 22

  14["#14 rabbitmq topology"] --> 23["#23 agg publish"]
  21 --> 23
  21 --> 83["#83 agg ingest pipeline"]

  11["#11 auth userdb schema"] --> 12["#12 auth login/refresh/logout rpcs"]
  11 --> 13["#13 auth register/createaccount rpcs"]

  12 --> 24["#24 cfa login/logout proxy"]
  12 --> 61["#61 integration tests: service + own storage"]
  24 --> 25["#25 cfa jwt middleware"]
  25 --> 26["#26 cfa transparent refresh"]
  25 --> 27["#27 cfa register/accounts"]
  13 --> 27
  25 --> 29["#29 cfa /hazards field auth"]
  22 --> 29

  23 --> 30["#30 dashboard updater"]
  23 --> 31["#31 notifier"]
  23 --> 32["#32 audit logger"]

  24 --> 33["#33 frontend scaffold"]
  27 --> 33
  29 --> 33

  29 --> 36["#36 compose smoke test"]
  30 --> 36
  31 --> 36
  32 --> 36

  28["#28 cfa rate limit + concurrency cap"] --> 37["#37 p2 load-test harness"]
  20 --> 37

  72["#72 structured json logging"] --> 34["#34 correlation id propagation"]
  36 --> 74["#74 readme run guide + scenarios"]
  71["#71 bmkg delay"]
  73["#73 db network isolation"]
  94["#94 integration tests: service to service"]

  classDef done fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
  class 1,2,5,6,7,10,11,12,13,14,15,16,17,18,20,22,23,24,25,26,27,28,29,31,32,69,71 done
```

## Notes on reading this

- **BMKG/PVMBG cluster** (#1–#9) is almost entirely parallelizable — the mocks don't depend on each other or on anything downstream.
- **Aggregator** (#15–#23) is the critical path: nothing in Client-Facing API's real data flow (#29 onward) or the consumers (#30–#32) can start until the Aggregator chain reaches #21 (upsert) and #22/#23 (the two things it feeds).
- **Auth Service** (#11–#13) and **Client-Facing API's auth chain** (#24–#27) can run fully in parallel with the Aggregator chain — they don't share a blocker until #29, which needs *both* #25 (JWT middleware) and #22 (Aggregator's read API).
- **#28** (rate limiting), **#35** (health checks), **#71** (BMKG delay), **#72** (structured logging) and **#73** (network isolation) are deliberately unblocked: they touch one or every service but need no other issue to land first. Fold them into whichever issue you are already touching rather than leaving them for a separate pass at the end. **#34** (correlation IDs) waits only on #72, so it reuses that logger instead of adding its own.
- **#83** (ingest pipeline) is what connects the pollers, the mapper and the store. It waits only on #21, and publishing plugs into it later through #23, which is not a blocker.
- **#36/#37** are integration-level checks, by design the last things that can go green, since they need real pieces on both sides of the stack. **#74** (README run guide and scenario triggers) follows #36 because it documents the finished stack.
