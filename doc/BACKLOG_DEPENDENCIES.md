# Backlog Dependency Graph

Not a schedule — an Activity-on-Node map of the M1 backlog (GitHub issues #1–#37), so anyone can answer "what can I start right now?" without reading every issue's `blocked-by` field individually. Dependencies are set as native GitHub issue relationships (`gh issue edit --add-blocked-by`), visible on each issue itself; this doc is the whole-graph view GitHub doesn't render on its own.

Regenerate after adding/closing issues: `gh issue list --repo brii26/distributed-hazard-coordination --json number,title,state`.

## Ready now (no open blockers)

These have zero dependencies and can be picked up immediately, by anyone, in any order:

- #1 BMKG mock: seed data and background event generator
- #3 BMKG mock: X-BMKG-Key validation
- #4 BMKG mock: structured logging and correlation ID
- #5 PVMBG mock: seed data and background event generator
- #6 PVMBG mock: OAuth token issuance (bootstrap + refresh rotation)
- #11 Auth Service: users and refresh_tokens schema
- #14 RabbitMQ: fanout exchange and durable per-consumer queue setup
- #28 Client-Facing API: concurrency cap and per-client rate limiting
- #34 Cross-cutting: correlation ID propagation across all services
- #35 Cross-cutting: health check depth per service

Once #1 lands, #2 (BMKG endpoints) unblocks — the seed store #2 reads from needs to exist first, so #2 isn't in the "ready now" list even though it has no *other* dependents.

## Full graph

```mermaid
graph TD
  1["#1 bmkg seed+generator"] --> 2["#2 bmkg endpoints"]
  5["#5 pvmbg seed+generator"] -.->|informs, not blocking| 7

  6["#6 pvmbg oauth issuance"] --> 15["#15 agg pvmbg token bootstrap/rotation"]
  6 --> 7["#7 pvmbg endpoint+delay"]

  2 --> 16["#16 agg pollers"]
  7 --> 16
  16 --> 17["#17 agg map seismic"]
  16 --> 18["#18 agg map volcanic"]
  16 --> 20["#20 agg circuit breaker"]
  17 --> 19["#19 agg unknown-field policy"]
  18 --> 19

  19 --> 21["#21 agg upsert"]
  21 --> 22["#22 agg grpc read api"]
  20 --> 22

  14["#14 rabbitmq topology"] --> 23["#23 agg publish"]
  21 --> 23

  11["#11 auth userdb schema"] --> 12["#12 auth login/refresh/logout rpcs"]
  11 --> 13["#13 auth register/createaccount rpcs"]

  12 --> 24["#24 cfa login proxy"]
  24 --> 25["#25 cfa jwt middleware"]
  25 --> 26["#26 cfa transparent refresh"]
  25 --> 13
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

  28 --> 37["#37 p2 load-test harness"]
  20 --> 37
```

## Notes on reading this

- **BMKG/PVMBG cluster** (#1–#9) is almost entirely parallelizable — the mocks don't depend on each other or on anything downstream.
- **Aggregator** (#15–#23) is the critical path: nothing in Client-Facing API's real data flow (#29 onward) or the consumers (#30–#32) can start until the Aggregator chain reaches #21 (upsert) and #22/#23 (the two things it feeds).
- **Auth Service** (#11–#13) and **Client-Facing API's auth chain** (#24–#27) can run fully in parallel with the Aggregator chain — they don't share a blocker until #29, which needs *both* #25 (JWT middleware) and #22 (Aggregator's read API).
- **#28** (rate limiting) and **#34/#35** (cross-cutting logging/health) are deliberately unblocked — they touch every service but don't require any other issue to land first; fold them into whichever issue you're already touching rather than treating them as a separate pass at the end.
- **#36/#37** are integration-level checks — by design, the last things that can go green, since they need real pieces on both sides of the stack.
