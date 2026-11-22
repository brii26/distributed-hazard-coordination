# The five Milestone 1 problems

Each problem is solved in the code and reproduced by a script in `tests/scenarios/`. The steps are in [TEST_SCENARIOS.md](TEST_SCENARIOS.md).

### Problem 1: the source changes its schema and nothing restarts

```mermaid
%%{init: {"theme":"base","themeVariables":{"primaryColor":"#f2f2f2","primaryTextColor":"#000000","primaryBorderColor":"#000000","lineColor":"#8b8b8b","secondaryColor":"#f2f2f2","tertiaryColor":"#ffffff","edgeLabelBackground":"#f2f2f2","noteBkgColor":"#d9d9d9","noteTextColor":"#000000","actorBkg":"#f2f2f2","actorBorder":"#000000","actorTextColor":"#000000","actorLineColor":"#8b8b8b","signalColor":"#8b8b8b","signalTextColor":"#8b8b8b","labelTextColor":"#000000","textColor":"#8b8b8b"}}}%%
flowchart LR
  SRC["PVMBG record<br/>schema changes"] --> MAP{"Mapper"}
  MAP -->|known field| CORE["canonical columns"]
  MAP -->|new or retyped field| ATTR["attributes (JSONB)"]
  MAP -->|missing field| NULL["null"]
  CORE --> DB[("Canonical Store")]
  ATTR --> DB
  NULL --> DB
```

- Unknown fields go to a JSONB `attributes` column, so the table never needs a migration and the Aggregator never restarts.
- Old rows stay as they were. A field that changes meaning, or a moved endpoint, is a documented limit.

Problem 3 schema policy: a PVMBG record must carry all six contract fields with their contract types. A record that misses one, sends `null`, or changes a type is rejected with a `schema_incompatible: ...` reason instead of being stored with nulls. New fields still go to `attributes`, and BMKG keeps the tolerant policy above. See [doc/P3_SCHEMA_POLICY.md](doc/P3_SCHEMA_POLICY.md).

---

### Problem 2: a slow or failing source must not drag the others down

```mermaid
%%{init: {"theme":"base","themeVariables":{"primaryColor":"#f2f2f2","primaryTextColor":"#000000","primaryBorderColor":"#000000","lineColor":"#8b8b8b","secondaryColor":"#f2f2f2","tertiaryColor":"#ffffff","edgeLabelBackground":"#f2f2f2","noteBkgColor":"#d9d9d9","noteTextColor":"#000000","actorBkg":"#f2f2f2","actorBorder":"#000000","actorTextColor":"#000000","actorLineColor":"#8b8b8b","signalColor":"#8b8b8b","signalTextColor":"#8b8b8b","labelTextColor":"#000000","textColor":"#8b8b8b"}}}%%
stateDiagram-v2
  [*] --> CLOSED
  CLOSED --> OPEN: 3 failed polls
  OPEN --> HALF_OPEN: after 10 s
  HALF_OPEN --> CLOSED: trial call ok
  HALF_OPEN --> OPEN: trial call fails
```

```mermaid
%%{init: {"theme":"base","themeVariables":{"primaryColor":"#f2f2f2","primaryTextColor":"#000000","primaryBorderColor":"#000000","lineColor":"#8b8b8b","secondaryColor":"#f2f2f2","tertiaryColor":"#ffffff","edgeLabelBackground":"#f2f2f2","noteBkgColor":"#d9d9d9","noteTextColor":"#000000","actorBkg":"#f2f2f2","actorBorder":"#000000","actorTextColor":"#000000","actorLineColor":"#8b8b8b","signalColor":"#8b8b8b","signalTextColor":"#8b8b8b","labelTextColor":"#000000","textColor":"#8b8b8b"}}}%%
flowchart LR
  P1["BMKG poller"] --> CS[("Canonical Store")]
  P2["PVMBG poller<br/>own timeout + breaker"] --> CS
  CS --> AGG["Aggregator read API"]
  AGG -->|gRPC| CFA["Client-Facing API"]
  CFA -->|"stale data + DOWN<br/>or 503 when empty"| C["Client"]
  CFA -->|"over the cap: 429 + Retry-After"| C
```

- One poller, timeout and circuit breaker per source. Reads come from the store, so an outage costs freshness and not availability.
- Overload is shed on purpose: a global concurrency cap, a token bucket per account, and a cap on the gRPC read API.

---

### Problem 3: authentication and per-field access

```mermaid
%%{init: {"theme":"base","themeVariables":{"primaryColor":"#f2f2f2","primaryTextColor":"#000000","primaryBorderColor":"#000000","lineColor":"#8b8b8b","secondaryColor":"#f2f2f2","tertiaryColor":"#ffffff","edgeLabelBackground":"#f2f2f2","noteBkgColor":"#d9d9d9","noteTextColor":"#000000","actorBkg":"#f2f2f2","actorBorder":"#000000","actorTextColor":"#000000","actorLineColor":"#8b8b8b","signalColor":"#8b8b8b","signalTextColor":"#8b8b8b","labelTextColor":"#000000","textColor":"#8b8b8b"}}}%%
sequenceDiagram
  participant B as Browser
  participant N as nginx
  participant C as Client-Facing API
  participant A as Auth Service
  B->>N: POST /api/auth/login
  N->>C: proxy
  C->>A: gRPC login
  A-->>C: JWT + refresh token
  C-->>B: HttpOnly cookies
  Note over B,C: access token expires after 60 s
  B->>C: GET /hazards (expired jwt)
  C->>A: gRPC refresh (rotating token)
  A-->>C: new JWT + new refresh token
  C-->>B: 200 + new cookies
```

| Role | Fields | Accounts |
|:---|:---|:---|
| Media | summary only, 403 on raw fields | none |
| Tim Lapangan | all | none |
| Operator | all | can create |

- BMKG takes a static key. PVMBG uses OAuth with a one-time code and rotating opaque tokens that the Aggregator renews by itself.
- Only the Client-Facing API verifies the JWT.

---

### Problem 4: independent services and storage ownership

```mermaid
%%{init: {"theme":"base","themeVariables":{"primaryColor":"#f2f2f2","primaryTextColor":"#000000","primaryBorderColor":"#000000","lineColor":"#8b8b8b","secondaryColor":"#f2f2f2","tertiaryColor":"#ffffff","edgeLabelBackground":"#f2f2f2","noteBkgColor":"#d9d9d9","noteTextColor":"#000000","actorBkg":"#f2f2f2","actorBorder":"#000000","actorTextColor":"#000000","actorLineColor":"#8b8b8b","signalColor":"#8b8b8b","signalTextColor":"#8b8b8b","labelTextColor":"#000000","textColor":"#8b8b8b"}}}%%
flowchart LR
  subgraph cn["canonical-net (internal)"]
    AGG["Aggregator"] --- CS[("Canonical Store")]
  end
  subgraph un["userdb-net (internal)"]
    AUTH["Auth Service"] --- UDB[("User DB")]
  end
  OTH["every other service"] -.-x CS
  OTH -.-x UDB
```

- Every component is its own container and image, with a `/health` that reports depth. One can be stopped, rebuilt and restarted while the rest keep running.
- Each database is reachable only by its owner and publishes no host port.

---

### Problem 5: event fan-out to independent consumers

```mermaid
%%{init: {"theme":"base","themeVariables":{"primaryColor":"#f2f2f2","primaryTextColor":"#000000","primaryBorderColor":"#000000","lineColor":"#8b8b8b","secondaryColor":"#f2f2f2","tertiaryColor":"#ffffff","edgeLabelBackground":"#f2f2f2","noteBkgColor":"#d9d9d9","noteTextColor":"#000000","actorBkg":"#f2f2f2","actorBorder":"#000000","actorTextColor":"#000000","actorLineColor":"#8b8b8b","signalColor":"#8b8b8b","signalTextColor":"#8b8b8b","labelTextColor":"#000000","textColor":"#8b8b8b"}}}%%
flowchart LR
  AGG["Aggregator<br/>bounded buffer + confirms"] -->|publish once| EX{{"hazard.events<br/>fanout"}}
  EX --> Q1["notifier queue"] --> N["Notifier"]
  EX --> Q2["audit-logger queue"] --> L["Audit Logger"]
  EX --> Q3["dashboard-updater queue"] --> D["Dashboard Updater"]
```

- One publish reaches every consumer through its own durable queue. A stopped consumer's queue keeps the backlog and it catches up on return, while the others carry on.
- Consumers acknowledge after processing (at-least-once), so each one deduplicates. A new consumer only declares a queue on the exchange, with no change to the producer.
