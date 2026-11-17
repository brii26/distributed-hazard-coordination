# Architecture

High level view of the system on Kubernetes. The cluster itself (nodes, ports, registry) is in [TOPOLOGY.md](TOPOLOGY.md), the deploy steps in [../docs/DEPLOY.md](../docs/DEPLOY.md).

```mermaid
flowchart TB
    user(["Users<br/>(Media, Tim Lapangan)"])
    tester(["Tester Agent<br/>namespace tester-agent"])

    subgraph ext["External sources"]
        bmkg["BMKG (mock)"]
        pvmbg["PVMBG (mock)"]
        pantai["Sensor Pantai"]
    end

    subgraph k8s["k3s cluster, 3 nodes (node-a, node-b, node-c)"]
        subgraph ns["Namespace sistem-informasi-kebencanaan-bnpb"]
            fe["Web Frontend"]
            api["Client-Facing API<br/>(sheds load with 429)"]
            auth["Auth"]
            userdb[("user-db")]

            subgraph agg["Aggregator x3 (one leader polls)"]
                aggL["Leader<br/>holds the lease"]
                aggF["Followers<br/>serve reads"]
            end

            rmq["RabbitMQ"]
            cons["3 consumers"]

            subgraph store["Store DaemonSet, role=store (16 shards, 2 copies each)"]
                s1[("Postgres node-a")]
                s2[("Postgres node-b")]
                s3[("Postgres node-c")]
            end

            obs["Observability<br/>Prometheus, Jaeger"]
        end
        reg["Image registry<br/>node-a:5000"]
    end

    user --> fe --> api
    api --> auth --> userdb
    api -->|gRPC read| aggL
    api -->|gRPC read| aggF

    aggL -->|poll| bmkg
    aggL -->|poll| pvmbg
    aggL -->|poll, validate| pantai

    aggL -->|"write to 2 owners<br/>(shard hash, fencing epoch)"| store
    aggF -->|"read across shards"| store
    aggL -->|publish events| rmq --> cons

    tester -.->|failure tests| store
    obs -.->|scrape, traces| agg
    obs -.-> api
    reg -.->|images| ns
```

## How data moves

- **In:** only the leader Aggregator polls BMKG, PVMBG and Sensor Pantai. Each record gets a `hazard_id` (UUID v5), is hashed to one of 16 shards, and is written to the two nodes that own that shard. New or changed records are published to RabbitMQ for the consumers.
- **Out:** a user reaches the Web Frontend, which calls the Client-Facing API. The API checks the caller through Auth and reads from the Aggregators over gRPC. Every Aggregator replica serves reads, merging the shards and marking the answer `partial` when a shard cannot be reached.
- **Failure handling:** the lease picks one poller and the fencing epoch makes the store refuse writes from an old leader ([P2_LEASE_DESIGN.md](P2_LEASE_DESIGN.md)). Two copies per shard keep reads working when one node is down ([P1_STORAGE_DESIGN.md](P1_STORAGE_DESIGN.md)). The Client-Facing API sheds load so Tim Lapangan keeps capacity when Media overloads it.

## What is in the cluster today

| Part | State |
|---|---|
| Namespace, ConfigMap, Secrets | Done (`deploy/`, #270) |
| Sharded store (DaemonSet) | Done |
| Observability | Done |
| Aggregator | Manifest in review (#272) |
| BMKG and PVMBG mocks | Not yet (#271) |
| Auth, user-db, Client-Facing API, Web Frontend | Not yet (#273) |
| RabbitMQ and the consumers | Not yet (#274) |
| Sensor Pantai poller | Not yet, the validator is in review (#319) |
| Lease and fencing | Lease in review (#283), fencing not yet (#284) |

Update this table when a part lands.
