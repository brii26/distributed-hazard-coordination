# Architecture

High level view of the system on Kubernetes. The cluster itself (nodes, ports, registry) is in [TOPOLOGY.md](TOPOLOGY.md), the deploy steps in [../docs/DEPLOY.md](../docs/DEPLOY.md).

```mermaid
flowchart TB
    user(["Users<br/>(Media, Tim Lapangan)"])
    tester(["Tester Agent<br/>namespace tester-agent"])

    subgraph ext["External sources (polled)"]
        bmkg["BMKG (mock)<br/>seismic"]
        pvmbg["PVMBG (mock)<br/>volcanic"]
        pantai["Sensor Pantai<br/>coastal"]
    end

    subgraph k8s["k3s cluster, 3 nodes (node-a, node-b, node-c)"]
        subgraph ns["Namespace sistem-informasi-kebencanaan-bnpb"]
            fe["Web Frontend"]
            api["Client-Facing API<br/>(sheds load with 429)"]
            auth["Auth"]
            userdb[("user-db")]

            subgraph agg["Aggregator, 3 replicas"]
                a1["Aggregator 1<br/>LEADER, holds the lease"]
                a2["Aggregator 2<br/>follower"]
                a3["Aggregator 3<br/>follower"]
            end

            subgraph store["Store DaemonSet, role=store (16 shards, 2 copies each)"]
                s1[("Postgres node-a")]
                s2[("Postgres node-b")]
                s3[("Postgres node-c")]
            end

            subgraph mq["RabbitMQ"]
                ex{{"fanout exchange<br/>hazard.events"}}
                q1["queue<br/>dashboard-updater"]
                q2["queue<br/>notifier"]
                q3["queue<br/>audit-logger"]
            end

            c1["dashboard-updater"]
            c2["notifier"]
            c3["audit-logger"]

            obs["Observability<br/>Prometheus, Jaeger"]
        end
        reg["Image registry<br/>node-a:5000"]
    end

    user --> fe --> api
    api --> auth --> userdb
    api -->|gRPC read| a1
    api -->|gRPC read| a2
    api -->|gRPC read| a3

    a1 -->|poll| bmkg
    a1 -->|poll| pvmbg
    a1 -->|poll, validate| pantai

    a1 -->|"write to 2 owners<br/>(shard hash, fencing epoch)"| store
    a2 -->|"read across shards"| store
    a3 -->|"read across shards"| store
    a1 -.->|"lease row"| s1

    a1 -->|publish| ex
    ex --> q1 --> c1
    ex --> q2 --> c2
    ex --> q3 --> c3

    tester -.->|failure tests| store
    obs -.->|scrape, traces| agg
    obs -.-> api
    reg -.->|images| ns
```

## How data moves

- **In:** only the leader Aggregator (one of the three replicas) polls the three sources: BMKG (seismic), PVMBG (volcanic) and Sensor Pantai (coastal). Each record gets a `hazard_id` (UUID v5), is hashed to one of 16 shards, and is written to the two nodes that own that shard. New or changed records are published once to the fanout exchange `hazard.events`. Each consumer (`dashboard-updater`, `notifier`, `audit-logger`) has its own durable queue bound to it, so every consumer gets every event and the Aggregator does not know they exist.
- **Out:** a user reaches the Web Frontend, which calls the Client-Facing API. The API checks the caller through Auth and reads from the Aggregators over gRPC. All three Aggregator replicas serve reads, merging the shards and marking the answer `partial` when a shard cannot be reached.
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
| RabbitMQ and the three consumers | Not yet in the cluster (#274), the consumers exist as services |
| Sensor Pantai poller | Not yet, the validator is in review (#319) |
| Lease and fencing | Lease in review (#283), fencing not yet (#284) |

Update this table when a part lands.
