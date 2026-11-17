# Deploying to Kubernetes

Back to the [README](../README.md). The cluster itself is in [TOPOLOGY.md](../doc/TOPOLOGY.md).

Everything lives in one namespace, `sistem-informasi-kebencanaan-bnpb`. Point `kubectl` at the cluster first (`export KUBECONFIG=$HOME/vms/k3s/kubeconfig` for the default k3s cluster).

## Steps

1. `cp .env.example .env` and replace the `change-me` values. The tokens from the assistants can stay as placeholders until you get them.
2. `kubectl apply -k deploy/` creates the namespace, the ConfigMap `bnpb-config`, the sharded store, the Aggregator and the observability stack.
3. `deploy/make-secrets.sh` creates the Secrets from `.env`. Run it again after a change, it updates in place.
4. `deploy/make-schema.sh` creates the ConfigMap `store-schema` from `services/aggregator/migrations`. The store pods start once it and `store-credentials` exist.

Postgres builds the tables from `store-schema` only on an empty data directory. A migration added later does not run on a node that already has data.

## What is created

| Object | Kind | Made by | Content |
|---|---|---|---|
| `bnpb-config` | ConfigMap | `kubectl apply -k` | Non-secret settings, same names as `.env.example` |
| `store-schema` | ConfigMap | `make-schema.sh` | The SQL migrations |
| `bmkg-credentials` | Secret | `make-secrets.sh` | `api_key` |
| `pvmbg-credentials` | Secret | `make-secrets.sh` | `client_id`, `client_secret`, `one_time_code` |
| `store-credentials` | Secret | `make-secrets.sh` | `user`, `password` of the sharded store |
| `user-db-credentials` | Secret | `make-secrets.sh` | `user`, `password` |
| `rabbitmq-credentials` | Secret | `make-secrets.sh` | `user`, `password` |
| `auth-secrets` | Secret | `make-secrets.sh` | `jwt_signing_secret`, `secret_salt`, `seed_operator_password` |
| `audit-token`, `pantai-token`, `satellite-token` | Secret | `make-secrets.sh` | `token` |
| `pantai-key` | Secret | `make-secrets.sh` | `key`, the file from `PANTAI_KEY_FILE` |

`make-secrets.sh` reads `.env` as plain `KEY=VALUE` lines and never runs it as shell. Values reach `kubectl` through a pipe, so they are not written to a file or shown in the process list. Never commit `.env`.

## The Aggregator

`deploy/aggregator.yaml` holds the Service `aggregator` (gRPC 9091, health 8091) and a Deployment with `role: aggregator` and 256Mi/512Mi memory. `replicas` is 1 for now, spreading three replicas over the nodes is a separate issue. An init container waits until the Service `store` answers.

The image name and tag are set once, in the `images` block of `deploy/kustomization.yaml` (`10.77.0.11:5000/aggregator:0.1.0`). Bump the tag there and below on every new build, so all replicas run the same image. Build and push from the host:

```bash
docker build -t 127.0.0.1:5000/aggregator:0.1.0 services/aggregator
docker push 127.0.0.1:5000/aggregator:0.1.0
```

Writes go to the owner shards through `STORE_SERVICE=store` and `STORE_DSN_TEMPLATE`, which is built from `store-credentials` and `CANONICAL_DB_NAME`. Reads still use `DATABASE_URL`, which points at the headless Service `store`, so a read lands on an arbitrary shard until the read path is changed to ask every shard.

The credentials sit inside the DSN, and the template goes through `Sprintf`. `make-secrets.sh` therefore only accepts letters, digits and `. _ ~ -` in the store and RabbitMQ user names and passwords, and stops with a message otherwise.

The PVMBG token file is on a per-pod `emptyDir`. A container restart keeps it, but a new pod starts without it and signs in again with the one-time code. Handing the token over on a takeover is part of the lease design, not of this manifest.

The BMKG and PVMBG mocks and RabbitMQ come in their own manifests. Until they exist the Aggregator logs `no such host` for them and its circuit breakers open, which is expected.

## Rules for this namespace

- The Tester Agent lives in its own `tester-agent` namespace. Do not add a default-deny policy or a quota there.
- Every pod gets a `role` label and memory requests and limits from Appendix B of the spec.
