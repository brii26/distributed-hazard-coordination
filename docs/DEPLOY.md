# Deploying to Kubernetes

Back to the [README](../README.md). The cluster itself is in [TOPOLOGY.md](../doc/TOPOLOGY.md).

Everything lives in one namespace, `sistem-informasi-kebencanaan-bnpb`. Point `kubectl` at the cluster first (`export KUBECONFIG=$HOME/vms/k3s/kubeconfig` for the default k3s cluster).

## Steps

1. `cp .env.example .env` and replace the `change-me` values. The tokens from the assistants can stay as placeholders until you get them.
2. `kubectl apply -k deploy/` creates the namespace, the ConfigMap `bnpb-config`, the sharded store and the observability stack.
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
| `grafana-admin` | Secret | `make-secrets.sh` (random, created once) | `password` for the Grafana user `admin` |
| `pantai-key` | Secret | `make-secrets.sh` | `key`, the file from `PANTAI_KEY_FILE` |

`make-secrets.sh` reads `.env` as plain `KEY=VALUE` lines and never runs it as shell. Values reach `kubectl` through a pipe, so they are not written to a file or shown in the process list. Never commit `.env`.

## Rules for this namespace

- The Tester Agent lives in its own `tester-agent` namespace. Do not add a default-deny policy or a quota there.
- Every pod gets a `role` label and memory requests and limits from Appendix B of the spec.
