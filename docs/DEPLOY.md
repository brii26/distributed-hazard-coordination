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
| `rabbitmq-definitions` | ConfigMap | `make-rabbitmq.sh` | The broker topology template, render script and config from `infra/rabbitmq` |
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

## The Aggregator

`deploy/aggregator.yaml` holds the Service `aggregator` (gRPC 9091, health 8091) and a Deployment with `role: aggregator` and 256Mi/512Mi memory. `replicas` is 1 for now, spreading three replicas over the nodes is a separate issue. An init container waits until the Service `store` answers.

The image name and tag are set once, in the `images` block of `deploy/kustomization.yaml` (`10.77.0.11:5000/aggregator:0.1.0`). Bump the tag there and below on every new build, so all replicas run the same image. Build and push from the host:

```bash
docker build -t 127.0.0.1:5000/aggregator:0.1.0 services/aggregator
docker push 127.0.0.1:5000/aggregator:0.1.0
```

Writes go to the owner shards through `STORE_SERVICE=store` and `STORE_DSN_TEMPLATE`, which is built from `store-credentials` and `CANONICAL_DB_NAME`. Reads still use `DATABASE_URL`, which points at the headless Service `store`, so a read lands on an arbitrary shard until the read path is changed to ask every shard.

A store node that already has data never runs `011_shard_state.sql` (the shard state table and the `rebalance` lease name), so apply it by hand on every store pod, after `deploy/make-schema.sh` has refreshed `store-schema`. The file is safe to apply twice:

```bash
for p in $(kubectl -n sistem-informasi-kebencanaan-bnpb get pods -l role=store -o name); do
  kubectl -n sistem-informasi-kebencanaan-bnpb exec "$p" -- sh -c \
    'psql -U "$POSTGRES_USER" -d canonical_store -f /docker-entrypoint-initdb.d/011_shard_state.sql'
done
```

Until it is applied the rebalancer cannot read the shard state of that node: its scan fails, nothing is copied to or from it, and reads keep their normal owner order without preferring synced copies. The error shows in the Aggregator log as a failed scan. The rebalancer starts working on its next round after the table exists.

The rebalancer is set with `STORE_REBALANCE_INTERVAL`, `REBALANCE_PAGE_PAUSE` and `REBALANCE_DELETE`, see [CONFIGURATION.md](CONFIGURATION.md). Keep `REBALANCE_DELETE` off until the coordination database exists, because the rebalance lease lives on the store nodes and several Aggregator replicas can then run a round at once. With delete off that is harmless, copies are idempotent.

The credentials sit inside the DSN, and the template goes through `Sprintf`. `make-secrets.sh` therefore only accepts letters, digits and `. _ ~ -` in the store and RabbitMQ user names and passwords, and stops with a message otherwise.

The PVMBG token file is on a per-pod `emptyDir`. A container restart keeps it, but a new pod starts without it and signs in again with the one-time code. Handing the token over on a takeover is part of the lease design, not of this manifest.

The BMKG and PVMBG mocks and RabbitMQ come in their own manifests. Until they exist the Aggregator logs `no such host` for them and its circuit breakers open, which is expected.

## The BMKG and PVMBG mocks

`deploy/bmkg-mock.yaml` (port 8081) and `deploy/pvmbg-mock.yaml` (port 8082) each hold a ClusterIP Service and a one-replica Deployment. Both pods carry `role: mock` and 64Mi/128Mi memory, with a readiness probe on `/health`. Only the Service `pvmbg-mock` is labelled `role: mock`, because the Tester Agent lists Services with that label and must find exactly one. Keys and credentials come from `bmkg-credentials` and `pvmbg-credentials`, the other settings from `bnpb-config`. The images are `10.77.0.11:5000/bmkg-mock:0.1.0` and `10.77.0.11:5000/pvmbg-mock:0.1.0`, set in the `images` block. Build and push from the host:

```bash
docker build -t 127.0.0.1:5000/bmkg-mock:0.1.0 services/bmkg-mock
docker push 127.0.0.1:5000/bmkg-mock:0.1.0
docker build -t 127.0.0.1:5000/pvmbg-mock:0.1.0 services/pvmbg-mock
docker push 127.0.0.1:5000/pvmbg-mock:0.1.0
```

## RabbitMQ and the consumers

`deploy/make-rabbitmq.sh` creates the ConfigMap `rabbitmq-definitions` from `infra/rabbitmq` (`definitions.json.tpl`, `render-definitions.sh`, `rabbitmq.conf`), so the files stay in one place. Run it before the first `kubectl apply -k deploy/` and again after a change, then `kubectl rollout restart statefulset/rabbitmq`. The repo holds the definitions as a template without credentials, so the broker renders `definitions.json` at start with `RABBITMQ_USER` and `RABBITMQ_PASSWORD` from the Secret `rabbitmq-credentials`, as in Compose. The definitions declare the user and the fanout exchange `hazard.events`. Each consumer declares and binds its own durable queue (`QUEUE_NAME`) when it first connects, so a queue appears once its consumer has run.

`deploy/rabbitmq.yaml` holds the Service `rabbitmq` (AMQP 5672, management 15672) and a StatefulSet with 1 replica, `role: broker`, 512Mi/1Gi memory. A StatefulSet gives the pod a fixed hostname (`rabbitmq-0`), and RabbitMQ names its data directory after the hostname. Queue data is on a 1Gi PersistentVolumeClaim from `volumeClaimTemplates`, served by the k3s `local-path` class. It survives a pod restart or deletion, and the pod then runs again on the same node, because the volume lives on that node's disk. It is lost if that node is wiped.

`deploy/dashboard-updater.yaml`, `notifier.yaml` and `audit-logger.yaml` are Deployments with `role: consumer`, 128Mi/256Mi memory and one replica each. They read `bnpb-config`, build `RABBITMQ_URL` from `rabbitmq-credentials` and set `QUEUE_NAME` and `HEALTH_PORT` (8093, 8094, 8095). Images come from the `images` block (`10.77.0.11:5000/<name>:0.1.0`, built and pushed like the Aggregator, from `services/<name>`). The notifier and the audit logger keep their dedup log (and the audit log) on an `emptyDir`: it survives a container restart but not a new pod, which then may process a redelivered message once more. The dashboard-updater keeps its state in memory.

```bash
kubectl exec -n sistem-informasi-kebencanaan-bnpb rabbitmq-0 -- rabbitmqctl list_queues name messages
```

## The Sensor Pantai

`deploy/sensor-pantai/` holds the Service `sensor-pantai` (8080) and the assistants' Deployment (`role: sensor`, 128Mi/256Mi, image pinned by digest, state on an `emptyDir` at `/data`). It differs from their example only by the namespace, the memory, the Service, the probes on `/healthz` and the env values coming from `bnpb-config`. The image is never modified.

`PANTAI_AUTH_TOKEN` comes from the Secret `pantai-token` (`PANTAI_AUTH_TOKEN` in `.env`). The Aggregator reads the same Secret and `PANTAI_BASE_URL=http://sensor-pantai:8080` from its own manifest, and it needs both to start polling the sensor. `PANTAI_PROFILE`, `PANTAI_SEED` and `PANTAI_SPEED` come from `bnpb-config`, practice by default.

For the exam profile, put the key file from the assistants in `PANTAI_KEY_FILE`, run `deploy/make-secrets.sh`, then `kubectl apply -k deploy/overlays/exam`. The overlay renders only the sensor Service and Deployment, sets `PANTAI_PROFILE=exam` and mounts the Secret `pantai-key` at `/var/run/pantai`. The sensor exits at start without a valid key. To go back to practice, apply `deploy/` again with `kubectl apply -k deploy/`. Applying the overlay or `deploy/` rolls the pod, because the Deployment changes.

## The user database, Auth, Client-Facing API and web frontend

`deploy/user-db.yaml`, `auth-service.yaml`, `client-facing-api.yaml` and `web-frontend.yaml` hold, in this order, the StatefulSet `user-db` (Postgres, `role: store`, 512Mi/1Gi, 1Gi local-path volume), and the Deployments `auth-service` and `client-facing-api` (`role: api`, 128Mi/256Mi) and `web-frontend` (`role: other`, 128Mi/256Mi). The API reaches the Aggregator at `aggregator:9091` and Auth at `auth-service:9092`.

`deploy/make-schema.sh` also creates the ConfigMap `user-db-init` from `services/auth-service/migrations`. Postgres runs the schema and the seed Operator script from it on an empty volume only, so deleting the `user-db` pod and its PVC (`data-user-db-0`) brings the account back on the next start.

The web frontend is on NodePort 30080 (it proxies `/api` to the API) and the Client-Facing API on NodePort 30081, both on every node address, for example `http://10.77.0.11:30080`. The cluster scripts forward no host port to them yet, so from the host use `kubectl port-forward svc/web-frontend 3000:80` and open http://localhost:3000. `COOKIE_SECURE` is `true`, which browsers accept on `localhost` but not on a plain-HTTP node address.

Build and push the three images from the host, then bump the tags in the `images` block of `deploy/kustomization.yaml` on a new build:

```bash
for s in auth-service client-facing-api web-frontend; do
  docker build -t 127.0.0.1:5000/$s:0.1.0 services/$s
  docker push 127.0.0.1:5000/$s:0.1.0
done
```

## Rules for this namespace

- The Tester Agent lives in its own `tester-agent` namespace. Do not add a default-deny policy or a quota there.
- Every pod gets a `role` label and memory requests and limits from Appendix B of the spec.
