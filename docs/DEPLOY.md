# Deploying to Kubernetes

Back to the [README](../README.md). The cluster itself is in [TOPOLOGY.md](../doc/TOPOLOGY.md).

Everything lives in one namespace, `sistem-informasi-kebencanaan-bnpb`. Point `kubectl` at the cluster first (`export KUBECONFIG=$HOME/vms/k3s/kubeconfig` for the default k3s cluster).

## Steps

The short way is `deploy/deploy.sh` after the images are pushed, see [One command deploy and the check](#one-command-deploy-and-the-check). The manual order is:

0. Build the cluster ([TOPOLOGY.md](../doc/TOPOLOGY.md)) and build and push every image to the registry (see [Build and push all images](#build-and-push-all-images)). `deploy/deploy.sh` and `kubectl apply -k` do not build images, so pods stay in `ImagePullBackOff` until they are in the registry.
1. `cp .env.example .env` and replace the `change-me` values. The tokens from the assistants can stay as placeholders until you get them.
2. `kubectl apply -k deploy/` creates the namespace, the ConfigMap `bnpb-config`, the sharded store, the Aggregator and the observability stack.
3. `deploy/make-secrets.sh` creates the Secrets from `.env`. Run it again after a change, it updates in place.
4. `deploy/make-schema.sh` creates the ConfigMap `store-schema` from `services/aggregator/migrations`. The store pods start once it and `store-credentials` exist.
5. `deploy/make-coordination.sh` creates the ConfigMap `coordination-db-init` from the coordination migrations only. The pod `coordination-db` starts once it and `store-credentials` exist.

Postgres builds the tables from `store-schema` only on an empty data directory. A migration added later does not run on a node that already has data.

## What is created

| Object | Kind | Made by | Content |
|---|---|---|---|
| `bnpb-config` | ConfigMap | `kubectl apply -k` | Non-secret settings, same names as `.env.example` |
| `store-schema` | ConfigMap | `make-schema.sh` | The SQL migrations |
| `coordination-db-init` | ConfigMap | `make-coordination.sh` | `004_source_lease.sql`, `005_source_lease_sources.sql`, `006_source_cursors.sql`, `011_shard_state.sql`, `012_source_token.sql` |
| `rabbitmq-definitions` | ConfigMap | `make-rabbitmq.sh` | The broker topology template, render script and config from `infra/rabbitmq` |
| `bmkg-credentials` | Secret | `make-secrets.sh` | `api_key` |
| `pvmbg-credentials` | Secret | `make-secrets.sh` | `client_id`, `client_secret`, `one_time_code` |
| `store-credentials` | Secret | `make-secrets.sh` | `user`, `password` of the sharded store |
| `user-db-credentials` | Secret | `make-secrets.sh` | `user`, `password` |
| `rabbitmq-credentials` | Secret | `make-secrets.sh` | `user`, `password` |
| `auth-secrets` | Secret | `make-secrets.sh` | `jwt_signing_secret`, `secret_salt`, `seed_operator_password` |
| `audit-token`, `admin-token`, `pantai-token`, `satellite-token` | Secret | `make-secrets.sh` | `token` |
| `grafana-admin` | Secret | `make-secrets.sh` (random, created once) | `password` for the Grafana user `admin` |
| `pantai-key` | Secret | `make-secrets.sh` | `key`, the file from `PANTAI_KEY_FILE` |

`make-secrets.sh` reads `.env` as plain `KEY=VALUE` lines and never runs it as shell. Values reach `kubectl` through a pipe, so they are not written to a file or shown in the process list. Never commit `.env`.

## The Aggregator

`deploy/aggregator.yaml` holds the headless Service `aggregator` (HTTP, health and metrics on 8091, `publishNotReadyAddresses: true`), the headless Service `aggregator-grpc` (gRPC 9091) and a Deployment (the replica count belongs to the HPA, see Autoscaling below), `role: aggregator` and 256Mi/512Mi memory each, plus a 200m CPU request. A topology spread constraint on `kubernetes.io/hostname` (`ScheduleAnyway`, so it still schedules on one node) spreads the pods over the nodes. The readiness probe is `/health`, which answers 503 only when the store ping fails, and the liveness probe is a TCP check that ignores the store. The Client-Facing API dials `dns:///aggregator-grpc:9091` with `round_robin` and the gRPC health check, so it only sends to pods whose store answers. The Aggregator closes each connection after 30 s, which makes the client resolve DNS again and meet new pods. Every pod runs the lease, so only one of them polls a source (the lease, the cursor and the token pair live in the coordination database, see below). An init container waits until the Services `store` and `coordination-db` answer. The HTTP Service lists every pod, ready or not, because it carries the Audit API and the metrics: during a store outage the readiness probe drops pods from `aggregator-grpc` but the auditor still reaches them, and Prometheus can read every replica through the DNS name `aggregator` (an A record per pod). Check readiness routing with `kubectl get endpoints aggregator-grpc`.

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

The same holds for `014_source_received_at.sql` (the column `source_received_at` of `hazard_events`, which the Aggregator now writes on every upsert): apply it the same way on every store pod, before the new Aggregator image rolls out, or every write fails. It is safe to apply twice and sets existing rows to their last write time.

Until `011_shard_state.sql` is applied the rebalancer cannot read the shard state of that node: its scan fails, nothing is copied to or from it, and reads keep their normal owner order without preferring synced copies. The error shows in the Aggregator log as a failed scan. The rebalancer starts working on its next round after the table exists.

The rebalancer is set with `STORE_REBALANCE_INTERVAL`, `REBALANCE_PAGE_PAUSE` and `REBALANCE_DELETE`, see [CONFIGURATION.md](CONFIGURATION.md). The rebalance lease lives in the coordination database too, so only one replica runs a round at a time and `REBALANCE_DELETE` no longer needs the extra care that several simultaneous rounds would have asked for.

The credentials sit inside the DSN, and the template goes through `Sprintf`. `make-secrets.sh` therefore only accepts letters, digits and `. _ ~ -` in the store and RabbitMQ user names and passwords, and stops with a message otherwise.

Only the replica that holds the PVMBG lease talks to the token endpoint. On acquiring the lease it loads the token pair from the table `source_token` (migration `012_source_token.sql`, in the coordination database next to the lease), then from the file on its `emptyDir`, and uses the one-time code only if neither exists. Every rotated pair is written to the table with the lease epoch before it is used, and a write from a lower epoch is refused, so a replica that took over continues with the pair its predecessor left and a replica that lost the lease cannot overwrite it. The file on the `emptyDir` is only a local cache. The one-time code is spent once, so wiping the table and the pods together needs a fresh code (or a restart of the PVMBG mock). If the write to the table fails after PVMBG issued a pair, the holder keeps that pair in memory and on the file and retries the write until it succeeds, so one database blip does not lose it. If a refresh is rejected with `invalid_grant`, the holder reloads the table and retries once with the stored refresh token before it touches the one-time code.

The BMKG and PVMBG mocks and RabbitMQ come in their own manifests, applied by the same `kubectl apply -k deploy/`. If the Aggregator starts before they answer, it logs `no such host` for them and its circuit breakers open until they are up, which is expected.

## Rolling update

The Aggregator Deployment uses `RollingUpdate` with `maxUnavailable: 0` and `maxSurge: 1`, so a new pod must be ready before an old one is stopped. A stopping pod goes through these steps, in this order:

1. The `preStop` sleep (5 s) runs first, so the pod leaves the Service endpoints while it still serves.
2. On SIGTERM `/health` answers 503 and the gRPC health check reports NOT_SERVING for good, then the pod waits `SHUTDOWN_DRAIN_DELAY` (default 2 s) with both listeners still open.
3. The HTTP and gRPC servers stop taking new connections and finish in-flight calls. `SHUTDOWN_TIMEOUT` (default 20 s) bounds this wait, after which the remaining connections are closed.
4. The pollers and the rebalancer stop and each lease is released, so another instance acquires it at its next try (at most `LEASE_RENEW_INTERVAL`, 3 s) and does not wait for `LEASE_TTL`. The epoch grows by one per hand-over.

`terminationGracePeriodSeconds` is 45, which covers 5 + 2 + 20 s and the lease release. The Client-Facing API balances over the pods with `round_robin` and retries `UNAVAILABLE` up to 3 times, so a call that meets a closing connection goes to another pod.

`tests/scenarios/rolling-update.sh` checks this on a running cluster. It runs k6 for 60 s, restarts the Aggregator after 10 s and prints PASS or FAIL. Failed means any status other than 200 and 429, and the 429 are counted apart. It also needs each lease epoch to go up, no lease gap above `GAP_MAX` (5 s, the renew interval of 3 s plus sampling, so a lease that is not released on SIGTERM and waits for the 15 s `LEASE_TTL` fails) and no repeated `source_ref_id` for BMKG, PVMBG and SENSOR_PANTAI in the Audit API. At least `MIN_OK_PCT` (90) percent of the requests must be 200, so a run that is mostly 429 fails.

For the run the script sets `ACCESS_TOKEN_TTL_S` to 3600 and `RATE_LIMIT_RPS` to 1000 in `bnpb-config` and restarts auth-service and client-facing-api, because k6 logs in once and a 60 s token would expire mid-run (the refresh token rotates, so the VUs cannot share it) and all VUs share one account rate bucket. On exit, also on Ctrl-C, it restores the old values and restarts them again. The Audit API token is read from the Secret `audit-token`, so only the Operator login is needed.

```bash
export KUBECONFIG=$HOME/vms/k3s/kubeconfig
EMAIL=<operator email> PASSWORD=<password> tests/scenarios/rolling-update.sh
```

## Autoscaling the Aggregator

`deploy/aggregator-hpa.yaml` is a HorizontalPodAutoscaler (`autoscaling/v2`) on the Deployment `aggregator`. It targets 60% of the CPU request (200m, so 120m per pod) with `minReplicas: 3` and `maxReplicas: 5`. The metrics come from the metrics-server that k3s ships. Scaling up waits 30 s of stabilization and adds one pod per 60 s. Scaling down waits 300 s and removes one pod per 120 s, so a short pause in the load does not make the count flap. The memory request and limit stay 256Mi/512Mi, so `deploy/check.sh` is unchanged. The CPU request is 200m because the pod that holds the polling leases uses 100m to 200m by itself. Measured on the cluster: with a 100m request the average crossed the target without any user traffic and the HPA scaled to 4 on its own, and with 200m it scaled back to 3 and stayed there. `autoscale.sh` now starts with an idle phase that asserts the count stays at 3.

Why the maximum is 5. The issue asks for 5, and the P2 load test (criterion 6) needs 5 Aggregators in its 3, 5, 2 sequence, so 5 has to fit. The requests of everything else in the namespace add up to 4608Mi: 3 stores, `user-db`, `coordination-db` and RabbitMQ at 512Mi each (3072Mi), plus 11 pods at 128Mi and 2 mocks at 64Mi (1536Mi). The 3 nodes of 2048Mi give 6144Mi, which leaves 1536Mi on paper, exactly 6 Aggregators. The VM does not offer all of it: the kernel, k3s, CoreDNS and the pods that carry no request (traefik, metrics-server, the local-path provisioner) take a share, and the pods must also fit node by node. Five pods need 1280Mi, so the margin is thin and the arithmetic cannot settle it. Result of `kubectl -n sistem-informasi-kebencanaan-bnpb scale deployment/aggregator --replicas=5` on k3s: TO BE FILLED by the maintainer (how many pods Running or Pending, and the output of `kubectl describe node`). If a pod stays Pending, 5 does not fit this cluster and the P2 test needs either a smaller request elsewhere or a larger node, so do not lower `maxReplicas` without telling the P2 owners. Until it is measured, treat 5 as unverified.

Scripts that set the replica count by hand (`tests/load/p2-k8s.sh`, `tests/load/p2-ratio.sh`) fight the HPA. With min 3 and max 5 the HPA raises a count of 1 or 2 back to 3 within a minute, and it removes pods above the CPU target after the 300 s window. Before they scale to 5, 2 and 1 they must pause the HPA, either `kubectl -n sistem-informasi-kebencanaan-bnpb delete hpa aggregator` or a patch that sets `minReplicas` and `maxReplicas` to the same number as the step, and they must restore it afterwards (`kubectl apply -f deploy/aggregator-hpa.yaml` or patching min 3 and max 5 back), also when the script fails, in an EXIT trap. `autoscale.sh` is the opposite: it needs the live HPA, refuses to run when the HPA is missing or pinned (min equals max), and must not run at the same time as a P2 script.

The Deployment has no `replicas` field, so `kubectl apply -k deploy/` does not reset the count the HPA chose. On a cluster that already runs with `replicas: 3` in the last applied manifest, drop the field from the annotation first, otherwise the apply sets the count to 1 for a moment: `kubectl -n sistem-informasi-kebencanaan-bnpb apply edit-last-applied deployment/aggregator`, delete the `replicas` line, save, then apply. On a new cluster the first apply creates 1 pod and the HPA raises it to 3 within a minute. `kube-state-metrics` also lists the HPA (`--resources` and a ClusterRole rule), which feeds the dashboard panel "Aggregator replicas".

Leases during a scale event. Every pod runs the lease, so a new pod only adds a candidate and does not change who polls. A pod removed by a scale down gets `preStop` (5 s) then SIGTERM. If it held a lease it stops renewing, another pod takes the lease after `LEASE_TTL` (15 s) with a higher epoch, and the stores refuse any late write of the old holder through the fence. Polling of that source pauses for up to 15 s, no event is duplicated. Releasing the lease on SIGTERM so the pause disappears belongs to the graceful shutdown work (#286). Scale down is also limited to one pod per 120 s, so at most one lease holder changes at a time.

`tests/scenarios/autoscale.sh` drives it from a host that has `kubectl` for the cluster and k6 (or Docker for `grafana/k6`). It first patches `bnpb-config` to `ACCESS_TOKEN_TTL_S` 7200 and `RATE_LIMIT_RPS` 100000 (`LOAD_TTL_S`, `LOAD_RATE_RPS`) and restarts `auth-service` and `client-facing-api`, because the default 60 s token expired mid run and the limit of 50 rps per account capped all VUs of one login far below the CPU target. An EXIT, INT and TERM trap puts both values back and restarts again. Then it idles for `IDLE_PERIOD` seconds (180) and requires the count to stay at the minimum the whole time, so a flapping HPA shows up as a FAIL. Then comes a k6 ramp (1 min to a third of `PEAK_VUS`, 2 min to `PEAK_VUS`, `HOLD` at the peak, 1 min down) against `API_URL`, printing the replica count and CPU every 15 s. It prints PASS when the idle count held, the count rises above the minimum, returns to 3 after the load, the k6 error rate without 429 stays under 1%, and the Audit API (port-forwarded) shows no duplicate id and a checksum equal to an independent sha256, for BMKG, PVMBG and SENSOR_PANTAI. The script is shellcheck clean and its syntax is checked, but it has not been run against a cluster, because the VMs were not available. `kubectl get hpa aggregator -w` shows the same moves.

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

`deploy/user-db.yaml`, `auth-service.yaml`, `client-facing-api.yaml` and `web-frontend.yaml` hold, in this order, the StatefulSet `user-db` (Postgres, `role: store`, 512Mi/1Gi, 1Gi local-path volume), and the Deployments `auth-service` and `client-facing-api` (`role: api`, 128Mi/256Mi) and `web-frontend` (`role: other`, 128Mi/256Mi). The API reaches the Aggregator at `dns:///aggregator-grpc:9091` and Auth at `auth-service:9092`.

`deploy/make-schema.sh` also creates the ConfigMap `user-db-init` from `services/auth-service/migrations`. Postgres runs the schema and the seed Operator script from it on an empty volume only, so deleting the `user-db` pod and its PVC (`data-user-db-0`) brings the account back on the next start.

The web frontend is on NodePort 30080 (it proxies `/api` to the API) and the Client-Facing API on NodePort 30081, both on every node address, for example `http://10.77.0.11:30080`. The cluster scripts forward no host port to them yet, so from the host use `kubectl port-forward svc/web-frontend 3000:80` and open http://localhost:3000. `COOKIE_SECURE` is `true`, which browsers accept on `localhost` but not on a plain-HTTP node address.

Build and push the three images from the host, then bump the tags in the `images` block of `deploy/kustomization.yaml` on a new build:

```bash
for s in auth-service client-facing-api web-frontend; do
  docker build -t 127.0.0.1:5000/$s:0.1.0 services/$s
  docker push 127.0.0.1:5000/$s:0.1.0
done
```

## Build and push all images

`deploy/kustomization.yaml` names nine images, each built from `services/<name>` and tagged `0.1.0`: `aggregator`, `bmkg-mock`, `pvmbg-mock`, `auth-service`, `client-facing-api`, `web-frontend`, `dashboard-updater`, `notifier` and `audit-logger`. Push them all from the host in one loop (the registry runs on node-a, port 5000, started by `deploy/cluster/k3s.sh`):

```bash
for s in aggregator bmkg-mock pvmbg-mock auth-service client-facing-api web-frontend dashboard-updater notifier audit-logger; do
  docker build -t 127.0.0.1:5000/$s:0.1.0 services/$s
  docker push 127.0.0.1:5000/$s:0.1.0
done
```

## Rules for this namespace

- The Tester Agent lives in its own `tester-agent` namespace. Do not add a default-deny policy or a quota there.
- Every pod gets a `role` label and memory requests and limits from Appendix B of the spec.

## One command deploy and the check

`deploy/deploy.sh [env-file]` does steps 2 to 5 above in the right order. It applies the namespace, runs `make-secrets.sh`, `make-schema.sh`, `make-coordination.sh` and `make-rabbitmq.sh`, runs `kubectl apply -k deploy/`, then waits for every Deployment, StatefulSet and DaemonSet (the store is a DaemonSet) with `kubectl rollout status --timeout $TIMEOUT` (default `300s` each). A workload that is not ready is listed with the pods that are not Running and a hint to describe them, and the script exits 1. It ends with `deploy/check.sh`. The name differs from `deploy/cluster/up.sh`, which starts the VMs.

```bash
export KUBECONFIG=$HOME/vms/k3s/kubeconfig
deploy/deploy.sh
```

`deploy/check.sh` prints one row per pod with its `role`, memory request and limit, and `ok` or the reason it fails. It exits 1 on a missing `role` label, a role outside `aggregator, store, broker, consumer, api, mock, sensor, other`, or memory that does not match the Appendix B class (`sensor` counts as other: 128Mi/256Mi). The memory of a pod is the sum of its containers, or the largest init container if that is bigger, as the scheduler counts it.

```bash
deploy/check.sh                                   # live pods of the namespace
kubectl get pods -n sistem-informasi-kebencanaan-bnpb -o json | deploy/check.sh -p -
deploy/check.sh -k                                # pod templates from kubectl kustomize deploy/, no cluster
deploy/tests/check_test.sh                        # offline tests with the fixtures in deploy/tests/fixtures
```

The `-k` mode needs `python3` with PyYAML, and `jq` is needed in every mode.

## The coordination database

`deploy/coordination-db.yaml` holds the StatefulSet `coordination-db` (one Postgres 16 replica, 512Mi/1Gi, a 1Gi local-path volume) and the headless Service `coordination-db`. The pod is labelled `app: coordination-db` and `role: store`, the same memory class as `user-db`, because `check.sh` has no other class for a database. It reuses the Secret `store-credentials` (no new key in `.env`) and a database named `coordination`.

Why: the data store is a DaemonSet behind the headless Service `store`, so each Aggregator connection may reach a different store pod. Lease rows, source cursors and the PVMBG token pair would then differ per pod, and replicas would disagree on lease epochs and on the token pair. The Aggregator reads `COORDINATION_DATABASE_URL` and opens a second pool from it (5 connections) for the tables `source_lease`, `source_cursors` and `source_token`. Hazard upserts, `critical_outbox` (same transaction as the upsert), `failed_messages`, `write_fence` and the shard state stay on `DATABASE_URL` and the shard owners, and fencing stays per store. When the variable is empty or equal to `DATABASE_URL` one pool serves both, which is what docker-compose and the tests use.

Schema: no migration is renumbered or copied. `make-coordination.sh` builds `coordination-db-init` from the five migration files listed above, straight from `services/aggregator/migrations`, so each table has one definition. `011_shard_state.sql` is needed for its change to the `source_lease` check, which allows the `rebalance` lease name. It also creates an unused `shard_state` table there. The store DaemonSet still receives the whole folder through `store-schema` and keeps its (now unused) lease tables, which are harmless. `deploy/deploy.sh` runs the script.

NetworkPolicy: `coordination-db-from-aggregator` lets only `role=aggregator` (and the node network) reach TCP 5432. The policy `store-from-aggregator` now excludes `app` in `user-db` and `coordination-db`.

Trade-off: this is a single point. While `coordination-db` is down no new lease is granted, and a holder that cannot renew stops polling at its lease deadline. Hazard ingestion from sources pauses until it returns, reads and the data store are unaffected. The data survives a pod restart on the local-path volume. Postgres runs init scripts on an empty volume only, so an existing cluster needs the one time steps below.

On a cluster that already runs, after pulling this change (`.env` stays as it is):

```bash
NS=sistem-informasi-kebencanaan-bnpb
deploy/make-coordination.sh
kubectl apply -k deploy/
kubectl -n $NS rollout status statefulset/coordination-db
kubectl -n $NS rollout status deployment/aggregator
```

The new pod has an empty volume, so it runs the five migrations itself. If you must re-apply them by hand: `kubectl -n $NS exec coordination-db-0 -- sh -c 'for f in /docker-entrypoint-initdb.d/*.sql; do psql -U "$POSTGRES_USER" -d coordination -f "$f"; done'`. The old lease rows, cursors and token pair stay on the store pods and are not copied. The new lease epochs start at 0, so a store that still holds a higher fence epoch from the old lease refuses writes until the new epoch passes it. If that happens, copy the rows once: `kubectl -n $NS exec store-<pod> -- pg_dump -U "$POSTGRES_USER" -d canonical_store -t source_lease -t source_cursors -t source_token --data-only | kubectl -n $NS exec -i coordination-db-0 -- psql -U "$POSTGRES_USER" -d coordination`.

### Known gaps

- `coordination-db` is a single replica without failover (see the trade-off above).
- The Service `store` selects `role: store` and `app: store`, so only the store DaemonSet pods answer on `DATABASE_URL`. `user-db` and `coordination-db` carry `role: store` for the memory class only. On a running cluster the label change restarts the store pods once (the data stays on the node).
- The PVMBG one-time code is spent once, see the token paragraph above.
- A holder that was paused past its lease deadline can still finish a refresh at PVMBG and rotate the pair before its own write is refused. The new holder then recovers through the stored pair or, if that one was also rotated away, only through a new one-time code. The lease deadline keeps this window short.
- Replicas and stores of an old cluster keep `write_fence` rows with the highest epochs of the earlier split leases. Reset them once (`truncate write_fence` on every store) when moving to the coordination database.

## Storage isolation (NetworkPolicy)

`deploy/networkpolicy.yaml` holds three ingress policies and no egress rule. `store-from-aggregator` lets only pods with `role=aggregator` (and the node network, for the hostNetwork Tester Agent) reach the store pods on TCP 5432. `user-db-from-auth` lets only `app=auth-service` (and the node network) reach the user DB on TCP 5432. `coordination-db-from-aggregator` lets only `role=aggregator` (and the node network) reach the coordination database on TCP 5432. The store pods do not talk to each other, because the Aggregator does the replication and repair. The kubelet probes are `exec` probes and DNS is egress, so neither is affected. No policy exists in the `tester-agent` namespace.

k3s enforces policies with its embedded kube-router controller. It is on unless the server runs with `--disable-network-policy`:

```bash
kubectl get netpol -n sistem-informasi-kebencanaan-bnpb
ssh node-a 'systemctl cat k3s | rg -c disable-network-policy'   # 0 matches means enforcement is on
ssh node-a 'sudo iptables -S | rg KUBE-NWPLCY | head -3'        # chains exist when it is on
```

Check from throwaway pods. A pod without the allowed label is refused, and a pod labelled like the Aggregator or Auth connects:

```bash
NS=sistem-informasi-kebencanaan-bnpb
kubectl run np-deny -n $NS --rm -it --restart=Never --image=busybox:1.36 --labels=role=other -- sh -c 'nc -zw2 store 5432; echo store=$?; nc -zw2 user-db 5432; echo userdb=$?'   # both non-zero
kubectl run np-agg -n $NS --rm -it --restart=Never --image=busybox:1.36 --labels=role=aggregator -- nc -zvw2 store 5432            # open
kubectl run np-auth -n $NS --rm -it --restart=Never --image=busybox:1.36 --labels=app=auth-service -- nc -zvw2 user-db 5432     # open
kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice kill-store                                              # still works
```
