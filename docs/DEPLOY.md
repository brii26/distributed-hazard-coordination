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

## Kill-leader, clock-skew and freeze checks (issue #287)

`tests/scenarios/problem2-k8s.sh` proves the lease behaviour of P2 on the cluster. Status: written and checked offline only (`bash -n`, `shellcheck`, `DRY_RUN=1` and a fake `kubectl` and `curl` fed with synthetic lease rows, agent logs, metrics and Audit pages for the PASS and FAIL cases). **It has not been run against the cluster yet.** The single fetch check of the issue (upstream request count by caller IP against the expected polls) is not in this script, `tests/load/p2-k8s.sh` covers the upstream request count. Issue #287 therefore stays open for that check, for `doc/P2_FREEZE_TEST.md` and for the freeze rehearsal, and the change refers to it with `Refs` and never closes it.

```bash
export KUBECONFIG=$HOME/vms/k3s/kubeconfig
DRY_RUN=1 tests/scenarios/problem2-k8s.sh                  # plan and every command, touches nothing
tests/scenarios/problem2-k8s.sh kill-leader clock-skew     # the two Tester Agent scenarios
```

It needs `kubectl`, `curl`, `jq` and `awk`, and no password: the Audit token comes from the Secret `audit-token`. Do not run it together with another job that uses the cluster, `gameday.sh`, the load scripts or the Tester Agent.

How a scenario runs. The script reads the leader per source from `source_lease` (psql in `coordination-db-0`, three sources: `BMKG`, `PVMBG`, `SENSOR_PANTAI`), starts a sampler that reads the table every second, then runs `kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <name>`. That call only submits, so it waits for `pemulihan akhir selesai` in `kubectl logs -n tester-agent -l app=tester-agent --since-time=<t>` (`SCENARIO_TIMEOUT`, 180 s), keeps sampling `SETTLE` seconds (45) and checks.

Checks, each printed as PASS or FAIL, per source unless noted:

- kill-leader: a new holder with a higher epoch, the takeover within `TAKEOVER_MAX` (60 s, counted from the request, which includes the 5 s before the agent starts). The agent step stops the Aggregator pods of one node, so only the sources led from there change hands. Before the run the script reads which pod holds each lease (the `lease acquired` log line) and the uid and restart count of every Aggregator pod. After the run a takeover is expected only for the sources whose holder pod is gone, recreated or restarted. The others print INFO and stay the leader. A holder that no log line names is a FAIL, and so is a run where no leader pod was stopped, because nothing was tested then. A source that was expected to change hands and kept its holder FAILs. Gaps and duplicates are checked in the Audit API, see below.
- clock-skew: the epoch may stay or go up, so only the checks below apply. The lease uses the clock of `coordination-db`, so a shift of a pod clock must not change who holds it. Criterion 4 (no two instances storing a result for the same period) is covered as far as it is observable. Rows and Audit items have no writer field, so a period is one lease epoch and the writers are the Aggregator pods whose log (`kubectl logs <pod> --since-time=<start>`) has an `ingest` line with `changed` above 0 for the source. If the epoch of a source did not move during the run, at most one pod may show such lines, else FAIL. If the epoch moved, the logs cannot tell a takeover from two writers (pod timestamps come from the skewed clock and are not used), so the script prints INFO and does not claim the criterion for that source. The lease row itself holds one holder, so it cannot show two writers. A pod log that cannot be read is counted in the line and not treated as a writer.
- Samples: the sampler reads the lease table every second, and a failing `psql` leaves an empty round. Every source must have a row in at least `MIN_SAMPLE_PCT` (80) percent of the rounds, else FAIL, so an unreachable cluster cannot pass with a gap of 0 s.
- All scenarios: the longest time without a valid holder at most `LEASE_TTL` (15 s) plus `GAP_MARGIN` (5 s), the epoch never going down in any sample, and the Audit API checks below. The table has one row per source, so two holders at the same time cannot show in the samples, the duplicate check is what shows two instances storing the same period.
- Audit API checks, per source, after every scenario. The Audit API items (`hazard_id`, `source_ref_id`, `occurred_at`, `ingested_at`, attributes) have no sequence number or writer field, so the script uses what exists. (a) Duplicates: no repeated `source_ref_id` when paging by cursor as `audit-checksum.sh` does. (b) Gap by loss: the ids are read before the scenario, and every one of them must still be there afterwards. (c) Gap in the paging: `/internal/audit/checksum` (count and sha256 of the byte-sorted ids) must equal an independent sha256 over the paged ids, the definition of `audit-checksum.sh`. The endpoints are not a snapshot and ingestion goes on, so a mismatch is read again up to three times. (d) Gap in the sequence: the ids of the BMKG and PVMBG mocks end in a counter (`BMKG-<start>-000012`, one new event every `EVENT_INTERVAL_S`, 10 s by default, also while the Aggregator is down), so in the newest mock run the numbers from the first to the last stored id must all be present. An event that the new leader did not fetch after the takeover shows as a hole. SENSOR_PANTAI ids have no sequence, so it only gets (a) to (c). What this proves and what not: no stored event was lost and no number is skipped between the oldest and newest stored id of the run. It cannot see events newer than the newest stored one (the settle time after the scenario covers that), and it does not compare with the number of polls, which is the single fetch check of the issue and is not in this script.
- `fenced_writes_total` is read from every Aggregator pod through the API server proxy (`kubectl get --raw /api/v1/namespaces/<ns>/pods/<pod>:8091/proxy/metrics`) before and after, and printed per source as INFO in every scenario, never as PASS or FAIL. The counter lives in the process, so a killed pod takes it away and a new pod counts from zero, and a healthy cluster may have had nothing in flight.

Freeze. The Tester Agent has no freeze scenario (`/app list`: clock-skew, kill-leader, kill-store, partition, schema, wipe-node), so the pause has to come from the dev host. The script runs two commands you give it, with `NODE` set to `FREEZE_NODE`:

```bash
FREEZE_NODE=node-b \
NODE_FREEZE_CMD='VBoxManage controlvm "$NODE" pause' \
NODE_THAW_CMD='VBoxManage controlvm "$NODE" resume' \
tests/scenarios/problem2-k8s.sh freeze
```

The VM name must equal the node name, else adapt the commands. Pick a node that is not the control plane and not the node of `coordination-db-0` (the script refuses that one, the authority would freeze with it) and that runs a pod holding a lease. `FREEZE_HOLD` (30 s) must be longer than `LEASE_TTL`.

Pausing one node only takes the leaders that live on it, so the script first finds out which source is led from `FREEZE_NODE`. The `holder` in `source_lease` is a random id of the lease elector, not a pod name. Each Aggregator pod prints it in its `lease acquired` log line (`source`, `holder`, `epoch`), so before the freeze the script reads those lines (`kubectl logs <pod> --tail=-1`) and the node of each pod (`kubectl get pods -l role=aggregator -o jsonpath`), and maps every source holder to a pod and a node. A holder that no log line names (for example the line has rotated out) is a FAIL, because the check cannot be judged. If no source has its holder on `FREEZE_NODE` the step FAILs before it freezes anything. Only the sources led from `FREEZE_NODE` must get a new holder with a higher epoch within `TAKEOVER_MAX`, for the others it prints INFO (the holder stays, the epoch may move) and still checks the gap, the epoch direction and the Audit API.

Criterion 5, the woken instance stores nothing. The Audit API items and the store rows carry no writer epoch or holder (`write_fence` only keeps `max_token` per source on each store node), so what the old holder stored cannot be read from the data. The script reads the log of the old holder pod since the thaw (`kubectl logs <pod> --since-time=<thaw time>`). For the source it held, every `ingest` line before the pod's own next `lease acquired` line must show `changed=0` (`changed` is the number of rows inserted or updated). After that `lease acquired` line the pod has a new epoch and its writes are legitimate, so they are not counted. The step also prints the `lease lost` and `fenced` lines it saw as INFO, and FAILs when the pod has no log line since the thaw at all, because then nothing is shown. The limit of this proof is that it trusts the pod log, a store write that does not reach the log would be missed. After the thaw the script samples `SETTLE` seconds so the woken instance can try to write. Then the checks above run, with `fenced_writes_total` as INFO only. Without the three variables the freeze check prints SKIP and the run can still PASS, `REQUIRE_FREEZE=1` turns the skip into a FAIL. The manual procedure document `doc/P2_FREEZE_TEST.md` and the rehearsal of the issue are not part of this change.

Restore. The script changes nothing in the cluster itself. kill-leader and clock-skew undo themselves in the agent (pods are recreated, the clock returns, see `doc/TESTER_AGENT_NOTES.md`). The EXIT, INT and TERM trap stops the sampler and the port-forward, and runs `NODE_THAW_CMD` when the script stops while the node is frozen. Before the next scenario it waits until every lease row is valid and the Aggregator rollout is done.

Settings (env): `NS`, `TESTER_NS`, `SOURCES`, `LEASE_TTL`, `GAP_MARGIN`, `TAKEOVER_MAX`, `SCENARIO_TIMEOUT`, `SETTLE`, `END_PATTERN`, `FAIL_PATTERN` (a guess at the failure wording, as in `gameday.sh`), `FREEZE_NODE`, `NODE_FREEZE_CMD`, `NODE_THAW_CMD`, `FREEZE_HOLD`, `MIN_SAMPLE_PCT`, `REQUIRE_FREEZE`, `AUDIT_PORT`. The exit code is 1 when any check is FAIL and 2 when the cluster is not ready.

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

`deploy/situation-summarizer.yaml` is the bonus consumer (#326): `role: consumer`, 128Mi/256Mi, one replica, health and summary on 8097, its own queue `situation-summarizer`. The LLM key comes from Secret `llm-credentials` (created by `deploy/make-secrets.sh`, a placeholder unless `LLM_API_KEY` is set in `.env`) and `AI_ENABLED` is false in `bnpb-config`, so a fresh cluster uses the rule based summary. The alert latency comparison is run with `tests/bonus/ai-isolation.sh`, described in its header.

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

## Running every practice scenario

`tests/scenarios/all-k8s.sh` runs the scenario scripts that exist, in a fixed order, against the cluster and ends with one table. It has no scenario logic of its own, it only starts the child scripts and judges them by their exit code. The Compose runner for `problem1.sh` to `problem5.sh` stays `tests/scenarios/all.sh`.

```bash
export KUBECONFIG=$HOME/vms/k3s/kubeconfig
tests/scenarios/all-k8s.sh --list                         # scenarios, scripts, timeouts, needed variables
DRY_RUN=1 tests/scenarios/all-k8s.sh                      # the plan, nothing is run and no cluster is touched
EMAIL=... PASSWORD=... API_URL=... BASE_URL=... tests/scenarios/all-k8s.sh
tests/scenarios/all-k8s.sh --only rolling-update,autoscale
tests/scenarios/all-k8s.sh --skip load-ratio
tests/scenarios/all-k8s.sh --include-destructive          # also gameday and wipe-node
```

Order and why:

| # | Name | Script | Issue | Kind |
|---|---|---|---|---|
| 1 | `audit-checksum` | `tests/scenarios/audit-checksum.sh` | #295 | read only, so it also shows the Audit API works before anything moves |
| 2 | `rolling-update` | `tests/scenarios/rolling-update.sh` | #286 | short, patches `bnpb-config` and restores it |
| 3 | `autoscale` | `tests/scenarios/autoscale.sh` | #327 | needs the live HPA at its minimum, so it runs before the load scripts that delete the HPA |
| 4 | `p2-leases` | `tests/scenarios/problem2-k8s.sh` | #287 | slot, in PR #388 |
| 5 | `p3-e2e` | `tests/scenarios/problem3-k8s.sh`, `p3-e2e.sh` or `problem3-e2e.sh` | #307 | slot, not written yet |
| 6 | `p4-60x` | `tests/scenarios/problem4-k8s.sh`, `p4-60x.sh` or `problem4-60x.sh` | #315 | slot, not written yet |
| 7 | `load-p2` | `tests/load/p2-k8s.sh` | #288 | heavy, scales the Aggregator and deletes the HPA, restores both |
| 8 | `load-ratio` | `tests/load/p2-ratio.sh` | #288 | heaviest, 1 against 3 Aggregators |
| 9 | `gameday` | `tests/scenarios/gameday.sh` | #324 | destructive, runs wipe-node and kill-store |
| 10 | `p1-wipe-node` | `tests/scenarios/problem1-k8s.sh`, `p1-wipe-node.sh` or `problem1-wipe-node.sh` | #298 | destructive slot, not written yet |

Short and read only checks come first so a broken cluster shows up cheaply. The scripts that change the ConfigMap or the HPA come next, then the heavy load, and the destructive ones come last because a node can stay down or a database can need a manual recovery. `wipe-node` is the last of all for that reason. A slot is an entry whose script has not landed. The orchestrator uses the first candidate file that exists, so the entry starts working as soon as the script is merged, with no change to `all-k8s.sh`. Until then it prints SKIP with the issue number. The candidate names are guesses, so adjust `REGISTRY` in `all-k8s.sh` if the real file is named differently. `critical-alerts.sh`, `trace-check.sh` and `problem1.sh` to `problem5.sh` need Docker Compose or are not practice scenarios, so they are not part of this run.

Before the first scenario it checks that every node is Ready, every pod in the namespace is ready, `coordination-db-0` is running and the leases of BMKG, PVMBG and SENSOR_PANTAI are held. If that fails nothing runs and the exit code is 2. It also saves the values of `ACCESS_TOKEN_TTL_S`, `RATE_LIMIT_RPS` and `CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS` in `bnpb-config`, whether `tester-run` exists and whether the HPA of the Aggregator exists.

After each scenario that ran, it puts back what the child may have left: the three ConfigMap values (and restarts `auth-service` and `client-facing-api`), a `tester-run` ConfigMap that was not there before, and the HPA from `deploy/aggregator-hpa.yaml`. Then it waits up to `HEALTH_WAIT` seconds (120) for the same health checks, and checks the Audit API through a port-forward: events and checksum for each source must answer 200, location and unprocessed are checked when the build has them (a 404 is noted as not available). The same health check runs before each scenario, so an unhealthy cluster gives a SKIP with the reason instead of a failure that is not the scenario's fault.

Results per scenario:

| Result | Meaning |
|---|---|
| PASS | the child exited 0 |
| FAIL | any other exit code |
| TIMEOUT | the child ran longer than its timeout and was stopped (exit 124 or 137) |
| SKIP | not run: script missing or not on main yet, a required variable not set, destructive without the flag, `--only` or `--skip`, the cluster not healthy, or the child exited 2 (it could not start) |

The Audit column is separate: an Audit failure does not change the scenario result but makes the overall exit code 1. Timeouts default to the values in `--list` and can be changed with `TIMEOUT_<NAME>` (for example `TIMEOUT_ROLLING_UPDATE=900`).

Destructive scenarios (`gameday`, `p1-wipe-node`) are skipped unless `--include-destructive` is given. With the flag the script prints what each one destroys, at the start and again just before it runs, and waits `CONFIRM_SECONDS` (10) so you can press Ctrl-C. `gameday` is started with `STRICT=1`, so a FAIL verdict gives a non-zero exit code.

Manual steps, also printed at the top of every run: the VM freeze of the P2 freeze check (`FREEZE_NODE`, `NODE_FREEZE_CMD`, `NODE_THAW_CMD`), joining a node in the P1 script, and starting the VM again after wipe-node (`NODE_UP_CMD`).

Output goes to `tests/results/` (not tracked): `all-<stamp>.json` and `all-<stamp>.txt` with the table, `all-<stamp>.log` with the whole run and `all-<stamp>/<scenario>.log` for each child. The exit code is 0 when nothing failed (PASS and SKIP only), 1 when a scenario FAILed or timed out, an Audit check failed, or a SKIP happened with `--fail-on-skip`, and 2 for a usage error or an unhealthy cluster at the start. Ctrl-C runs the cleanup and writes the results gathered so far.

`tests/scenarios/all-k8s_test.sh` checks the orchestrator offline, with fake child scripts (PASS, FAIL, hang, exit 2, missing), a fake `kubectl` and a fake Audit API. It touches no cluster. The orchestrator itself has not been run against the cluster.

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
