# Problem 1: test notes

How to prove, on the cluster, that a node can lose all its data, that a node can join without stopping traffic, and that the system survives the next loss. The script `tests/scenarios/problem1-k8s.sh` does the wipe and the checks. This page has the manual steps for joining a new node, and the facts the script relies on.

## The script

```bash
tests/scenarios/problem1-k8s.sh              # A, B and E
ONLY="A B" tests/scenarios/problem1-k8s.sh   # some scenarios
DRY_RUN=1 tests/scenarios/problem1-k8s.sh    # the plan only
```

- **A** pages through the cross-shard list (`GET /hazards?limit=50`) and the audit events, and checks status 200 and no repeated id.
- **B** saves the audit ids, starts a probe (a list page and an audit page every second), stops the store on the first worker, empties `/var/lib/rancher/k3s/storage/bnpb-store` there with a one-shot pod, starts the store on the empty disk, waits for the recovery and checks: no saved id is missing, the writes continued, the audit checksum equals a SHA-256 over the listed ids, the probe saw no 5xx and no hang, every sampled id has two copies, and the returned node holds copies again.
- **E** does the same for the second worker, so the system is shown to survive the next loss (criterion 6).

A FAIL means the implementation is wrong, NOT RUN means the script could not run. Exit codes: 0, 1 (a FAIL), 2 (cluster not ready). Run it on a cluster nobody else uses: it stops the store on one node and restores the DaemonSet on exit.

## Why the script wipes the folder itself

The Tester Agent's `wipe-node` empties `/var/lib/rancher/k3s/storage` (the local-path volumes), and the store keeps its data in `/var/lib/rancher/k3s/storage/bnpb-store`, a folder under it, so the agent's wipe is real (the first measurement in [TESTER_AGENT_NOTES.md](TESTER_AGENT_NOTES.md) was made when the store was still in `/var/lib/bnpb-store`, which the agent never touched). The M2 text allows a `hostPath` for `role=store` data (Tester Agent contract, item 7). Take a `pg_dump` of every store node with `deploy/store-dump.sh` before the exam.

## Joining a new node by hand (criterion 5)

The DaemonSet `store` starts a store pod on every node by itself, and the rebalance loop (#294) copies the shards the new node owns. Nothing is stopped.

1. Create the VM and join it to k3s. Both scripts are safe to run again, existing nodes are kept:
   ```bash
   NODE_COUNT=4 deploy/cluster/up.sh
   NODE_COUNT=4 deploy/cluster/k3s.sh
   kubectl get nodes
   ```
2. Check that the store pod started on the new node: `kubectl -n sistem-informasi-kebencanaan-bnpb get pods -l app=store -o wide`.
3. Keep traffic running while the node fills up, for example `tests/scenarios/audit-checksum.sh` in a loop or the probe of `problem1-k8s.sh`.
4. Within a few rebalance rounds (`STORE_REBALANCE_INTERVAL`, 30 s by default) the new node holds data. Check the share with the Audit API: for a sample of ids, `GET /internal/audit/location?hazard_id=<id>` lists the nodes with their role (`primary` or `replica`). Count the ids per node.
5. Check the answers stayed complete: `GET /internal/audit/events?limit=1` has `partial: false` once the rebalance is done.

The same node with an empty disk (the other form of criterion 5) is what scenario B does.

## Facts the script relies on

- Shards: 16 hash shards, two copies each, routing and repair in the Aggregator (#289 to #294).
- The audit checksum is a SHA-256 over the sorted `source_ref_id` values of one source (M2, Appendix A). The sources keep writing, so the script compares the checksum with a SHA-256 over the ids it just listed, and retries while the data moves. A source that never stands still is reported as NOT RUN.
- A login token lasts about 60 seconds. The probe sends its cookie jar back with every request so the token is refreshed.
