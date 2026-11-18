# Tester Agent notes

Run on 2026-11-18 on the 3 node k3s cluster (node-a control plane, node-b, node-c) with the Aggregator at 3 replicas, 3 store pods, `coordination-db`, `user-db` and RabbitMQ deployed. The manifest was applied unmodified from the pinned commit:

```
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
```

## Installation

- One pod per node, all `Running`: `tester-agent-b27j5` on node-a, `tester-agent-ftt72` on node-b, `tester-agent-tdpld` on node-c.
- The DaemonSet is privileged with `hostNetwork`, `hostPID` and the host root at `/host`. Its RBAC covers leases and ConfigMaps in `tester-agent`, `list` and `delete` of pods and `list` of Services in the app namespace, and `list` of nodes. Nothing in our manifests blocks it (no NetworkPolicy, taint or resource limit on `tester-agent`).
- Each agent logs a sha256 of its binary at start, `86d511c735a67b21a406116da594a12927585867cb391dfdd4d6a222965aa418` on all three nodes.

`kubectl exec -n tester-agent daemonset/tester-agent -- /app list` printed:

```
clock-skew
kill-leader
kill-store
partition
schema
wipe-node
```

## How a run works

- `/app run practice <name>` only submits the plan and prints `Permintaan <id> dikirim`. The steps run later, about 5 s after the request, and the result is in `kubectl logs -n tester-agent -l app=tester-agent` (JSON lines with `action`, `prev` and `mac`, a hash chain).
- The current run is a ConfigMap `tester-run` in `tester-agent`. It stays there after the scenario. A run that shuts a node down (`wipe-node`) is replayed by the agent on that node every time the node boots, so the node powers off again after a few minutes. Delete the ConfigMap before restarting the node: `kubectl -n tester-agent delete cm tester-run`.

## Scenarios

### schema

- Did: one step, `call_mock_admin POST /admin/schema-version`, answered HTTP 200 by the mock, then "pemulihan akhir selesai".
- Needs: a mock Service in the app namespace (it found `bmkg-mock`). Nothing else.
- Open: the request body sent to the mock is not logged.

### kill-leader

- Did: `stop_pod role=aggregator nodes=any`. It stopped two Aggregator pods (the ones holding leases), both on node-c.
- Effect measured: every lease row got a new holder with epoch + 1 within 15 s of the request (BMKG 32 to 33, PVMBG 8 to 9, SENSOR_PANTAI 50 to 51, rebalance 8 to 9). The pods were recreated within 55 s.
- Needs: pods labelled `role=aggregator`.

### kill-store

- Did: `stop_pod role=store nodes=any`. It stopped every pod with the label `role=store` on one node. On node-c that was `store-cn9gw`, `coordination-db-0` and `user-db-0`, because `coordination-db` and `user-db` carry `role: store` for the memory class.
- Effect: the three pods were back in about 70 s. Aggregators stayed Ready, all leases came back with epoch + 1.
- Needs: pods labelled `role=store`.

### clock-skew

- Did: `shift_clock +300s selama 30s nodes=any`.
- Effect: no source had two holders at any time. Epochs grew by 1 and polling continued. The lease uses the time of `coordination-db`, not the clock of the Aggregator node.
- Open: the clock of the nodes could not be read from the host during the window, so the shift itself is only known from the agent log.

### partition

- First run: failed with `biner iptables tidak ditemukan di host (/host/sbin, /host/usr/sbin, dsb)`. On the Ubuntu 24.04 nodes `/usr/sbin/iptables` is a symlink to `/etc/alternatives/iptables`, an absolute link. Inside the agent container the host is mounted at `/host`, so the link points into the container filesystem and the binary looks missing.
- Fix: replace the link with a real copy, `cp --remove-destination /usr/sbin/xtables-nft-multi /usr/sbin/iptables`, on every node. It is now in `deploy/cluster/cloud-init/user-data.tmpl`. Existing nodes need the same command once.
- Second run: `block_internode` for 30 s on all nodes. No Pod became unready and there was no lease event (no `lease lost`, no `lease acquire failed`) in the Aggregator logs, so the block did not affect pod traffic to `coordination-db` in our setup.
- Rules check (third and fourth run, sampled on node-b every second for 50 s and at +14 s and +56 s): `iptables -S` stayed at the three default ACCEPT policies, `iptables-legacy -S` had 3 lines, and `nft list ruleset` stayed at 799 lines with an identical hash (counters excluded) before, during and after the 30 s window. Node-b could ping node-c and open TCP 6443 on node-a during the window. So no rule was visible on the host at any moment, and nothing was left behind either. The agent log shows `block_internode selama 30s`, `langkah 1 selesai` and `pemulihan akhir selesai; keadaan bersih`, but the step has no observable effect from the host. We do not know why (the agent image has no shell, so its own view cannot be inspected). Treat `partition` as not working in this setup until the agent author explains it.
- Packaging notes: the `xtables-nft-multi` copy is not updated when iptables is upgraded with apt, so repeat the `cp` after an upgrade. `ip6tables`, `iptables-save` and `iptables-restore` are still absolute symlinks and would fail the same way if the agent calls them.

### wipe-node

- Did: `wipe_and_shutdown role=store nodes=any`. It picked node-c, deleted the content of `/var/lib/rancher/k3s/storage` (the local-path volumes) and powered the VM off. `TESTER_HOST_STORE_DIR` is not set in the manifest. Our store data in the hostPath `/var/lib/bnpb-store` kept its size (70 MB before and after), so the agent did not wipe it.
- Effect on `coordination-db`: while node-c was off no replica could take or renew a lease (`failed to connect ... database=coordination`) and polling stopped. After the node came back its volume was gone and Postgres started with a broken data directory, so the PVC had to be deleted and recreated. The lease rows rebuilt themselves, the PVMBG token pair was lost and the one time code was already spent, so polling PVMBG needed a restart of the mock.
- Same effect on `user-db` (accounts and the seeded Operator) and on RabbitMQ when it is on the wiped node.
- Needs: pods labelled `role=store` to pick the node.

## Log lines to wait for a run

Every node runs one agent pod and each pod logs the run. `/app run practice <name>` only submits (`Permintaan <id> dikirim`), the steps start about 5 s later. Lines seen (JSON field `action`, the `[L]` prefix is part of the text):

- `[L] skenario "partition" (practice) mulai; 1 langkah`
- `[L] langkah 1 mulai: block_internode selama 30s`
- `[L] langkah 1 selesai`
- `[L] pemulihan akhir selesai; keadaan bersih`
- `[L] skenario selesai`

The run is finished when `pemulihan akhir selesai` appears (`skenario selesai` follows). With `kubectl logs -n tester-agent -l app=tester-agent --prefix` the prefix names the pod, so the pod that carries a step line is the node that acted. We checked this only from the pod names and did not verify it for a single-node scenario such as `wipe-node`. For wipe-node the node that powers off also stops answering, so its pod log may not be readable until it is back.

## Open questions and findings for the team

1. `wipe-node` wipes the k3s local-path storage, not our hostPath. Our store would survive a wipe, so the P1 loss and recovery check does not exercise it, while PVC based workloads (`coordination-db`, `user-db`, RabbitMQ) do get wiped. Decide where store data should live.
2. `coordination-db` is a single replica on a local volume, so one `wipe-node` or `kill-store` on its node stops all polling and can lose the PVMBG token. It needs either replication or a place that survives a node wipe.
3. Node powers off again after a reboot until `tester-run` is deleted (see above). The runbook for the exam must include it.
4. `kill-store` stops everything labelled `role=store` on the node, including `user-db` and `coordination-db`.
