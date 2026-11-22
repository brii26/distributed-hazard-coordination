# P2 freeze test: one VM paused, rehearsed on the k3s cluster

The Tester Agent has no freeze scenario, so the VM is paused from the dev host. `tests/scenarios/problem2-k8s.sh freeze` does it and checks the leases and the data.

## Run it

```sh
FREEZE_NODE=node-b \
NODE_FREEZE_CMD='VBoxManage controlvm "$NODE" pause' \
NODE_THAW_CMD='VBoxManage controlvm "$NODE" resume' \
tests/scenarios/problem2-k8s.sh freeze
```

`FREEZE_NODE` must be a worker that is not the node of `coordination-db-0` (the script refuses that one). `NODE` in both commands is the node name, so the VM name must equal it. The thaw command also runs from the exit trap if the script stops while the node is frozen. Without both commands the freeze check prints SKIP, and `REQUIRE_FREEZE=1` makes the SKIP a FAIL.

The script reads the lease holder of each source, maps the holder id to a pod through the `lease acquired` log line, and maps the pod to a node. Only the sources whose holder is on `FREEZE_NODE` must change hands. The Aggregator pods must have a recent `lease acquired` line in their log, so restart the Aggregator first if the pods have run for hours, as the log rotates (see the rehearsal).

## Rehearsal, 2026-11-22

3 VM k3s cluster, 4 Aggregator pods, `FREEZE_NODE=node-b`. The leaders of BMKG, PVMBG and SENSOR_PANTAI sat on node-b (BMKG and SENSOR_PANTAI on one pod, PVMBG on another).

| Check | Result |
|---|---|
| New holder with a higher epoch | BMKG 79 to 80, PVMBG 55 to 56, SENSOR_PANTAI 97 to 98 |
| Takeover | 33 s, limit 60 s |
| Longest time without a valid holder | 18 s, limit 20 s (TTL 15 s + 5 s) |
| Epoch never went back | all three sources |
| Audit API | no repeated `source_ref_id`, no id lost, checksum equals an independent SHA-256, no hole in the BMKG and PVMBG sequences |
| After the thaw | both woken pods stored nothing (`ingest changed=0`) until they took a lease again |

Result: 29 PASS, 0 FAIL.

## What it does not show

- The 18 s gap is close to its 20 s limit. A slower node or a busier `coordination-db-0` can break it, so read the limit as a margin, not as a promise.
- After the thaw the woken pods logged no `lease lost` and no fenced write (`fenced_writes_total` +0), and did not take the lease again. They stored nothing because they never tried. The fencing path itself was not exercised by this run.
- Only the leaders on one node were frozen. A run where the leaders sit on different nodes would need a second freeze.
- The counter `fenced_writes_total` lives in the process, so a pod that was replaced starts again from zero.
