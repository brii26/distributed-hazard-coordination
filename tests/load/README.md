# Load tests

| File | What it does |
|---|---|
| `run.sh`, `p2.js` | Problem 2 load test on the Compose stack (50 connections, PVMBG delayed 3 s) |
| `priority.sh`, `priority.js` | Mixed-role priority load on an isolated API fixture |
| `p2-k8s.js` | k6 test for the cluster, 10000 VUs or more for 120 s or more, 429 counted apart from errors |
| `p2-k8s.sh` | Runs `p2-k8s.js`, scales the Aggregator 3 -> 5 -> 2 -> 3 during the hold, then checks the Audit checksum and the upstream mock logs |
| `p2-ratio.sh` | Runs the same test with 1 and with 3 Aggregators, prints the ratio, PASS at 2.0 or more |
| `k8s-lib.sh` | Machine check, k6 launcher, ConfigMap and HPA set and restore, 200 share check and single fetch check used by the two scripts above |
| `results/` | Summaries and configs of each run (generated files are not tracked) |

## Cluster run (issue #288)

Status: written and checked offline only (`k6 inspect`, `shellcheck`, `bash -n`, a 5 VU smoke run against a fake local server). **The 10k VU run, the scaling during load, the Audit and mock checks and the 2.0 ratio have not been run on the cluster.** Do not quote any number from this folder until they have.

Run from the dev machine or another host on the cluster network, not inside a pod, so k6 stays out of the memory table. Needs `kubectl` for the cluster, `curl`, `jq`, and either `k6` or Docker (image `grafana/k6:1.4.2`).

```bash
export BASE_URL=http://10.77.0.11:30081     # Client-Facing API NodePort, or the public entry point
export EMAIL=operator@bnpb.example PASSWORD=...
export AUDIT_TOKEN=...                       # token of the Audit API on the Aggregator
tests/load/p2-k8s.sh                         # error rate, 429 count, scaling, audit, single fetch
tests/load/p2-ratio.sh                       # 1 vs 3 Aggregators
```

What the two scripts change on the cluster for the run, and put back on exit (EXIT, INT and TERM trap):

- ConfigMap `bnpb-config`: `ACCESS_TOKEN_TTL_S` 7200 (`LOAD_TOKEN_TTL_S`), `RATE_LIMIT_RPS` 1000000 (`LOAD_RATE_LIMIT_RPS`) and `CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS` 20000 (`LOAD_MAX_INFLIGHT`). The old values are saved in `results/configmap-saved-<stamp>.json`. `auth-service` (it signs the token) and `client-facing-api` (it reads the limits) are restarted after the change and after the restore. The default 60 s token would be 401 after a minute because the refresh token rotates and one login cannot be shared by 10k VUs, and login per VU is limited per IP (`AUTH_RATE_LIMIT_RPS`, 10 by default). The default 50 rps per user and per API pod would answer almost everything with 429 for a single account.
- The HorizontalPodAutoscaler that targets the `aggregator` Deployment (issue 327, min 3 and max 4 or 5), if there is one: it is saved in `results/hpa-saved-<stamp>.json`, deleted for the run and created again on exit. Without an HPA nothing happens.
- Aggregator replicas: back to 3 on exit.

If a script is killed with `kill -9` the trap does not run, so check `kubectl get cm bnpb-config` and the HPA afterwards (`deploy/deploy.sh` puts the committed values back).

Settings (env): `VUS` (10000), `HOLD` (150s, must be at least 120s), `RAMP` (30s), `THINK_S` (1), `SCALE_UP_AT` and `SCALE_DOWN_AT` (seconds into the hold, default 25% and 60%), `MIN_200_SHARE` (0.95), `LOAD_TOKEN_TTL_S`, `LOAD_RATE_LIMIT_RPS`, `LOAD_MAX_INFLIGHT`, `FORCE=1`, `SKIP_CHECKS=1`, `NS`, `K6_IMAGE`.

### How the acceptance criteria are checked

- Error rate under 1% excluding 429: the k6 threshold `failed rate<0.01` counts any status other than 200 and 429. `p2-k8s.sh` also reads `error_rate_excluding_429` from the compact summary and prints the number of 429 (`limited_429`).
- At least 10000 VUs for at least 120 s: the script prints PASS only if `vus_max >= 10000` and `HOLD >= 120s`.
- Scaling: the driver scales to 3, then 5, then 2 inside the hold, logs the times in `p2-k8s-<stamp>.scale.log`, and returns to 3 on exit even after Ctrl-C or a failure (trap).
- 200 share: both scripts print the share of 200 answers and FAIL below `MIN_200_SHARE` (0.95), with a loud message. A run that is mostly 429 or 401 can no longer pass on its error rate. The request mix is only `/hazards` (it reaches the Aggregator), `/health` is not in it.
- No duplicate events: `tests/scenarios/audit-checksum.sh` for BMKG, PVMBG and SENSOR_PANTAI over a port-forward to `svc/aggregator:8091`, plus a check that every page of events (paged by cursor, 1000 per page) holds no repeated `source_ref_id`.
- Single fetch: the two mocks log `remote_addr`, the IP of the caller, which is the Aggregator pod. For each mock (one source) the script takes the data requests since the start of the run (`/health` left out), sorts them by time and FAILS only when a request from one caller starts before the previous request of a different caller has ended, so two pods fetched the same source at once. A request spans from its logged time minus `duration_ms` to its logged time. A line without `duration_ms` spans `SINGLE_FETCH_TOLERANCE_MS` (100 by default). A handover where the old caller stops and a new one takes over, for example when the pod that holds the lease is scaled away from 5 to 2, does not overlap and passes. It also FAILS when the log has no requests or no `remote_addr` (an old mock image). The mock images must be rebuilt and deployed with the `remote_addr` change before the run.
- Ratio at least 2.0: `p2-ratio.sh` divides the 200 answers per second in the hold window with 3 replicas by the same with 1 replica. A 429 is not throughput. The script raises `RATE_LIMIT_RPS` for the run and restores it, and the ratio is only valid when the 200 share of both runs passes, otherwise the result is FAIL.

### Can this machine generate 10k VUs

Both scripts call `check_machine` first. It prints free RAM, `ulimit -n` and CPU count, and refuses to start (unless `FORCE=1`) when free RAM is below about 2 MB per VU plus 1 GB or `ulimit -n` is below VUS + 1000. This is a rule of thumb, k6 memory depends on the script. Raise the limit with `ulimit -n 20000` first. A single source IP also has a limited range of ephemeral ports (check `sysctl net.ipv4.ip_local_port_range`), which caps connections to one address and port.

If the machine is too small, run k6 on a second machine on the cluster network, with the same environment variables:

```bash
k6 run -e BASE_URL=... -e EMAIL=... -e PASSWORD=... -e SUMMARY_FILE=$PWD/tests/load/results/p2-k8s-manual.json tests/load/p2-k8s.js
```

Scale the Aggregator by hand from a machine with `kubectl` while it runs (`kubectl -n sistem-informasi-kebencanaan-bnpb scale deployment/aggregator --replicas=5`, then `--replicas=2`, then `--replicas=3`), then run the audit and mock checks. State in the report that k6 ran on a second machine.

### Results

`tests/load/results/` holds, per run: `p2-k8s-<stamp>.config.json` (settings and machine), `.txt` (k6 output), `.json` (full k6 summary), `.compact.json` (counts, 429, error rate, throughput), `.scale.log`, `.audit.txt` and the mock logs. These files are ignored by git, copy the ones the report cites.
