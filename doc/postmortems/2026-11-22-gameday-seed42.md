# Post-mortem: game day seed 42, 22 November 2026

## Header

| Field | Value |
|---|---|
| Title | Game day with six scenarios and three injected faults on the three node k3s cluster |
| Date and time window | 2026-11-22 22:00:24 to 22:36:28 (UTC+7), 2164 s |
| Game day seed and run stamp | `SEED=42`, `20261122-220016` |
| Failure kinds involved | kill-leader with PVMBG outage, schema, partition, clock-skew, wipe-node with sensor-down, kill-store (k6 spike planned, not run) |
| Evidence folder | `tests/results/gameday-20261122-220016*` (log, summary, probe CSV, alerts CSV) |
| Status | draft |

## Summary

The run finished and the script ended with FAIL: read availability of the probe was 39.477 percent against a target of 99.5. Two causes explain almost all of it and neither is a slow recovery of the data path: about 6 minutes without an answer from the API while node-c was wiped and restarted, then 10 minutes of HTTP 401 because the probe account was lost with the `user-db` volume. The probe also asked for a hazard type the API does not accept, which made one of three read checks fail for the whole run. The SLO queries returned no data, so the critical alert SLI and the error budget are not known for this run.

## Timeline

| Time | Kind | Event | Source |
|---|---|---|---|
| 22:01:24 | injected | kill-leader, with a PVMBG outage 14 s later; clean after 8 s | run log |
| 22:03:35 | injected | schema, clean after 7 s | run log |
| 22:05:04 | injected | partition, clean after 36 s | run log |
| 22:08:18 | injected | clock-skew, no end marker for 600 s, ended as timeout at 22:18:18 | run log |
| 22:19:25 | injected | wipe-node, node-c powered off after 62 s, sensor-down 25 s after the start | run log |
| 22:20:23 | observed | first probe answer of 000 or 502 | probe CSV |
| 22:21:28 | action | the script deleted `tester-run` and started node-c (this is the one place it acts) | run log |
| 22:26:20 | observed | `user-db` ready again | run log |
| 22:26:22 | observed | first HTTP 401 for the probe user, until the end | probe CSV |
| 22:29:18 | injected | kill-store, clean after 7 s; the k6 spike did not start (login of the Media and Lapangan users failed) | run log |
| 22:31:28 | observed | recovery window of 300 s, nobody touches anything | run log |
| 22:36:28 | observed | end, verdict FAIL | run log |

## Detection

- The signal was the probe CSV. Per endpoint the longest failing streak was 48 s for health and 791 s for seismic and volcanic.
- Prometheus could not be read by the script (all SLO queries returned no data), so no burn rate or rule alert is available for this run. That is a detection gap in the run, not a statement about the rules.
- The 401 streak looks like an outage in the probe but the API answered. Someone reading only the availability number would be misled, which is the main finding of the run.

## Impact

- Reads by Tim Lapangan: failed for 22:20:23 to 22:26:21 (no answer or 502) and for 22:26:22 to the end (401, the account did not exist any more).
- Probe samples: health 1790 of 1876 good (95.416 percent), seismic 1156 of 1947 (59.373 percent), volcanic 1153 of 1946 (59.250 percent). Coastal 0 of 1956: all answers were 400 because `/hazards?type=COASTAL` is not valid, the API accepts `SEISMIC` or `VOLCANIC`. The probe is wrong here, not the system.
- Critical alerts: no data (no SLI), so delivery in 5 s and zero loss are not shown by this run.
- The k6 load spike was not part of the run.

## Root cause

- Wiping node-c removed the volumes of everything pinned there. While it was down and restarting the API gave no answer or 502. It came back after the script started the VM and `user-db`, `coordination-db` and the store were ready.
- `user-db` is one replica on a local-path volume. After the wipe it started from the init scripts, which only create the seed Operator. The accounts the game day had created (probe, Media, second Lapangan) were gone, so every login failed with 401 and the k6 spike could not start. This is the documented limit of `wipe-node` for `user-db`, and the game day script cannot undo it.
- The clock-skew scenario never wrote its end marker where the script looks. The script polls the Tester Agent log with `--since-time`, and a shifted node clock moves the log lines, the same cause that was fixed in `problem2-k8s.sh` earlier. This is a likely cause, not checked on this run.
- The coastal check is a bug in the probe list of `gameday.sh`.

## Mitigation

What the system did by itself: the Aggregator leases moved after kill-leader (clean after 8 s), the store and `coordination-db` came back after node-c was started, and the API served again after `user-db` was ready. What it did not do: restore user accounts, and it did not need an operator for anything but the node start the script performs.

## Fixes

| # | Fix | Type | Owner | Issue or PR | Due |
|---|---|---|---|---|---|
| 1 | Remove the coastal check from the probe (`SEISMIC` and `VOLCANIC` only) | prevent | brii26 | this change | done |
| 2 | Re-create the game day accounts after a wipe, or seed them in the user-db init script, so a wiped `user-db` is not read as an outage | mitigate | brii26 | follow up | open |
| 3 | Poll the clock-skew end marker without `--since-time`, as `problem2-k8s.sh` does | prevent | brii26 | follow up | open |
| 4 | Reach Prometheus from the script so the SLO verdict has data | detect | brii26 | follow up | open |
| 5 | Run the k6 spike before wipe-node, or log in again just before it | prevent | brii26 | follow up | open |
