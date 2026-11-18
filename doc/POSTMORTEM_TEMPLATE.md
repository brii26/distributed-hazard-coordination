# Post-mortem template

Copy this file to `doc/postmortems/<date>-<short-name>.md` for each game day or incident and fill every section. Blameless: describe what the system and the process did, not who erred. Write only what the evidence shows. If a number or a cause is not backed by a log, a panel or a trace, write "not established" and say what would establish it.

The template is empty on purpose. A filled post-mortem must come from a real run (`tests/scenarios/gameday.sh`, see the acceptance criteria of #324), never be written from expectation.

## Header

| Field | Value |
|---|---|
| Title | |
| Date and time window (with time zone) | |
| Game day seed (`SEED`) and run stamp | |
| Author and reviewers | |
| Status | draft, reviewed, closed |
| Failure kinds involved | for example kill-leader, PVMBG outage, k6 spike |
| Evidence folder | `tests/results/gameday-<stamp>*` and the Grafana time range used |

## Summary

Two or three sentences: what failed, how long users were affected, whether the SLO and the zero-loss rule for critical alerts held.

## Timeline

Times in one time zone, taken from the run log (`tests/results/gameday-<stamp>.log`), Grafana and Jaeger. One line per event. Mark each line as injected, observed or action.

| Time | Kind | Event | Source of the time |
|---|---|---|---|
| | injected | | run log |
| | observed | first signal on the dashboard | panel id and title |
| | observed | first alert or burn rate crossing | `slo:*_burn_rate:rate5m` |
| | observed | system back to normal | panel id and title |

## Detection

- How was the failure first noticed, and by what (a panel, a rule, a probe sample, a person)? Give the panel id and title or the rule name.
- Time from the start of the fault to the first signal.
- Was the signal clear enough for someone who did not cause the fault to explain it? Check it against `doc/EXPLAIN_FAILURE.md` and list what was missing.
- Detection gaps: failures that no panel or rule showed.

## Impact

- Users and paths affected (Tim Lapangan reads, Media reads, critical alert delivery, ingestion from a source).
- Duration of user visible impact, from the probe CSV (longest failing streak per endpoint).
- SLO result, copied from the game day summary: critical alert SLI and read availability against their targets, error budget used and remaining, peak burn rates, alerts that fired.
- Critical alerts: delivered, late (over 5 s), undelivered. Any undelivered alert is a requirement failure whatever the percentage says.
- Data impact: stale data shown with `source_status` and `stale_since`, rejected or set aside records, repaired rows.

## Root cause

Direct cause and contributing causes, each with its evidence (a trace id, a metric query and its value, a log line). Separate what was injected from what the system did in response.

## Mitigation

What reduced or ended the impact during the run. During the recovery window nobody intervenes, so list what the system did by itself (lease takeover, retry, circuit breaker, load shedding, shard repair) with the evidence of each. If a human did act, say when and why, and mark the run as not hands off.

## Fixes

| # | Fix | Type (prevent, detect, mitigate) | Owner | Issue or PR | Due |
|---|---|---|---|---|---|
| 1 | | | | | |

Include fixes for detection gaps, not only for the cause. Each fix links an issue.

## What went well, what went badly, where we were lucky

Short lists.

## Evidence links

- Run log, summary, probe CSV and alerts CSV under `tests/results/`.
- Grafana dashboard `hazard-overview` with the time range in the URL, and the panel ids used.
- Jaeger trace ids or saved search URLs for each failure, with the service and operation names.
- Prometheus queries used, with the value and the evaluation time.
- Related issues, PRs and the `doc/EXPLAIN_FAILURE.md` checklist section followed.

## Explanation check

Name of a teammate who did not run the scenario and explained the failure using only the dashboard and the trace, the failure explained, and the date. Leave empty until it happened.
