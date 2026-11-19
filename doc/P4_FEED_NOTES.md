# P4: satellite feed notes

Date: 2026-11-18. Owner: Shanice. Issue: #308 (spike). Status: measured, for review. Feeds the P4 issues #309, #311, #312, #313, #314, #315 and the lease from #283 (section 3 says what each one gets).

Everything below was measured against `https://satellite.naufarrel.dev` with our group token, using only `GET /v1/session` and `GET /v1/stream`. The session was the **practice profile at speed 60** the whole time. Every number is an observation of that profile, not a promise about the exam profile. Where a number feeds a design value, the value has a safety margin and must stay configurable. The spec says handling that depends on IDs, seeds, order or patterns of the practice profile does not count.

Total open-stream time: about 44 minutes (budget 60). The raw captures stay local in `tools/feedprobe/out/` (gitignored). Section 6 regenerates them. A sample of each event type is in [`p4_feed_samples/`](p4_feed_samples/).

## 1. Summary

| Fact | Finding | Evidence | Feeds |
|---|---|---|---|
| F1 replay depth and speed | `?from=0` is accepted: the whole session history (offsets 0 to 436,475, about 38 feed days) is kept. Replay ran at ~2,600 events/s (peak 8,224 in one second) and reached live in 169 s. Live averages ~7 events/s. | E2, `e2-replay.ndjson` | #313: recovery by re-reading is viable |
| F2 heartbeats | `event: heartbeat` with `{"offset","virtual_now"}`. They carry an SSE `id` and consume offsets. Live interval: 15.8 s wall (p50), 21.8 s max, which is 15 to 22 feed minutes. In the replayed history, heartbeats are missing for spans of up to 31 feed hours. | E3, E2 | #312, #309 |
| F3 lateness | Feed time at arrival minus acq time (live, 15,340 detections): p50 8 min, p99 3 h 26 min, p99.9 3 h 59 min, max 4 h 49 min. Out of order by offset: p99.9 3 h 55 min, max 5 h 1 min (replay, 405,268 detections). | E3, E2 | #312: L = 12 h |
| F4 clock skew | No acq time after the feed clock (0 of 15,340). Minimum delay per satellite is 57 to 64 s for N, N20 and N21. Not visible in the practice profile. | E3 | #312: feed clock uses the newest acq unclamped |
| F5 payload shapes | `detection` is a full record under `detection`. `update` is a delta: `key` plus `changes` (subset of `bright_ti4`, `bright_ti5`, `frp`, `confidence`). `retract` is `key` plus `reason` (`false_positive`). Targets are named by the 5-field detection key only. | E2, E3 | #314 |
| F6 raw text | Coordinates are JSON numbers with 5 decimals. 63 of 405,271 latitudes near 0 use exponent form (`-6.0e-5`) while keys write `-0.00006`. `acq_time` is an unpadded integer (`25`, `2350`). | E2 | #311, #314: detection key |
| F7 bursts | Live bursts of ~1,028 events (~945 detections) in 11 s, flat at ≤101 events/s, every 3 to 6 wall minutes (3 to 6 feed hours). Mostly 1 to 9 events/s otherwise. | E3 | #315 |
| F8 reconnect | `?from=N` is inclusive. `Last-Event-ID: N` is exclusive. With both, `?from` wins. The server closes the stream by itself every 9 to 90 s (p50 65 s). 41 resumes, no gaps, and no offset ever sent twice by the server. No `retry:` field is ever sent. | E2, E3, E6 | #309: `?from=0` on the first connect, then `Last-Event-ID` only |
| F9 connection limit | A second stream gets `429 too_many_connections` with no `Retry-After`, and the server closes the existing stream at the same moment. After a client process is killed, the next connect is accepted at once (0.94 s). | E4, E5 | #283 lease, #309 |
| F10 slow reader | Paused 60 s: the server held a bounded backlog (499 events), stopped sending, and closed the stream. The backlog plus EOF arrived on resume. Resuming by offset lost nothing. | E3 | #309, #315 |
| F11 session | `profile: practice`, `speed: 60` in all 56 snapshots over ~70 min. `virtual_now` advances 60.0 feed seconds per wall second. `latest_offset` is the id of the last emitted event. | E0, E2, E3 | #315 lag metrics |
| F12 data quality | 6.5% of detections are re-sends of an earlier key at a new offset (identical body). One key collision with different values. Updates and re-sent detections arrive after a retract. No malformed JSON. Satellites N, N20, N21 in equal shares. | E2, E3 | #311, #314 |

## 2. Findings per fact

### F1. Replay depth and speed

- `GET /v1/stream?from=0` returned 200 and replayed from offset 0 (E2). No smallest offset had to be searched for.
- At the time, `latest_offset` was 435,762 and `virtual_now` 2027-01-05T00:49Z. The first acq date is 2026-11-27, so about 38 feed days were kept.
- Replay reached the `latest_offset` seen at connect in 168 s, across 4 connections (the server closes replay streams too, after 31, 65 and 53 s).
- **Rates:**
  - replay per connection: 2,068, 2,700 and 2,726 events/s;
  - replay peak: 8,224 events in one wall second;
  - live: 17,013 events in 40 minutes, about 7 events/s.

  Replay is therefore about 25 times faster than the live burst rate and about 370 times faster than the live average.

Interpretation: re-reading the feed after a crash is cheap. Even the full history replays in under 3 minutes.

### F2. Heartbeats

- **Shape:** `id: N`, `event: heartbeat`, `data: {"offset":N,"virtual_now":"2027-01-07T06:13:24Z"}`. Every heartbeat (616 in E2, 38 in E3) carries an SSE id that is part of the gap-free offset sequence.
- **Live interval (E3):**

  | measure | min | p50 | p90 | max |
  |---|---|---|---|---|
  | wall time | 2.3 s | 15.8 s | 17.4 s | 21.8 s |
  | feed time | 15 min | 15 min 48 s | 17 min 24 s | 21 min 48 s |

  Heartbeats keep coming during bursts. Their `virtual_now` advances 60.0 feed seconds per wall second.
- **Replayed history (E2):** feed-time gaps between heartbeats have p50 16 min but p90 4 h and max 31 h. The history has long spans without heartbeats, possibly spans when no client was connected (open question 5).

Interpretation: heartbeats are the only replay-safe source of feed time, because they live in the offset log. They cannot be the only thing that advances the feed clock, because of the gaps in history.

### F3. Lateness and order

Delay is the feed time at arrival minus the detection time (`acq_date` + `acq_time`, UTC). Feed time at arrival comes from the last heartbeat's `virtual_now` + 60 × wall time since then, or from `/v1/session` polled just before each connect. Live only (E3): replay has no arrival time. The resolution is ±1 minute (acq time has minute precision).

| E3 live, 15,340 detections | min | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| all | 57 s | 8 min 4 s | 54 min | 3 h 26 min | 3 h 59 min | 4 h 49 min |
| satellite N | 1 min 4 s | 8 min 4 s | 55 min | 3 h 15 min | 3 h 58 min | 4 h 14 min |
| satellite N20 | 57 s | 8 min 8 s | 54 min | 3 h 32 min | 4 h 12 min | 4 h 49 min |
| satellite N21 | 59 s | 8 min 3 s | 53 min | 3 h 23 min | 3 h 58 min | 4 h 20 min |

Out of order is measured from offset order alone, so it also works on replay. It is how far a detection's acq time is behind the newest acq time seen so far.

| Detections | p50 | p90 | p99 | p99.9 | max | share behind |
|---|---|---|---|---|---|---|
| E2 replay, 405,268 | 5 min | 42 min | 3 h 22 min | 3 h 54 min | 5 h 1 min | 86% |
| E3 live, 15,703 | 5 min | 42 min | 3 h 20 min | 3 h 55 min | 4 h 9 min | 86% |
| E3 live, same satellite only | 4 min | 41 min | 3 h 20 min | 3 h 53 min | 4 h 9 min | 78% |

Interpretation: lateness has a long tail of several hours ("laporan dari lintasan tadi pagi baru sampai siang hari"). The disorder exists within each satellite, not just between satellites. Replay and live have the same shape.

### F4. Clock skew

- **No acq time in the future:** no detection had an acq time later than the feed clock at arrival (0 of 15,340).
- **No per-satellite offset:** minimum delay per satellite is 57 s (N20), 59 s (N21) and 64 s (N), so no satellite stamps ahead of the others.
- **Same disorder within and across satellites:** same-satellite and any-satellite out-of-order lag are almost identical (F3).

Interpretation: the "jam di tiap satelit tidak selalu sepakat" behavior does not show in the practice profile. It may appear in the exam profile. The feed clock `C` (section 3) takes the newest acq unclamped, so a satellite stamping ahead would make other detections look later; S5 records this.

### F5. Payload shapes and targets

```text
detection  {"offset":N,"detection":{"version","scan","acq_date","acq_time","bright_ti4","bright_ti5",
                                    "confidence","daynight","frp","instrument","latitude","longitude",
                                    "satellite","track"}}
update     {"offset":N,"key":"N20|2026-11-28|1005|-1.86812|134.32357","changes":{<subset of bright_ti4, bright_ti5, frp, confidence>}}
retract    {"offset":N,"reason":"false_positive","key":"..."}
heartbeat  {"offset":N,"virtual_now":"..."}
```

- **How targets are named:** an update or retract names its target only by `key` = `satellite|acq_date|acq_time|latitude|longitude`.
  - The key matched a key built from an earlier detection's fields for 21,821 of 21,829 updates and 8,762 of 8,763 retracts (E2), and for 944 of 948 updates and 325 of 325 retracts (E3).
  - The unmatched ones are all the exponent-latitude case (F6).
  - No update or retract arrived before its detection. All arrived within 99 offsets after it.
- **Updates are deltas:** only the changed fields, never a full record.
  - **Fields changed (E2):** `frp` 12,107, `bright_ti5` 11,618, `bright_ti4` 11,609, `confidence` 7,827.
  - **Confidence moves both ways across low:** n→l 2,889, h→l 566, l→n 433, l→h 486.
  - **Effect:** an update can remove a detection from a fire or add one to it.
  - **Repeats:** up to 4 updates per key.
- **Retracts:** reason was always `false_positive`. 111 retracts repeat a key that was already retracted.
- **After a retract:** 304 updates and 345 re-sent detection copies arrived for keys that were already retracted (E2).

### F6. Raw text of the key fields

| Field | JSON type | Raw form observed |
|---|---|---|
| `latitude`, `longitude` | number | 5 decimals, e.g. `-9.43923`, `113.81356`. No value ended in `0` (405,271 detections). 63 latitudes with \|lat\| < 0.001 are in exponent form: `-6.0e-5`, `6.2e-4`, `-8.3e-4`. |
| `acq_time` | number | integer HHMM without padding: `0`, `25`, `100`, `2350` |
| `acq_date` | string | `"2026-11-28"` |
| `satellite` | string | `"N"`, `"N20"`, `"N21"` |
| `confidence` | string | `"l"`, `"n"`, `"h"` |
| `frp` | number | `1.02`, `327.58` |

- **The feed's own keys:** update and retract keys write `acq_time` the same unpadded way (`N21|2026-11-28|25|-9.43258|113.81175`). They write coordinates as plain decimals with 5 digits: `-0.00006`, never `-6.0e-5`.
- **Consequence:** concatenating the raw JSON number text does **not** always reproduce the feed's key.
- **Two rules that match every observed key:**
  - (a) expand exponent notation to a plain decimal and drop trailing fraction zeros;
  - (b) format with exactly 5 decimals.

  They differ only for a value ending in `0`, which never occurred. The spec's Lampiran A example (`-2.51230`) points to (b) (open question 1).

### F7. Bursts

- **Live (E3):** 9 high-rate bursts in 40 minutes. Each is ~1,026 to 1,028 events (939 to 969 detections) over 11 s, flat at 100 to 102 events/s.
  - **Spacing:** 113 to 373 s apart (median about 300 s), which is 2 to 6 feed hours.
  - **Between bursts:** 1 to 9 events/s.
- **Reading the flat top:** it suggests the server caps live emission at about 100 events/s rather than sending a pass all at once.
- **In feed time:** a pass is not a clean block. By acq time, one satellite's detections form runs with gaps of 21 to 39 feed minutes, and each run spans hours.
- **Replay (E2):** one continuous stream at 2,000 to 8,000 events/s.

### F8. Reconnect semantics

| Request | First id returned | Meaning |
|---|---|---|
| `?from=65183` (E2, and 41 resumes in E2+E3) | 65183 | `from` is inclusive: resuming with the last seen id repeats it once |
| `Last-Event-ID: 464837`, no `from` (E6b) | 464838 | the header is exclusive: standard SSE resume |
| `?from=464737` and `Last-Event-ID: 464837` (E6a) | 464737 | `from` wins |
| `?from=latest_offset` (E3) | `latest_offset` | `latest_offset` is the last emitted id |
| `?from=<latest+~10,000>` (E1) | none | 200 `text/event-stream`, then silence (no heartbeat in 5 s) |

- **The server ends the stream itself:**
  - live: every 9 to 90 s (p50 65 s, 36 connections);
  - replay: every 31 to 65 s;
  - it is a clean EOF, not an error.
- **No gaps:** across 41 resumes (3 in E2, 38 in E3, including one forced by the probe at 15 min), the offset sequence had no gap.
- **Repeated ids come only from our resume:** the probe resumed with `?from=<last id>`, so each resume repeated exactly one id: 41 in total (3 in E2, 38 in E3). Apart from those, the server never sent an offset twice (0 in E2 and E3). The analyzer counts the two separately.
- **No `retry:` field** was sent in about 453,000 events.
- **Transport:** HTTP/2 through Cloudflare. Stream responses carry `Cache-Control: no-cache`; errors carry `no-store`.
- **Errors match Lampiran A:**
  - `401 {"error":{"code":"unauthorized","message":"token tidak valid"}}`;
  - `400 {"error":{"code":"bad_request","message":"parameter from tidak valid"}}` for `from=abc` and `from=-1`.

### F9. Connection limit

- **E4:** a second stream opened while E3 was connected got `429 {"error":{"code":"too_many_connections","message":"koneksi aktif lain ditutup; sambung ulang"}}` with no `Retry-After` header.
  - **The first stream was evicted:** E3 received EOF 2 ms later, matching the message ("the other active connection was closed").
  - **Recovery:** E3 reconnected 0.64 s later with 200.
- **E5:** a connected probe was force-killed. The next attempt 0.94 s later was accepted with 200, with no 429.
  - **Limit of this test:** on Windows the OS closes the killed process's socket. A peer that vanishes without any TCP close (cable pulled) was not tested.

Interpretation: there is one stream slot per group. A new connect evicts the current holder and is refused itself. Two readers reconnecting in a loop keep evicting each other.

### F10. Slow reader

- **E3 stopped reading at 30 min for 60 s, with the socket left open.**
  - Just before the pause the stream was at offset 460,222.
  - When reading resumed, 499 buffered events (460,223 to 460,722) arrived within 9 ms, then EOF.
  - `/v1/session` at the reconnect showed `latest_offset` 460,982, so the server had stopped sending to the paused reader after a bounded backlog and closed it.
- **Resume:** `?from=460722` continued without a gap and delivered the remaining ~260 events in the first second.
- **Unknown layer:** some of the buffering may be Cloudflare's or HTTP/2 flow control. Which layer closed the stream is not visible from the client.

Interpretation: a reader that stops reading loses its connection, not data. Stopping reading when the local pile is full is safe, as long as the client resumes from its own durable offset.

### F11. Session

- **Constant settings:** all 56 `/v1/session` snapshots logged by E0, E2 and E3 (about 70 wall minutes) showed `profile: "practice"` and `speed: 60`.
- **Clock rate:** `virtual_now` advanced 60.0 feed seconds per wall second (median of consecutive snapshot pairs; single pairs vary 39 to 92 because polls are a few seconds apart and the request time is not subtracted).
- **Offset growth:** `latest_offset` grew 0 to 25 per second between polls (median 4.2 during E3) and equals the last emitted id (F8).

### F12. Data quality

| Measure | E2 replay | E3 live |
|---|---|---|
| detections / updates / retracts / heartbeats | 405,268 / 21,829 / 8,763 / 616 | 15,703 / 948 / 325 / 37 |
| confidence n / h / l | 76% / 12% / 12% | 76% / 12% / 12% |
| satellites N / N20 / N21 | 33% each | 33% each |
| ids repeated by our resume (`?from=<last id>`, F8) | 3 | 38 |
| ids the server sent twice | 0 | 0 |
| detections that repeat an earlier key at a new offset | 26,466 (6.5%) | 1,039 (6.6%) |
| malformed JSON, unknown SSE fields, comment lines | none | none |

- **Key re-sends are the server's, not our resume.** They are counted over unique offsets, so the resume repeats above are not in them: each one is a new offset carrying a key already seen.
- **Repeats are exact copies.** 26,465 of the 26,466 have an identical body. They arrive at most 144 offsets after the first copy.
- **One key collision:** offsets 210,076 and 210,220 share the key `N21|2026-12-16|508|-3.01158|114.22781` but have different `scan`, `bright_ti4`, `bright_ti5`, `frp` and `track` (open question 2).
- **Coordinate range:** latitude −11.0 to 6.0, longitude 95.0 to 141.0 (Indonesia).

## 3. Impact on the P4 issues

| Issue | What it gets from here |
|---|---|
| #309 SSE client | resume rule, reconnect backoff, silence timeout, pile and stop reading |
| #311 stable id | detection key text |
| #312 time windows and lateness | allowed lateness L, feed clock, closing rule |
| #313 durable state and recovery | re-reading the feed is viable, time to recover |
| #314 update and retract | target matching, tombstones, duplicates |
| #315 test at 60× | throughput target, lag metrics |
| #283 lease | only the lease holder may hold the stream |

- **#312, allowed lateness L = 12 h** (configurable).
  - **The numbers:**
    - out of order by offset: p99.9 is 3 h 54 min to 3 h 55 min, max 5 h 1 min;
    - feed-clock delay: p99.9 is 3 h 59 min, max 4 h 49 min.
  - **Choice:** 12 h, more than twice the largest value seen. The practice data produces no late events at this limit; the late count is still reported (criterion 2). The cost is that fires close later and stay in memory longer.
  - **Feed clock `C`:** `max(last heartbeat virtual_now, newest acq seen)`. Only values from the offset-ordered stream are used, so a re-read makes the same decisions. The wall clock and `/v1/session` are not replay-safe.
  - **Lateness per event:** a detection is judged by its own acq; an update or retract by its target detection's acq. It is accepted only if `C − acq ≤ L`; otherwise it is parked in `failed_messages` and counted.
  - **Measured on the #308 captures** (`C − target acq` at arrival):

    | Event type | p50 | p99 | max | over 12 h |
    |---|---|---|---|---|
    | detections, E2 replay (405,268) | 0.08 h | 3.37 h | 5.02 h | 0 |
    | updates, E2 (21,829) | 0.15 h | 3.47 h | 4.72 h | 0 |
    | retracts, E2 (8,763) | 0.15 h | 3.45 h | 5.12 h | 0 |

    All 30,592 update and retract keys in E2 matched a detection key built with 5-decimal coordinates.
  - **Closing ("selesai dikenali"):** a fire is final when `C > its newest detection acq + 360 min + L`, and is then dropped from memory. Nothing accepted later can change it: a new detection would be more than 360 min from every dot of the fire, and an update or retract for one of its dots would fail the lateness check.
- **#313, recovery by re-reading the feed: viable.**
  - **Retention:** the full session history is kept (F1).
  - **Rule:** on every start, replay from offset 0. Up to the saved bookmark, rebuild memory silently (no saves, no publishes, no parking; counters are rebuilt), then continue normally. A smaller start offset is not computed: it could rename a fire whose anchor was retracted, re-create part of a closed fire, or misjudge lateness.
  - **Time:** the full 436k-event history replayed in 169 s (about 2,600 events/s).
  - **Requirement:** the reader must handle the server's periodic EOF during replay.
- **#309 and #315, throughput and pile.**
  - **Live load:** design for bursts of ~1,000 events at up to ~100 events/s (F7).
  - **Criterion 4 (current within 60 s after a burst):** needs only about 17 events/s even if processing starts when the burst ends.
  - **Target:** ≥ 100 events/s sustained end to end. Catch-up after downtime faces replay at 2,000 to 8,000 events/s, so a bounded pile plus "stop reading when full" is the right shape: the server tolerates a stalled reader by closing it (F10).
  - **Pile size:** at least one burst, e.g. 2,048 events.
  - **Lag metrics for the dashboard:**
    - offset lag = `latest_offset` (from `/v1/session`) − last committed offset;
    - feed-time lag = `C` at the reader − `C` at the last saved batch.
- **#309, reconnect and silence.**
  - **Normal path:** reconnecting is routine, since the server closes every stream within 90 s (F8). On EOF, reconnect after a short backoff (0.5 s, doubling to 10 s).
  - **Resume rule:** the first connect of a run uses `?from=0`; every reconnect sends only `Last-Event-ID: <last seen>` and never `?from`. The header is exclusive, so a resume has no repeat (E6b). `?from` is still needed once, because the header cannot say "from 0".
  - **Stay idempotent anyway:** a crash between saving and moving the bookmark re-processes that batch, and versioned saves make the repeat a no-op.
  - **Silence timeout:** 3 × the largest heartbeat interval seen, 3 × 21.8 s ≈ **65 s** at speed 60. It should follow the heartbeat interval if speed changes (open question 4).
  - **Slot release:** under 1 s after a client dies (F9), so restarts do not need to wait.
  - **On 429:** back off with jitter. There is no `Retry-After` to honour.
  - **`retry:` field:** never sent, so the client cannot rely on it.
- **#283, single reader: required, not just preferred.** A second connection evicts the first and gets 429 (F9). Two replicas that both read would keep knocking each other off and neither would hold a stable stream, so only the lease holder may open the stream.
- **#314, update and retract.**
  - **Matching:** match by the 5-field key only. An update is a delta to merge into the stored detection.
  - **Updates can change fire membership:** an update that sets `confidence` to `l` removes the detection from its fire, which can split the fire; one that raises it from `l` adds it.
  - **Retract:** it must remove the detection and can split a fire, as decided.
  - **Tombstones are needed:** in the practice data, 345 re-sent detection copies and 304 updates arrived after their key was retracted. A tombstone must win over a later copy of the same key, or the false positive comes back.
  - **Duplicates:** about 6.5% of detections are exact re-sends at a new offset, so ingest must be idempotent by key, not only by offset.
  - **Unknown targets:** updates and retracts never came before their detection here. One whose target is not in memory (never seen, or in a closed fire) is parked as `unknown_or_closed_target` and never creates a detection (S4).
- **#311 and #314, detection key (F6).** Build it from the raw text, but write coordinates in plain decimal form (no exponent), so detection keys equal the keys in update and retract events. Until the assistants answer question 1, use the feed's own `key` string wherever an event carries one.

## 4. Assumptions

These replace questions the assistants have not answered yet. Each is checked against the reference result (#316).

| # | Assumption | If wrong |
|---|---|---|
| S1 | The detection key is written exactly like the feed's `key` field (coordinates with 5 decimals, no exponent, `acq_time` unpadded). | `detection_keys` and the checksum differ from the reference. |
| S2 | The same key with different content replaces the detection; an identical copy is a re-send and is skipped. | Very small FRP or confidence differences. |
| S3 | A retract is final; later re-sends and updates for that key are counted and ignored. | Some fires are smaller than the reference. |
| S4 | An update or retract for a key not in memory is parked and never creates a detection. | That detection is missing from its fire. |
| S5 | Lateness is measured by the feed clock with L = 12 h, exam-profile delays stay under 12 h, and no satellite stamps its acq ahead of the feed clock. | Late detections are parked; fires are smaller than the reference. |
| S6 | An exam session keeps its full history from offset 0, and heartbeats stay in the offset sequence. | Crash recovery by replay fails. |
| S7 | The server closing streams every 9 to 90 s is by design. | Nothing; the reader reconnects anyway. |
| S8 | Distance is haversine with R = 6371.0 km; `≤ 1.0 km` and `≤ 360 min` are inclusive. | Boundary pairs differ. |
| S9 | Withdrawn fires (fewer than 2 detections, or merged) are left out of the audit `count` and checksum. | Our count is lower than the reference. |
| S10 | Confidence `l` is low and excluded from fires; `n` and `h` are included. | Fire membership differs. |

## 5. Open questions for the assistants

1. **Coordinate text in keys:**
   - What is the canonical text of `latitude`/`longitude` in a detection key?
   - The JSON sends `-6.0e-5` while update and retract keys say `-0.00006`. Should keys use exactly 5 decimals (like `-2.51230` in Lampiran A) or the shortest plain decimal?
2. **Key collision:**
   - Offsets 210,076 and 210,220 carry the same 5-field key with different measurements.
   - Is "the same detection received again counts once, first one wins" the intended reading?
3. **Eviction:**
   - A second stream evicts the active one and itself gets 429. Is the limit exactly one stream per group, and is the eviction intended?
4. **Heartbeat interval:**
   - Is it defined in feed time (15 min) or wall time (15 s)? At a speed other than 60 the two differ, and the silence timeout depends on it.
5. **Heartbeat gaps in history:**
   - The replayed history has spans of up to 31 feed hours without heartbeats. Are heartbeats only written while a client is connected?
6. **Periodic EOF:**
   - The server closes each stream after 9 to 90 s. Is that deliberate, and does the exam profile do the same?
7. **Rate cap and 60×:**
   - Live emission looks capped at about 100 events/s.
   - What does "percepatan 60×" in criterion 4 mean, when the practice session already runs at speed 60 all the time?
8. **Updates after a retract:**
   - 304 updates target already retracted keys. Should an update ever revive a retracted detection? (We assume not.)
9. **Exam retention:**
   - Will an exam session keep its full history from offset 0 for replay, as the practice session does?
10. **Offsets beyond `latest_offset`:**
    - `?from` beyond `latest_offset` returns 200 and then silence, with no heartbeat for at least 5 s. Should a client expect heartbeats while it waits there?

## 6. How to reproduce

From the repo root, with the token in `.env` (never printed):

```bash
export SATELLITE_GROUP_TOKEN=$(grep '^SATELLITE_GROUP_TOKEN=' .env | cut -d= -f2-)
go build -o tools/feedprobe/out/feedprobe ./tools/feedprobe
P=tools/feedprobe/out/feedprobe; O=tools/feedprobe/out

# E0 session clock
$P session --poll 5s --for 60s > $O/e0-session.ndjson
# E1 bad inputs (one attempt each)
$P capture --once --duration 5s --bad-token --from 0 --out $O/e1-badtoken.ndjson
$P capture --once --duration 5s --from abc --out $O/e1-from-abc.ndjson
$P capture --once --duration 5s --from -1 --out $O/e1-from-neg.ndjson
$P capture --once --duration 5s --from <latest+10000> --out $O/e1-from-future.ndjson
# E2 replay from 0 until live (+30 s)
$P capture --from 0 --duration 12m --live-tail 30s --out $O/e2-replay.ndjson
# E3 live with a forced disconnect at 15 min and a 60 s read pause at 30 min
$P capture --from latest --duration 40m --disconnect-at 15m --pause-at 30m --pause-for 60s --out $O/e3-live.ndjson
# E4 one extra connection while E3 runs (around minute 22), closed at once
$P capture --once --duration 2s --from latest --out $O/e4-second.ndjson
# E5 after E3: start, kill -9 / taskkill /F after 5 s, then retry every 1 s
$P capture --from latest --duration 60s --out $O/e5-killed.ndjson   # killed
$P capture --from latest --retry-fixed 1s --duration 60s --live-tail 2s --out $O/e5-retry.ndjson
# E6 from vs Last-Event-ID (X = latest-200, Y = latest-100)
$P capture --once --duration 4s --from X --last-event-id Y --out $O/e6a-both.ndjson
$P capture --once --duration 4s --last-event-id Y --out $O/e6b-header.ndjson

# Analysis (Markdown tables for F1-F12)
$P analyze --in $O/e2-replay.ndjson --session $O/e0-session.ndjson
$P analyze --in $O/e3-live.ndjson --session $O/e0-session.ndjson
```

Each capture writes one NDJSON line per SSE event (wall time, monotonic time, connection number, `sse_id`, `event`, `retry`, the exact `data` text) and a `<name>.conn.ndjson` log of connects, statuses, response headers (never request headers), error bodies, waits and disconnect reasons. The tool lives in [`tools/feedprobe/`](../tools/feedprobe/) and uses only the standard library.

## 7. Samples

[`p4_feed_samples/`](p4_feed_samples/) holds 50 events per type from the E2 replay: `detection.ndjson`, `update.ndjson`, `retract.ndjson` and `heartbeat.ndjson`. Each line is `{"id","event","data"}`, with `data` copied byte for byte from the stream. The detection file ends with five of the exponent-latitude cases from F6. The samples contain no token or request headers.
