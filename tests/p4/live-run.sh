#!/usr/bin/env bash
# P4 (#315, criterion 4): live run of the satellite pipeline on Compose against the real feed.
# Samples the aggregator's /metrics, waits for the first catch-up, then measures for DURATION and checks, per burst,
# that every event of the burst is saved within RECOVER_MAX seconds. Also reports p50 and p95 from read to saved,
# and checks that every offset was saved exactly once (records timed = offsets the bookmark moved over).
#
# Usage:  tests/p4/live-run.sh
#   DURATION (1500)     seconds measured after the warm-up; bursts come every 3 to 6 minutes (doc/P4_FEED_NOTES.md F7)
#   WARMUP_MAX (1800)   seconds to wait for the first catch-up; a first start with no bookmark saves the whole history
#   LAG_OK (50)         offset lag that counts as caught up
#   INTERVAL (2)        seconds between samples
#   BURST_RATE (40)     growth of latest_offset in events per second that counts as a burst (1 to 9 between bursts, ~100 in one)
#   BURST_MIN (300)     events a burst must have to count
#   RECOVER_MAX (60)    seconds a burst may take to be saved (criterion 4)
#   OUT_DIR (tests/p4/results)
# Needs the Compose stack up with SATELIT_ENABLED=true and an aggregator image that has the #315 metrics.
# Only one stream per group token: a run evicts any teammate streaming with it, so book a time slot first.
# It never reads .env or the token, only the aggregator's /metrics and its satellite log lines.
# Exit codes: 0 every check passed, 1 a check failed (results still written), 2 bad setup.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

DURATION="${DURATION:-1500}"
WARMUP_MAX="${WARMUP_MAX:-1800}"
LAG_OK="${LAG_OK:-50}"
INTERVAL="${INTERVAL:-2}"
BURST_RATE="${BURST_RATE:-40}"
BURST_MIN="${BURST_MIN:-300}"
RECOVER_MAX="${RECOVER_MAX:-60}"
OUT_DIR="${OUT_DIR:-$HERE/results}"

die() { echo "error: $*" >&2; exit 2; }
for v in DURATION WARMUP_MAX LAG_OK INTERVAL BURST_RATE BURST_MIN RECOVER_MAX; do
  [[ "${!v}" =~ ^[1-9][0-9]*$ ]] || die "$v must be a positive integer, got '${!v}'"
done
for t in docker awk date; do command -v "$t" >/dev/null || die "$t is required"; done

cd "$REPO" || die "cannot enter $REPO"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN="$OUT_DIR/live-$STAMP"
mkdir -p "$RUN" || die "cannot create $RUN"
CSV="$RUN/samples.csv"
SUMMARY="$RUN/summary.txt"
SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

scrape() { docker compose exec -T aggregator wget -qO- http://localhost:8091/metrics 2>/dev/null; }
value() { awk -v n="$1" '$1 == n { print $2; exit }' <<<"$2"; }
count() { awk -v k="$1" 'index($1, "aggregator_satelit_engine_count{kind=\"" k "\"}") == 1 { print $2; exit }' <<<"$2"; }
buckets() { grep '^aggregator_satelit_read_to_saved_seconds_bucket' <<<"$1"; }

TEXT="$(scrape)" || die "cannot read the aggregator's /metrics (is the Compose stack up?)"
grep -q '^aggregator_satelit_read_to_saved_seconds_count ' <<<"$TEXT" ||
  die "no satellite metrics: set SATELIT_ENABLED=true and rebuild the aggregator (docker compose up -d --build aggregator)"

echo "t_ms,phase,latest_offset,saved_offset,offset_lag,feed_time_lag_s,pile,saved_records,too_late,singles,parked" >"$CSV"

# sample PHASE: one scrape, one CSV row. Leaves the scrape in TEXT.
sample() {
  TEXT="$(scrape)" || { echo "warning: scrape failed" >&2; return 1; }
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$(date +%s%3N)" "$1" \
    "$(value aggregator_satelit_latest_offset "$TEXT")" "$(value aggregator_satelit_saved_offset "$TEXT")" \
    "$(value aggregator_satelit_offset_lag "$TEXT")" "$(value aggregator_satelit_feed_time_lag_seconds "$TEXT")" \
    "$(value aggregator_satelit_pile "$TEXT")" "$(value aggregator_satelit_read_to_saved_seconds_count "$TEXT")" \
    "$(count too_late "$TEXT")" "$(count singles "$TEXT")" "$(value aggregator_satelit_parked_total "$TEXT")" >>"$CSV"
}

# The last "satelit pipeline stopped" log line, when stopping is the pipeline's latest start or stop (for example a 401 on the token).
pipeline_stopped() {
  docker compose logs --no-log-prefix aggregator 2>/dev/null |
    grep -E '"message":"satelit pipeline (started|stopped)"' | tail -n 1 | grep '"message":"satelit pipeline stopped"'
}

echo "warm-up: waiting until the offset lag is at most $LAG_OK (up to ${WARMUP_MAX}s)"
deadline=$((SECONDS + WARMUP_MAX))
caught_up=0
idle=0
while [ "$SECONDS" -lt "$deadline" ]; do
  if sample warmup; then
    lag="$(value aggregator_satelit_offset_lag "$TEXT")"
    if [ -n "$lag" ] && [ "$lag" -le "$LAG_OK" ]; then caught_up=1; break; fi
    if [ -z "$(value aggregator_satelit_saved_offset "$TEXT")" ]; then idle=$((idle + 1)); else idle=0; fi
    if [ "$idle" -ge 5 ] && stopped_line="$(pipeline_stopped)"; then
      echo
      die "the satellite pipeline is not running; fix the cause and restart the aggregator. Last log line: $stopped_line"
    fi
    printf '\r  offset lag %s, saved offset %s   ' "${lag:-unknown}" "$(value aggregator_satelit_saved_offset "$TEXT")"
  fi
  sleep "$INTERVAL"
done
echo
[ "$caught_up" = 1 ] || { echo "warm-up did not catch up within ${WARMUP_MAX}s, see $CSV" >&2; exit 1; }
REBUILD="$(value aggregator_satelit_rebuild_seconds "$TEXT")"

echo "measuring for ${DURATION}s"
buckets "$TEXT" >"$RUN/buckets-start.prom"
START_TEXT="$TEXT"
end=$((SECONDS + DURATION))
while [ "$SECONDS" -lt "$end" ]; do
  sample measure
  sleep "$INTERVAL"
done
sample measure || die "last scrape failed"
buckets "$TEXT" >"$RUN/buckets-end.prom"

docker compose logs --no-log-prefix --since "$SINCE" aggregator 2>/dev/null | grep '"message":"satelit' >"$RUN/aggregator-satelit.log"
LOG="$RUN/aggregator-satelit.log"
logcount() { grep -c "$1" "$LOG"; }

{
  echo "P4 live run $STAMP (#315, criterion 4)"
  echo "settings: DURATION=$DURATION INTERVAL=$INTERVAL BURST_RATE=$BURST_RATE BURST_MIN=$BURST_MIN RECOVER_MAX=$RECOVER_MAX"
  echo "rebuild at start: ${REBUILD:-?}s"
  echo
} >"$SUMMARY"

# Bursts: change points of latest_offset growing at BURST_RATE or more, merged when under 15 s apart.
# latest_offset comes from /v1/session polled every 5 s, so a burst's end is known to about 5 s.
awk -F, -v rate="$BURST_RATE" -v bmin="$BURST_MIN" -v rmax="$RECOVER_MAX" '
  $2 == "measure" && $3 != "" && $4 != "" { n++; T[n] = $1 / 1000; L[n] = $3; S[n] = $4 }
  END {
    t0 = T[1]; prevL = -1; nb = 0
    for (i = 1; i <= n; i++) {
      if (L[i] == prevL) continue
      if (prevL >= 0 && (L[i] - prevL) / (T[i] - prevT) >= rate) {
        if (nb == 0 || T[i] - endT[nb] > 15) { nb++; from[nb] = prevL }
        endT[nb] = T[i]; endI[nb] = i; target[nb] = L[i]
      }
      prevL = L[i]; prevT = T[i]
    }
    counted = 0; failed = 0
    for (b = 1; b <= nb; b++) {
      events = target[b] - from[b]
      if (events < bmin) { printf "small rise %d: %d events, not counted\n", b, events; continue }
      counted++
      rec = -1
      for (j = endI[b]; j <= n; j++) if (S[j] >= target[b]) { rec = T[j] - endT[b]; break }
      if (rec < 0) { failed++; printf "burst %d: at least %d events up to offset %d, ended at +%.0fs, NOT saved before the run ended: FAIL\n", counted, events, target[b], endT[b] - t0; continue }
      ok = rec <= rmax ? "PASS" : "FAIL"; if (ok == "FAIL") failed++
      printf "burst %d: at least %d events up to offset %d, ended at +%.0fs, all saved %.1fs later: %s\n", counted, events, target[b], endT[b] - t0, rec, ok
    }
    printf "bursts counted: %d (need at least 3), over %ds: %s\n", counted, rmax, failed == 0 && counted >= 3 ? "PASS" : "FAIL"
  }' "$CSV" >>"$SUMMARY"

# p50 and p95 over the measured window, interpolated inside a bucket the way histogram_quantile does.
quantile() {
  awk -v q="$1" '
    { match($1, /le="[^"]*"/); le = substr($1, RSTART + 4, RLENGTH - 5) }
    FNR == NR { start[le] = $2; next }
    { les[++k] = le; d[k] = $2 - start[le] }
    END {
      total = d[k]; if (total <= 0) { print "no records"; exit }
      rank = q * total; prevLe = 0; prevC = 0
      for (i = 1; i <= k; i++) {
        if (d[i] >= rank) {
          if (les[i] == "+Inf") { printf "above %ss", prevLe; exit }
          printf "%.3fs", prevLe + (les[i] - prevLe) * (rank - prevC) / (d[i] - prevC); exit
        }
        prevLe = les[i]; prevC = d[i]
      }
    }' "$RUN/buckets-start.prom" "$RUN/buckets-end.prom"
}

saved0="$(value aggregator_satelit_saved_offset "$START_TEXT")"
saved1="$(value aggregator_satelit_saved_offset "$TEXT")"
timed0="$(value aggregator_satelit_read_to_saved_seconds_count "$START_TEXT")"
timed1="$(value aggregator_satelit_read_to_saved_seconds_count "$TEXT")"
moved=$((saved1 - saved0))
timed=$((timed1 - timed0))
once="FAIL"; [ "$moved" -eq "$timed" ] && [ "$moved" -gt 0 ] && once="PASS"
stopped="$(logcount '"message":"satelit pipeline stopped"')"

{
  echo
  echo "read to saved over the window: p50 $(quantile 0.5), p95 $(quantile 0.95), $timed records"
  echo "offsets saved exactly once: bookmark moved over $moved offsets, $timed records timed: $once"
  echo "stream connects: $(logcount '"message":"satelit stream connected"') (the server closes every stream within about 90 s, so reconnects are normal)"
  echo "gaps detected and resumed: $(logcount 'broke the offset sequence')"
  echo "repeated offsets skipped: $(logcount '"message":"satelit repeated offset skipped"')"
  echo "batch save retries: $(logcount '"message":"satelit batch not saved"')"
  echo "pipeline stopped with an error: $stopped"
  echo "engine counts at the end: too_late $(count too_late "$TEXT"), unknown_or_closed_target $(count unknown_or_closed_target "$TEXT"), resent $(count resent "$TEXT"), tombstone_rejected $(count tombstone_rejected "$TEXT"), singles $(count singles "$TEXT")"
} >>"$SUMMARY"

cat "$SUMMARY"
echo
echo "results in $RUN"

grep -q 'need at least 3), over [0-9]*s: PASS' "$SUMMARY" && [ "$once" = PASS ] && [ "$stopped" = 0 ]
