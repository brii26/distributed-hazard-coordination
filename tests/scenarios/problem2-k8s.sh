#!/usr/bin/env bash
# Problem 2 on the cluster (#287): kill-leader, clock-skew and VM freeze against the source leases.
#
#   tests/scenarios/problem2-k8s.sh                          kill-leader and clock-skew, freeze is skipped unless configured
#   tests/scenarios/problem2-k8s.sh kill-leader              one scenario (kill-leader, clock-skew or freeze)
#   DRY_RUN=1 tests/scenarios/problem2-k8s.sh                print the plan and every command, touches no cluster
#
# Needs: kubectl (context of the cluster), curl, jq, awk. The Audit token is read from the Secret audit-token.
#
# kill-leader and clock-skew: `kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <name>` only submits.
# The script waits for END_PATTERN in the agent logs, like gameday.sh, then SETTLE more seconds. The lease table is sampled
# every second through psql in coordination-db-0 for the whole run.
#
# freeze: the Tester Agent has no freeze scenario (doc/TESTER_AGENT_NOTES.md lists clock-skew, kill-leader, kill-store, partition,
# schema and wipe-node), so the VM must be paused from the dev host. Set both commands, NODE is the name of FREEZE_NODE:
#   FREEZE_NODE=node-b NODE_FREEZE_CMD='VBoxManage controlvm "$NODE" pause' NODE_THAW_CMD='VBoxManage controlvm "$NODE" resume'
# FREEZE_NODE should hold the leader of a source and must not be the control plane or the node of coordination-db-0 (the script
# refuses the latter). Without the commands the freeze check prints SKIP, and REQUIRE_FREEZE=1 turns the skip into a FAIL.
# The thaw command also runs in the exit trap if the script stops while the node is frozen.
# Pausing one node only takes the leaders that live on it. The lease row holds a random holder id that each Aggregator pod prints
# in its "lease acquired" log line, so before the freeze the script maps holder to pod to node (kubectl logs, kubectl get pods).
# Only the sources whose holder is on FREEZE_NODE must change hands, for the others it prints INFO.
#
# Per source (BMKG, PVMBG, SENSOR_PANTAI) it prints PASS or FAIL for: a new holder with a higher epoch (kill-leader and freeze),
# the takeover time, the longest time without a valid holder against LEASE_TTL + GAP_MARGIN, an epoch that never goes back,
# and in the Audit API: no repeated source_ref_id, no event lost (every id seen before the scenario is still there) and the
# Audit checksum equal to an independent sha256 over the paged ids (the definition of audit-checksum.sh). BMKG and PVMBG ids of
# the mocks end in a sequence number (BMKG-<start>-000012), so for them there is also no hole in the sequence of the newest mock run.
# SENSOR_PANTAI ids carry no sequence, so it only gets the loss and checksum checks. The tail of the sequence cannot be checked,
# an event newer than the newest stored one is not known to the script.
# Freeze, criterion 5: the Audit API items and the store rows carry no writer epoch or holder (write_fence only keeps max_token
# per source on each store node), so what the woken instance stored is read from its own log. After the thaw its "ingest" lines
# must show changed=0 for the source it held, until its own next "lease acquired" (a new epoch, legitimate writes).
# Criterion 4 (clock-skew): no two instances storing for the same period. A period is one lease epoch. If the epoch of a source
# did not move, only one pod may show "ingest" lines with changed>0 for it since the start, else FAIL. If the epoch moved, the
# logs cannot tell a takeover from two writers, so it prints INFO and does not claim the criterion.
# If psql fails the sampler leaves empty rounds, so every source needs a lease row in MIN_SAMPLE_PCT (80) percent of the rounds.
# fenced_writes_total is read from every Aggregator pod through the API server proxy before and after and printed as INFO only.
# The counter lives in the process, so a killed pod loses it: new pods count from zero.
# Exit code 1 when any check is FAIL, 2 when the cluster is not ready.
set -uo pipefail

DRY_RUN=${DRY_RUN:-0}
NS=${NS:-sistem-informasi-kebencanaan-bnpb}
TESTER_NS=${TESTER_NS:-tester-agent}
DB_POD=${DB_POD:-coordination-db-0}
DB_NAME=${DB_NAME:-coordination}
SOURCES=${SOURCES:-BMKG PVMBG SENSOR_PANTAI}
LEASE_TTL=${LEASE_TTL:-15}
GAP_MARGIN=${GAP_MARGIN:-5}                         # design budget: expiry 15 s, next try within 3 s, acquire within 2 s
TAKEOVER_MAX=${TAKEOVER_MAX:-60}                    # seconds from the request, includes the ~5 s before the agent starts the steps
SCENARIO_TIMEOUT=${SCENARIO_TIMEOUT:-180}
SETTLE=${SETTLE:-45}                                # seconds of sampling after the end marker
POLL_INTERVAL=${POLL_INTERVAL:-2}
SAMPLE_INTERVAL=${SAMPLE_INTERVAL:-1}
END_PATTERN=${END_PATTERN:-pemulihan akhir selesai}
FAIL_PATTERN=${FAIL_PATTERN:-gagal|tidak ditemukan|\"level\":\"error\"}
FREEZE_NODE=${FREEZE_NODE:-}
NODE_FREEZE_CMD=${NODE_FREEZE_CMD:-}
NODE_THAW_CMD=${NODE_THAW_CMD:-}
FREEZE_HOLD=${FREEZE_HOLD:-$((LEASE_TTL + 15))}     # must be longer than LEASE_TTL
REQUIRE_FREEZE=${REQUIRE_FREEZE:-0}
AUDIT_PORT=${AUDIT_PORT:-38091}
MIN_SAMPLE_PCT=${MIN_SAMPLE_PCT:-80}                # share of sampler rounds that must return a row per source

GAP_MAX=$((LEASE_TTL + GAP_MARGIN))
PASSED=0 FAILED=0 SKIPPED=0
PIDS=()
FROZEN=0
FROZEN_SRC=" "                                    # sources whose holder is on FREEZE_NODE, set by run_freeze
AUDIT_PF=""
AUDIT_TOKEN=""
WORK=$(mktemp -d)

say() { printf '%s\n' "$*"; }
pass() { say "PASS: $*"; PASSED=$((PASSED + 1)); }
fail() { say "FAIL: $*"; FAILED=$((FAILED + 1)); }
skip() { say "SKIP: $*"; SKIPPED=$((SKIPPED + 1)); }

# x <cmd...>: the only way the script changes anything, so a dry run prints instead.
x() {
  if [ "$DRY_RUN" = 1 ]; then printf '[dry-run] %s\n' "$*"; return 0; fi
  "$@"
}

# nap <seconds>: sleep that a TERM can interrupt, so the exit trap runs at once.
nap() { sleep "$1" & wait $!; }

kc() { x kubectl -n "$NS" "$@"; }

cleanup() {
  trap - EXIT INT TERM
  local p
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  if [ "$FROZEN" = 1 ]; then
    say "trap: thaw $FREEZE_NODE"
    NODE=$FREEZE_NODE bash -c "$NODE_THAW_CMD" || say "trap: thaw failed, run it by hand: NODE=$FREEZE_NODE $NODE_THAW_CMD"
  fi
  rm -rf "$WORK"
}

# ---------- lease sampling ----------

# One row per source: source,epoch,holder,valid. valid is 1 when a holder exists and its lease has not expired in DB time.
lease_rows() {
  local in sql
  # shellcheck disable=SC2086 # SOURCES is a space separated list
  in=$(printf "'%s'," $SOURCES)
  sql="SELECT source, epoch, COALESCE(holder::text, '-'), (holder IS NOT NULL AND expires_at > clock_timestamp())::int FROM source_lease WHERE source IN (${in%,}) ORDER BY source"
  kc exec "$DB_POD" -- sh -c "psql -U \"\$POSTGRES_USER\" -d $DB_NAME -tAF, -c \"$sql\""
}

start_sampler() { # start_sampler <file>
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] sample lease_rows every ${SAMPLE_INTERVAL}s into $1 in the background"; return 0; fi
  (
    while :; do
      { printf '%s;' "$(date +%s)"; lease_rows 2>/dev/null | paste -sd';' -; } >>"$1"
      sleep "$SAMPLE_INTERVAL"
    done
  ) &
  SAMPLER=$!
  PIDS+=("$SAMPLER")
}

stop_sampler() { [ "$DRY_RUN" = 1 ] || kill "$SAMPLER" 2>/dev/null; }

# analyze <samples> <source> <base epoch> <base holder> <t0>: prints "back worst takeover epoch holder".
# back is the number of samples where the epoch dropped, worst the longest run without a valid holder in seconds,
# takeover the seconds from t0 to the first valid sample with a higher epoch and another holder (-1 when none).
analyze() {
  awk -F';' -v s="$2" -v be="$3" -v bh="$4" -v t0="$5" '
    { ts = $1 + 0
      for (i = 2; i <= NF; i++) {
        split($i, c, ",")
        if (c[1] != s) continue
        e = c[2] + 0
        if (e < last) back++
        last = e
        if (c[4] + 0 == 0) { if (!ingap) { gstart = ts; ingap = 1 } if (ts - gstart + 1 > worst) worst = ts - gstart + 1 }
        else {
          ingap = 0
          if (!tk && e > be && c[3] != bh) { tk = 1; take = ts - t0; ne = e; nh = c[3] }
        }
      } }
    END { printf "%d %d %d %d %s\n", back + 0, worst + 0, tk ? take : -1, ne + 0, nh == "" ? "-" : nh }' "$1"
}

# check_lease <scenario> <samples> <before rows file> <t0> <expect takeover 0|1|sel>, sel: only the sources in FROZEN_SRC (holder pod stopped or frozen)
check_lease() {
  local scen=$1 samples=$2 before=$3 t0=$4 takeover=$5 src be bh back worst take ne nh want attempted with
  attempted=$(wc -l <"$samples" | tr -d ' ')
  while IFS=, read -r src be bh _; do
    # A failing psql leaves an empty round, and no rows would read as a gap of 0 seconds on a cluster that did not answer.
    with=$(awk -F';' -v s="$src" '{ for (i = 2; i <= NF; i++) { split($i, c, ","); if (c[1] == s) { k++; break } } } END { print k + 0 }' "$samples")
    if [ "$attempted" -gt 0 ] && [ $((with * 100)) -ge $((attempted * MIN_SAMPLE_PCT)) ]; then
      pass "$scen $src the lease row was read in $with of $attempted samples, at least ${MIN_SAMPLE_PCT}%"
    else
      fail "$scen $src the lease row was read in only $with of $attempted samples, under ${MIN_SAMPLE_PCT}% (psql failing or the cluster unreachable), the other lease checks prove nothing"
    fi
    read -r back worst take ne nh < <(analyze "$samples" "$src" "$be" "$bh" "$t0")
    want=$takeover
    if [ "$takeover" = sel ]; then
      case $FROZEN_SRC in
        *" $src "*) want=1 ;;
        *) want=0; say "INFO: $scen $src holder ${bh:0:8} was not stopped or frozen by this scenario, no takeover expected" ;;
      esac
    fi
    if [ "$want" = 1 ]; then
      if [ "$take" -ge 0 ]; then
        pass "$scen $src has a new holder with a higher epoch (epoch $be to $ne, holder ${bh:0:8} to ${nh:0:8})"
        if [ "$take" -le "$TAKEOVER_MAX" ]; then pass "$scen $src takeover in ${take}s, at most ${TAKEOVER_MAX}s"
        else fail "$scen $src takeover in ${take}s, more than ${TAKEOVER_MAX}s"; fi
      else
        fail "$scen $src kept epoch $be and holder ${bh:0:8}, no new holder was seen"
      fi
    else
      say "INFO: $scen $src epoch $be to $(last_epoch "$samples" "$src"), a bump is allowed here"
    fi
    if [ "$worst" -le "$GAP_MAX" ]; then pass "$scen $src longest time without a valid holder ${worst}s, at most ${GAP_MAX}s (TTL ${LEASE_TTL}s + ${GAP_MARGIN}s)"
    else fail "$scen $src longest time without a valid holder ${worst}s, more than ${GAP_MAX}s"; fi
    if [ "$back" -eq 0 ]; then pass "$scen $src epoch never went back"; else fail "$scen $src epoch went back in $back samples"; fi
  done <"$before"
}

# check_writers <scenario> <since>: criterion 4 for clock-skew, as far as it is observable. Rows and Audit items have no writer field,
# so a period is one lease epoch and the writers are the Aggregator pods whose log shows an "ingest" line with changed>0 since
# the start. While the epoch of a source stays the same only one pod may have stored for it. When the epoch moved the logs cannot
# separate a takeover from two writers (the timestamps are of the skewed clock), so that case is INFO and not a pass.
check_writers() {
  local scen=$1 since=$2 src be bh le pod n unreadable stored log=$WORK/writers.log
  while IFS=, read -r src be bh _; do
    le=$(last_epoch "$SAMPLES" "$src")
    if [ "$le" != "$be" ]; then say "INFO: $scen $src epoch moved $be to $le, who stored in which epoch cannot be told from the logs, criterion 4 not checked for it"; continue; fi
    n=0 unreadable=0
    for pod in $(kc get pods -l role=aggregator -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
      if ! kc logs "$pod" --since-time="$since" --tail=-1 >"$log" 2>/dev/null; then unreadable=$((unreadable + 1)); continue; fi
      stored=$(jq -Rr --arg s "$src" 'fromjson? | select(.message == "ingest" and .source == $s) | (.changed // 0)' "$log" | awk '{ t += $1 } END { print t + 0 }')
      [ "$stored" -gt 0 ] && n=$((n + 1))
    done
    if [ "$n" -gt 1 ]; then fail "$scen $src $n different pods stored rows in the same epoch $be, two writers for one period"
    elif [ "$n" -eq 1 ]; then pass "$scen $src epoch stayed $be and only one pod stored rows ($unreadable pod logs unreadable)"
    else say "INFO: $scen $src no pod logged stored rows since the start ($unreadable pod logs unreadable), nothing to compare"; fi
  done <"$BEFORE"
}

last_epoch() { awk -F';' -v s="$2" '{ for (i = 2; i <= NF; i++) { split($i, c, ","); if (c[1] == s) e = c[2] } } END { print e + 0 }' "$1"; }

# ---------- fenced_writes_total ----------

# fenced_snapshot <file>: lines "pod source value" for every Aggregator pod.
fenced_snapshot() {
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] kubectl get --raw /api/v1/namespaces/$NS/pods/<aggregator pod>:8091/proxy/metrics, keep fenced_writes_total into $1"; return 0; fi
  local pod
  : >"$1"
  for pod in $(kc get pods -l role=aggregator -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    kubectl get --raw "/api/v1/namespaces/$NS/pods/$pod:8091/proxy/metrics" 2>/dev/null |
      awk -v p="$pod" -F'"' '/^fenced_writes_total\{/ { v = $3; sub(/.*} */, "", v); print p, $2, v }' >>"$1"
  done
}

# fenced_delta <before> <after> <source>: stale writes rejected since the first snapshot, pods that appeared later count from zero.
fenced_delta() {
  awk -v s="$3" 'NR == FNR { if ($2 == s) b[$1] = $3; next } $2 == s { d += $3 - ($1 in b ? b[$1] : 0) } END { print d + 0 }' "$1" "$2"
}

report_fenced() { # report_fenced <scenario> <before> <after>: INFO only, a healthy cluster may have nothing in flight
  local scen=$1 src d pods
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] compare fenced_writes_total before and after per source"; return 0; fi
  pods=$(awk '{print $1}' "$3" | sort -u | wc -l | tr -d ' ')
  if [ "$pods" -eq 0 ]; then say "INFO: $scen fenced_writes_total could not be read from any Aggregator pod"; return; fi
  for src in $SOURCES; do
    d=$(fenced_delta "$2" "$3" "$src")
    say "INFO: $scen $src fenced_writes_total +$d across $pods pods"
  done
}

# ---------- Audit API ----------

audit_open() { # starts the port-forward to one Aggregator pod and reads the token, returns 1 when the API does not answer
  local i
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] kubectl -n $NS port-forward deployment/aggregator $AUDIT_PORT:8091"; return 0; fi
  AUDIT_TOKEN=$(kc get secret audit-token -o jsonpath='{.data.token}' | base64 -d)
  [ -n "$AUDIT_TOKEN" ] || return 1
  kubectl -n "$NS" port-forward deployment/aggregator "$AUDIT_PORT:8091" >"$WORK/pf-audit.log" 2>&1 &
  AUDIT_PF=$!
  PIDS+=("$AUDIT_PF")
  for ((i = 0; i < 30; i++)); do curl -sf -m 2 "http://127.0.0.1:$AUDIT_PORT/health" >/dev/null 2>&1 && return 0; sleep 1; done
  audit_close
  return 1
}

audit_close() { [ -z "$AUDIT_PF" ] || { kill "$AUDIT_PF" 2>/dev/null; wait "$AUDIT_PF" 2>/dev/null; AUDIT_PF=""; }; }

audit_get() { curl -sf -H "Authorization: Bearer $AUDIT_TOKEN" -G "http://127.0.0.1:$AUDIT_PORT$1" "${@:2}"; }

# audit_ids <source> <file>: every source_ref_id the events endpoint returns, paged by cursor like audit-checksum.sh.
audit_ids() {
  local cursor="" page
  : >"$2"
  while :; do
    page=$(audit_get /internal/audit/events --data-urlencode "source=$1" --data-urlencode "limit=1000" --data-urlencode "cursor=$cursor") || return 1
    jq -r '.items[].source_ref_id' <<<"$page" >>"$2" || return 1
    cursor=$(jq -r '.next_cursor // empty' <<<"$page")
    [ -n "$cursor" ] || return 0
  done
}

# audit_snapshot <scenario>: the ids of every source before the scenario, to find lost events afterwards.
audit_snapshot() {
  local src
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] snapshot the source_ref_id of each of: $SOURCES through the Audit API, before the scenario"; return 0; fi
  mkdir -p "$WORK/aud-$1"
  audit_open || { say "Audit API does not answer" >&2; return 1; }
  for src in $SOURCES; do audit_ids "$src" "$WORK/aud-$1/$src.before" || { say "Audit API request failed for $src" >&2; audit_close; return 1; }; done
  audit_close
}

# audit_sum <ids file>: sha256 of the byte-sorted ids joined by newline without trailing newline, as audit-checksum.sh and the endpoint do.
audit_sum() { printf '%s' "$(LC_ALL=C sort "$1" | paste -sd'\n')" | sha256sum | cut -d' ' -f1; }

# seq_gaps <source> <ids file>: "none" when no id has the mock form <SOURCE>-<start>-<number>, else "run first last distinct missing"
# for the newest run (largest start). missing is the count of numbers between first and last that no id has.
seq_gaps() {
  awk -v s="$1" '
    BEGIN { re = "^" s "-[0-9]+T[0-9]+-[0-9]+$" }
    $0 ~ re { split($0, p, "-"); r = p[2]; q = p[3] + 0
              if (!((r, q) in seen)) { seen[r, q] = 1; n[r]++; if (!(r in lo) || q < lo[r]) lo[r] = q; if (q > hi[r]) hi[r] = q }
              if (r > newest) newest = r }
    END { if (newest == "") print "none"; else printf "%s %d %d %d %d\n", newest, lo[newest], hi[newest], n[newest], hi[newest] - lo[newest] + 1 - n[newest] }' "$2"
}

audit_check() { # audit_check <scenario>: duplicates, lost events, checksum and sequence holes after the scenario
  local scen=$1 src d=$WORK/aud-$1 try ok req n u lost nb got want_c want_s run first last cnt miss
  if [ "$DRY_RUN" = 1 ]; then
    audit_open
    say "[dry-run] curl -G /internal/audit/events?source=<src>&limit=1000&cursor=<next_cursor> for each of: $SOURCES"
    say "[dry-run] per source: repeated source_ref_id, ids seen before and missing now, /internal/audit/checksum against sha256 of the paged ids, sequence holes of the newest BMKG and PVMBG mock run"
    return 0
  fi
  mkdir -p "$d"
  audit_open || { fail "$scen Audit API does not answer"; return; }
  for src in $SOURCES; do
    ok=0 req=0
    for ((try = 0; try < 3; try++)); do # the endpoints are not a snapshot and ingestion goes on, so a mismatch is read again
      audit_ids "$src" "$d/$src.after" || { req=1; break; }
      got=$(audit_get /internal/audit/checksum --data-urlencode "source=$src") || { req=1; break; }
      want_c=$(wc -l <"$d/$src.after" | tr -d ' ')
      want_s=$(audit_sum "$d/$src.after")
      if [ "$(jq -r .count <<<"$got")" = "$want_c" ] && [ "$(jq -r .checksum <<<"$got")" = "$want_s" ]; then ok=1; break; fi
      sleep 2
    done
    if [ "$req" = 1 ]; then fail "$scen $src Audit API request failed"; continue; fi
    n=$(wc -l <"$d/$src.after" | tr -d ' ')
    u=$(sort -u "$d/$src.after" | wc -l | tr -d ' ')
    if [ "$n" -eq "$u" ]; then pass "$scen $src no duplicate source_ref_id ($n events)"
    else fail "$scen $src $((n - u)) repeated source_ref_id out of $n events"; fi
    if [ -f "$d/$src.before" ]; then
      nb=$(sort -u "$d/$src.before" | wc -l | tr -d ' ')
      lost=$(comm -23 <(LC_ALL=C sort -u "$d/$src.before") <(LC_ALL=C sort -u "$d/$src.after") | wc -l | tr -d ' ')
      if [ "$lost" -eq 0 ]; then pass "$scen $src no gap by loss, all $nb ids seen before are still stored ($u now)"
      else fail "$scen $src gap by loss, $lost of $nb ids seen before are missing now"; fi
    fi
    if [ "$ok" = 1 ]; then pass "$scen $src Audit checksum equals sha256 of the paged ids ($want_c ids), the paging skipped nothing"
    else fail "$scen $src Audit checksum differs from the paged ids (endpoint $(jq -r '.count' <<<"$got") ids, paged $want_c), three reads"; fi
    case $src in
      BMKG | PVMBG)
        read -r run first last cnt miss < <(seq_gaps "$src" "$d/$src.after")
        if [ "$run" = none ]; then say "INFO: $scen $src no id of the form $src-<start>-<number>, sequence not checked"
        elif [ "$miss" -eq 0 ]; then pass "$scen $src no gap in the sequence of run $run ($first to $last, $cnt ids)"
        else fail "$scen $src gap in the sequence of run $run, $miss numbers missing between $first and $last ($cnt ids stored)"; fi ;;
      *) say "INFO: $scen $src ids carry no sequence, gaps are checked by loss and checksum only" ;;
    esac
  done
  audit_close
}

# ---------- Tester Agent ----------

# tester_logs <base file>: the agent log lines that are not in the base file, so only what the scenario wrote. A time filter does not
# work here: clock-skew moves the node clock, and the log lines then carry the shifted time and fall outside any --since-time.
tester_logs() {
  local out
  out=$(timeout 30 kubectl logs -n "$TESTER_NS" -l app=tester-agent --tail=-1 --ignore-errors 2>/dev/null) || true
  if [ -s "$1" ]; then printf '%s\n' "$out" | grep -vxFf "$1"; else printf '%s\n' "$out"; fi
}

# wait_end <base file> <t0 epoch>: sets WAIT_STATE to ended, failed or timeout.
wait_end() {
  local logs
  WAIT_STATE=timeout
  while [ $(($(date +%s) - $2)) -lt "$SCENARIO_TIMEOUT" ]; do
    logs=$(tester_logs "$1")
    if printf '%s\n' "$logs" | grep -qiE "$END_PATTERN"; then
      if printf '%s\n' "$logs" | grep -qiE "$FAIL_PATTERN"; then WAIT_STATE=failed; else WAIT_STATE=ended; fi
      return 0
    fi
    nap "$POLL_INTERVAL"
  done
  return 1
}

# ---------- scenarios ----------

all_valid() {
  local rows
  rows=$(lease_rows 2>/dev/null) || return 1
  [ -n "$rows" ] && ! awk -F, '$4 != 1 { bad = 1 } END { exit !bad }' <<<"$rows"
}

wait_stable() { # leases valid and the Aggregator Deployment rolled out
  [ "$DRY_RUN" = 1 ] && { say "[dry-run] wait until every lease row is valid and rollout status deployment/aggregator is done"; return 0; }
  local i
  kc rollout status deployment/aggregator --timeout=180s >/dev/null 2>&1
  for ((i = 0; i < 60; i++)); do all_valid && return 0; sleep 2; done
  return 1
}

begin() { # begin <scenario>: sets BEFORE, SAMPLES, FENCED_B
  BEFORE=$WORK/before-$1.csv SAMPLES=$WORK/samples-$1.csv FENCED_B=$WORK/fenced-before-$1.txt FENCED_A=$WORK/fenced-after-$1.txt
  : >"$SAMPLES"
  if ! wait_stable; then say "leases are not all valid before $1, not starting" >&2; return 1; fi
  if [ "$DRY_RUN" = 1 ]; then lease_rows; else lease_rows >"$BEFORE" || { say "cannot read source_lease" >&2; return 1; }; fi
  if [ "$DRY_RUN" != 1 ]; then
    cat "$BEFORE"
    for s in $SOURCES; do grep -q "^$s," "$BEFORE" || { say "no lease row for $s, is the source configured" >&2; return 1; }; done
    awk -F, '$3 == "-" { bad = 1 } END { exit bad }' "$BEFORE" || { say "a source has no holder yet" >&2; return 1; }
  fi
  fenced_snapshot "$FENCED_B"
  audit_snapshot "$1" || return 1
}

finish_run() { # finish_run <scenario> <takeover 0|1|sel> <t0> <since RFC3339 UTC, optional>
  stop_sampler
  if [ "$DRY_RUN" = 1 ]; then say "[dry-run] check per source: new holder and higher epoch where expected (takeover $2, kill-leader only for the sources whose holder pod was stopped), takeover at most ${TAKEOVER_MAX}s, gap at most ${GAP_MAX}s, epoch never back"
  else
    check_lease "$1" "$SAMPLES" "$BEFORE" "$3" "$2"
    if [ "$2" = 0 ] && [ -n "${4:-}" ]; then check_writers "$1" "$4"; fi
  fi
  fenced_snapshot "$FENCED_A"
  report_fenced "$1" "$FENCED_B" "$FENCED_A"
  audit_check "$1"
}

run_tester() { # run_tester <name> <takeover 0|1>
  local name=$1 t0 since
  say; say "== $name"
  if [ "$DRY_RUN" != 1 ] && ! kubectl get ds tester-agent -n "$TESTER_NS" >/dev/null 2>&1; then fail "$name no DaemonSet tester-agent in $TESTER_NS"; return; fi
  begin "$name" || { fail "$name could not read the starting state"; return; }
  # kill-leader stops the Aggregator pods of one node, so only the sources led from there change hands: remember the pods now.
  if [ "$name" = kill-leader ] && [ "$DRY_RUN" != 1 ]; then
    holder_map "$WORK/holders-$name.txt"; pods_snapshot "$WORK/pods-before-$name.txt"
  fi
  start_sampler "$SAMPLES"
  [ "$DRY_RUN" = 1 ] || tester_logs /dev/null >"$WORK/tl-base-$name.txt"
  t0=$(date +%s) since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  x kubectl exec -n "$TESTER_NS" daemonset/tester-agent -- /app run practice "$name" || { fail "$name the Tester Agent refused the request"; stop_sampler; return; }
  if [ "$DRY_RUN" = 1 ]; then
    say "[dry-run] poll kubectl logs -n $TESTER_NS -l app=tester-agent --tail=-1 --ignore-errors and look for \"$END_PATTERN\" in the lines that are new compared with the log before the request, at most ${SCENARIO_TIMEOUT}s"
    say "[dry-run] keep sampling ${SETTLE}s after the end marker"
  else
    if wait_end "$WORK/tl-base-$name.txt" "$t0"; then
      if [ "$WAIT_STATE" = failed ]; then fail "$name the agent log has a failure line (FAIL_PATTERN), see kubectl logs -n $TESTER_NS -l app=tester-agent"
      else pass "$name the scenario ended in the agent log (${END_PATTERN})"; fi
    else
      fail "$name no \"$END_PATTERN\" in the agent log within ${SCENARIO_TIMEOUT}s"
    fi
    nap "$SETTLE"
  fi
  if [ "$name" = kill-leader ] && [ "$DRY_RUN" != 1 ]; then
    pods_snapshot "$WORK/pods-after-$name.txt"
    if mark_stopped "$name" "$WORK/holders-$name.txt" "$WORK/pods-before-$name.txt" "$WORK/pods-after-$name.txt"; then finish_run "$name" sel "$t0" "$since"
    else stop_sampler; fi
    return
  fi
  finish_run "$name" "$2" "$t0" "$since"
}

# holder_map <file>: lines "source holder pod node" from the "lease acquired" log line of every Aggregator pod. The holder in
# source_lease is a random id of the lease elector, not a pod name, and the pod prints it when it takes the lease.
holder_map() {
  local pod node
  : >"$1"
  while read -r pod node; do
    [ -n "$pod" ] || continue
    kc logs "$pod" --tail=-1 2>/dev/null |
      jq -Rr 'fromjson? | select(.message == "lease acquired") | "\(.source) \(.holder)"' 2>/dev/null | sort -u |
      while read -r src h; do printf '%s %s %s %s\n' "$src" "$h" "$pod" "$node"; done >>"$1"
  done < <(kc get pods -l role=aggregator -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName}{"\n"}{end}')
}

# holder_lookup <map> <source> <holder>: prints "pod node", empty when no pod logged taking that lease.
holder_lookup() { awk -v s="$2" -v h="$3" '$1 == s && $2 == h { print $3, $4; exit }' "$1"; }

# pods_snapshot <file>: lines "pod uid restartCount" of the Aggregator pods.
pods_snapshot() { kc get pods -l role=aggregator -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.uid} {.status.containerStatuses[0].restartCount}{"\n"}{end}' >"$1" 2>/dev/null; }

# pod_stopped <pod> <before file> <after file>: true when the pod is gone, was recreated under the same name (uid changed) or restarted its container.
pod_stopped() { [ "$(awk -v p="$1" '$1 == p { print $2, $3 }' "$2")" != "$(awk -v p="$1" '$1 == p { print $2, $3 }' "$3")" ]; }

# mark_stopped <scenario> <holder map> <pods before> <pods after>: sets FROZEN_SRC to the sources whose holder pod was stopped
# and prints INFO for the others. Returns 1 when a holder cannot be mapped or no leader was stopped (nothing was tested).
mark_stopped() {
  local scen=$1 map=$2 pb=$3 pa=$4 src be bh hp hnode bad=0 n=0
  FROZEN_SRC=" "
  while IFS=, read -r src be bh _; do
    read -r hp hnode < <(holder_lookup "$map" "$src" "$bh")
    if [ -z "$hp" ]; then fail "$scen $src holder ${bh:0:8} is in no \"lease acquired\" log line of the Aggregator pods, cannot tell whether it was stopped"; bad=1
    elif pod_stopped "$hp" "$pb" "$pa"; then FROZEN_SRC+="$src "; n=$((n + 1)); say "$scen $src holder ${bh:0:8} ran in $hp on $hnode, the pod was stopped, takeover expected"
    else say "INFO: $scen $src holder ${bh:0:8} runs in $hp on $hnode, the pod was not stopped, stays the leader"; fi
  done <"$BEFORE"
  [ "$bad" = 0 ] || return 1
  [ "$n" -gt 0 ] || { fail "$scen no source had its holder pod stopped, the scenario tested no takeover"; return 1; }
}

# woken_check <scenario> <source> <pod> <since>: criterion 5. The old holder stored nothing after the thaw. Store rows and Audit
# items have no writer field, so this reads the pod log since the thaw: every "ingest" line of the source before the pod's own
# next "lease acquired" must have changed=0. After that line the pod has a new epoch and its writes are legitimate.
woken_check() {
  local scen=$1 src=$2 pod=$3 since=$4 log=$WORK/woken-$3.log i ok=0 stored lost fenced again
  for ((i = 0; i < 10; i++)); do
    if kc logs "$pod" --since-time="$since" --tail=-1 >"$log" 2>/dev/null && [ -s "$log" ]; then ok=1; break; fi
    nap 3
  done
  if [ "$ok" = 0 ]; then fail "$scen $src no log of $pod since the thaw, cannot show what the woken instance stored"; return; fi
  read -r stored lost fenced again < <(jq -Rr --arg s "$src" 'fromjson? | select(.source == $s) | [.message, (.changed // 0)] | @tsv' "$log" |
    awk -F'\t' '$1 == "lease acquired" { re = 1 }
                 !re && $1 == "ingest" { st += $2 }
                 !re && $1 == "lease lost" { lo++ }
                 !re && $1 == "fenced" { fe++ }
                 END { printf "%d %d %d %d\n", st, lo, fe, re }')
  say "INFO: $scen $src $pod after the thaw: lease lost lines $lost, fenced lines $fenced, took the lease again $([ "$again" = 1 ] && echo yes || echo no)"
  if [ "$stored" -eq 0 ]; then pass "$scen $src the woken instance $pod stored nothing after the thaw (ingest changed=0 until it took the lease again)"
  else fail "$scen $src the woken instance $pod stored $stored rows after the thaw while it still had the old epoch"; fi
}

run_freeze() {
  local node=$FREEZE_NODE t0 dbnode map=$WORK/holders.txt src be bh found hp hnode thaw_ts bad=0 woken=()
  say; say "== freeze"
  if [ -z "$NODE_FREEZE_CMD" ] || [ -z "$NODE_THAW_CMD" ] || [ -z "$node" ]; then
    if [ "$REQUIRE_FREEZE" = 1 ]; then fail "freeze needs FREEZE_NODE, NODE_FREEZE_CMD and NODE_THAW_CMD (REQUIRE_FREEZE=1)"
    else skip "freeze, the Tester Agent has no freeze scenario: set FREEZE_NODE, NODE_FREEZE_CMD and NODE_THAW_CMD (see the header of this script and docs/DEPLOY.md)"; fi
    return
  fi
  if [ "$FREEZE_HOLD" -le "$LEASE_TTL" ]; then fail "freeze FREEZE_HOLD ${FREEZE_HOLD}s must be longer than LEASE_TTL ${LEASE_TTL}s"; return; fi
  dbnode=$(kc get pod "$DB_POD" -o jsonpath='{.spec.nodeName}' 2>/dev/null)
  if [ "$DRY_RUN" != 1 ] && [ "$dbnode" = "$node" ]; then fail "freeze $node hosts $DB_POD, the lease authority would freeze with it, pick another node"; return; fi
  begin freeze || { fail "freeze could not read the starting state"; return; }
  # Which holder runs on which node, read before the freeze (the frozen pod cannot answer afterwards).
  FROZEN_SRC=" "
  if [ "$DRY_RUN" = 1 ]; then
    say "[dry-run] kubectl logs <aggregator pod> --tail=-1, keep the holder of each \"lease acquired\" line, and kubectl get pods -l role=aggregator -o jsonpath for the node of each pod"
    say "[dry-run] a takeover is expected only for the sources whose holder pod is on $node, INFO for the others"
    FROZEN_SRC=" $SOURCES "
  else
    holder_map "$map"
    while IFS=, read -r src be bh _; do
      read -r hp hnode < <(holder_lookup "$map" "$src" "$bh")
      if [ -z "$hp" ]; then fail "freeze $src holder ${bh:0:8} is in no \"lease acquired\" log line of the Aggregator pods, cannot tell its node"; bad=1
      elif [ "$hnode" = "$node" ]; then FROZEN_SRC+="$src "; woken+=("$src $hp"); say "freeze $src holder ${bh:0:8} runs in $hp on $node, takeover expected"
      else say "INFO: freeze $src holder ${bh:0:8} runs in $hp on $hnode, not on $node, stays the leader"; fi
    done <"$BEFORE"
    if [ "$bad" = 1 ]; then return; fi
    if [ ${#woken[@]} -eq 0 ]; then fail "freeze no source has its holder on $node, pausing it tests nothing, pick the node of a leader"; return; fi
  fi
  start_sampler "$SAMPLES"
  t0=$(date +%s)
  say "freeze $node for ${FREEZE_HOLD}s"
  FROZEN=1
  if [ "$DRY_RUN" = 1 ]; then
    say "[dry-run] NODE=$node bash -c '$NODE_FREEZE_CMD'"
    say "[dry-run] sleep $FREEZE_HOLD"
    say "[dry-run] NODE=$node bash -c '$NODE_THAW_CMD'"
    FROZEN=0
  else
    NODE=$node bash -c "$NODE_FREEZE_CMD" || { fail "freeze NODE_FREEZE_CMD failed for $node"; FROZEN=0; stop_sampler; return; }
    nap "$FREEZE_HOLD"
    NODE=$node bash -c "$NODE_THAW_CMD" || { fail "freeze NODE_THAW_CMD failed for $node, thaw by hand"; stop_sampler; return; }
    FROZEN=0
    thaw_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    say "thawed $node, sampling ${SETTLE}s so the woken instance can try to write"
    nap "$SETTLE"
  fi
  finish_run freeze sel "$t0"
  if [ "$DRY_RUN" = 1 ]; then
    say "[dry-run] for each source held on $node: kubectl logs <old holder pod> --since-time=<thaw time> --tail=-1, the \"ingest\" lines of the source must show changed=0 until the pod's own next \"lease acquired\""
  else
    for found in "${woken[@]}"; do read -r src hp <<<"$found"; woken_check freeze "$src" "$hp" "$thaw_ts"; done
  fi
}

main() {
  local scen want=("$@") sc
  [ ${#want[@]} -gt 0 ] || want=(kill-leader clock-skew freeze)
  for sc in "${want[@]}"; do
    case $sc in kill-leader | clock-skew | freeze) ;; *) say "unknown scenario $sc" >&2; exit 2 ;; esac
  done
  trap cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  if [ "$DRY_RUN" = 1 ]; then
    say "DRY_RUN plan: ${want[*]}, sources $SOURCES, lease gap limit ${GAP_MAX}s, takeover limit ${TAKEOVER_MAX}s, settle ${SETTLE}s"
    say "Changes made to the cluster: none by the script itself. kill-leader stops Aggregator pods and clock-skew shifts a node clock through the Tester Agent, both undo themselves."
    say "Freeze pauses FREEZE_NODE with NODE_FREEZE_CMD, the exit trap runs NODE_THAW_CMD if the script stops while it is frozen."
  else
    for c in kubectl curl jq awk; do command -v "$c" >/dev/null || { say "missing $c" >&2; exit 2; }; done
    kc get pod "$DB_POD" >/dev/null 2>&1 || { say "no pod $DB_POD in $NS, is kubectl on the right cluster" >&2; exit 2; }
  fi
  for scen in "${want[@]}"; do
    case $scen in
      kill-leader) run_tester kill-leader 1 ;;
      clock-skew) run_tester clock-skew 0 ;;
      freeze) run_freeze ;;
    esac
  done
  say
  [ "$DRY_RUN" = 1 ] && { say "DRY_RUN done, nothing was run"; return 0; }
  say "PASS $PASSED, FAIL $FAILED, SKIP $SKIPPED"
  if [ "$FAILED" -eq 0 ]; then say "RESULT: PASS$([ "$SKIPPED" -gt 0 ] && echo " ($SKIPPED skipped)")"; else say "RESULT: FAIL"; exit 1; fi
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then main "$@"; fi
