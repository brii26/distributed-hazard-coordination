#!/usr/bin/env bash
# Bonus (#325): measures latency and throughput between every pair of cluster nodes (ping and iperf3, run over ssh),
# and reads the data delay signals the store already exports (shards short of a copy, repair rows) from Prometheus.
# Writes a timestamped results file. Read only for the hosts: it never touches routes, route metrics or interface priority.
#
# Usage:  tests/bonus/net-latency.sh
#   DRY_RUN=1                print every command and write nothing
#   NODES="name,ip[,sshhost[,sshport]] ..."   node list, default is the one from deploy/cluster/env.sh
#                            (ssh through the forwarded 127.0.0.1 ports). For a physical node use its LAN address:
#                            NODES="node-a,192.168.1.10 node-b,192.168.1.20"
#   SSH_USER (ubuntu)  SSH_KEY (cluster key)  PING_COUNT (20)  IPERF_SECONDS (5)  IPERF_PORT (5201)
#   CMD_TIMEOUT (30)         seconds for one remote ping or ssh probe, iperf3 gets IPERF_SECONDS + CMD_TIMEOUT
#   PROM_URL (http://127.0.0.1:9090)   Prometheus, for example from: kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/prometheus 9090
#   OUT_DIR (tests/bonus/results)
# Exit codes: 0 all measurements done, 1 some measurement failed (results file still written), 2 bad setup (missing tool, bad node list).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
# shellcheck source=deploy/cluster/env.sh
. "$REPO/deploy/cluster/env.sh"

DRY_RUN="${DRY_RUN:-0}"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-$CLUSTER_SSH_KEY}"
PING_COUNT="${PING_COUNT:-20}"
IPERF_SECONDS="${IPERF_SECONDS:-5}"
IPERF_PORT="${IPERF_PORT:-5201}"
CMD_TIMEOUT="${CMD_TIMEOUT:-30}"
PROM_URL="${PROM_URL:-http://127.0.0.1:9090}"
OUT_DIR="${OUT_DIR:-$HERE/results}"

die() { echo "error: $*" >&2; exit 2; }
for v in PING_COUNT IPERF_SECONDS IPERF_PORT CMD_TIMEOUT; do
  [[ "${!v}" =~ ^[1-9][0-9]*$ ]] || die "$v must be a positive integer, got '${!v}'"
done

default_nodes() {
  local i out=()
  for i in $(seq 1 "$NODE_COUNT"); do out+=("$(node_name "$i"),$(node_ip "$i"),127.0.0.1,$(ssh_port "$i")"); done
  echo "${out[*]}"
}
read -r -a NODE_SPECS <<<"${NODES:-$(default_nodes)}"
[ "${#NODE_SPECS[@]}" -ge 2 ] || die "need at least 2 nodes, got ${#NODE_SPECS[@]}"

N_NAME=(); N_IP=(); N_SSH=(); N_PORT=()
for spec in "${NODE_SPECS[@]}"; do
  IFS=, read -r name ip sshhost sshport <<<"$spec"
  [ -n "$name" ] && [ -n "$ip" ] || die "bad node spec '$spec', expected name,ip[,sshhost[,sshport]]"
  N_NAME+=("$name"); N_IP+=("$ip"); N_SSH+=("${sshhost:-$ip}"); N_PORT+=("${sshport:-22}")
done

if [ "$DRY_RUN" != 1 ]; then
  for t in ssh timeout curl; do command -v "$t" >/dev/null || die "$t is required"; done
fi

SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR)
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULT="$OUT_DIR/net-latency-$STAMP.txt"
PROM_OUT="$OUT_DIR/net-latency-$STAMP.prom"
FAILURES=0

# run_remote <index> <timeout seconds> <command...>: ssh with a local and a remote timeout, stdout is the answer.
run_remote() {
  local i="$1" secs="$2"; shift 2
  local cmd="timeout $secs $*"
  if [ "$DRY_RUN" = 1 ]; then
    echo "DRY_RUN: timeout $((secs + 10)) ssh ${SSH_OPTS[*]} -p ${N_PORT[$i]} $SSH_USER@${N_SSH[$i]} '$cmd'" >&2
    return 0
  fi
  timeout "$((secs + 10))" ssh "${SSH_OPTS[@]}" -p "${N_PORT[$i]}" "$SSH_USER@${N_SSH[$i]}" "$cmd"
}

# parse_ping: reads ping output, prints "avg_ms min_ms max_ms mdev_ms loss_pct" or nothing.
parse_ping() {
  awk '
    /packet loss/ { for (i = 1; i <= NF; i++) if ($i ~ /%$/) { loss = $i; sub(/%/, "", loss) } }
    /^(rtt|round-trip)/ { split($4, a, "/"); min = a[1]; avg = a[2]; max = a[3]; mdev = a[4] }
    END { if (avg != "") print avg, min, max, mdev, loss }'
}

# parse_iperf: reads iperf3 text output, prints "mbit_per_s retransmits" of the sender summary line, or nothing.
parse_iperf() {
  awk '
    $NF == "sender" && /bits\/sec/ {
      for (i = 1; i <= NF; i++) if ($i ~ /^[KMG]?bits\/sec$/) { v = $(i - 1); u = $i; r = (i + 1 < NF) ? $(i + 1) : 0 }
      if (u == "Gbits/sec") v *= 1000; else if (u == "Kbits/sec") v /= 1000; else if (u == "bits/sec") v /= 1000000
      mbit = v; retr = r
    }
    END { if (mbit != "") printf "%.1f %s\n", mbit, retr }'
}

# prom_query <promql>: prints the first sample value, or n/a.
prom_query() {
  if [ "$DRY_RUN" = 1 ]; then
    echo "DRY_RUN: curl -sf --max-time $CMD_TIMEOUT -G $PROM_URL/api/v1/query --data-urlencode 'query=$1'" >&2
    echo "dry-run"
    return 0
  fi
  local body val
  body="$(curl -sf --max-time "$CMD_TIMEOUT" -G "$PROM_URL/api/v1/query" --data-urlencode "query=$1")" || { echo "n/a"; return 1; }
  val="$(sed -n 's/.*"value":\[[0-9.eE+-]*,"\([^"]*\)"\].*/\1/p' <<<"$body" | head -1)"
  echo "${val:-n/a}"
}

emit() { if [ "$DRY_RUN" = 1 ]; then echo "$*"; else echo "$*" | tee -a "$RESULT"; fi; }
prom_line() { [ "$DRY_RUN" = 1 ] || echo "$*" >>"$PROM_OUT"; }

measure_pair() { # src index, dst index
  local s="$1" d="$2" src="${N_NAME[$1]}" dst="${N_NAME[$2]}" dip="${N_IP[$2]}" out p r
  local rtt="n/a n/a n/a n/a n/a" bw="n/a n/a"

  if out="$(run_remote "$s" "$CMD_TIMEOUT" "ping -c $PING_COUNT -i 0.2 -q $dip")" && [ "$DRY_RUN" != 1 ]; then
    p="$(parse_ping <<<"$out")"; [ -n "$p" ] && rtt="$p"
  fi
  [ "$DRY_RUN" = 1 ] || [ "${rtt%% *}" != "n/a" ] || { FAILURES=$((FAILURES + 1)); echo "warn: ping $src -> $dst failed" >&2; }

  # one-shot iperf3 server on the destination (-1 exits after one test, the remote timeout ends it if no client comes)
  run_remote "$d" "$((IPERF_SECONDS + CMD_TIMEOUT))" "iperf3 -s -1 -D -p $IPERF_PORT >/dev/null 2>&1" >/dev/null
  [ "$DRY_RUN" = 1 ] || sleep 1
  if out="$(run_remote "$s" "$((IPERF_SECONDS + CMD_TIMEOUT))" "iperf3 -c $dip -p $IPERF_PORT -t $IPERF_SECONDS")" && [ "$DRY_RUN" != 1 ]; then
    r="$(parse_iperf <<<"$out")"; [ -n "$r" ] && bw="$r"
  fi
  [ "$DRY_RUN" = 1 ] || [ "${bw%% *}" != "n/a" ] || { FAILURES=$((FAILURES + 1)); echo "warn: iperf3 $src -> $dst failed" >&2; }

  read -r avg min max mdev loss <<<"$rtt"
  read -r mbit retr <<<"$bw"
  emit "$(printf '%-8s %-8s %-10s %-10s %-10s %-10s %-8s %-12s %s' "$src" "$dst" "$avg" "$min" "$max" "$mdev" "$loss" "$mbit" "$retr")"
  if [ "$avg" != n/a ]; then
    prom_line "bonus_internode_rtt_ms{src=\"$src\",dst=\"$dst\"} $avg"
    prom_line "bonus_internode_packet_loss_percent{src=\"$src\",dst=\"$dst\"} $loss"
  fi
  [ "$mbit" = n/a ] || prom_line "bonus_internode_throughput_mbit{src=\"$src\",dst=\"$dst\"} $mbit"
}

sample_delay() { # label
  local under misplaced repair
  under="$(prom_query 'max(store_shards_underreplicated)')" || FAILURES=$((FAILURES + 1))
  misplaced="$(prom_query 'max(store_shards_misplaced)')" || true
  repair="$(prom_query 'sum(increase(store_repair_rows_total[5m]))')" || true
  emit "$(printf '%-10s shards_underreplicated=%s shards_misplaced=%s repair_rows_last_5m=%s' "$1" "$under" "$misplaced" "$repair")"
}

[ "$DRY_RUN" = 1 ] || { mkdir -p "$OUT_DIR" || die "cannot create $OUT_DIR"; : >"$PROM_OUT"; }

emit "# net-latency $STAMP (DRY_RUN=$DRY_RUN)"
emit "# nodes: $(for i in "${!N_NAME[@]}"; do printf '%s=%s ' "${N_NAME[$i]}" "${N_IP[$i]}"; done)"
emit "# ping: $PING_COUNT probes, iperf3: ${IPERF_SECONDS}s TCP on port $IPERF_PORT, prometheus: $PROM_URL"
emit "# RTT in ms from the first node to the second, throughput in Mbit/s, retransmits as reported by iperf3"
emit
emit "## Data delay signals before the run (store_* metrics from the aggregator repair loop, see doc/P1_STORAGE_DESIGN.md)"
sample_delay before
emit
emit "## Node pairs"
emit "$(printf '%-8s %-8s %-10s %-10s %-10s %-10s %-8s %-12s %s' src dst rtt_avg rtt_min rtt_max rtt_mdev loss_pct Mbit_per_s retransmits)"
for ((i = 0; i < ${#N_NAME[@]}; i++)); do
  for ((j = i + 1; j < ${#N_NAME[@]}; j++)); do
    measure_pair "$i" "$j"
  done
done
emit
emit "## Data delay signals after the run"
sample_delay after
emit
if [ "$DRY_RUN" = 1 ]; then
  echo "DRY_RUN: nothing was run on any node and no file was written (would write $RESULT and $PROM_OUT)"
  exit 0
fi
emit "# failed measurements: $FAILURES"
echo "results: $RESULT"
[ "$FAILURES" -eq 0 ] || exit 1
