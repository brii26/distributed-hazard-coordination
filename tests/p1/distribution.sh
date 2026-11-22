#!/usr/bin/env bash
# Shows how the earthquake load spreads over the store nodes: shard units per node (primary and replica), rows per node, and the writes each node took in a time window.
#
#   tests/p1/distribution.sh units                  units and rows per node, now
#   tests/p1/distribution.sh watch 180 [10]         writes per node over 180 s, sampled every 10 s
#   AUDIT_URL=http://localhost:8091 AUDIT_TOKEN=... tests/p1/distribution.sh count 12345
#                                                    the Audit count must equal the unique rows the loader printed
#
# It reads /metrics of every Aggregator pod through the API server. Set METRICS_FILE to read a saved copy instead.
# Exit 0 for PASS, 1 for FAIL, 2 when the script could not run.
set -euo pipefail

ns=${NS:-sistem-informasi-kebencanaan-bnpb}
port=${AGG_HTTP_PORT:-8091}

metrics() {
  if [ -n "${METRICS_FILE:-}" ]; then cat "$METRICS_FILE"; return; fi
  local pods p
  pods=$(kubectl -n "$ns" get pods -l role=aggregator --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}') || exit 2
  [ -n "$pods" ] || { echo "no running aggregator pods in $ns" >&2; exit 2; }
  # A pod that restarts during a watch is skipped with a warning, not a reason to stop.
  for p in $pods; do kubectl get --raw "/api/v1/namespaces/$ns/pods/$p:$port/proxy/metrics" || echo "warning: $p did not answer" >&2; done
}

# Every pod counts its own writes, so the counters are summed per node.
writes() {
  metrics | awk '/^store_node_writes_total\{/ { match($0, /node="[^"]*"/); n = substr($0, RSTART + 6, RLENGTH - 7); w[n] += $NF } END { for (n in w) print n, w[n] }' | sort
}

# Prints node, writes gained, and share of all gains between two writes() snapshots.
gain() {
  awk 'NR == FNR { a[$1] = $2; next } { g[$1] = $2 - a[$1]; t += g[$1] } END { for (n in g) printf "%s %d %.0f%%\n", n, g[n], (t ? 100 * g[n] / t : 0) }' <(echo "$1") <(echo "$2") | sort
}

units() {
  local out total
  out=$(metrics | awk '
    function lab(s, k) { if (match(s, k "=\"[^\"]*\"")) return substr(s, RSTART + length(k) + 2, RLENGTH - length(k) - 3); return "" }
    /^store_shard_owner\{/ { n = lab($0, "node"); u[n " " lab($0, "role")]++; nodes[n] = 1 }
    /^store_shard_rows\{/  { n = lab($0, "node"); rows[n] += $NF; nodes[n] = 1 }
    END { for (n in nodes) printf "%s %d %d %d\n", n, u[n " primary"], u[n " replica"], rows[n] }' | sort)
  [ -n "$out" ] || { echo "no store_shard_owner lines: no leader pod answered" >&2; exit 2; }
  printf '%-14s %8s %8s %10s\n' node primary replica rows
  awk '{ printf "%-14s %8d %8d %10d\n", $1, $2, $3, $4 }' <<<"$out"
  total=$(awk '{ s += $2 } END { print s }' <<<"$out")
  # No node may be the primary of every unit (M2 Problem 1, criterion 1).
  if [ "$total" -gt 0 ] && ! awk -v t="$total" '$2 == t { bad = 1 } END { exit bad }' <<<"$out"; then
    echo "FAIL: one node is the primary of all $total units"; exit 1
  fi
  echo "PASS: $total primary units spread over $(awk '$2 > 0' <<<"$out" | wc -l | tr -d ' ') nodes"
}

watch() {
  local secs=${1:?seconds} every=${2:-10} t=0 base prev cur
  base=$(writes); prev=$base
  [ -n "$base" ] || { echo "no store_node_writes_total lines yet" >&2; exit 2; }
  while [ "$t" -lt "$secs" ]; do
    sleep "$every"; t=$((t + every)); cur=$(writes)
    echo "t=${t}s writes gained per node: $(gain "$prev" "$cur" | awk '{ printf "%s=%s ", $1, $2 }')"
    prev=$cur
  done
  echo "total over ${secs}s (node, writes, share):"
  gain "$base" "$prev"
  echo "note: counters restart with a pod, so a restart during the window lowers the gain"
}

count() {
  local want=${1:?unique rows} got
  got=$(curl -sf -H "Authorization: Bearer ${AUDIT_TOKEN:?set AUDIT_TOKEN}" "${AUDIT_URL:-http://localhost:8091}/internal/audit/checksum?source=${SOURCE:-BMKG}" | jq -r .count) || { echo "Audit count not reachable" >&2; exit 2; }
  echo "Audit count $got, unique rows in the file $want (an empty store; the load-quakes tool checks the gain instead when events were there before)"
  if [ "$got" = "$want" ]; then echo PASS; else echo FAIL; exit 1; fi
}

[ "${BASH_SOURCE[0]}" = "$0" ] || return 0

case ${1:-} in
  units) units ;;
  watch) shift; watch "$@" ;;
  count) shift; count "$@" ;;
  *) head -n 10 "$0" | tail -n 9 >&2; exit 2 ;;
esac
