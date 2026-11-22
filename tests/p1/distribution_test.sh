#!/usr/bin/env bash
# Offline self test of distribution.sh on saved /metrics text: a healthy spread passes, one node holding every primary fails.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

nodes=(node-a node-b node-c)

make_metrics() { # primary-offset: 0 spreads the primaries, "all" puts every primary on node-a
  local s p r
  echo "# TYPE store_shard_owner gauge"
  for s in $(seq 0 15); do
    if [ "$1" = all ]; then p=0; else p=$((s % 3)); fi
    r=$(((p + 1) % 3))
    echo "store_shard_owner{shard=\"$s\",node=\"${nodes[$p]}\",role=\"primary\"} 1"
    echo "store_shard_owner{shard=\"$s\",node=\"${nodes[$r]}\",role=\"replica\"} 1"
    echo "store_shard_rows{node=\"${nodes[$p]}\",shard=\"$s\"} 100"
    echo "store_shard_rows{node=\"${nodes[$r]}\",shard=\"$s\"} 100"
  done
}

fail=0
check() { # name, expected exit code, expected text, command...
  local name=$1 want=$2 text=$3 got out; shift 3
  set +e; out=$("$@" 2>&1); got=$?; set -e
  if [ "$got" = "$want" ] && grep -q -- "$text" <<<"$out"; then echo "ok   $name"; else echo "FAIL $name (exit $got)"; echo "$out"; fail=1; fi
}

make_metrics spread >"$tmp/spread.txt"
make_metrics all >"$tmp/all.txt"
printf '# TYPE store_node_writes_total counter\nstore_node_writes_total{node="node-a"} 10\nstore_node_writes_total{node="node-b"} 5\n' >"$tmp/w1.txt"
printf '# TYPE store_node_writes_total counter\nstore_node_writes_total{node="node-a"} 10\nstore_node_writes_total{node="node-b"} 5\nstore_node_writes_total{node="node-c"} 1\n' >"$tmp/w2.txt"

check "spread passes" 0 "PASS: 16 primary units spread over 3 nodes" env METRICS_FILE="$tmp/spread.txt" "$here/distribution.sh" units
check "node-a primaries counted" 0 "node-a *6 " env METRICS_FILE="$tmp/spread.txt" "$here/distribution.sh" units
check "one node holding all primaries fails" 1 "FAIL: one node is the primary of all 16 units" env METRICS_FILE="$tmp/all.txt" "$here/distribution.sh" units
check "empty metrics is not run" 2 "no store_shard_owner" env METRICS_FILE=/dev/null "$here/distribution.sh" units
check "no argument is not run" 2 "units" "$here/distribution.sh"

# gain() and writes() are the real functions, loaded from the script.
# shellcheck source=distribution.sh
source "$here/distribution.sh"

METRICS_FILE="$tmp/w1.txt" before=$(writes)
METRICS_FILE="$tmp/w2.txt" after=$(writes)
gain_out=$(gain "$before" "$after")

if [ "$gain_out" = "$(printf 'node-a 0 0%%\nnode-b 0 0%%\nnode-c 1 100%%')" ]; then echo "ok   gain per node and share"; else echo "FAIL gain: $gain_out"; fail=1; fi

[ "$fail" = 0 ] && echo "all passed"
exit "$fail"
