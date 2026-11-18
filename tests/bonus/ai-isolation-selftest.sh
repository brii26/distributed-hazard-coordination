#!/usr/bin/env bash
# Checks tests/bonus/ai-isolation.sh against a fake kubectl and a fake curl with DRY_RUN=1. Touches no cluster.
#   tests/bonus/ai-isolation-selftest.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
state=$(mktemp -d)
trap 'rm -rf "$state"' EXIT
failures=0

run() { # run <name> <env...>: runs the script, output in $state/out
  rm -f "$state"/{calls,reads,fast,slow}
  env PATH="$HERE/testdata/bin:$PATH" KUBECTL="$HERE/testdata/bin/kubectl" FAKE_STATE="$state" DRY_RUN=1 WINDOW_S=5 MIN_ALERTS=20 \
    FAKE_LLM_IMAGE=registry.example/situation-summarizer:test BASE_URL=http://api.example EMAIL=a@b.example PASSWORD=x "${@:2}" \
    "$HERE/ai-isolation.sh" >"$state/out" 2>"$state/err"
  echo $?
}

expect() { # expect <description> <actual> <wanted>
  if [ "$2" = "$3" ]; then echo "PASS: $1"; else echo "FAIL: $1 (got $2, wanted $3)"; failures=$((failures + 1)); fi
}

has() { if grep -q -- "$2" "$1"; then echo yes; else echo no; fi; }

expect "unchanged p95 in every mode passes" "$(run ok)" 0
expect "table has the five phases" "$(grep -cE '^(baseline|slow|invalid-json|dead|pod-killed) ' "$state/out")" 5
expect "DRY_RUN prints the changes" "$(has "$state/err" '^DRY_RUN: kubectl -n .* scale deployment/situation-summarizer --replicas=0')" yes
expect "DRY_RUN sends nothing that changes the cluster to kubectl" "$(grep -cE 'set env|scale|patch|apply|delete|rollout' "$state/calls")" 0
expect "the fake LLM was switched to slow" "$(has "$state/err" 'set env deployment/fake-llm MODE=slow')" yes
expect "the fake LLM was switched to invalid-json" "$(has "$state/err" 'set env deployment/fake-llm MODE=invalid-json')" yes
expect "the fake LLM was switched to dead" "$(has "$state/err" 'set env deployment/fake-llm MODE=dead')" yes
expect "the summarizer was scaled to 0 and back to 1" "$(has "$state/err" '--replicas=0')$(has "$state/err" '--replicas=1')" yesyes
expect "the old key is restored" "$(has "$state/err" 'b2xkLWtleQ==')" yes
expect "the fake key is never printed in clear text" "$(has "$state/err" 'fake-test-key')" no

expect "with DRY_RUN off the changes do reach kubectl" "$(run live DRY_RUN=0 WINDOW_S=0 >/dev/null; has "$state/calls" 'set env deployment/fake-llm MODE=dead')" yes
expect "a slower p95 under the slow mode fails" "$(run bad FAKE_DEGRADE=slow)" 1
expect "the failing phase is named" "$(grep -E '^slow ' "$state/out" | grep -c FAIL)" 1
expect "other phases still pass" "$(grep -E '^(dead|invalid-json) ' "$state/out" | grep -c FAIL)" 0
expect "too few alerts is not a valid result" "$(run few MIN_ALERTS=500)" 1

if [ "$failures" -gt 0 ]; then echo "$failures failed"; exit 1; fi
echo "all passed"
