#!/usr/bin/env bash
# Runs problem1.sh to problem5.sh in order and prints one line per problem.
#
#   tests/scenarios/all.sh
cd "$(dirname "$0")" || exit 1
rc=0
for n in 1 2 3 4 5; do
  if ./problem$n.sh; then r=PASS; else r=FAIL; rc=1; fi
  summary+="Problem $n: $r"$'\n'
done
printf '\n== Summary\n%s' "$summary"
exit $rc
