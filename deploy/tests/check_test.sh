#!/usr/bin/env bash
# Offline tests for deploy/check.sh. Usage: deploy/tests/check_test.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK="$HERE/../check.sh"
FX="$HERE/fixtures"
fails=0

# expect <name> <exit code> <text that must appear in the output or ""> <args...>
expect() {
  local name="$1" want="$2" text="$3" out rc
  shift 3
  out="$("$CHECK" "$@" 2>&1)"
  rc=$?
  if [ "$rc" -ne "$want" ] || { [ -n "$text" ] && ! grep -q -- "$text" <<< "$out"; }; then
    echo "FAIL $name (exit $rc, want $want, text '$text')"
    echo "$out" | sed 's/^/     /'
    fails=$((fails + 1))
  else
    echo "ok   $name"
  fi
}

expect good-pods 0 "all 8 passed" -p "$FX/pods-good.json"
expect missing-role 1 "nolabel-1.*no role label" -p "$FX/pods-no-role.json"
expect role-outside-list 1 'role "database" is not allowed' -p "$FX/pods-bad-role.json"
expect wrong-memory 1 "request should be 256Mi, limit should be 512Mi" -p "$FX/pods-bad-memory.json"
expect missing-limit 1 "memory limit missing" -p "$FX/pods-no-limit.json"
expect nothing-found 1 "nothing was checked" -p "$FX/pods-empty.json"
expect stdin-input 0 "all 8 passed" -p - < "$FX/pods-good.json"
expect rendered-file 1 "StatefulSet/nolabel.*no role label" -k "$FX/rendered-bad.yaml"
expect bad-argument 2 "unknown argument" --nope

if [ "$fails" -gt 0 ]; then
  echo "$fails failed"
  exit 1
fi
echo "all passed"
