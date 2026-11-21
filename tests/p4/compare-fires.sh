#!/usr/bin/env bash
# P4 crash test (criterion 5): compares the satellite fires of two runs that stopped at the same offset (SATELIT_STOP_AT).
# A run with a kill must hold exactly the fires of a run without one: same names, places, times, severity and attributes,
# detection_keys included. ingested_at is left out, because it is the wall clock of the save.
#
# Usage:
#   AUDIT_TOKEN=... tests/p4/compare-fires.sh dump FILE      saves the live satellite fires of the aggregator at AUDIT_URL to FILE
#   tests/p4/compare-fires.sh compare FILE_A FILE_B          exits 0 when both dumps are identical, 1 with the differences
#   AUDIT_URL (http://localhost:8091)   for example from: kubectl -n sistem-informasi-kebencanaan-bnpb port-forward deploy/aggregator 8091
#   AUDIT_TOKEN                         the Audit API token, from the environment only; it is never printed or put on a command line
# Needs curl and jq for dump, and jq for compare. Exit codes: 0 identical (or dump done), 1 different, 2 bad setup.
set -euo pipefail

die() { echo "error: $*" >&2; exit 2; }

TMP_FILES=()
trap 'rm -f "${TMP_FILES[@]}"' EXIT

dump() {
  local out="$1" url="${AUDIT_URL:-http://localhost:8091}" cursor="" page headers raw
  [ -n "${AUDIT_TOKEN:-}" ] || die "set AUDIT_TOKEN"
  for t in curl jq; do command -v "$t" >/dev/null || die "$t is required"; done

  headers="$(mktemp)"
  raw="$(mktemp)"
  TMP_FILES+=("$headers" "$raw")
  chmod 600 "$headers"
  printf 'Authorization: Bearer %s\n' "$AUDIT_TOKEN" >"$headers"

  while :; do
    page="$(curl -sSf -H @"$headers" -G "$url/internal/audit/events" \
      --data-urlencode "source=SATELIT" --data-urlencode "limit=1000" --data-urlencode "cursor=$cursor")" ||
      die "the Audit API at $url did not answer"
    [ "$(jq -r '.partial' <<<"$page")" = false ] || die "the Audit API answered partial (a store node is down), try again"
    jq -c '.items[] | del(.ingested_at)' <<<"$page" >>"$raw"
    cursor="$(jq -r '.next_cursor // empty' <<<"$page")"
    [ -n "$cursor" ] || break
  done

  # One fire per line, keys sorted inside each fire and lines sorted by bytes, so equal runs give equal files.
  jq -cS . "$raw" | LC_ALL=C sort >"$out"
  echo "$(wc -l <"$out" | tr -d ' ') fires saved to $out (sha256 $(sha256sum "$out" | cut -d' ' -f1))"
}

compare() {
  local a="$1" b="$2"
  if [ ! -f "$a" ] || [ ! -f "$b" ]; then die "both dump files must exist"; fi
  command -v jq >/dev/null || die "jq is required"

  if cmp -s "$a" "$b"; then
    echo "identical: $(wc -l <"$a" | tr -d ' ') fires, sha256 $(sha256sum "$a" | cut -d' ' -f1)"
    return 0
  fi

  echo "DIFFERENT: $(wc -l <"$a" | tr -d ' ') fires in $a, $(wc -l <"$b" | tr -d ' ') in $b"
  local ids_a ids_b
  ids_a="$(jq -r .source_ref_id "$a" | LC_ALL=C sort)"
  ids_b="$(jq -r .source_ref_id "$b" | LC_ALL=C sort)"
  echo "only in $a: $(LC_ALL=C comm -23 <(echo "$ids_a") <(echo "$ids_b") | grep -c . || true)"
  echo "only in $b: $(LC_ALL=C comm -13 <(echo "$ids_a") <(echo "$ids_b") | grep -c . || true)"
  echo "first differing lines (< $a, > $b):"
  diff "$a" "$b" | grep '^[<>]' | head -n 10 | cut -c1-400
  return 1
}

case "${1:-}" in
  dump) [ $# -eq 2 ] || die "usage: $0 dump FILE"; dump "$2" ;;
  compare) [ $# -eq 3 ] || die "usage: $0 compare FILE_A FILE_B"; compare "$2" "$3" ;;
  *) die "usage: $0 dump FILE | compare FILE_A FILE_B" ;;
esac
