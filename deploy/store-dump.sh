#!/usr/bin/env bash
# Takes a pg_dump of every store node into one folder, as a copy before a wipe-node test or the exam.
#
#   deploy/store-dump.sh [OUT_DIR]      default: store-dump-<date> in the current folder
#
# Each file is store-<node>.sql, a plain SQL dump of the database canonical_store. Restore one with
#   kubectl -n <ns> exec -i <store pod> -- sh -c 'psql -U "$POSTGRES_USER" -d canonical_store' < store-<node>.sql
# Exit 0 when every node was dumped, 1 when one failed, 2 when the script could not run.
set -euo pipefail

ns=${NS:-sistem-informasi-kebencanaan-bnpb}
out=${1:-store-dump-$(date +%Y%m%d-%H%M%S)}

pods=$(kubectl -n "$ns" get pods -l app=store,role=store -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{"\n"}{end}') || exit 2
[ -n "$pods" ] || { echo "no store pods in $ns" >&2; exit 2; }

mkdir -p "$out"
failed=0

while read -r pod node; do
  [ -n "$pod" ] || continue

  if kubectl -n "$ns" exec "$pod" -- sh -c 'pg_dump -U "$POSTGRES_USER" -d canonical_store' >"$out/store-$node.sql"; then
    echo "$node: $(wc -c <"$out/store-$node.sql" | tr -d ' ') bytes, $(grep -c '^COPY ' "$out/store-$node.sql") tables"
  else
    echo "$node: pg_dump failed" >&2
    failed=1
  fi
done <<<"$pods"

echo "dump folder: $out"
exit "$failed"
