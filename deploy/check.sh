#!/usr/bin/env bash
# Checks the role label and the memory requests and limits of every pod against Appendix B.
# Usage:
#   deploy/check.sh [-n namespace]            pods of the live namespace
#   deploy/check.sh -p pods.json|-            output of kubectl get pods -o json
#   deploy/check.sh -k [dir|rendered.yaml]    pod templates of kubectl kustomize (default deploy/), no cluster
# Exit 0 when all pass, 1 when one fails or nothing was found, 2 on a usage error.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
mode=live
src=""

while [ $# -gt 0 ]; do
  case "$1" in
    -n | --namespace) NAMESPACE="${2:?namespace missing}"; shift 2 ;;
    -p | --pods) mode=pods; src="${2:?file or - missing}"; shift 2 ;;
    -k | --kustomize)
      mode=kustomize
      if [ $# -gt 1 ] && [[ "$2" != -* ]]; then src="$2"; shift 2; else src="$HERE"; shift; fi ;;
    -h | --help) sed -n '2,8p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

read -r -d '' PROGRAM <<'JQ' || true
def bytes:
  if . == null then null
  else tostring | capture("^(?<n>[0-9.]+)(?<u>[A-Za-z]*)$") as $m
    | ($m.n | tonumber) * ({"": 1, "k": 1e3, "K": 1e3, "M": 1e6, "G": 1e9, "Ki": 1024, "Mi": 1048576, "Gi": 1073741824}[$m.u] // 0)
  end;
def mi: if . == null then "-" else "\(. / 1048576 | floor)Mi" end;
def sum(f): [.[] | f] | if any(. == null) then null else add // 0 end;
def total(k):
  ((.spec.containers // []) | sum(.resources[k].memory | bytes)) as $c
  | ((.spec.initContainers // []) | map(.resources[k].memory | bytes) | if any(. == null) then null else max // 0 end) as $i
  | if $c == null or $i == null then null else ([$c, $i] | max) end;
def want:
  {aggregator: [256, 512], store: [512, 1024], broker: [512, 1024], consumer: [128, 256],
   api: [128, 256], mock: [64, 128], sensor: [128, 256], other: [128, 256]};
def pods:
  if type == "array" then .[] else . end
  | if .kind == "List" then .items[] else . end
  | select(.kind == "Pod" and .status.phase != "Succeeded")
  | {name: .metadata.name, labels: (.metadata.labels // {}), spec: .spec};
def templates:
  if type == "array" then .[] else . end
  | select(. != null)
  | if .kind == "List" then .items[] else . end
  | select(.kind == "Pod" or ((.kind // "") | IN("Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job", "CronJob")))
  | (if .kind == "Pod" then {labels: (.metadata.labels // {}), spec: .spec}
     elif .kind == "CronJob" then .spec.jobTemplate.spec.template | {labels: (.metadata.labels // {}), spec: .spec}
     else .spec.template | {labels: (.metadata.labels // {}), spec: .spec} end) as $t
  | {name: "\(.kind)/\(.metadata.name)", labels: $t.labels, spec: $t.spec};
(if $kind == "templates" then templates else pods end)
| . as $p
| ($p.labels.role) as $role
| ($p | total("requests")) as $rq
| ($p | total("limits")) as $lm
| (want[$role // ""]) as $w
| [ (if $role == null then "no role label"
     elif $w == null then "role \"\($role)\" is not allowed" else empty end),
    (if $rq == null then "memory request missing" else empty end),
    (if $lm == null then "memory limit missing" else empty end),
    (if $w != null and $rq != null and $rq != $w[0] * 1048576 then "request should be \($w[0])Mi" else empty end),
    (if $w != null and $lm != null and $lm != $w[1] * 1048576 then "limit should be \($w[1])Mi" else empty end) ] as $bad
| [$p.name, ($role // "-"), ($rq | mi), ($lm | mi), (if ($bad | length) == 0 then "ok" else "FAIL: " + ($bad | join(", ")) end)]
| @tsv
JQ

to_json() {
  python3 -I -c 'import sys, json, yaml; print(json.dumps([d for d in yaml.safe_load_all(sys.stdin) if d]))' \
    || { echo "python3 with PyYAML is needed to read rendered manifests" >&2; exit 2; }
}

case "$mode" in
  live) input="$(kubectl get pods --namespace "$NAMESPACE" -o json)"; kind=pods ;;
  pods) if [ "$src" = "-" ]; then input="$(cat)"; else input="$(cat "$src")"; fi; kind=pods ;;
  kustomize)
    if [ -f "$src" ]; then input="$(to_json < "$src")"; else input="$(kubectl kustomize "$src" | to_json)"; fi
    kind=templates ;;
esac

rows="$(printf '%s' "$input" | jq -r --arg kind "$kind" "$PROGRAM")"

if [ -z "$rows" ]; then
  echo "no pods or workloads found, nothing was checked" >&2
  exit 1
fi

fmt="%-45s %-11s %9s %9s  %s\n"
# shellcheck disable=SC2059
printf "$fmt" TARGET ROLE REQUEST LIMIT RESULT
failures=0
while IFS=$'\t' read -r name role rq lm res; do
  # shellcheck disable=SC2059
  printf "$fmt" "$name" "$role" "$rq" "$lm" "$res"
  case "$res" in FAIL*) failures=$((failures + 1)) ;; esac
done <<< "$rows"

total="$(printf '%s\n' "$rows" | wc -l)"
if [ "$failures" -gt 0 ]; then
  echo "$failures of $total failed" >&2
  exit 1
fi
echo "all $total passed"
