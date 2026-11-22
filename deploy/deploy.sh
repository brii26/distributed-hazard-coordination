#!/usr/bin/env bash
# One command deploy: namespace, Secrets, ConfigMaps, manifests, then waits for every rollout.
# Named deploy.sh because deploy/cluster/up.sh already starts the VMs.
# Usage: deploy/deploy.sh [env-file]      TIMEOUT=300s per workload, NAMESPACE to override
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
TIMEOUT="${TIMEOUT:-300s}"
export NAMESPACE

step() { echo "==> $*"; }

step "namespace"
kubectl apply -f "$HERE/namespace.yaml"

step "secrets"
"$HERE/make-secrets.sh" ${1:+"$1"}

step "schema and broker ConfigMaps"
"$HERE/make-schema.sh"
"$HERE/make-coordination.sh"
"$HERE/make-rabbitmq.sh"

step "manifests"
kubectl apply -k "$HERE"

# rollout status refuses OnDelete DaemonSets (the store), so wait for every pod to be ready instead.
wait_ready() {
  local w=$1 end d r
  if [ "$(kubectl get "$w" --namespace "$NAMESPACE" -o jsonpath='{.spec.updateStrategy.type}' 2>/dev/null)" != OnDelete ]; then
    kubectl rollout status "$w" --namespace "$NAMESPACE" --timeout "$TIMEOUT"
    return
  fi
  end=$((SECONDS + ${TIMEOUT%s}))
  while [ "$SECONDS" -lt "$end" ]; do
    read -r d r < <(kubectl get "$w" --namespace "$NAMESPACE" -o jsonpath='{.status.desiredNumberScheduled} {.status.numberReady}')
    [ "${d:-0}" -gt 0 ] && [ "$d" = "$r" ] && return 0
    sleep 3
  done
  return 1
}

step "waiting for rollouts (timeout $TIMEOUT each)"
failed=()
while IFS= read -r w; do
  [ -n "$w" ] || continue
  if wait_ready "$w"; then
    echo "ok     $w"
  else
    echo "FAILED $w" >&2
    failed+=("$w")
  fi
done < <(kubectl get deployments,statefulsets,daemonsets --namespace "$NAMESPACE" -o name)

if [ "${#failed[@]}" -gt 0 ]; then
  echo >&2
  echo "not ready after $TIMEOUT: ${failed[*]}" >&2
  echo "pods that are not Running:" >&2
  kubectl get pods --namespace "$NAMESPACE" --field-selector=status.phase!=Running >&2 || true
  echo "next: kubectl describe pod <pod> -n $NAMESPACE, kubectl logs <pod> -n $NAMESPACE" >&2
  exit 1
fi

step "label and resource check"
"$HERE/check.sh" --namespace "$NAMESPACE"
echo "deploy done"
