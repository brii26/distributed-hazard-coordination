#!/usr/bin/env bash
# Usage: deploy/cluster/down.sh [destroy]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/cluster/env.sh
. "$HERE/env.sh"

registered() { VBoxManage list vms | sed -n 's/^"\(node-[a-i]\)".*/\1/p'; }

vms="$(registered)"
for n in $vms; do
  state="$(VBoxManage showvminfo "$n" --machinereadable | sed -n 's/^VMState="\(.*\)"$/\1/p')"
  [ "$state" = "running" ] && VBoxManage controlvm "$n" acpipowerbutton && echo "shutting down $n"
done

if [ "${1:-}" = "destroy" ]; then
  sleep 15
  for n in $vms; do
    VBoxManage controlvm "$n" poweroff 2>/dev/null || true
    sleep 2
    VBoxManage unregistervm "$n" --delete >/dev/null
    echo "deleted $n"
  done
  left="$(registered)"
  if [ -n "$left" ]; then
    echo "still registered: $(echo "$left" | tr '\n' ' '), disks not deleted" >&2
    exit 1
  fi
  rm -f "$CLUSTER_WORK_DIR"/disks/node-?.vdi "$CLUSTER_WORK_DIR/kubeconfig"
  rm -rf "$SEED_DIR"
  VBoxManage natnetwork remove --netname "$CLUSTER_NET" 2>/dev/null || true
  echo "kept the template VM and the base disk (delete $CLUSTER_WORK_DIR to remove everything)"
fi
