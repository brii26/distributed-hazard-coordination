#!/usr/bin/env bash
# Stop or destroy the cluster VMs.
#
# Usage: deploy/cluster/down.sh           power the VMs off, keep their disks
#        deploy/cluster/down.sh destroy   delete the VMs, their disks and the NAT Network
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/cluster/env.sh
. "$HERE/env.sh"

exists() { VBoxManage showvminfo "$1" >/dev/null 2>&1; }

for n in $(for i in $(seq 9); do node_name "$i"; done); do
  exists "$n" || continue
  state="$(VBoxManage showvminfo "$n" --machinereadable | sed -n 's/^VMState="\(.*\)"$/\1/p')"
  [ "$state" = "running" ] && VBoxManage controlvm "$n" acpipowerbutton && echo "shutting down $n"
done

if [ "${1:-}" = "destroy" ]; then
  sleep 15
  for n in $(for i in $(seq 9); do node_name "$i"; done); do
    exists "$n" || continue
    VBoxManage controlvm "$n" poweroff 2>/dev/null || true
    sleep 2
    VBoxManage unregistervm "$n" --delete >/dev/null
    echo "deleted $n"
  done
  rm -f "$CLUSTER_WORK_DIR"/disks/node-?.vdi "$CLUSTER_WORK_DIR/kubeconfig"
  rm -rf "$SEED_DIR"
  VBoxManage natnetwork remove --netname "$CLUSTER_NET" 2>/dev/null || true
  echo "kept the template VM and the base disk (delete $CLUSTER_WORK_DIR to remove everything)"
fi
