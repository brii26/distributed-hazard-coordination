#!/usr/bin/env bash
# Print the topology declaration for the machine and cluster you are running now,
# as a markdown table to paste into the report or the README.
# Read only: it changes nothing.
#
# Usage: export KUBECONFIG=<path from k3s.sh>; deploy/cluster/topology.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/cluster/env.sh
. "$HERE/env.sh"
export KUBECONFIG="${KUBECONFIG:-$CLUSTER_WORK_DIR/kubeconfig}"

cpu="$(lscpu | sed -n 's/^Model name: *//p' | head -1)"
threads="$(nproc)"
mem="$(awk '/MemTotal/ {printf "%.0f GiB", $2/1024/1024}' /proc/meminfo)"
os="$(. /etc/os-release && echo "$PRETTY_NAME"), kernel $(uname -r)"
hv="$(VBoxManage --version 2>/dev/null || echo none)"

echo "Declared on $(date -u +%Y-%m-%d) by deploy/cluster/topology.sh"
echo
echo "| Node | Role | Kind | Physical host | vCPU | RAM | Address | Kernel |"
echo "|---|---|---|---|---|---|---|---|"
kubectl get nodes -o json | python3 -c '
import json, sys
for n in json.load(sys.stdin)["items"]:
    name = n["metadata"]["name"]
    labels = n["metadata"]["labels"]
    role = "control plane and worker" if "node-role.kubernetes.io/control-plane" in labels else "worker"
    ip = next(a["address"] for a in n["status"]["addresses"] if a["type"] == "InternalIP")
    k = n["status"]["nodeInfo"]["kernelVersion"]
    print("\t".join([name, role, ip, k]))
' | while IFS=$'\t' read -r name role ip kernel; do
  if VBoxManage showvminfo "$name" >/dev/null 2>&1; then
    info="$(VBoxManage showvminfo "$name" --machinereadable)"
    vcpu="$(sed -n 's/^cpus=//p' <<<"$info")"
    ram="$(sed -n 's/^memory=//p' <<<"$info")"
    echo "| $name | $role | VM (VirtualBox) | this machine (host 1) | $vcpu | $ram MiB | $ip | $kernel |"
  else
    echo "| $name | $role | not managed by these scripts (fill in: VM or native, which machine) | fill in | fill in | fill in | $ip | $kernel |"
  fi
done
echo
echo "| Host | CPU | Threads | RAM | OS | Hypervisor |"
echo "|---|---|---|---|---|---|"
echo "| host 1 (this machine) | $cpu | $threads | $mem | $os | VirtualBox $hv |"
echo
echo "k3s: $(kubectl version -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])' 2>/dev/null || echo unknown)"
echo "Add one row per extra physical machine, with its CPU and RAM."
