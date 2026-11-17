#!/usr/bin/env bash
# Install k3s on the two VMs from up.sh: server on node-a (control plane and worker),
# agent on node-b, a local image registry on node-a, and a kubeconfig on the host.
# Safe to run again.
#
# Usage: deploy/cluster/k3s.sh
#        K3S_VERSION=v1.34.1+k3s1 deploy/cluster/k3s.sh   pin a version
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/cluster/env.sh
. "$HERE/env.sh"

K3S_VERSION="${K3S_VERSION:-}"
KUBECONFIG_OUT="${KUBECONFIG_OUT:-$CLUSTER_WORK_DIR/kubeconfig}"
REGISTRY_ADDR="$NODE_A_IP:$REGISTRY_PORT"

ssh_node() { # port command...
  local port="$1"; shift
  ssh -i "$CLUSTER_SSH_KEY" -p "$port" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      -o LogLevel=ERROR -o BatchMode=yes ubuntu@127.0.0.1 "$@"
}
version_env() { [ -n "$K3S_VERSION" ] && printf 'INSTALL_K3S_VERSION=%s' "$K3S_VERSION" || true; }

registries_yaml() {
  cat <<EOF
mirrors:
  "$REGISTRY_ADDR":
    endpoint:
      - "http://$REGISTRY_ADDR"
EOF
}

# Server on node-a
if ! ssh_node "$(ssh_port 1)" 'systemctl is-active --quiet k3s'; then
  registries_yaml | ssh_node "$(ssh_port 1)" 'sudo mkdir -p /etc/rancher/k3s && sudo tee /etc/rancher/k3s/registries.yaml >/dev/null'
  ssh_node "$(ssh_port 1)" 'sudo mkdir -p /var/lib/rancher/k3s/server/manifests /var/lib/registry'
  ssh_node "$(ssh_port 1)" 'sudo tee /var/lib/rancher/k3s/server/manifests/registry.yaml >/dev/null' < "$HERE/registry.yaml"
  ssh_node "$(ssh_port 1)" "iface=\$(ip -o -4 addr show | awk '\$4 ~ /^$NODE_A_IP\\// {print \$2}'); \
    curl -sfL https://get.k3s.io | $(version_env) \
    INSTALL_K3S_EXEC=\"server --node-ip $NODE_A_IP --advertise-address $NODE_A_IP --flannel-iface \$iface \
    --tls-san 127.0.0.1 --tls-san $NODE_A_IP --write-kubeconfig-mode 644\" sh -"
else
  echo "k3s server already running on node-a"
fi

# Agents on node-b, node-c, ...
TOKEN="$(ssh_node "$(ssh_port 1)" 'sudo cat /var/lib/rancher/k3s/server/node-token')"
for i in $(seq 2 "$NODE_COUNT"); do
  n="$(node_name "$i")"; ip="$(node_ip "$i")"; port="$(ssh_port "$i")"
  if ! ssh_node "$port" 'systemctl is-active --quiet k3s-agent'; then
    registries_yaml | ssh_node "$port" 'sudo mkdir -p /etc/rancher/k3s && sudo tee /etc/rancher/k3s/registries.yaml >/dev/null'
    ssh_node "$port" "iface=\$(ip -o -4 addr show | awk '\$4 ~ /^$ip\\// {print \$2}'); \
      curl -sfL https://get.k3s.io | $(version_env) K3S_URL=https://$NODE_A_IP:6443 K3S_TOKEN='$TOKEN' \
      INSTALL_K3S_EXEC=\"agent --node-ip $ip --flannel-iface \$iface\" sh -"
  else
    echo "k3s agent already running on $n"
  fi
done

# Kubeconfig for the host. The API is reachable on 127.0.0.1:$API_PORT through the port forward.
ssh_node "$(ssh_port 1)" 'cat /etc/rancher/k3s/k3s.yaml' \
  | sed -e "s|https://127.0.0.1:6443|https://127.0.0.1:$API_PORT|" -e 's/: default$/: k3s-bnpb/; s/name: default/name: k3s-bnpb/' \
  > "$KUBECONFIG_OUT"
chmod 600 "$KUBECONFIG_OUT"
echo "kubeconfig written to $KUBECONFIG_OUT"
echo "use it with: export KUBECONFIG=$KUBECONFIG_OUT"

export KUBECONFIG="$KUBECONFIG_OUT"
for _ in $(seq 60); do
  ready="$(kubectl get nodes --no-headers 2>/dev/null | awk '$2=="Ready"' | wc -l)"
  [ "$ready" -ge "$NODE_COUNT" ] && break
  sleep 5
done
kubectl get nodes -o wide
