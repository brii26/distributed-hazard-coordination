#!/usr/bin/env bash
# Shared settings for the cluster scripts. Source it, every value can be overridden from the environment.
CLUSTER_WORK_DIR="${CLUSTER_WORK_DIR:-$HOME/vms/k3s}"
VM_DIR="${VM_DIR:-$CLUSTER_WORK_DIR/vm}"
SEED_DIR="${SEED_DIR:-$CLUSTER_WORK_DIR/seed}"
CLUSTER_OVA_URL="${CLUSTER_OVA_URL:-https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.ova}"
CLUSTER_OVA="${CLUSTER_OVA:-$CLUSTER_WORK_DIR/noble.ova}"
CLUSTER_SSH_KEY="${CLUSTER_SSH_KEY:-$HOME/.ssh/k3s_vm_ed25519}"

TEMPLATE_VM="${TEMPLATE_VM:-k3s-template}"
CLUSTER_NET="${CLUSTER_NET:-k3s-net}"
CLUSTER_CIDR="${CLUSTER_CIDR:-10.77.0.0/24}"
CLUSTER_GATEWAY="${CLUSTER_GATEWAY:-10.77.0.1}"

NODE_COUNT="${NODE_COUNT:-3}"
NODE_IP_PREFIX="${NODE_IP_PREFIX:-10.77.0}"   # node-a is .11, node-b is .12, ...
NODE_IP_START="${NODE_IP_START:-11}"
SSH_PORT_BASE="${SSH_PORT_BASE:-2200}"        # node-a is 2201, node-b is 2202, ...

NODE_RAM_MB="${NODE_RAM_MB:-2048}"
NODE_CPUS="${NODE_CPUS:-2}"
NODE_DISK_MB="${NODE_DISK_MB:-30720}"

API_PORT="${API_PORT:-6443}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"

node_name() { local l=abcdefghi; printf 'node-%s' "${l:$(($1 - 1)):1}"; }
node_ip() { printf '%s.%s' "$NODE_IP_PREFIX" "$((NODE_IP_START + $1 - 1))"; }
ssh_port() { printf '%s' "$((SSH_PORT_BASE + $1))"; }
NODE_A_IP="$(node_ip 1)"
