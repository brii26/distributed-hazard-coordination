#!/usr/bin/env bash
# Creates the VirtualBox VMs for the k3s cluster. Safe to run again.
# Usage: [NODE_COUNT=3] deploy/cluster/up.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/cluster/env.sh
. "$HERE/env.sh"

need() { command -v "$1" >/dev/null || { echo "missing tool: $1" >&2; exit 1; }; }
for t in VBoxManage ssh ssh-keygen curl mkfs.vfat mcopy dd; do need "$t"; done

mkdir -p "$CLUSTER_WORK_DIR"

if [ ! -f "$CLUSTER_SSH_KEY" ]; then
  ssh-keygen -q -t ed25519 -N "" -C "k3s-vm" -f "$CLUSTER_SSH_KEY"
fi

if [ ! -f "$CLUSTER_OVA" ]; then
  echo "downloading $CLUSTER_OVA_URL"
  curl -fL -o "$CLUSTER_OVA.part" "$CLUSTER_OVA_URL"
  want="$(curl -fsSL "$(dirname "$CLUSTER_OVA_URL")/SHA256SUMS" | awk -v f="*$(basename "$CLUSTER_OVA_URL")" '$2==f{print $1}')"
  have="$(sha256sum "$CLUSTER_OVA.part" | awk '{print $1}')"
  [ -n "$want" ] && [ "$want" = "$have" ] || { echo "checksum mismatch for the OVA" >&2; exit 1; }
  mv "$CLUSTER_OVA.part" "$CLUSTER_OVA"
fi

vm_exists() { VBoxManage showvminfo "$1" >/dev/null 2>&1; }

if ! VBoxManage natnetwork list | grep -q "^Name:  *$CLUSTER_NET\$"; then
  VBoxManage natnetwork add --netname "$CLUSTER_NET" --network "$CLUSTER_CIDR" --enable --dhcp off
fi

if ! vm_exists "$TEMPLATE_VM"; then
  VBoxManage import "$CLUSTER_OVA" --vsys 0 --vmname "$TEMPLATE_VM" \
    --cpus "$NODE_CPUS" --memory "$NODE_RAM_MB" --basefolder "$VM_DIR" >/dev/null
fi

[ "$NODE_COUNT" -ge 2 ] && [ "$NODE_COUNT" -le 9 ] || { echo "NODE_COUNT must be 2 to 9" >&2; exit 1; }

HOSTS=""
for i in $(seq "$NODE_COUNT"); do HOSTS+="$(node_ip "$i") $(node_name "$i")\\n"; done

render() { # render <template> <out> name ip
  local t pub
  t="$(cat "$1")"
  pub="$(cat "$CLUSTER_SSH_KEY.pub")"
  t="${t//@NAME@/$3}"; t="${t//@IP@/$4}"; t="${t//@GATEWAY@/$CLUSTER_GATEWAY}"
  t="${t//@HOSTS@/$HOSTS}"; t="${t//@SSH_PUBKEY@/$pub}"
  printf '%s\n' "$t" > "$2"
}

make_seed() { # name ip -> $SEED_DIR/<name>.vdi (cloud-init NoCloud disk, label cidata)
  local name="$1" ip="$2" dir="$SEED_DIR/$1"
  mkdir -p "$dir"
  render "$HERE/cloud-init/user-data.tmpl" "$dir/user-data" "$name" "$ip"
  render "$HERE/cloud-init/meta-data.tmpl" "$dir/meta-data" "$name" "$ip"
  render "$HERE/cloud-init/network-config.tmpl" "$dir/network-config" "$name" "$ip"
  rm -f "$dir/seed.img" "$SEED_DIR/$name.vdi"
  dd if=/dev/zero of="$dir/seed.img" bs=1M count=4 status=none
  mkfs.vfat -n cidata "$dir/seed.img" >/dev/null
  mcopy -i "$dir/seed.img" "$dir/user-data" "$dir/meta-data" "$dir/network-config" ::
  VBoxManage convertfromraw "$dir/seed.img" "$SEED_DIR/$name.vdi" --format VDI >/dev/null
}

BASE_DISK="$CLUSTER_WORK_DIR/disks/base.vdi"
make_base_disk() {
  [ -f "$BASE_DISK" ] && return
  mkdir -p "$(dirname "$BASE_DISK")"
  local src
  src="$(VBoxManage showvminfo "$TEMPLATE_VM" --machinereadable | sed -n 's/^"SCSI-0-0"="\(.*\)"$/\1/p')"
  VBoxManage clonemedium disk "$src" "$BASE_DISK" --format VDI >/dev/null
  VBoxManage modifymedium disk "$BASE_DISK" --resize "$NODE_DISK_MB" >/dev/null
}

create_node() { # name ip
  local name="$1" ip="$2"
  if vm_exists "$name"; then
    echo "$name already exists, keeping it"
    return
  fi
  make_seed "$name" "$ip"
  make_base_disk
  VBoxManage clonevm "$TEMPLATE_VM" --name "$name" --basefolder "$VM_DIR" --register >/dev/null
  local disk="$CLUSTER_WORK_DIR/disks/$name.vdi"
  local cloned
  cloned="$(VBoxManage showvminfo "$name" --machinereadable | sed -n 's/^"SCSI-0-0"="\(.*\)"$/\1/p')"
  VBoxManage storageattach "$name" --storagectl SCSI --port 0 --device 0 --medium none
  VBoxManage storagectl "$name" --name SCSI --remove
  [ -f "$cloned" ] && VBoxManage closemedium disk "$cloned" --delete >/dev/null
  [ -f "$disk" ] || VBoxManage clonemedium disk "$BASE_DISK" "$disk" --format VDI >/dev/null
  VBoxManage modifyvm "$name" --memory "$NODE_RAM_MB" --cpus "$NODE_CPUS" \
    --nic1 natnetwork --nat-network1 "$CLUSTER_NET" --nictype1 virtio --cableconnected1 on \
    --uart1 off --audio-driver none --usb off --boot1 disk --boot2 none --boot3 none --boot4 none
  VBoxManage storagectl "$name" --name SATA --add sata --controller IntelAhci --bootable on >/dev/null
  VBoxManage storageattach "$name" --storagectl SATA --port 0 --device 0 --type hdd --medium "$disk"
  VBoxManage storageattach "$name" --storagectl SATA --port 1 --device 0 --type hdd --medium "$SEED_DIR/$name.vdi"
  echo "created $name ($ip, ${NODE_RAM_MB} MiB, $NODE_CPUS vCPU)"
}

for i in $(seq "$NODE_COUNT"); do create_node "$(node_name "$i")" "$(node_ip "$i")"; done

fwd() { # rule-name host-port guest-ip guest-port
  VBoxManage natnetwork modify --netname "$CLUSTER_NET" --port-forward-4 delete "$1" 2>/dev/null || true
  VBoxManage natnetwork modify --netname "$CLUSTER_NET" \
    --port-forward-4 "$1:tcp:[127.0.0.1]:$2:[$3]:$4"
}
for i in $(seq "$NODE_COUNT"); do fwd "ssh-$(node_name "$i" | sed s/node-//)" "$(ssh_port "$i")" "$(node_ip "$i")" 22; done
fwd k3s-api "$API_PORT" "$NODE_A_IP" 6443
fwd registry "$REGISTRY_PORT" "$NODE_A_IP" 5000

for i in $(seq "$NODE_COUNT"); do
  n="$(node_name "$i")"
  state="$(VBoxManage showvminfo "$n" --machinereadable | sed -n 's/^VMState="\(.*\)"$/\1/p')"
  if [ "$state" != "running" ]; then
    VBoxManage startvm "$n" --type headless >/dev/null
    echo "started $n"
  fi
done

wait_ssh() { # port
  for _ in $(seq 60); do
    if ssh -i "$CLUSTER_SSH_KEY" -p "$1" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=3 -o BatchMode=yes ubuntu@127.0.0.1 true 2>/dev/null; then
      return 0
    fi
    sleep 5
  done
  echo "ssh on port $1 did not come up in 5 minutes" >&2
  return 1
}
for i in $(seq "$NODE_COUNT"); do wait_ssh "$(ssh_port "$i")"; done
echo "all $NODE_COUNT VMs answer on ssh. Next: deploy/cluster/k3s.sh"
