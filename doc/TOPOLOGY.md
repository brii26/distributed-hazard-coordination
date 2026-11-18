# Cluster topology (Milestone 2)

How the Kubernetes cluster is built and what the spec asks us to declare. The scripts live in `deploy/cluster/`.

## Topology declaration

The spec asks the report and the README to declare the number of nodes, the kind of each node (VM or native), the physical host of each node, and the CPU and RAM of each host. The cluster can run on any machine, so these values are not written here by hand. Generate them on the machine that runs the cluster, and paste the output into the report:

```bash
export KUBECONFIG=$HOME/vms/k3s/kubeconfig
deploy/cluster/topology.sh
```

It prints one row per node (role, kind, vCPU, RAM, address, kernel) and one row for the host (CPU, threads, RAM, OS, hypervisor). It is read only. When a node runs on another physical machine (bonus: physical hosts), add a row for that machine by hand.

Defaults used by the scripts (all overridable, see `deploy/cluster/env.sh`):

| Item | Default |
|---|---|
| Nodes | 3: `node-a` (control plane and worker, `10.77.0.11`), `node-b` (worker, `10.77.0.12`) and `node-c` (worker, `10.77.0.13`). `NODE_COUNT=2` gives the minimum the spec allows |
| VM size | 2 vCPU, 2 GiB RAM, 30 GB disk each (`NODE_RAM_MB` changes it for new VMs, resize existing ones with `VBoxManage modifyvm` while they are off) |
| Guest OS | Ubuntu 24.04 LTS cloud image, each VM boots its own kernel |
| Hypervisor | VirtualBox |
| Network | VirtualBox NAT Network `k3s-net`, `10.77.0.0/24`, static addresses |
| Distribution | k3s, `stable` channel unless `K3S_VERSION` is set |
| Image registry | `registry:2` on node-a, port 5000 (`10.77.0.11:5000` for the nodes, `127.0.0.1:5000` for the host) |

The node names are exactly what `kubectl get nodes` prints. The Audit API reports these names.

## Build the cluster

Needs VirtualBox, `kubectl`, `ssh`, `curl`, `mkfs.vfat` and `mcopy` (dosfstools and mtools) on the machine that hosts the VMs. About 30 GB of free disk and about 7 GiB of free RAM with the default sizes (3 VMs of 2 GiB). Any machine can host the cluster. Lower `NODE_RAM_MB` on a smaller machine.

```bash
deploy/cluster/up.sh      # create and boot the three VMs (first run downloads about 590 MB)
deploy/cluster/k3s.sh     # install k3s, the registry, and write the kubeconfig
export KUBECONFIG=$HOME/vms/k3s/kubeconfig
kubectl get nodes
```

Both scripts are safe to run again. Settings can be overridden from the environment, see `deploy/cluster/env.sh` (for example `NODE_RAM_MB=2048 deploy/cluster/up.sh`).

Stop and destroy:

```bash
deploy/cluster/down.sh            # power off, keep the disks
deploy/cluster/down.sh destroy    # delete the VMs and the NAT Network
```

## How it works

- `up.sh` imports the Ubuntu 24.04 cloud image once as a template VM (`k3s-template`), converts its disk to VDI, grows it, and clones it for each node. A VMDK grown in place does not boot, which is why the VDI copy exists.
- Each node gets a small cloud-init disk (label `cidata`) rendered from `deploy/cluster/cloud-init/*.tmpl`: hostname, a static address, the `ubuntu` user with a dedicated SSH key (`~/.ssh/k3s_vm_ed25519`, created if missing), and the `/etc/hosts` entries for every node.
- `k3s.sh` runs the official installer over SSH: the server on node-a with `--node-ip`, `--advertise-address`, `--flannel-iface` and `--tls-san`, then the agent on every other node with the server token. It writes `registries.yaml` on every node so that `10.77.0.11:5000` is pulled over plain HTTP, and puts `deploy/cluster/registry.yaml` in the k3s manifests directory.

## Access from the host

Port forwards on the NAT Network, all bound to `127.0.0.1` only (not reachable from the LAN):

| Host port | Goes to |
|---|---|
| 2201, 2202, 2203 | SSH to node-a, node-b, node-c |
| 6443 | Kubernetes API on node-a |
| 5000 | image registry on node-a |

```bash
ssh -i ~/.ssh/k3s_vm_ed25519 -p 2201 ubuntu@127.0.0.1     # node-a
ssh -i ~/.ssh/k3s_vm_ed25519 -p 2202 ubuntu@127.0.0.1     # node-b (node-c is port 2203)
```

## Push an image and use it in a pod

```bash
docker tag myimage:tag 127.0.0.1:5000/myimage:tag
docker push 127.0.0.1:5000/myimage:tag
# in a manifest, use the node address:
#   image: 10.77.0.11:5000/myimage:tag
```

Checked on 2026-11-17: an image pushed from the host was pulled and run by a pod pinned to node-b.

## Network safety

The scripts do not change the host routing table, route metrics or interface priority. The VMs sit on a VirtualBox NAT Network, which adds no network interface to the host.

## Things to know

- No node is tainted, so workloads run on every node. With two nodes the Tester Agent may only kill the worker (node-b). With three nodes the spec lets us allow any node.
- The Tester Agent is installed only inside the VMs, never on the host.
- The k3s `stable` channel picked v1.37.2. Pin a version with `K3S_VERSION=... deploy/cluster/k3s.sh` on a fresh cluster if the team needs the same one everywhere.
- k3s bundles Traefik, metrics-server, local-path storage and the network policy controller.

## Moving a node to another machine

Needed for the physical hosts bonus. The manifests do not change.

1. `kubectl drain node-b --ignore-daemonsets --delete-emptydir-data` then `kubectl delete node node-b`.
2. On the other machine, install k3s as an agent with the server address and token (`K3S_URL=https://<node-a address>:6443`). The node-a address must be reachable from that machine, so port 6443 and the flannel port 8472/UDP must be open, and a NAT Network is not enough (use a bridged interface or a forward on the LAN address, chosen without changing the host routing).
3. Data stored on the old node-b is gone, which is what replication (P1) is for.

## Why three nodes

P1 asks that no single node holds all the data, that no HazardEvent is lost when a worker is killed and its data wiped, and that the system then survives losing one more node.
- Sharding alone splits the data, so with two nodes each holds half, but a wiped node loses its half.
- Adding a copy of every shard (replication factor 2) on two nodes puts all the data on both nodes, which breaks the first rule.
- With three nodes each shard has its primary on one node and its copy on another, so each node holds about two thirds of the data, and losing one node loses nothing.

Two nodes stay available with `NODE_COUNT=2` for light development. Whether the assistants count a "node" as a machine or as a storage unit is still to be confirmed with them. The spec lets a group with three or more nodes allow the Tester Agent scenarios on any node.

Memory is the limit: the full stack needs about 5.5 GiB of memory requests (Appendix B sizes), so three small VMs on one laptop only fit a part of it. A node can be moved to another machine, see above.
