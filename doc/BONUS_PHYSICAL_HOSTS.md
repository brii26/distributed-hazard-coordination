# Bonus: nodes on separate physical hosts (issue #325)

Status: the measurement script and the dashboard panel are written and checked offline only (see "What was checked"). **No second physical machine has been joined and no real network cut has been done yet.** Every result table below is a template. Do not quote a number from this file until it is filled from a real run. Bonus work counts only if it is both in the code and in the report, so the filled results go into the report too.

What exists in the repo for this bonus:

| File | What it is |
|---|---|
| `tests/bonus/net-latency.sh` | Measures `ping` and `iperf3` between every node pair over ssh, reads the data delay signals from Prometheus, writes a timestamped results file |
| `deploy/observability/grafana/dashboard.json` | Panel 30 (data delay, from real metrics) and panel 31 (explains where the latency numbers live) |
| `doc/TOPOLOGY.md` | Section "Physical hosts" lists the machines |

## Data delay signal (what really exists)

The code has no replication lag in seconds. Each shard is written to two nodes by the Aggregator, and its repair loop copies rows to a node that is missing them (`doc/P1_STORAGE_DESIGN.md`, `services/aggregator/rebalance.go`). The lease holder exports these metrics on `/metrics` of the Aggregator (port 8091, scraped by the `aggregator` job):

| Metric | Meaning for this bonus |
|---|---|
| `store_shards_underreplicated` | Shards that do not have both copies yet. Grows while a node is cut off and returns to 0 once repair has copied the rows. This is the data delay signal |
| `store_shards_misplaced` | Shards stored on a node that should not hold them |
| `store_repair_rows_total` | Rows copied by the repair loop, use `rate()` or `increase()` |
| `store_shard_rows{node,shard}` | Row count per shard and node |

Round trip time and throughput between nodes are not Prometheus metrics, because nothing on the nodes exports them and the cluster has no Pushgateway or node exporter. They are measured by `tests/bonus/net-latency.sh` and kept in the results file. The script also writes a `.prom` file with the same numbers, as `bonus_internode_rtt_ms`, `bonus_internode_packet_loss_percent` and `bonus_internode_throughput_mbit`. These names belong to the script, they are not scraped by Prometheus. Dashboard panel 31 says so.

## Join a node on a second physical machine

Needs two machines on one LAN (a switch, a router or a direct cable), Ubuntu 24.04 on the second one, root access there, and the k3s server reachable from it. Replace `LAN_A` with the LAN address of node-a and `LAN_B` with the LAN address of the second machine.

The spec requires nodes with addresses from different physical machines. The default VM nodes sit on the VirtualBox NAT Network (`10.77.0.0/24`), which another machine cannot reach (`doc/TOPOLOGY.md`). So node-a needs a LAN address too. Either give its VM a bridged adapter, or run node-a natively on the first machine. This step is not rehearsed yet and depends on how the first machine is set up. Then the server must advertise that address. These are the flags `deploy/cluster/k3s.sh` already uses, with `LAN_A` in place of `10.77.0.11`:

```bash
# on node-a
iface=$(ip -o -4 addr show | awk '$4 ~ /^LAN_A\// {print $2}')
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server --node-ip LAN_A --advertise-address LAN_A --flannel-iface $iface \
  --tls-san 127.0.0.1 --tls-san LAN_A --write-kubeconfig-mode 644" sh -
sudo cat /var/lib/rancher/k3s/server/node-token       # the token for the next step
```

Open these ports between the two machines: 6443/tcp (API), 8472/udp (flannel VXLAN), 10250/tcp (kubelet) and 5000/tcp (registry on node-a). If `ufw` is on, add allow rules for them. The scripts here never change routes, route metrics or interface priority, and neither do the commands below.

On the second machine, as an agent, the same way `k3s.sh` joins node-b. The manifests pull images from `10.77.0.11:5000`, so the mirror keeps that name and points at the real registry address, which needs no manifest change:

```bash
# on the second machine
sudo mkdir -p /etc/rancher/k3s
sudo tee /etc/rancher/k3s/registries.yaml >/dev/null <<EOF
mirrors:
  "10.77.0.11:5000":
    endpoint:
      - "http://LAN_A:5000"
EOF
sudo hostnamectl set-hostname node-b
iface=$(ip -o -4 addr show | awk '$4 ~ /^LAN_B\// {print $2}')
curl -sfL https://get.k3s.io | K3S_URL=https://LAN_A:6443 K3S_TOKEN='<token from node-a>' \
  INSTALL_K3S_EXEC="agent --node-ip LAN_B --flannel-iface $iface" sh -
```

If node-b already exists as a VM, remove it first as in "Moving a node to another machine" in `doc/TOPOLOGY.md` (`kubectl drain node-b --ignore-daemonsets --delete-emptydir-data`, then `kubectl delete node node-b`). The data on the old node-b is gone, and the repair loop refills the new one.

Check from the first machine:

```bash
export KUBECONFIG=$HOME/vms/k3s/kubeconfig
kubectl get nodes -o wide        # INTERNAL-IP must show addresses of two different machines
deploy/cluster/topology.sh       # then add one row per extra physical machine by hand
```

Acceptance evidence 1: paste the `kubectl get nodes -o wide` output in the results template.

## Capture the host views

Run on each physical machine and keep the output with the date:

```bash
hostnamectl
lscpu
free -h
ip -br addr        # shows the LAN address, read only
```

## Measure latency and data delay

Make sure each node has `iperf3` and `ping` (`sudo apt-get install -y iperf3 iputils-ping`) and that the machine that runs the script can ssh to every node with the cluster key. Open Prometheus on the machine that runs the script:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/prometheus 9090:9090 &
```

Preview first, nothing is run on any node and no file is written:

```bash
DRY_RUN=1 NODES="node-a,LAN_A node-b,LAN_B" tests/bonus/net-latency.sh
```

Then the real run. `NODES` entries are `name,ip[,sshhost[,sshport]]`. Without `NODES` the script uses the node list of `deploy/cluster/env.sh` through the forwarded `127.0.0.1` ssh ports, which only works for VMs on the first machine.

```bash
NODES="node-a,LAN_A node-b,LAN_B" tests/bonus/net-latency.sh
```

| Setting | Default | Meaning |
|---|---|---|
| `PING_COUNT` | 20 | ping probes per pair |
| `IPERF_SECONDS` | 5 | iperf3 test length |
| `CMD_TIMEOUT` | 30 | seconds allowed for one remote command (iperf3 gets the test length on top) |
| `PROM_URL` | `http://127.0.0.1:9090` | Prometheus |
| `OUT_DIR` | `tests/bonus/results` | where the timestamped files go |

Exit codes: 0 all measurements done, 1 some measurement failed (the file is still written, `n/a` marks the gap), 2 bad setup (missing tool, bad node list, bad number). The script only runs `ping`, `iperf3`, `curl` and `ssh`. It starts a one-shot `iperf3 -s -1 -D` on the destination node. It does not touch routes, interface metrics or priorities.

Run it twice and keep both files: once on an idle system (baseline) and once while the load test runs (step below).

## Rehearse the real network cut

Goal: show what the system does when a physical node really loses the network, under load. Do this once as a rehearsal and write down what happened, even if it goes wrong.

Prepare three terminals and the Grafana dashboard (panel 30 and the existing panels) on the first machine:

```bash
kubectl get nodes -o wide -w                                   # terminal 1
kubectl -n sistem-informasi-kebencanaan-bnpb get pods -o wide -w   # terminal 2
```

1. Start the load as in `tests/load/README.md` (cluster run), for example `tests/load/p2-k8s.sh`. Note the start time.
2. Wait about one minute so the baseline is on the dashboard. Run `tests/bonus/net-latency.sh` once (baseline under load).
3. Cut the network of the second machine physically: unplug its Ethernet cable, or turn its Wi-Fi off with the hardware switch or the desktop Wi-Fi toggle. Do not edit routes or interface settings on either machine. Note the exact time.
4. Keep the load running for the planned cut length (suggested 2 minutes, short enough that pod eviction timers have not fired, then repeat once with longer if time allows). Record, with timestamps:
   - when `kubectl get nodes` shows node-b `NotReady`,
   - what happens to pods on node-b,
   - the error rate and the 429 and 5xx counts of the load run,
   - `store_shards_underreplicated` and `store_repair_rows_total` on panel 30,
   - whether reads and writes kept working and whether any HazardEvent was lost (`tests/scenarios/problem1.sh` style checks, or the Audit checksum).
5. Reconnect the cable or Wi-Fi. Note the time. Record how long node-b takes to turn `Ready`, how long `store_shards_underreplicated` takes to return to 0, and whether the repair loop moved rows (`increase(store_repair_rows_total[10m])`).
6. Run `tests/bonus/net-latency.sh` again (after recovery) and compare with the baseline.
7. Screenshot the dashboard over the whole window and keep the terminal logs.

## Results template

Fill every placeholder from a real run. Delete none, write "not measured" where something was not.

### Hosts

| Node | Machine | `hostnamectl` (OS, kernel) | `lscpu` (model, threads) | `free -h` (total RAM) | LAN address |
|---|---|---|---|---|---|
| node-a | `<machine name>` | `<...>` | `<...>` | `<...>` | `<LAN_A>` |
| node-b | `<machine name>` | `<...>` | `<...>` | `<...>` | `<LAN_B>` |

`kubectl get nodes -o wide` output: `<paste>`

Network between the machines: `<switch, router, cable or Wi-Fi, link speed>`

### Latency and throughput (file `tests/bonus/results/net-latency-<time>.txt`)

| Run | Pair | RTT avg ms | RTT max ms | Loss % | Mbit/s | Retransmits |
|---|---|---|---|---|---|---|
| idle | node-a to node-b | `<>` | `<>` | `<>` | `<>` | `<>` |
| under load | node-a to node-b | `<>` | `<>` | `<>` | `<>` | `<>` |
| after recovery | node-a to node-b | `<>` | `<>` | `<>` | `<>` | `<>` |

### Data delay (panel 30 or the "before" and "after" lines of the results file)

| Moment | `store_shards_underreplicated` | `store_shards_misplaced` | repair rows per second |
|---|---|---|---|
| before the cut | `<>` | `<>` | `<>` |
| during the cut (peak) | `<>` | `<>` | `<>` |
| after reconnect, time to reach 0 | `<>` | `<>` | `<>` |

### Network cut

| Item | Value |
|---|---|
| Cut type | `<cable or Wi-Fi>` |
| Cut start and end (time) | `<>` |
| Load running | `<tool, VUs, duration>` |
| node-b `NotReady` after | `<seconds>` |
| Pods on node-b during the cut | `<what happened>` |
| Error rate, 429 and 5xx during the cut | `<>` |
| Reads and writes during the cut | `<worked or failed, how>` |
| HazardEvents lost | `<count and how checked>` |
| node-b `Ready` again after | `<seconds>` |
| Time until no shard is short of a copy | `<seconds>` |
| What surprised us | `<>` |

## Acceptance checklist (issue #325)

- [ ] `kubectl get nodes -o wide` shows nodes with IPs from different physical machines
- [ ] Latency and delay numbers measured and saved (`tests/bonus/results/net-latency-*.txt`, copied into the report)
- [ ] A real cable or Wi-Fi cut was rehearsed once and what happened is written down above
- [ ] The topology doc lists the physical hosts (`doc/TOPOLOGY.md`, section "Physical hosts")
- [ ] Host views (`hostnamectl`, `lscpu`, `free -h`) captured for each machine
- [ ] The scripts changed no host routing, interface priority or route metrics
- [ ] The same content is in the report (the bonus counts only if it is in both code and report)

## What was checked offline

- `bash -n` and `shellcheck -x` on `tests/bonus/net-latency.sh`.
- `DRY_RUN=1` prints every ssh and curl command and writes nothing.
- A fake `ssh` and `curl` with synthetic output exercised the ping, iperf3 and Prometheus parsing, the results and `.prom` files and the exit codes 0, 1 and 2. The numbers in that run were synthetic and mean nothing.
- `python3 -m json.tool` and `jq` on the dashboard, panel ids are unique.
