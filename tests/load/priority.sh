#!/usr/bin/env bash
# Isolated API fixture plus mixed-role k6 load, without starting Docker Compose or VMs.
set -euo pipefail
cd "$(dirname "$0")/../.."
root=$PWD
fixture=$(mktemp -d /tmp/hazard-priority.XXXXXX)
out="$root/tests/load/results"
mkdir -p "$out"
stamp=$(date +%Y%m%d-%H%M%S)
pid=
base=
cleanup() {
  if [ -n "$pid" ]; then
    if [ -n "$base" ]; then curl --silent --max-time 2 -X POST "$base/stop" >/dev/null || true; fi
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

PRIORITY_LOAD_DIR="$fixture" go test ./services/client-facing-api -run '^TestPriorityLoadHarness$' -count=1 -timeout=6m >"$fixture/server.log" 2>&1 &
pid=$!
for ((i=0; i<120; i++)); do
  if [ -f "$fixture/config.json" ]; then break; fi
  if ! kill -0 "$pid" 2>/dev/null; then cat "$fixture/server.log"; exit 1; fi
  sleep 1
done
if [ ! -f "$fixture/config.json" ]; then cat "$fixture/server.log"; exit 1; fi
base=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_url"])' "$fixture/config.json")

# k6 runs as the host UID so it can read the fixture's private config file.
userns=()
if [ "$(docker info --format '{{.Host.Security.Rootless}}' 2>/dev/null)" = true ]; then
  userns=(--userns=keep-id)
fi
docker run --rm --network host "${userns[@]}" -u "$(id -u):$(id -g)" \
  -v "$root/tests/load:/load" -v "$fixture:/fixture:ro" \
  -e CONFIG_FILE=/fixture/config.json -e DURATION="${DURATION:-30s}" \
  -e MEDIA_VUS="${MEDIA_VUS:-40}" -e FIELD_VUS="${FIELD_VUS:-4}" \
  "${K6_IMAGE:-grafana/k6:1.4.2}" run --summary-export "/load/results/priority-$stamp.json" /load/priority.js \
  2>&1 | tee "$out/priority-$stamp.txt"
curl --fail --silent "$base/metrics" >"$out/priority-$stamp.prom"
curl --fail --silent -X POST "$base/stop" >/dev/null
wait "$pid"
pid=
echo "Saved k6 summary and final admission metrics: tests/load/results/priority-$stamp.*"
