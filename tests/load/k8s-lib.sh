#!/usr/bin/env bash
# Helpers shared by p2-k8s.sh and p2-ratio.sh. Source it, don't run it.

NS=${NS:-sistem-informasi-kebencanaan-bnpb}
K6_IMAGE=${K6_IMAGE:-grafana/k6:1.4.2}
CM=${CM:-bnpb-config}
# The run needs these ConfigMap values. The cluster defaults (60 s tokens, 50 rps per user per API pod, 100 in flight) would turn a 10k VU run into 401 and 429.
LOAD_TOKEN_TTL_S=${LOAD_TOKEN_TTL_S:-7200}
LOAD_RATE_LIMIT_RPS=${LOAD_RATE_LIMIT_RPS:-1000000}
LOAD_MAX_INFLIGHT=${LOAD_MAX_INFLIGHT:-20000}
# A run where fewer than this share of the answers is 200 is not a valid result.
MIN_200_SHARE=${MIN_200_SHARE:-0.95}

kc() { kubectl -n "$NS" "$@"; }

_cfg_saved=0; _cfg_file=; _hpa_file=

# rollout_wait: restarts the pods that read the ConfigMap and waits for them.
# ACCESS_TOKEN_TTL_S is read by auth-service (it signs the token), the two limits by client-facing-api.
rollout_wait() {
  kc rollout restart deployment/auth-service deployment/client-facing-api >&2
  kc rollout status deployment/auth-service --timeout=180s >&2
  kc rollout status deployment/client-facing-api --timeout=180s >&2
}

# load_prepare <dir> <stamp>: saves the ConfigMap values and the Aggregator HPA (if any) under <dir>, deletes the HPA, raises the limits and restarts.
# Call load_restore from an EXIT/INT/TERM trap. Both are safe to run twice.
load_prepare() {
  local dir=$1 stamp=$2 hpa
  mkdir -p "$dir"
  local saved="$dir/configmap-saved-$stamp.json"
  kc get configmap "$CM" -o json | jq -c '{data: (.data | with_entries(select(.key == "ACCESS_TOKEN_TTL_S" or .key == "RATE_LIMIT_RPS" or .key == "CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS")))}' >"$saved"
  _cfg_saved=1; _cfg_file=$saved
  echo "saved ConfigMap values: $(cat "$saved")"

  # The HPA (issue 327) would undo the scaling steps (min 3, max 4 or 5). Keep a copy, delete it, put it back on exit. No HPA is fine.
  hpa=$(kc get hpa -o json 2>/dev/null | jq -r '.items[]? | select(.spec.scaleTargetRef.kind == "Deployment" and .spec.scaleTargetRef.name == "aggregator") | .metadata.name' | head -1 || true)
  if [ -n "$hpa" ]; then
    _hpa_file="$dir/hpa-saved-$stamp.json"
    kc get hpa "$hpa" -o json | jq '{apiVersion, kind, metadata: {name: .metadata.name, namespace: .metadata.namespace, labels: .metadata.labels}, spec}' >"$_hpa_file"
    echo "HPA $hpa targets the aggregator: saved to $_hpa_file, deleting it for the run"
    kc delete hpa "$hpa" >&2
  else
    echo "no HPA targets the aggregator, nothing to pause"
  fi

  kc patch configmap "$CM" --type merge -p "{\"data\":{\"ACCESS_TOKEN_TTL_S\":\"$LOAD_TOKEN_TTL_S\",\"RATE_LIMIT_RPS\":\"$LOAD_RATE_LIMIT_RPS\",\"CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS\":\"$LOAD_MAX_INFLIGHT\"}}" >&2
  echo "ConfigMap $CM set for the run: ACCESS_TOKEN_TTL_S=$LOAD_TOKEN_TTL_S RATE_LIMIT_RPS=$LOAD_RATE_LIMIT_RPS CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS=$LOAD_MAX_INFLIGHT"
  rollout_wait
}

# load_restore: puts back the saved ConfigMap values and the HPA. Never fails.
load_restore() {
  if [ "$_cfg_saved" = 1 ]; then
    _cfg_saved=0
    echo "restoring ConfigMap $CM from $_cfg_file" >&2
    # Keys that were absent before get null (removed), the others get their old value.
    local patch
    patch=$(jq -nc --slurpfile s "$_cfg_file" '$s[0].data as $d | {data: (["ACCESS_TOKEN_TTL_S", "RATE_LIMIT_RPS", "CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS"] | map({key: ., value: ($d[.] // null)}) | from_entries)}')
    kc patch configmap "$CM" --type merge -p "$patch" >&2 || echo "WARN: could not restore the ConfigMap, run deploy/deploy.sh" >&2
    rollout_wait || echo "WARN: rollout after the restore did not finish" >&2
  fi
  if [ -n "$_hpa_file" ]; then
    echo "restoring the HPA from $_hpa_file" >&2
    kc create -f "$_hpa_file" >&2 || kc apply -f "$_hpa_file" >&2 || echo "WARN: could not restore the HPA, kubectl apply -f $_hpa_file" >&2
    _hpa_file=
  fi
  return 0
}

# share_check <compact.json>: prints the 200 share and returns 1 when it is below MIN_200_SHARE.
share_check() {
  local f=$1 share ok total
  share=$(jq -r '.share_200' "$f"); ok=$(jq -r '.ok_200' "$f"); total=$(jq -r '.requests' "$f")
  echo "200 share: $share ($ok of $total answers), minimum $MIN_200_SHARE"
  if awk -v s="$share" -v m="$MIN_200_SHARE" -v t="$total" 'BEGIN{exit !(t > 0 && s >= m)}'; then return 0; fi
  echo "!!!!!!!!!! FAIL: only $share of the answers were 200. The rate limit, the token or the in-flight cap is the bottleneck, so this run measures it and not the Aggregator. !!!!!!!!!!" >&2
  return 1
}

# audit_ids <base-url> <token> <source> <out-file>: every source_ref_id of a source, paged by cursor like tests/scenarios/audit-checksum.sh.
audit_ids() {
  local url=$1 token=$2 src=$3 out=$4 cursor="" page
  : >"$out"
  while :; do
    page=$(curl -sf -H "Authorization: Bearer $token" -G "$url/internal/audit/events" \
      --data-urlencode "source=$src" --data-urlencode limit=1000 --data-urlencode "cursor=$cursor") || return 1
    jq -r '.items[].source_ref_id' <<<"$page" >>"$out"
    cursor=$(jq -r '.next_cursor // empty' <<<"$page")
    [ -n "$cursor" ] || break
  done
}

# single_fetch <mock-log> <tolerance-ms>: reads the request lines of an upstream mock (they carry remote_addr, the Aggregator pod that called).
# Prints one JSON line. Per mock (one source) it counts data requests, callers, and pairs of requests from different callers whose time spans overlap.
# A span is the logged timestamp (the end) minus duration_ms. When a line has no duration_ms the span is tolerance-ms long.
# Only an overlap is a violation. A lease handover, where the old caller stops and a new one takes over, has no overlap and passes.
single_fetch() {
  jq -Rnc --argjson tol "$2" '
    def ms: capture("^(?<b>[0-9T:-]+)(\\.(?<f>[0-9]+))?(?<z>Z|[+-][0-9]{2}:[0-9]{2})$")
      | ((.b + "Z") | fromdateiso8601) as $s
      | (if .z == "Z" then 0 else (((.z[1:3] | tonumber) * 3600 + (.z[4:6] | tonumber) * 60) * (if .z[0:1] == "+" then 1 else -1 end)) end) as $o
      | (($s - $o) * 1000 + (((.f // "0") + "000")[0:3] | tonumber));
    [inputs | fromjson? | select(.message == "request" and ((.endpoint // "") | startswith("GET ")) and ((.endpoint // "") | test("health") | not))] as $reqs
    | ([$reqs[] | select((.remote_addr // "") == "")] | length) as $nocaller
    | ([$reqs[] | select((.remote_addr // "") != "")
        | {c: .remote_addr, e: (.timestamp | ms), d: (if (.duration_ms // 0) > 0 then .duration_ms else $tol end)}
        | .s = (.e - .d)] | sort_by(.s)) as $r
    | ([range(0; $r | length) as $i | range($i + 1; $r | length) as $j
        | select($r[$i].c != $r[$j].c and $r[$j].s < $r[$i].e and $r[$i].s < $r[$j].e)] | length) as $over
    | {total: ($reqs | length), without_caller: $nocaller, callers: ([$r[].c] | unique), overlapping_pairs: $over}
  ' <"$1"
}

# check_machine <vus>: warns when this machine probably cannot generate that many VUs.
# Rule of thumb 2 MB of RAM per VU plus 1 GB, one socket per VU plus 1000 spare.
check_machine() {
  local vus=$1 ok=1 avail_kb need_mb nofile
  avail_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
  need_mb=$((vus * 2 + 1024))
  nofile=$(ulimit -n)
  echo "machine check: $vus VUs need about ${need_mb} MB free RAM and ulimit -n >= $((vus + 1000))"
  echo "machine check: MemAvailable $((avail_kb / 1024)) MB, ulimit -n $nofile, cpus $(nproc)"
  if [ "$((avail_kb / 1024))" -lt "$need_mb" ]; then
    echo "WARN: not enough free RAM for $vus VUs" >&2; ok=0
  fi
  if [ "$nofile" != unlimited ] && [ "$nofile" -lt $((vus + 1000)) ]; then
    echo "WARN: ulimit -n $nofile is too low, try: ulimit -n $((vus + 5000))" >&2; ok=0
  fi
  if [ "$ok" = 0 ]; then
    echo "WARN: this machine may not generate $vus VUs. Run k6 on a second machine on the cluster network," >&2
    echo "      for example: k6 run -e BASE_URL=... -e EMAIL=... -e PASSWORD=... tests/load/p2-k8s.js (see tests/load/README.md)," >&2
    echo "      and say so in the report. Set FORCE=1 to continue here anyway." >&2
    [ "${FORCE:-0}" = 1 ] || return 1
  fi
  return 0
}

# run_k6 <summary-json-path>: runs p2-k8s.js with native k6 if installed, else the k6 container.
run_k6() {
  local summary=$1 root dir base
  root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
  dir=$(dirname "$summary"); base=$(basename "$summary")
  local envs=(-e BASE_URL="$BASE_URL" -e EMAIL="$EMAIL" -e PASSWORD="$PASSWORD" -e VUS="$VUS" -e HOLD="$HOLD" -e RAMP="$RAMP" -e THINK_S="$THINK_S")
  if command -v k6 >/dev/null 2>&1; then
    k6 run "${envs[@]}" -e SUMMARY_FILE="$summary" "$root/tests/load/p2-k8s.js"
  else
    local userns=()
    [ "$(docker info --format '{{.Host.Security.Rootless}}' 2>/dev/null)" = true ] && userns=(--userns=keep-id)
    docker run --rm --network host "${userns[@]}" -u "$(id -u):$(id -g)" --ulimit "nofile=$((VUS + 5000)):$((VUS + 5000))" \
      -v "$root/tests/load:/load" -v "$dir:/out" "${envs[@]}" -e SUMMARY_FILE="/out/$base" \
      "$K6_IMAGE" run /load/p2-k8s.js
  fi
}

# jqnum <file> <expr>
jqnum() { jq -r "$2" "$1"; }
