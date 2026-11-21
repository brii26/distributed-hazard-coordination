#!/usr/bin/env bash
# Shows that the Situation Summarizer never disturbs the critical alert path (#326). Run it on the cluster by hand, never in CI.
#
#   FAKE_LLM_IMAGE=10.77.0.11:5000/situation-summarizer:0.1.0 \
#   BASE_URL=http://10.77.0.11:30081 EMAIL=operator@bnpb.example PASSWORD=... tests/bonus/ai-isolation.sh
#
# Phases, each WINDOW_S seconds (300) of the live Sensor Pantai critical alerts:
#   baseline       AI_ENABLED=false, the summarizer runs and builds the rule based summary
#   slow           fake LLM sleeps 60 s per call
#   invalid-json   fake LLM answers a body that is not JSON
#   dead           fake LLM closes every connection
#   pod-killed     the summarizer is scaled to 0
# For each phase it reads the Notifier's critical_alert_latency_seconds histogram before and after, takes the p95 of the alerts delivered in
# between (the bucket upper bound that holds the 95th percentile, from ingest at the sensor to delivery) and compares it with the baseline.
# A phase passes when its p95 is at most baseline * (1 + TOLERANCE_PCT/100), and it needs MIN_ALERTS alerts to count. While each phase runs,
# GET /situation is called every 2 s (needs BASE_URL, EMAIL, PASSWORD) and every answer must be under 300 ms.
# The script patches Secret llm-credentials with a fake key and restores the old value, the AI settings and the replica count on exit.
#
# Env: NS, KUBECTL (kubectl), WINDOW_S (300), MIN_ALERTS (20), TOLERANCE_PCT (25), FAKE_LLM_IMAGE (required),
#      DRY_RUN=1 prints every command that changes the cluster instead of running it (reads still go through KUBECTL, sleeps are skipped).
# Needs kubectl pointed at the cluster, curl, jq and awk. Exit 0 when every phase passes, 1 when one fails, 2 on a usage error.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS=${NS:-sistem-informasi-kebencanaan-bnpb}
KUBECTL=${KUBECTL:-kubectl}
WINDOW_S=${WINDOW_S:-300}
MIN_ALERTS=${MIN_ALERTS:-20}
TOLERANCE_PCT=${TOLERANCE_PCT:-25}
DRY_RUN=${DRY_RUN:-0}
FAKE_LLM_IMAGE=${FAKE_LLM_IMAGE:?set FAKE_LLM_IMAGE, the situation-summarizer image (it holds /fakellm)}
FAKE_KEY=fake-test-key

kc() { "$KUBECTL" -n "$NS" "$@"; }

# mutate <args...>: a command that changes the cluster, only printed when DRY_RUN=1.
mutate() {
  if [ "$DRY_RUN" = 1 ]; then echo "DRY_RUN: kubectl -n $NS $*" >&2; else kc "$@" >&2; fi
}

pause() { if [ "$DRY_RUN" = 1 ]; then echo "DRY_RUN: would wait $1 s" >&2; else sleep "$1"; fi; }

# The Notifier's latency histogram, lines "le count" with +Inf last.
histogram() {
  kc exec deployment/notifier -- wget -qO- http://localhost:8094/metrics \
    | awk -F'[{}="]+' '/^critical_alert_latency_seconds_bucket/ {split($0, a, " "); print $3, a[2]}'
}

# p95 <before> <after>: prints "p95_seconds total", p95 is "inf" above the last bucket and "none" without alerts.
p95() {
  join -j1 <(printf '%s\n' "$1" | sort) <(printf '%s\n' "$2" | sort) \
    | awk '{ le = $1; d = $3 - $2; if (le == "+Inf") total = d; else { b[n] = le; c[n++] = d } }
           END { if (total <= 0) { print "none 0"; exit }
                 for (i = 0; i < n; i++) if (c[i] >= 0.95 * total) { print b[i], total; exit }
                 print "inf", total }'
}

login_probe() {
  [ -n "${BASE_URL:-}" ] && [ -n "${EMAIL:-}" ] && [ -n "${PASSWORD:-}" ] || return 1
  JAR=$(mktemp); trap_files+=("$JAR")
  curl -s -c "$JAR" -X POST "$BASE_URL/auth/login" -H 'Content-Type: application/json' -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" >/dev/null
}

# probe_situation: one GET /situation, prints "<http code> <seconds>".
probe_situation() { curl -s -b "$JAR" -o /dev/null -w '%{http_code} %{time_total}\n' --max-time 5 "$BASE_URL/situation"; }

old_key=""
trap_files=()
# shellcheck disable=SC2329 # trap handler
restore() {
  trap - EXIT INT TERM
  echo "restoring" >&2
  rm -f "${trap_files[@]}" 2>/dev/null
  mutate delete -f "$MANIFEST" --ignore-not-found
  mutate set env deployment/situation-summarizer AI_ENABLED=false LLM_API_URL=
  mutate scale deployment/situation-summarizer --replicas=1
  if [ -n "$old_key" ]; then mutate patch secret llm-credentials --type merge -p "{\"data\":{\"api_key\":\"$old_key\"}}"; fi
}

MANIFEST=$(mktemp); trap_files+=("$MANIFEST")
sed "s|FAKE_LLM_IMAGE|$FAKE_LLM_IMAGE|" "$HERE/fake-llm.yaml" >"$MANIFEST"
trap restore EXIT INT TERM

JAR=""
if login_probe; then have_probe=1; else have_probe=0; echo "no BASE_URL, EMAIL and PASSWORD: GET /situation is not probed" >&2; fi

old_key=$(kc get secret llm-credentials -o jsonpath='{.data.api_key}' 2>/dev/null || true)
mutate patch secret llm-credentials --type merge -p "{\"data\":{\"api_key\":\"$(printf '%s' "$FAKE_KEY" | base64)\"}}"
mutate apply -f "$MANIFEST"
mutate rollout status deployment/fake-llm --timeout=120s
mutate set env deployment/situation-summarizer LLM_API_URL=http://fake-llm:9000
mutate scale deployment/situation-summarizer --replicas=1

declare -A P95 TOTAL SLOW
failed=0

# phase <name> <command...>: runs the setup command, waits WINDOW_S while probing /situation, records the p95.
phase() {
  local name=$1; shift
  echo "== phase $name" >&2
  "$@"
  mutate rollout status deployment/situation-summarizer --timeout=120s

  local before after slow=0 end=$((SECONDS + WINDOW_S)) line
  before=$(histogram)
  if [ "$DRY_RUN" = 1 ]; then
    pause "$WINDOW_S"
    [ "$have_probe" = 1 ] && probe_situation >/dev/null
  else
    while [ "$SECONDS" -lt "$end" ]; do
      if [ "$have_probe" = 1 ]; then
        line=$(probe_situation)
        awk -v l="$line" 'BEGIN { split(l, a, " "); exit !(a[2] > 0.3) }' && { slow=$((slow + 1)); echo "slow answer: $line" >&2; }
      fi
      sleep 2
    done
  fi
  after=$(histogram)
  read -r "P95[$name]" "TOTAL[$name]" < <(p95 "$before" "$after")
  SLOW[$name]=$slow
}

# shellcheck disable=SC2329 # called through phase
setup_baseline() { mutate set env deployment/situation-summarizer AI_ENABLED=false; }
# shellcheck disable=SC2329
setup_llm() { fake_mode "$1"; mutate set env deployment/situation-summarizer AI_ENABLED=true; }
# shellcheck disable=SC2329
setup_killed() { mutate scale deployment/situation-summarizer --replicas=0; }
# shellcheck disable=SC2329
fake_mode() { mutate set env deployment/fake-llm "MODE=$1"; mutate rollout status deployment/fake-llm --timeout=120s; }

phase baseline setup_baseline
phase slow setup_llm slow
phase invalid-json setup_llm invalid-json
phase dead setup_llm dead
phase pod-killed setup_killed

printf '\n%-14s %10s %8s %10s %s\n' PHASE P95_SECONDS ALERTS SLOW_SITUATION RESULT
base=${P95[baseline]}
for name in baseline slow invalid-json dead pod-killed; do
  res=ok
  if [ "${TOTAL[$name]}" -lt "$MIN_ALERTS" ]; then
    res="FAIL: only ${TOTAL[$name]} alerts, need $MIN_ALERTS"
  elif [ "${SLOW[$name]}" -gt 0 ]; then
    res="FAIL: GET /situation took over 300 ms ${SLOW[$name]} times"
  elif [ "$name" != baseline ]; then
    awk -v p="${P95[$name]}" -v b="$base" -v t="$TOLERANCE_PCT" 'BEGIN { exit !(p != "inf" && b != "inf" && p <= b * (1 + t / 100)) }' \
      || res="FAIL: p95 ${P95[$name]} s against baseline $base s"
  fi
  case $res in FAIL*) failed=1 ;; esac
  printf '%-14s %10s %8s %10s %s\n' "$name" "${P95[$name]}" "${TOTAL[$name]}" "${SLOW[$name]}" "$res"
done

exit "$failed"
