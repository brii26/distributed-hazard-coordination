#!/usr/bin/env bash
# Crash and duplicate checks for the field report consumer, all on this machine.
# Needs podman with postgres:16-alpine and rabbitmq:3.13-management-alpine already pulled (never pulls),
# plus go. Builds the aggregator, the processor and a small publisher from the repo sources.
# Usage: tests/scenarios/field-reports.sh [N=1000]
# Keep go offline by setting GOMODCACHE, GOFLAGS, GOPROXY and GOWORK yourself. Stops everything it starts.
set -u
N=${1:-1000}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
W=$(mktemp -d "$ROOT/.scenario-XXXX"); PIDS=(); FAIL=0
PG=55432; AMQP=55672; GRPC=59091
DBURL="postgres://postgres:x@localhost:$PG/postgres?sslmode=disable"
MQ="amqp://t:t@localhost:$AMQP/"
cleanup() { for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done; podman rm -f fr-pg fr-mq >/dev/null 2>&1; rm -rf "$W"; }
trap cleanup EXIT
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1 ($2)"; FAIL=1; }
sql() { podman exec fr-pg psql -U postgres -tAc "$1"; }
q() { RABBITMQ_URL=$MQ "$W/pub" depth 2>/dev/null; }
pub() { RABBITMQ_URL=$MQ "$W/pub" pub "$@"; }
count() { sql "select count(*)||'|'||count(distinct report_id) from field_reports where report_id like '$1%'"; }
wait_for() { for _ in $(seq 1 "${3:-120}"); do eval "$1" && return 0; sleep 0.5; done; bad "$2" timeout; return 1; }
start_proc() { RABBITMQ_URL=$MQ AGGREGATOR_ADDR="localhost:$GRPC" HEALTH_PORT=58096 "$W/proc" >>"$W/proc.log" 2>&1 & PROC=$!; PIDS+=("$PROC"); }
start_agg() {
  DATABASE_URL="$DBURL" RABBITMQ_URL=$MQ GRPC_PORT=$GRPC HEALTH_PORT=58091 BMKG_BASE_URL=http://localhost:1 PVMBG_BASE_URL=http://localhost:1 \
    BMKG_API_KEY=x PVMBG_OAUTH_CLIENT_ID=x PVMBG_OAUTH_CLIENT_SECRET=x PVMBG_ONE_TIME_CODE=x PVMBG_TOKEN_VOLUME_PATH="$W/tok.json" \
    "$W/agg" >>"$W/agg.log" 2>&1 &
  AGG=$!; PIDS+=("$AGG")
}

podman run -d --rm --pull=never --name fr-pg -p $PG:5432 -e POSTGRES_PASSWORD=x docker.io/library/postgres:16-alpine >/dev/null || exit 2
podman run -d --rm --pull=never --name fr-mq -p $AMQP:5672 -e RABBITMQ_DEFAULT_USER=t -e RABBITMQ_DEFAULT_PASS=t docker.io/library/rabbitmq:3.13-management-alpine >/dev/null || exit 2
(cd "$ROOT/services/aggregator" && go build -o "$W/agg" .) || exit 2
(cd "$ROOT/services/field-report-processor" && go build -o "$W/proc" . && go build -o "$W/pub" ./cmd/fieldpub) || exit 2
wait_for "sql 'select 1' >/dev/null 2>&1" pg-up 120
sleep 3
for f in "$ROOT"/services/aggregator/migrations/*.sql; do podman exec -i fr-pg psql -U postgres -q <"$f" >/dev/null 2>&1; done
start_proc
wait_for "[ -n \"\$(q)\" ]" queue-declared 240 || exit 2
start_agg

echo "== smoke"
pub smoke-1
wait_for "[ \"\$(count smoke-)\" = '1|1' ]" smoke && ok smoke

echo "== crash before commit (aggregator frozen, processor killed -9)"
kill -STOP "$AGG"
pub cb-1
wait_for "[ \"\$(q)\" = 0 ]" cb-delivered
sleep 1
kill -9 "$PROC" "$AGG"
sleep 1
start_agg
start_proc
wait_for "[ \"\$(count cb-)\" = '1|1' ]" cb-stored && wait_for "[ \"\$(q)\" = 0 ]" cb-queue && ok "crash before commit: 1 row, queue 0"

echo "== duplicate delivery"
pub dup-1
pub dup-1
wait_for "[ \"\$(count dup-)\" = '1|1' ]" dup-stored
sleep 1
[ "$(count dup-)" = '1|1' ] && [ "$(q)" = 0 ] && ok "duplicate: 1 row, queue 0" || bad duplicate "$(count dup-)"

echo "== poison between good reports"
pub p-1
pub p-bad1 '{"report_id":'
pub p-bad2 BIG
pub p-bad3 "$(printf '{"report_id":"p-bad3","description":"\xff\xfe"}')"
pub p-2
wait_for "[ \"\$(count p-)\" = '2|2' ]" poison-good
sleep 1
parked=$(sql "select count(*) from failed_messages where source='FIELD_REPORT' and status='parked'")
{ [ "$parked" = 3 ] && [ "$(q)" = 0 ] && kill -0 "$PROC" 2>/dev/null; } && ok "poison: 2 good stored, 3 parked, queue 0, processor alive" || bad poison "parked=$parked"

echo "== poison that used to loop for ever: NUL byte and a 5 MiB body"
pub n-1
pub n-nul NUL
pub n-huge HUGE
pub n-2
wait_for "[ \"\$(count n-)\" = '2|2' ]" nul-good
wait_for "[ \"\$(q)\" = 0 ]" nul-queue
sleep 2
parked=$(sql "select count(*) from failed_messages where source='FIELD_REPORT' and status='parked'")
size=$(sql "select payload_size from failed_messages where source='FIELD_REPORT' and reason like 'body too large%'")
loops=$(grep "save failed" "$W/proc.log" | grep -c "n-nul\|n-huge")
{ [ "$parked" = 5 ] && [ "$(q)" = 0 ] && [ "$size" = 65536 ] && [ "$loops" -le 5 ] && kill -0 "$PROC" 2>/dev/null; } && ok "NUL and 5 MiB: both parked (total 5), others stored, queue 0, body kept 64 KiB, no endless retry ($loops failures)" || bad nul-huge "parked=$parked size=$size loops=$loops"

echo "== broker restart in the middle of 200 (processor not restarted)"
for i in $(seq 1 100); do pub br-$i; done
podman restart fr-mq >/dev/null
wait_for "[ -n \"\$(q)\" ]" mq-back 240
for i in $(seq 101 200); do pub br-$i; done
wait_for "[ \"\$(count br-)\" = '200|200' ]" broker-restart 240 && kill -0 "$PROC" 2>/dev/null && ok "broker restart: 200 stored once" || bad broker "$(count br-)"

echo "== acceptance: $N reports, processor and aggregator killed 5 times at random moments"
step=$((N / 6))
for i in $(seq 1 "$N"); do
  pub ac-$i
  if [ $((i % step)) -eq 0 ] && [ $((i / step)) -le 5 ]; then
    kill -9 "$PROC" "$AGG" 2>/dev/null
    sleep $((RANDOM % 2))
    start_agg
    start_proc
  fi
done
wait_for "[ \"\$(count ac-)\" = '$N|$N' ]" acceptance 1200 && ok "acceptance: $(count ac-) rows (total|distinct)" || bad acceptance "$(count ac-)"
sleep 1
[ "$(q)" = 0 ] || bad acceptance-queue "$(q)"
[ $FAIL = 0 ] && echo ALL PASS || echo SOME FAILED
exit $FAIL
