#!/usr/bin/env bash
# 2026-11-18 ClickHouse 장애 주입을 현재 구성(Compose spark, restart: on-failure)으로 재실행한다.
# 재시작은 사람이 하지 않고 Compose 재시작 정책에 맡긴다. 주입은 관리자 DDL·권한 변경뿐이다.
set -u
ROOT=/home/kang/projects/log-etlm-2
OUT=${1:?out dir}
mkdir -p "$OUT"
cd "$ROOT"
EV=$OUT/events.txt
CK=/opt/spark/checkpoints/ch-fault-20261126
ch() { docker compose exec -T clickhouse clickhouse-client -q "$1"; }
started() { docker inspect -f '{{.State.StartedAt}}' log-etlm-2-spark-1; }
T0=0
ev() { echo "$(date -u +%FT%T.%3N) $(awk -v n=$(date +%s.%N) -v t=$T0 'BEGIN{printf "%.3f", (t>0?n-t:0)}') $*" | tee -a "$EV"; }
at() { while (( $(date +%s) < T0i + $1 )); do sleep 0.2; done; }
# Spark 컨테이너가 다시 시작될 때까지 기다린다(종료 → Compose 재시작).
wait_restart() { local s0=$1; until [ "$(started)" != "$s0" ]; do sleep 0.2; done; ev "spark_restarted restart_count=$(docker inspect -f '{{.RestartCount}}' log-etlm-2-spark-1) exit=$(docker inspect -f '{{.State.ExitCode}}' log-etlm-2-spark-1)"; }

SPARK_CHECKPOINT_DIR=$CK SPARK_STARTING_OFFSETS=latest docker compose up -d spark >/dev/null 2>&1
ev spark_started
until [ "$(ch "select count() from ops.spark_batch_attempts where callback_started_at > now() - interval 2 minute and status='succeeded'")" -gt 0 ] 2>/dev/null; do sleep 2; done
ev sources_ready
ch "CREATE TABLE IF NOT EXISTS default.fi_null (x UInt8) ENGINE = Null"

cd apps/log_generator
PRODUCER_EPS=5000 PRODUCER_MAX_EVENTS=4500000 PRODUCER_LOAD_PROFILE= .venv/bin/log-generator > /dev/null 2> "$OUT/gen.err" &
G=$!
cd "$ROOT"
T0=$(date +%s.%N); T0i=${T0%.*}
ev generator_started

at 240; ch "CREATE MATERIALIZED VIEW default.fi_delay TO default.fi_null AS SELECT toUInt8(sleep(3)) AS x FROM silver.order_events"; ev inject_write_delay
at 300; ch "DROP VIEW default.fi_delay"; ev revert_write_delay

at 420; s=$(started); ch "CREATE MATERIALIZED VIEW default.fi_throw TO default.fi_null AS SELECT toUInt8(throwIf(1)) AS x FROM silver.order_events"; ev inject_stored_then_fail
until [ "$(docker inspect -f '{{.State.Running}}' log-etlm-2-spark-1)" = false ] || [ "$(started)" != "$s" ]; do sleep 0.2; done
ch "DROP VIEW default.fi_throw"; ev revert_stored_then_fail
wait_restart "$s"

at 600; s=$(started); ch "REVOKE INSERT ON silver.order_events FROM spark_writer"; ev inject_access_denied
until [ "$(docker inspect -f '{{.State.Running}}' log-etlm-2-spark-1)" = false ] || [ "$(started)" != "$s" ]; do sleep 0.2; done
ch "GRANT INSERT ON silver.order_events TO spark_writer"; ev revert_access_denied
wait_restart "$s"

wait $G; ev "generator_exited code=$?"
for i in $(seq 1 90); do
  b=$(ch "select num_input_rows from ops.spark_query_progress order by trigger_at desc limit 1" 2>/dev/null)
  [ "$b" = 0 ] && { ev "drained last_input_rows=0"; break; }; sleep 2
done
ch "DROP TABLE IF EXISTS default.fi_null"
ev "done restart_count=$(docker inspect -f '{{.RestartCount}}' log-etlm-2-spark-1) grants=$(ch 'show grants for spark_writer' | grep -c order_events)"
