#!/usr/bin/env bash
# 2026-11-18 멈춤 감시·자동 재시작 검증(S1~S3)을 현재 구성(restart: on-failure, 상한 없음)으로 재실행한다.
# PRD 04 '멈춤 감시·자동 재시작 검증'. 재시작은 Compose에 맡기고, 스크립트는 장애 주입·되돌림만 한다.
# 사용: run_dlq_stall_fault.sh <출력 디렉터리>
set -u
ROOT=/home/kang/projects/log-etlm-2
OUT=${1:?out dir}
mkdir -p "$OUT"
cd "$ROOT"
EV=$OUT/events.txt
CK=/opt/spark/checkpoints/ch-stall-20261126
DLQ=dlq.ecommerce.orders.v1
T0=0
ev() { echo "$(date -u +%FT%T.%3N) $(awk -v n=$(date +%s.%N) -v t=$T0 'BEGIN{printf "%.3f", (t>0?n-t:0)}') $*" | tee -a "$EV"; }
at() { while (( $(date +%s) < T0i + $1 )); do sleep 0.2; done; }
ch() { docker compose exec -T clickhouse clickhouse-client -q "$1"; }
started() { docker inspect -f '{{.State.StartedAt}}' log-etlm-2-spark-1; }
rc() { docker inspect -f '{{.RestartCount}}' log-etlm-2-spark-1; }
# DLQ 토픽 메시지 크기 상한을 바꾸거나(값) 되돌린다(빈 값).
dlq_cfg() {
  local a="--add-config max.message.bytes=$1"; [ -z "$1" ] && a="--delete-config max.message.bytes"
  docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type topics --entity-name $DLQ --alter $a > /dev/null
}
# 컨테이너 재시작 횟수가 바뀔 때마다 기록하는 감시(백그라운드).
watch_restarts() { local last; last=$(rc); while :; do local n; n=$(rc); [ "$n" != "$last" ] && { ev "spark_restarted restart_count=$n"; last=$n; }; sleep 0.5; done; }

SPARK_CHECKPOINT_DIR=$CK SPARK_STARTING_OFFSETS=latest docker compose up -d spark > /dev/null 2>&1
ev spark_started
until [ "$(ch "select count() from ops.spark_batch_attempts where callback_started_at > now() - interval 2 minute and status='succeeded'")" -gt 0 ] 2>/dev/null; do sleep 2; done
ev "sources_ready restart_count=$(rc)"

cd apps/log_generator
PRODUCER_EPS=2000 PRODUCER_MAX_EVENTS=1320000 PRODUCER_LOAD_PROFILE= .venv/bin/log-generator > /dev/null 2> "$OUT/gen.err" &
G=$!
cd "$ROOT"
T0=$(date +%s.%N); T0i=${T0%.*}
ev generator_started
watch_restarts & W=$!

# S1 태스크 멈춤: Python worker를 멈춰 진행 이벤트를 끊는다. 감시(90초)가 종료시키면 재시작으로 풀린다.
at 60; docker compose exec -T spark pkill -STOP -f pyspark.daemon; ev inject_s1_sigstop

# S2 DLQ 장애 유지: 80초 유지 후 되돌린다.
at 240; dlq_cfg 100; ev inject_s2_dlq_too_small
at 320; dlq_cfg ""; ev revert_s2

# S3 재시도 중 되돌림: 19초 뒤 되돌린다.
at 450; dlq_cfg 100; ev inject_s3_dlq_too_small
at 469; dlq_cfg ""; ev revert_s3

wait $G; ev "generator_exited code=$?"
sleep 60
kill $W 2>/dev/null
ev "done restart_count=$(rc) running=$(docker inspect -f '{{.State.Running}}' log-etlm-2-spark-1)"
