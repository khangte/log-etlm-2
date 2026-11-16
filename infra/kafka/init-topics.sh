#!/bin/bash
# 원본·DLQ 토픽을 만든다. 기존 토픽은 삭제하거나 설정을 바꾸지 않는다(--if-not-exists).
# 설정 변경은 별도 kafka-configs.sh --alter 또는 볼륨 초기화로 적용한다.
# 필요 환경변수: KAFKA_TOPIC, KAFKA_DLQ_TOPIC (compose.yaml이 주입한다.)
set -euo pipefail

BOOTSTRAP="kafka:19092"

# 공통 설정: 복제 계수·ISR은 단일 브로커에 맞추고, 브로커 기록 시각(LogAppendTime)으로 지연을 계산한다.
# 보관 크기는 파티션당이며 삭제는 세그먼트 단위(128MiB 또는 5분 롤링)로 비동기 처리된다.
create_topic() {
  local topic="$1" partitions="$2" retention_ms="$3" retention_bytes="$4"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$topic" \
    --partitions "$partitions" --replication-factor 1 \
    --config min.insync.replicas=1 \
    --config message.timestamp.type=LogAppendTime \
    --config cleanup.policy=delete \
    --config "retention.ms=$retention_ms" \
    --config "retention.bytes=$retention_bytes" \
    --config segment.bytes=134217728 \
    --config segment.ms=300000
}

# 원본: 3파티션, 1시간, 파티션당 8GiB
create_topic "$KAFKA_TOPIC" 3 3600000 8589934592
# DLQ: 1파티션, 6시간, 12GiB
create_topic "$KAFKA_DLQ_TOPIC" 1 21600000 12884901888
