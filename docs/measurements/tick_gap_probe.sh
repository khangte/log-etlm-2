#!/usr/bin/env bash
# host-monitor와 같은 1초 루프로 틱 간격을 재되, CPU 제한 없이 실행해 감지기 자신의 throttle 영향을 가린다.
# PRD 03 'daily4 VM 지연 원인 조사' 감지기 비교 실험. ClickHouse에는 쓰지 않고 파일에만 남긴다.
# 사용: tick_gap_probe.sh <출력 파일> [기록 임계 ms=1500]
# 줄 형식: <깨어난 UTC 시각> <간격 ms> <CLOCK_MONOTONIC 간격 ms>
set -u
OUT=${1:?출력 파일}
threshold_ms=${2:-1500}

# 벽시계(ms)와 /proc/uptime 기반 단조 시각(ms)을 함께 읽는다.
now_ms() { echo $(( $(date +%s%N) / 1000000 )); }
up_ms() { local u; u=$(cut -d' ' -f1 /proc/uptime); echo $(( ${u%.*} * 1000 + 10#${u#*.} * 10 )); }

echo "probe started $(date -u +%FT%T.%3N) threshold_ms=$threshold_ms" >> "$OUT"
prev=$(now_ms); prev_up=$(up_ms)
while true; do
  sleep 1
  now=$(now_ms); now_up=$(up_ms)
  gap=$(( now - prev )); gap_up=$(( now_up - prev_up ))
  prev=$now; prev_up=$now_up
  if (( gap >= threshold_ms )); then
    echo "$(date -u -d "@$(( now / 1000 )).$(printf '%03d' $(( now % 1000 )))" +%FT%T.%3N) $gap $gap_up" >> "$OUT"
  fi
done
