#!/bin/bash
# 호스트(WSL VM) 멈춤 감지기. 1초마다 벽시계를 읽고, 직전 틱과의 간격이 임계값 이상이면
# 멈춤 1건으로 보고 /proc 압박 지표와 함께 ops.host_stalls에 INSERT하고 stdout에도 남긴다.
# 필요 환경변수: CLICKHOUSE_MONITOR_PASSWORD, HOST_STALL_THRESHOLD_MS(기본 2000)
# 근거: docs/prd/04-clickhouse-storage.md '호스트 멈춤 기록'
set -uo pipefail

threshold_ms="${HOST_STALL_THRESHOLD_MS:-2000}"

# 현재 벽시계를 밀리초로 출력한다.
now_ms() {
  echo $(( $(date +%s%N) / 1000000 ))
}

# /proc/pressure/<자원>에서 지정한 줄(some|full)의 avg10 값을 출력한다. 파일이 없으면 0.
psi_avg10() {
  awk -v kind="$2" '$1 == kind { sub("avg10=", "", $2); print $2; found = 1 } END { if (!found) print 0 }' \
    "/proc/pressure/$1" 2>/dev/null || echo 0
}

# 멈춤 1건을 TSV 한 줄로 만든다. 인자: 깨어난 시각(ms), 간격(ms)
stall_row() {
  local mem_kb load1
  mem_kb=$(awk '$1 == "MemAvailable:" { print $2 }' /proc/meminfo)
  load1=$(cut -d' ' -f1 /proc/loadavg)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(date -u -d "@$(( $1 / 1000 )).$(printf '%03d' $(( $1 % 1000 )))" '+%F %T.%3N')" "$2" \
    "$(psi_avg10 cpu some)" "$(psi_avg10 memory some)" "$(psi_avg10 memory full)" \
    "$(psi_avg10 io some)" "$(psi_avg10 io full)" "$(( mem_kb / 1024 ))" "$load1"
}

# 행을 ClickHouse에 INSERT한다. 실패는 경고만 남긴다(지표 손실 허용).
insert_row() {
  timeout 30 clickhouse-client --host clickhouse --user monitor_writer --password "$CLICKHOUSE_MONITOR_PASSWORD" \
    --query "INSERT INTO ops.host_stalls FORMAT TSV" <<<"$1" \
    || echo "host-monitor: WARN insert failed: $1" >&2
}

echo "host-monitor: started threshold_ms=$threshold_ms"
prev=$(now_ms)
while true; do
  sleep 1
  now=$(now_ms)
  gap=$(( now - prev ))
  prev=$now
  if (( gap >= threshold_ms )); then
    row=$(stall_row "$now" "$gap")
    echo "host-monitor: stall $row"
    # 루프가 INSERT 응답을 기다리느라 다음 틱을 놓치지 않게 백그라운드로 보낸다.
    insert_row "$row" &
  fi
done
