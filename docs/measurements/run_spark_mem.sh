#!/usr/bin/env bash
# steady.csv 부하 중 Spark 힙(jstat)·storage memory·cgroup 메모리·Python 워커 RSS를 수집한다.
set -u
ROOT=/home/kang/projects/log-etlm-2
OUT=${1:?out dir}
PROFILE=${2:-steady.csv}
mkdir -p "$OUT"
CS="log-etlm-2-spark-1 log-etlm-2-clickhouse-1 log-etlm-2-kafka-1"
ts() { date -u +%Y-%m-%dT%H:%M:%S.%3N; }

cg() {  # analyze_profile.py 형식: 첫 줄 시각, 이후 "이름 키 값 ..."
  echo "$(ts)"
  for c in $CS; do
    echo "$c $(docker exec $c sh -c 'cat /sys/fs/cgroup/cpu.stat; echo peak $(cat /sys/fs/cgroup/memory.peak); grep oom_kill /sys/fs/cgroup/memory.events' | tr '\n' ' ')"
  done
}

cd "$ROOT"
cg > "$OUT/cg_before.txt"

# 1초 cgroup·VM 커널 기록(PRD 03 'daily 동시 정지 원인 조사' 다음 단계 1). 분석은 host_1s_summary.py.
ARGS=$(for c in kafka spark clickhouse; do echo -n "$c=$(docker inspect -f '{{.Id}}' log-etlm-2-$c-1) "; done)
docker rm -f host-1s-sampler >/dev/null 2>&1
docker run -d --name host-1s-sampler --privileged --cgroupns=host --pid=host --cpus 0.5 \
  -v "$ROOT/docs/measurements/sample_host_1s.sh:/s.sh:ro" -v "$(realpath "$OUT"):/out" \
  --entrypoint /bin/bash clickhouse/clickhouse-server:26.8.38.9 /s.sh /out/host_1s.txt $ARGS > /dev/null

# 감지기 비교(PRD 03 'daily4 VM 지연 원인 조사'): CPU 제한 없는 컨테이너 감지기와 WSL 셸 감지기.
docker rm -f tick-probe-nolimit >/dev/null 2>&1
docker run -d --name tick-probe-nolimit \
  -v "$ROOT/docs/measurements/tick_gap_probe.sh:/p.sh:ro" -v "$(realpath "$OUT"):/out" \
  --entrypoint /bin/bash clickhouse/clickhouse-server:26.8.38.9 /p.sh /out/tick_container.txt > /dev/null
bash "$ROOT/docs/measurements/tick_gap_probe.sh" "$OUT/tick_wsl.txt" &
P=$!

docker exec log-etlm-2-spark-1 jstat -gc -t 1 1000 | while read -r l; do echo "$(ts) $l"; done > "$OUT/jstat.txt" &
J=$!
docker stats --format '{{.Name}} {{.CPUPerc}}' $CS | while read -r l; do
  l=$(echo "$l" | sed 's/\x1b\[[0-9;]*[A-Za-z]//g'); [ -n "$l" ] && echo "$(ts) $l"; done > "$OUT/docker_stats.txt" &
D=$!
(
  APP=$(curl -s 127.0.0.1:4040/api/v1/applications | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["id"])')
  while :; do
    t=$(ts)
    curl -s "127.0.0.1:4040/api/v1/applications/$APP/executors" | python3 -c "import json,sys
for e in json.load(sys.stdin): print('$t', e['id'], e['memoryUsed'], e['maxMemory'], e['diskUsed'], e.get('memoryMetrics',{}).get('usedOnHeapStorageMemory',''), e.get('peakMemoryMetrics',{}).get('JVMHeapMemory',''), e.get('peakMemoryMetrics',{}).get('JVMOffHeapMemory',''))" >> "$OUT/storage.txt"
    docker exec log-etlm-2-spark-1 sh -c "echo $t \$(grep -E '^(anon|file|shmem|kernel|sock) ' /sys/fs/cgroup/memory.stat | tr '\n' ' ') current \$(cat /sys/fs/cgroup/memory.current)" >> "$OUT/memstat.txt"
    docker exec log-etlm-2-spark-1 sh -c "ps -eo rss=,comm=,args= | awk -v t=$t '{print t, \$1, \$2, \$NF}'" >> "$OUT/rss.txt"
    sleep 5
  done
) &
S=$!
( while :; do echo "=== $(ts)"; docker exec log-etlm-2-spark-1 jcmd 1 VM.native_memory summary scale=MB; sleep 30; done ) > "$OUT/nmt.txt" 2>&1 &
N=$!
# 호스트 경합 원인 확인용: CPU 상위 프로세스 10개(WSL 안에서 보이는 것만)
( while :; do echo "=== $(ts)"; top -b -n1 -o %CPU -w 200 | sed -n '8,17p'; sleep 5; done ) > "$OUT/host_top.txt" 2>&1 &
H=$!

cd "$ROOT/apps/log_generator"
PRODUCER_MAX_EVENTS=0 PRODUCER_LOAD_PROFILE="$ROOT/apps/log_generator/profiles/$PROFILE" .venv/bin/log-generator > /dev/null 2> "$OUT/gen.err" &
G=$!
(while kill -0 $G 2>/dev/null; do
  [ -r /proc/$G/stat ] && echo "$(ts) $G $(awk '{print $14, $15}' /proc/$G/stat)"; sleep 1; done) > "$OUT/gen_cpu.txt" &
wait $G
echo "generator exit $?" >> "$OUT/gen.err"
sleep 30  # 남은 배치 처리
cd "$ROOT"; cg > "$OUT/cg_after.txt"
kill $J $D $S $N $H 2>/dev/null; pkill -f "jstat -gc" ; docker exec log-etlm-2-spark-1 pkill jstat
docker rm -f host-1s-sampler tick-probe-nolimit > /dev/null
kill $P 2>/dev/null
echo done
