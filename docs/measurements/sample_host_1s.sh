#!/usr/bin/env bash
# 1초마다 컨테이너 cgroup·VM 커널 누적값을 그대로 기록한다(PRD 03 'daily 동시 정지 원인 조사' 다음 단계 1).
# privileged 1회성 컨테이너(--cgroupns=host --pid=host) 안에서 실행한다. delta 계산은 host_1s_summary.py가 한다.
# 사용: sample_host_1s.sh <출력 파일> <이름=컨테이너ID> ...
# 줄 형식(공백 구분, 첫 필드는 /proc/uptime 초):
#   <up> T <UTC ISO>                                   수집 시작
#   <up> C <이름> io <장치> rbytes wbytes rios wios     장치별 io.stat
#   <up> C <이름> iop <some_total> <full_total>         io.pressure(마이크로초 누적)
#   <up> C <이름> cpup <some_total> <full_total>        cpu.pressure
#   <up> C <이름> cpu <usage_usec> <nr_throttled> <throttled_usec>
#   <up> C <이름> mem <pgmajfault> <pswpin> <file_dirty> <file_writeback>
#   <up> V vm <nr_dirty> <nr_writeback> <nr_dirtied> <nr_written> <pswpin> <pswpout> <pgmajfault>
#   <up> V psi <io_some> <io_full> <cpu_some> <mem_some> <mem_full>   (/proc/pressure total)
#   <up> V blk <장치> <stat 17필드>
#   <up> D <pid> <state> <wchan> <comm>                 D 상태 task
set -u
OUT=${1:?출력 파일}; shift
CG=/mnt/cg
mkdir -p "$CG"
mountpoint -q "$CG" || mount -t cgroup2 none "$CG"

declare -A DIR
for pair in "$@"; do
    name=${pair%%=*}; id=${pair#*=}
    DIR[$name]=$CG/docker/$id
    [ -d "${DIR[$name]}" ] || { echo "cgroup 없음: $name $id" >&2; exit 1; }
done

# pressure 파일의 some·full total 값만 꺼낸다.
psi() { awk '{for(i=2;i<=NF;i++) if ($i ~ /^total=/) {sub("total=","",$i); printf "%s ", $i}}' "$1"; }

tick() {
    local up name d
    up=$(cut -d' ' -f1 /proc/uptime)
    echo "$up T $(date -u +%Y-%m-%dT%H:%M:%S.%3N)"
    for name in "${!DIR[@]}"; do
        d=${DIR[$name]}
        awk -v u="$up" -v n="$name" '{r=w=ri=wi=0; for(i=2;i<=NF;i++){split($i,a,"="); if(a[1]=="rbytes")r=a[2]; else if(a[1]=="wbytes")w=a[2]; else if(a[1]=="rios")ri=a[2]; else if(a[1]=="wios")wi=a[2]} print u, "C", n, "io", $1, r, w, ri, wi}' "$d/io.stat"
        echo "$up C $name iop $(psi "$d/io.pressure")"
        echo "$up C $name cpup $(psi "$d/cpu.pressure")"
        awk -v u="$up" -v n="$name" '{v[$1]=$2} END{print u, "C", n, "cpu", v["usage_usec"], v["nr_throttled"], v["throttled_usec"]}' "$d/cpu.stat"
        awk -v u="$up" -v n="$name" '{v[$1]=$2} END{print u, "C", n, "mem", v["pgmajfault"]+0, v["pswpin"]+0, v["file_dirty"]+0, v["file_writeback"]+0}' "$d/memory.stat"
    done
    awk -v u="$up" '{v[$1]=$2} END{print u, "V", "vm", v["nr_dirty"], v["nr_writeback"], v["nr_dirtied"], v["nr_written"], v["pswpin"], v["pswpout"], v["pgmajfault"]}' /proc/vmstat
    echo "$up V psi $(psi /proc/pressure/io)$(psi /proc/pressure/cpu | cut -d' ' -f1) $(psi /proc/pressure/memory)"
    for dev in sdd sde; do echo "$up V blk $dev $(cat /sys/block/$dev/stat)"; done
    # D 상태 task. comm에 공백이 있으면 필드가 밀리므로 state는 ')' 뒤에서 읽는다.
    for f in /proc/[0-9]*/stat; do
        s=$(<"$f") 2>/dev/null || continue
        st=${s##*) }; st=${st%% *}
        [ "$st" = D ] || continue
        pid=${f#/proc/}; pid=${pid%/stat}
        comm=${s#*(}; comm=${comm%)*}
        echo "$up D $pid $st $(cat /proc/$pid/wchan 2>/dev/null || echo -) ${comm// /_}"
    done
}

while :; do
    tick >> "$OUT"
    sleep 1
done
