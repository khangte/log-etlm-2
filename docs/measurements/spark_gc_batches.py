"""Spark GC 로그(-Xlog:gc:file=...:time,uptime)의 정지 시간을 배치 실행 구간과 대조한다.

사용: python3 docs/measurements/spark_gc_batches.py <gc.log> <Spark run_id> <t0_utc> [기준 ms]
- 배치 구간은 ops.spark_query_progress의 trigger_at부터 trigger_execution_ms까지다.
- t0_utc(생성기 첫 1초 구간 시작) 이후 120초(워밍업)부터 집계한다.
- 출력: 전체 GC 정지 요약, 느린 배치(기준 ms 초과)와 보통 배치의 배치당 GC 정지 합 비교(JSON).
"""

import json
import re
import statistics
import sys
from datetime import datetime, timedelta

from analyze_profile import q

LINE = re.compile(r"^\[([^\]]+)\]\[[^\]]+\] GC\(\d+\) (Pause .*?) \d+[KMG]->\d+[KMG]\(\d+[KMG]\) ([\d.]+)ms")
WARMUP_SEC = 120


def parse_gc(path: str) -> list[tuple[datetime, str, float]]:
    """GC 로그에서 (종료 시각, 정지 종류, 정지 ms) 목록을 읽는다. Remark·Cleanup도 정지로 센다."""
    out = []
    for line in open(path, encoding="utf-8"):
        m = LINE.match(line)
        if m:
            end = datetime.fromisoformat(m.group(1).replace("+0000", "+00:00"))
            out.append((end, m.group(2).strip(), float(m.group(3))))
    return out


def main() -> None:
    """GC 정지를 배치 구간에 배정해 느린 배치와 보통 배치를 비교한다."""
    gc_path, run_id, t0 = sys.argv[1:4]
    slow_ms = int(sys.argv[4]) if len(sys.argv) > 4 else 3500
    start = datetime.fromisoformat(t0.replace(" ", "T") + "+00:00") + timedelta(seconds=WARMUP_SEC)
    batches = q(f"""SELECT batch_id, toString(trigger_at) t, trigger_execution_ms ms FROM ops.spark_query_progress
                    WHERE run_id = '{run_id}' AND num_input_rows > 0 ORDER BY batch_id""")
    pauses = [p for p in parse_gc(gc_path) if p[0] >= start]
    rows = []
    for b in batches:
        b0 = datetime.fromisoformat(b["t"].replace(" ", "T") + "+00:00")
        if b0 < start:
            continue
        b1 = b0 + timedelta(milliseconds=b["ms"])
        inside = [p for p in pauses if b0 <= p[0] <= b1]
        rows.append({"batch_id": b["batch_id"], "trigger_ms": b["ms"], "gc_count": len(inside),
                     "gc_ms": round(sum(p[2] for p in inside), 1), "gc_max_ms": round(max((p[2] for p in inside), default=0), 1)})
    group = lambda rs: {"batches": len(rs), "trigger_avg_ms": round(statistics.fmean(r["trigger_ms"] for r in rs)) if rs else None,  # noqa: E731
                        "gc_ms_per_batch_avg": round(statistics.fmean(r["gc_ms"] for r in rs), 1) if rs else None,
                        "gc_ms_per_batch_max": max((r["gc_ms"] for r in rs), default=None)}
    kinds = {}
    for _, kind, ms in pauses:
        k = kinds.setdefault(kind, {"count": 0, "total_ms": 0.0, "max_ms": 0.0})
        k["count"] += 1
        k["total_ms"] = round(k["total_ms"] + ms, 1)
        k["max_ms"] = max(k["max_ms"], ms)
    print(json.dumps({"slow_threshold_ms": slow_ms, "pauses_by_kind": kinds,
                      "slow": group([r for r in rows if r["trigger_ms"] > slow_ms]),
                      "normal": group([r for r in rows if r["trigger_ms"] <= slow_ms]),
                      "slow_batches": [r for r in rows if r["trigger_ms"] > slow_ms]}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
