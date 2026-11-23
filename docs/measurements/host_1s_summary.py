"""sample_host_1s.sh 누적 기록을 표본 간 delta로 바꾸고 Spark 배치 실행 구간과 대조해 JSON으로 출력한다.

사용: python3 docs/measurements/host_1s_summary.py <h.txt> <Spark run_id> [느린 배치 기준 ms=3500]
- delta는 실제 수집 간격(/proc/uptime 차)으로 나눠 초당 값·%로 바꾼다(PRD 03 'daily 동시 정지 원인 조사' 재검토 반영).
- 블록 장치는 sde(8:64, Docker 볼륨 디스크)를 본다.
ClickHouse에는 조회만 한다.
"""

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta

from analyze_profile import q

DEV = "8:64"  # sde
BLK = "sde"


def parse(path: str) -> list[dict]:
    """줄을 수집 시각(up)별 누적값 묶음으로 모은다."""
    ticks = defaultdict(lambda: {"c": defaultdict(dict), "d": []})
    for line in open(path):
        f = line.split()
        if len(f) < 3:
            continue
        up, kind = float(f[0]), f[1]
        t = ticks[up]
        if kind == "T":
            t["wall"] = f[2]
        elif kind == "C":
            name, key, vals = f[2], f[3], f[4:]
            if key == "io":
                if vals[0] == DEV:
                    t["c"][name]["io"] = [int(v) for v in vals[1:]]
            else:
                t["c"][name][key] = [int(v) for v in vals]
        elif kind == "V":
            if f[2] == "blk":
                if f[3] == BLK:
                    t["blk"] = [int(v) for v in f[4:]]
            else:
                t[f[2]] = [int(v) for v in f[3:]]
        elif kind == "D":
            t["d"].append(f"{f[5] if len(f) > 5 else '-'}@{f[4]}")
    return [dict(v, up=k) for k, v in sorted(ticks.items()) if "wall" in v and "vm" in v and "blk" in v]


def deltas(ticks: list[dict]) -> list[dict]:
    """연속 표본의 차이를 초당 값·%로 바꾼다. 앞 표본이 없는 첫 줄은 버린다."""
    rows = []
    for a, b in zip(ticks, ticks[1:]):
        dt = b["up"] - a["up"]
        if dt <= 0:
            continue
        pct = lambda x, y: round((y - x) / dt / 1e4, 1)  # noqa: E731  마이크로초 누적 → %
        row = {"wall": b["wall"], "dt": round(dt, 2),
               "vm_io_some_pct": pct(a["psi"][0], b["psi"][0]), "vm_io_full_pct": pct(a["psi"][1], b["psi"][1]),
               "vm_cpu_some_pct": pct(a["psi"][2], b["psi"][2]), "vm_mem_full_pct": pct(a["psi"][4], b["psi"][4]),
               "nr_dirty_mb": round(b["vm"][0] * 4096 / 1e6, 1), "nr_writeback_mb": round(b["vm"][1] * 4096 / 1e6, 1),
               "written_mb_s": round((b["vm"][3] - a["vm"][3]) * 4096 / 1e6 / dt, 1),
               "pswpin_s": round((b["vm"][4] - a["vm"][4]) / dt), "majflt_s": round((b["vm"][6] - a["vm"][6]) / dt),
               # /sys/block stat: 4=쓰기 완료 수, 6=쓰기 섹터, 7=쓰기 ms, 8=in_flight, 9=io_ticks ms, 10=time_in_queue ms
               "sde_w_done_s": round((b["blk"][4] - a["blk"][4]) / dt),
               "sde_w_mb_s": round((b["blk"][6] - a["blk"][6]) * 512 / 1e6 / dt, 1),
               "sde_in_flight": b["blk"][8], "sde_util_pct": round((b["blk"][9] - a["blk"][9]) / dt / 10, 1),
               "sde_queue_ms_s": round((b["blk"][10] - a["blk"][10]) / dt),
               "d_tasks": b["d"]}
        for name, c in b["c"].items():
            p = a["c"].get(name, {})
            if not all(k in p for k in ("io", "iop", "cpup", "cpu", "mem")):
                continue
            row[f"{name}_w_mb_s"] = round((c["io"][1] - p["io"][1]) / 1e6 / dt, 1)
            row[f"{name}_io_full_pct"] = pct(p["iop"][1], c["iop"][1])
            row[f"{name}_cpu_some_pct"] = pct(p["cpup"][0], c["cpup"][0])
            row[f"{name}_cores"] = round((c["cpu"][0] - p["cpu"][0]) / 1e6 / dt, 2)
            row[f"{name}_throttled_pct"] = pct(p["cpu"][2], c["cpu"][2])
            row[f"{name}_majflt_s"] = round((c["mem"][0] - p["mem"][0]) / dt)
        rows.append(row)
    return rows


def batch_join(rows: list[dict], run_id: str, slow_ms: int) -> dict:
    """배치 실행 구간 [trigger_at-1초, 종료+1초] 안 표본 최댓값을 붙이고 느린·정상 배치를 비교한다."""
    parse_t = lambda s: datetime.fromisoformat(s[:23])  # noqa: E731
    times = [parse_t(r["wall"]) for r in rows]
    batches = q(f"""SELECT batch_id, toString(trigger_at) ta, trigger_execution_ms ms, wal_commit_ms wal
        FROM ops.spark_query_progress WHERE run_id='{run_id}' ORDER BY batch_id""")
    keys = [k for k in rows[0] if k.endswith(("_pct", "_in_flight", "_mb_s", "_queue_ms_s"))] if rows else []
    out = []
    for b in batches:
        s = parse_t(b["ta"]) - timedelta(seconds=1)
        e = parse_t(b["ta"]) + timedelta(milliseconds=b["ms"] + 1000)
        win = [r for r, t in zip(rows, times) if s <= t <= e]
        if not win:
            continue
        item = {"batch_id": b["batch_id"], "trigger_at": b["ta"], "trigger_ms": b["ms"], "wal_commit_ms": b["wal"]}
        item.update({f"max_{k}": max(r.get(k, 0) or 0 for r in win) for k in keys})
        item["d_tasks"] = sorted({x for r in win for x in r["d_tasks"]})
        out.append(item)
    slow = [x for x in out if x["trigger_ms"] > slow_ms]
    normal = [x for x in out if x["trigger_ms"] <= slow_ms]
    share = lambda g, k, th: f"{sum(x.get(k, 0) > th for x in g)}/{len(g)}"  # noqa: E731
    checks = [("max_vm_io_full_pct", 20), ("max_vm_cpu_some_pct", 50), ("max_sde_in_flight", 10),
              ("max_kafka_io_full_pct", 20), ("max_clickhouse_io_full_pct", 20), ("max_spark_io_full_pct", 20)]
    summary = {f"{k}>{th}": {"slow": share(slow, k, th), "normal": share(normal, k, th)} for k, th in checks}
    return {"summary": summary, "slow_batches": slow, "batches": out}


if __name__ == "__main__":
    path, run_id = sys.argv[1:3]
    slow_ms = int(sys.argv[3]) if len(sys.argv) > 3 else 3500
    rows = deltas(parse(path))
    report = {"source": path, "spark_run_id": run_id, "samples": len(rows),
              "dt": {"max": max(r["dt"] for r in rows), "over_1_5s": sum(r["dt"] > 1.5 for r in rows)} if rows else None}
    report.update(batch_join(rows, run_id, slow_ms))
    report["samples_series"] = rows
    print(json.dumps(report, ensure_ascii=False))
