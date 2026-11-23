"""run_mem.sh 샘플을 구간(base·peak, 워밍업 120초 제외)별로 요약한다."""
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

D = Path(sys.argv[1])
SEGS = [("warmup", 0, 120), ("base", 120, 300), ("peak", 300, 600), ("tail", 600, 10**6)]
t = lambda s: datetime.fromisoformat(s).timestamp()  # noqa: E731
t0 = t(D.joinpath("gen_cpu.txt").read_text().split()[0])
seg = lambda ts: next(n for n, a, b in SEGS if a <= t(ts) - t0 < b) if t(ts) >= t0 else "pre"  # noqa: E731
MB = 1024 * 1024

out = defaultdict(dict)
# jstat -gc: KB 단위. 힙 사용 = S0U+S1U+EU+OU
rows = [l.split() for l in D.joinpath("jstat.txt").read_text().splitlines() if l.split()[1][0].isdigit()]
by = defaultdict(list)
for r in rows:
    by[seg(r[0])].append(r)
for s, rs in by.items():
    heap = [sum(float(r[i]) for i in (4, 5, 7, 9)) / 1024 for r in rs]
    old = [float(r[9]) / 1024 for r in rs]
    cap = [sum(float(r[i]) for i in (2, 3, 6, 8)) / 1024 for r in rs]
    gct = float(rs[-1][20]) - float(rs[0][20])
    span = t(rs[-1][0]) - t(rs[0][0])
    out[s]["heap_used_mb"] = {"max": round(max(heap)), "min": round(min(heap)), "avg": round(sum(heap) / len(heap))}
    out[s]["old_used_mb_max"] = round(max(old))
    out[s]["heap_committed_mb_max"] = round(max(cap))
    out[s]["gc"] = {"young": int(rs[-1][14]) - int(rs[0][14]), "full": int(rs[-1][16]) - int(rs[0][16]),
                    "concurrent": int(rs[-1][18]) - int(rs[0][18]), "gc_sec": round(gct, 2),
                    "gc_ratio": round(gct / span, 4) if span else None}
for l in D.joinpath("storage.txt").read_text().splitlines():
    ts, _, used, mx, disk, *rest = l.split()
    d = out[seg(ts)].setdefault("storage", {"used_mb_max": 0, "max_mb": round(int(mx) / MB), "disk_used_max": 0})
    d["used_mb_max"] = max(d["used_mb_max"], round(int(used) / MB, 1))
    d["disk_used_max"] = max(d["disk_used_max"], int(disk))
    if len(rest) >= 3 and rest[1]:
        d["peak_jvm_heap_mb"], d["peak_jvm_offheap_mb"] = round(int(rest[1]) / MB), round(int(rest[2]) / MB)
for l in D.joinpath("memstat.txt").read_text().splitlines():
    p = l.split()
    kv = dict(zip(p[1::2], map(int, p[2::2])))
    d = out[seg(p[0])].setdefault("cgroup_mb", {})
    for k in ("anon", "file", "current"):
        v = kv[k] / MB
        d[k + "_max"] = round(max(d.get(k + "_max", 0), v))
        d[k + "_min"] = round(min(d.get(k + "_min", 1e9), v))
rss = defaultdict(lambda: defaultdict(int))
for l in D.joinpath("rss.txt").read_text().splitlines():
    ts, kb, comm, last = l.split()[:4]
    k = "java" if comm == "java" else "py_main" if last.endswith("main.py") else "py_workers" if comm.startswith("python") else None
    if k:
        rss[ts][k] += int(kb)
        rss[ts]["n_" + k] += 1
for ts, d in rss.items():
    o = out[seg(ts)].setdefault("rss_mb_max", {})
    for k, v in d.items():
        o[k] = max(o.get(k, 0), v if k.startswith("n_") else round(v / 1024))
print(json.dumps(out, ensure_ascii=False, indent=1))
