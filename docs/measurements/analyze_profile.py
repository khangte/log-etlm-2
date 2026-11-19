"""부하 프로파일 실행 결과를 PRD 06 판정 기준대로 구간별로 집계·판정해 JSON으로 출력한다.

사용: python3 docs/measurements/analyze_profile.py <profile.csv> <생성기 run_id[,run_id]> <Spark run_id> [샘플 디렉터리]
- 구간 경계는 프로파일 CSV의 누적 duration_sec, 기준 시각(t0)은 생성기 첫 1초 구간 시작이다.
- 판정 대상: steady·daily는 평시 기준, step은 지속 용량 기준, spike는 스파이크 기준(PRD 06 '판정 기준').
- 샘플 디렉터리(cg_before.txt·cg_after.txt·gen_cpu.txt·docker_stats.txt)가 있으면 자원 기록을 붙인다.
관리자 계정으로 ClickHouse HTTP에 조회만 한다. 데이터는 바꾸지 않는다.
"""

import csv
import json
import re
import statistics
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENV = dict(re.findall(r"^([A-Z_]+)=(.*)$", (ROOT / ".env").read_text(), re.M))
WARMUP_SEC = 120  # 실행 시작 후 판정 제외 구간
TRIGGER_SEC = 5  # SPARK_TRIGGER_INTERVAL. 배치 입력은 트리거 직전 5초 동안 쌓인 것이다
ACK_P95_MAX_MS = 100
ACK_P95_OK_RATIO = 0.99  # 1초 구간 중 ACK p95가 ACK_P95_MAX_MS 이하여야 하는 비율
STEADY_TRIGGER_P99_MS = 3500
E2E_P99_SEC = 10
RECOVERY_SEC = 60


def q(sql: str) -> list[dict]:
    """관리자 계정으로 쿼리를 실행해 JSONEachRow 행 목록을 돌려준다."""
    req = urllib.request.Request(
        f"http://127.0.0.1:{ENV['CLICKHOUSE_HTTP_HOST_PORT']}/?default_format=JSONEachRow",
        data=sql.encode(), headers={"X-ClickHouse-User": ENV["CLICKHOUSE_USER"],
                                    "X-ClickHouse-Key": ENV["CLICKHOUSE_PASSWORD"]})
    try:
        body = urllib.request.urlopen(req).read().decode()
    except urllib.error.HTTPError as exc:
        sys.exit(exc.read().decode()[:500] + "\n" + sql)
    return [json.loads(line) for line in body.splitlines()]


def load_segments(path: str) -> list[dict]:
    """프로파일 CSV를 구간 목록(label·eps·start·end 초)으로 읽는다."""
    segments, start = [], 0
    with open(ROOT / path if not Path(path).is_absolute() else path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            end = start + int(row["duration_sec"])
            segments.append({"label": row["label"].strip(), "eps": int(row["eps"]), "start": start, "end": end})
            start = end
    return segments


def slope(points: list[tuple[float, float]]) -> float | None:
    """최소제곱 기울기(값/초). 점이 2개 미만이면 None."""
    if len(points) < 2:
        return None
    xs, ys = [p[0] for p in points], [p[1] for p in points]
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    den = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in points) / den if den else None


def pct(values: list[float], p: float) -> float | None:
    """nearest-rank 백분위. 값이 없으면 None."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, -(-len(ordered) * p // 100) - 1)]


def analyze(profile: str, gen_ids: list[str], spark_id: str) -> dict:
    """ClickHouse 지표를 구간별로 모아 판정한다."""
    segments = load_segments(profile)
    gen = "run_id IN (" + ",".join(f"'{g}'" for g in gen_ids) + ")"
    t0 = q(f"SELECT toString(min(interval_start)) t FROM ops.producer_stats WHERE {gen}")[0]["t"]
    t0_sql = f"toDateTime64('{t0}', 3, 'UTC')"
    off = lambda col: f"dateDiff('millisecond', {t0_sql}, {col}) / 1000"  # noqa: E731

    # 1초 구간: 여러 생성기는 같은 초끼리 합친다(구간 시작을 초 단위로 내림).
    producer = q(f"""
        SELECT intDiv(toUInt64({off('interval_start')} * 1000), 1000) sec, sum(generated) generated, sum(acked) acked,
               sum(target_eps) target_eps, max(ack_latency_ms_p95) ack_p95, max(queue_len_max) queue_max
        FROM ops.producer_stats WHERE {gen} GROUP BY sec ORDER BY sec""")
    batches = q(f"""
        SELECT p.batch_id batch_id, {off('p.trigger_at')} offset_sec, p.trigger_execution_ms trigger_ms,
               p.num_input_rows input_rows,
               arraySum(mapValues(p.latest_offsets)) - arraySum(mapValues(p.end_offsets)) backlog,
               a.kafka_to_callback_ms_p95 k2c_p95
        FROM ops.spark_query_progress p
        LEFT JOIN (SELECT batch_id, max(kafka_to_callback_ms_p95) kafka_to_callback_ms_p95 FROM ops.spark_batch_attempts
                   WHERE run_id = '{spark_id}' AND status = 'succeeded' GROUP BY batch_id) a ON a.batch_id = p.batch_id
        WHERE p.run_id = '{spark_id}' ORDER BY p.batch_id""")
    e2e_by_seg = {s["label"]: q(f"""
        SELECT count() rows, round(quantileExact(0.5)(d), 2) p50, round(quantileExact(0.95)(d), 2) p95,
               round(quantileExact(0.99)(d), 2) p99, round(max(d), 2) max, countIf(d > {E2E_P99_SEC}) over10
        FROM (SELECT dateDiff('millisecond', event_timestamp, ingested_at) / 1000 d FROM silver.order_events
              WHERE spark_run_id = '{spark_id}'
                AND {off('event_timestamp')} >= {max(s['start'], WARMUP_SEC)} AND {off('event_timestamp')} < {s['end']})
        """)[0] for s in segments if s["end"] > WARMUP_SEC}

    status = q(f"SELECT status, count() n FROM ops.spark_batch_attempts WHERE run_id = '{spark_id}' GROUP BY status")
    starts = q(f"SELECT uniqExact(run_id) runs FROM ops.spark_query_progress WHERE query_id IN "
               f"(SELECT query_id FROM ops.spark_query_progress WHERE run_id = '{spark_id}')")[0]["runs"]
    silver = q(f"SELECT count() rows, uniqExact(event_id) uniq FROM silver.order_events WHERE spark_run_id = '{spark_id}'")[0]
    host_cpu = q(f"""
        SELECT round(avg(b), 2) avg, round(quantile(0.95)(b), 2) p95, round(max(b), 2) max FROM (
          SELECT 8 - 2 * value b FROM system.asynchronous_metric_log
          WHERE metric = 'OSIdleTimeNormalized' AND event_time BETWEEN {t0_sql} AND {t0_sql} + {segments[-1]['end']})""")[0]
    stalls = q(f"""SELECT toString(detected_at) at, round({off('detected_at')}, 1) offset_sec, gap_ms FROM ops.host_stalls
                  WHERE detected_at BETWEEN {t0_sql} AND {t0_sql} + {segments[-1]['end'] + 120} ORDER BY detected_at""")

    kind = Path(profile).stem
    warmup_k2c = [b["k2c_p95"] for b in batches
                  if b["k2c_p95"] is not None and WARMUP_SEC <= b["offset_sec"] - TRIGGER_SEC
                  and b["offset_sec"] < segments[0]["end"]]
    out_segments = []
    for s in segments:
        lo = max(s["start"], WARMUP_SEC)
        if lo >= s["end"]:
            continue
        # 1초 행: 행 전체가 구간 안에 있고 목표 EPS가 구간 값인 행만 쓴다(경계 행 제외).
        rows = [r for r in producer if lo <= r["sec"] and r["sec"] + 1 <= s["end"] and r["target_eps"] == s["eps"]]
        # 배치: 입력(트리거 직전 5초)이 구간 안에 있는 배치만 쓴다. 입력 0 배치도 포함한다.
        seg_batches = [b for b in batches if lo <= b["offset_sec"] - TRIGGER_SEC and b["offset_sec"] <= s["end"]]
        triggers = [b["trigger_ms"] for b in seg_batches]
        backlog = [(b["offset_sec"], b["backlog"]) for b in seg_batches]
        carried = sum(1 for a, b in zip(seg_batches, seg_batches[1:]) if a["backlog"] > 0 and b["backlog"] > 0)
        last60 = [v for t, v in backlog if t >= s["end"] - 60]
        eps = sum(r["generated"] for r in rows) / len(rows) if rows else None
        item = {
            "label": s["label"], "target_eps": s["eps"], "start_sec": s["start"], "end_sec": s["end"], "judged_from_sec": lo,
            "producer_rows": len(rows), "generated_eps": round(eps) if eps else None,
            "acked_eps": round(sum(r["acked"] for r in rows) / len(rows)) if rows else None,
            "ack_p95_max_ms": max((r["ack_p95"] for r in rows), default=None),
            "ack_p95_over_limit_rows": sum(r["ack_p95"] > ACK_P95_MAX_MS for r in rows),
            "ack_p95_ok_ratio": round(sum(r["ack_p95"] <= ACK_P95_MAX_MS for r in rows) / len(rows), 4) if rows else None,
            "queue_max": max((r["queue_max"] for r in rows), default=None),
            "batches": len(seg_batches), "input_rows": sum(b["input_rows"] for b in seg_batches),
            "trigger_avg_ms": round(statistics.fmean(triggers)) if triggers else None,
            "trigger_p99_ms": pct(triggers, 99), "trigger_max_ms": max(triggers, default=None),
            "trigger_over_5s": sum(t > TRIGGER_SEC * 1000 for t in triggers),
            "throughput_eps": round(sum(b["input_rows"] for b in seg_batches) / (sum(triggers) / 1000)) if triggers else None,
            "backlog_max": max((v for _, v in backlog), default=None),
            "backlog_last60_max": max(last60, default=None),
            "backlog_slope_per_sec": round(slope(backlog), 1) if slope(backlog) is not None else None,
            "backlog_carried_batches": carried,
            "e2e_sec": e2e_by_seg.get(s["label"]),
        }
        gen_ok = eps is not None and abs(eps - s["eps"]) <= s["eps"] * 0.05
        not_growing = item["backlog_slope_per_sec"] is None or item["backlog_slope_per_sec"] <= 0 or \
            (item["backlog_last60_max"] or 0) == 0
        if kind in ("steady", "daily"):
            item["judgement"] = {
                "generation": gen_ok and item["ack_p95_ok_ratio"] is not None and item["ack_p95_ok_ratio"] >= ACK_P95_OK_RATIO,
                "trigger_p99": item["trigger_p99_ms"] is not None and item["trigger_p99_ms"] <= STEADY_TRIGGER_P99_MS,
                "e2e_p99": item["e2e_sec"] is not None and item["e2e_sec"]["p99"] <= E2E_P99_SEC,
                "backlog": not_growing and carried == 0,
            }
        elif kind == "step":
            item["judgement"] = {
                "generation": gen_ok,
                "no_accumulation": (item["backlog_last60_max"] or 0) <= s["eps"] * TRIGGER_SEC and not_growing,
            }
        out_segments.append(item)

    result = {"profile": profile, "t0_utc": t0, "generator_run_ids": gen_ids, "spark_run_id": spark_id,
              "segments": out_segments}
    if kind == "step":
        capacity = None
        for item in out_segments:
            if item["label"] == "warmup":
                continue
            if not all(item["judgement"].values()):
                break
            capacity = item["target_eps"]
        result["sustained_capacity_eps"] = capacity
    if kind == "spike":
        spike = next(s for s in segments if s["label"] == "spike")
        limit = 1.5 * statistics.fmean(warmup_k2c) if warmup_k2c else None
        recovered = next((b for b in batches if b["offset_sec"] >= spike["end"] and b["backlog"] == 0
                          and limit is not None and b["k2c_p95"] is not None and b["k2c_p95"] <= limit), None)
        result["recovery"] = {"warmup_k2c_p95_avg_ms": round(limit / 1.5) if limit else None,
                              "recovered_at_sec": recovered["offset_sec"] if recovered else None,
                              "recovery_sec": round(recovered["offset_sec"] - spike["end"], 1) if recovered else None}
        result["recovery"]["pass"] = result["recovery"]["recovery_sec"] is not None and \
            result["recovery"]["recovery_sec"] <= RECOVERY_SEC
    generated = sum(r["generated"] for r in producer)
    acked = sum(r["acked"] for r in producer)
    result["common"] = {"generated": generated, "acked": acked, "silver_rows": silver["rows"],
                        "silver_uniq_event_id": silver["uniq"], "spark_attempts": status,
                        "spark_runs_in_query": starts, "host_cpu_busy_cores": host_cpu, "host_stalls": stalls}
    result["batches"] = [{k: (round(v, 1) if isinstance(v, float) else v) for k, v in b.items()} for b in batches]
    return result


def resources(sample_dir: Path, t0: str, segments: list[dict]) -> dict:
    """샘플러 기록에서 cgroup 증분과 구간별 생성기·컨테이너 CPU를 만든다."""
    def snap(path):
        lines = path.read_text().splitlines()
        return lines[0], {line.split()[0]: {k: int(v) for k, v in re.findall(r"(\w+)[ =](\d+)", " ".join(line.split()[1:]))}
                          for line in lines[1:]}
    (tb, before), (ta, after) = snap(sample_dir / "cg_before.txt"), snap(sample_dir / "cg_after.txt")
    cgroup = {"window_utc": [tb, ta]}
    for c in after:
        periods = after[c]["nr_periods"] - before[c]["nr_periods"]
        throttled = after[c]["nr_throttled"] - before[c]["nr_throttled"]
        cgroup[c] = {"cpu_sec": round((after[c]["usage_usec"] - before[c]["usage_usec"]) / 1e6, 1),
                     "throttled_ratio": round(throttled / periods, 3) if periods else None,
                     "throttled_sec": round((after[c]["throttled_usec"] - before[c]["throttled_usec"]) / 1e6, 1),
                     "memory_peak_after": after[c]["peak"], "oom_kill_delta": after[c]["oom_kill"] - before[c]["oom_kill"]}
    base = datetime.fromisoformat(t0.replace(" ", "T")).replace(tzinfo=timezone.utc)
    offset = lambda ts: (datetime.fromisoformat(ts).replace(tzinfo=timezone.utc) - base).total_seconds()  # noqa: E731
    label = lambda sec: next((s["label"] for s in segments if s["start"] <= sec < s["end"]), None)  # noqa: E731
    gen = {}
    rows = [line.split() for line in (sample_dir / "gen_cpu.txt").read_text().splitlines()]
    for pid in sorted({r[1] for r in rows}):
        series = [r for r in rows if r[1] == pid]
        for a, b in zip(series, series[1:]):
            dt, seg = offset(b[0]) - offset(a[0]), label(offset(b[0]))
            if seg and dt > 0:
                gen.setdefault(f"{pid}/{seg}", []).append((int(b[2]) + int(b[3]) - int(a[2]) - int(a[3])) / 100 / dt)
    containers = {}
    for line in (sample_dir / "docker_stats.txt").read_text().splitlines():
        ts, name, cpu = line.split()[:3]
        seg = label(offset(ts))
        if seg:
            containers.setdefault(f"{name.split('-')[3]}/{seg}", []).append(float(cpu.rstrip("%")) / 100)
    summary = lambda d: {k: {"avg": round(statistics.fmean(v), 2), "max": round(max(v), 2)} for k, v in d.items()}  # noqa: E731
    return {"cgroup_delta": cgroup, "generator_cpu_cores": summary(gen), "container_cpu_cores_5s": summary(containers)}


if __name__ == "__main__":
    profile_path, gen_arg, spark_arg = sys.argv[1:4]
    report = analyze(profile_path, gen_arg.split(","), spark_arg)
    if len(sys.argv) > 4:
        report["resources"] = resources(Path(sys.argv[4]), report["t0_utc"], load_segments(profile_path))
    print(json.dumps(report, ensure_ascii=False, indent=1))
