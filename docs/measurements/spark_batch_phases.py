"""배치 한 개의 트리거 시간을 Spark job 구간과 driver 구간(job이 돌지 않는 시간)으로 나눈다(PRD 03 '배치 시간 재분해').

사용: python3 docs/measurements/spark_batch_phases.py <spark_jobs.json> <Spark run_id> <from_utc> <to_utc>
- spark_jobs.json: 실행 직후 Spark REST `/api/v1/applications/<app>/jobs` 응답.
- 배치당 job 순서(제출 순): 지표 집계 3, 중복 제거 셔플 map 1, JDBC save 1, DLQ save 0~1. 이 구성이 아닌 배치는 건너뛴다.
- 트리거 시각·시간은 ops.spark_query_progress, 콜백 시각·단계 시간은 ops.spark_batch_attempts(성공 시도)에서 읽는다.
"""

import json
import statistics
import sys
from datetime import datetime, timedelta

from analyze_profile import q

AGG_JOBS = 3


def ts(text: str) -> datetime:
    """Spark REST 시각('...GMT')과 ClickHouse 시각 문자열을 naive UTC datetime으로 바꾼다."""
    return datetime.fromisoformat(text.replace("GMT", "").replace("Z", "").replace(" ", "T"))


def ms(a: datetime, b: datetime) -> float:
    """a에서 b까지 ms."""
    return (b - a) / timedelta(milliseconds=1)


def phases(jobs: list[dict], run_id: str, start: str, end: str) -> list[dict]:
    """구간 안 배치별 단계 시간(ms)을 계산한다."""
    by_batch: dict[int, list[dict]] = {}
    for job in jobs:
        desc = job.get("description") or ""
        if f"runId = {run_id}" in desc and "batch = " in desc and job.get("completionTime"):
            by_batch.setdefault(int(desc.rsplit("batch = ", 1)[1]), []).append(job)
    rows = q(f"""
        SELECT p.batch_id b, toString(p.trigger_at) trig_at, p.trigger_execution_ms trig, p.num_input_rows n,
               toString(a.callback_started_at) cb_at, a.callback_total_ms cb, a.classification_ms cls,
               a.orders_write_ms ow, a.dlq_write_ms dw
        FROM ops.spark_query_progress p
        JOIN (SELECT * FROM ops.spark_batch_attempts WHERE run_id = '{run_id}' AND status = 'succeeded') a
          ON a.batch_id = p.batch_id
        WHERE p.run_id = '{run_id}' AND p.trigger_at >= '{start}' AND p.trigger_at < '{end}' ORDER BY b""")
    out = []
    for r in rows:
        js = sorted(by_batch.get(r["b"], []), key=lambda j: j["jobId"])
        if len(js) not in (AGG_JOBS + 2, AGG_JOBS + 3):
            continue
        sub = [ts(j["submissionTime"]) for j in js]
        done = [ts(j["completionTime"]) for j in js]
        trig_at, cb_at = ts(r["trig_at"]), ts(r["cb_at"])
        cb_end = cb_at + timedelta(milliseconds=r["cb"])
        row = {"b": r["b"], "n": r["n"], "trig": r["trig"], "pre_cb_ms": ms(trig_at, cb_at),
               "cb_to_first_job": ms(cb_at, sub[0]), "agg_span": ms(sub[0], done[AGG_JOBS - 1]),
               "gap_agg_to_orders": ms(done[AGG_JOBS - 1], sub[AGG_JOBS]),
               "dedup_map_job": ms(sub[AGG_JOBS], done[AGG_JOBS]),
               "orders_save_job": ms(sub[AGG_JOBS + 1], done[AGG_JOBS + 1]),
               "cb_end_to_trig_end": ms(cb_end, trig_at + timedelta(milliseconds=r["trig"])),
               "cls": r["cls"], "ow": r["ow"], "dw": r["dw"], "cb": r["cb"], "jobs": len(js)}
        if len(js) == AGG_JOBS + 3:
            row["gap_orders_to_dlq"] = ms(done[AGG_JOBS + 1], sub[AGG_JOBS + 2])
            row["dlq_save_job"] = ms(sub[AGG_JOBS + 2], done[AGG_JOBS + 2])
        # 콜백 안에서 job이 돌지 않은 시간 합(콜백 밖 앞뒤 구간 포함)
        busy = ms(sub[0], done[AGG_JOBS - 1]) + sum(ms(s, d) for s, d in zip(sub[AGG_JOBS:], done[AGG_JOBS:]))
        row["driver_total"] = r["trig"] - busy
        out.append(row)
    return out


def summary(rows: list[dict]) -> dict:
    """단계별 평균·p50·최대."""
    keys = [k for k in rows[0] if k not in ("b", "jobs")]
    return {k: {"avg": round(statistics.fmean(v), 1), "p50": round(statistics.median(v), 1), "max": round(max(v), 1)}
            for k in keys if (v := [r[k] for r in rows if r.get(k) is not None])}


if __name__ == "__main__":
    jobs_path, run_id, start, end = sys.argv[1:5]
    result = phases(json.load(open(jobs_path)), run_id, start, end)
    assert result, "구간 안에 분해할 수 있는 배치가 없다"
    print(json.dumps({"batches_n": len(result), "summary": summary(result), "batches": result}, ensure_ascii=False,
                     indent=1))
