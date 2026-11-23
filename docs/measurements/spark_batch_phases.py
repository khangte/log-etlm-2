"""배치 한 개의 트리거 시간을 Spark job 구간과 driver 구간(job이 돌지 않는 시간)으로 나눈다(PRD 03 '배치 시간 재분해').

사용: python3 docs/measurements/spark_batch_phases.py <spark_jobs.json> <Spark run_id> <from_utc> <to_utc>
- spark_jobs.json: 실행 직후 Spark REST `/api/v1/applications/<app>/jobs` 응답.
- 배치당 job(제출 순): 지표 집계 3, 중복 제거 셔플 map 0~1(D1 이후 0), JDBC save 1, DLQ save 0~1. 다른 구성의 배치는 건너뛴다.
- 트리거 시각·시간은 ops.spark_query_progress, 콜백 시각·단계 시간은 ops.spark_batch_attempts(성공 시도)에서 읽는다.
"""

import json
import statistics
import sys
from datetime import datetime, timedelta

from analyze_profile import q

AGG_JOBS = 3
DEDUP_JOB = "withThreadLocalCaptured"


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
        agg, rest = js[:AGG_JOBS], js[AGG_JOBS:]
        # 중복 제거 셔플 map job은 AQE가 따로 제출해 이름이 다르다(D1 이후 없음). 나머지는 정상 출력·DLQ save 순서다.
        dedup = [j for j in rest if DEDUP_JOB in j["name"]]
        saves = [j for j in rest if j["name"].startswith("save at")]
        if len(agg) != AGG_JOBS or len(dedup) > 1 or len(saves) not in (1, 2) or len(dedup) + len(saves) != len(rest):
            continue
        span = lambda j: (ts(j["submissionTime"]), ts(j["completionTime"]))  # noqa: E731
        agg_start, agg_end = span(agg[0])[0], span(agg[-1])[1]
        orders_first = span((dedup or saves)[0])[0]
        trig_at, cb_at = ts(r["trig_at"]), ts(r["cb_at"])
        cb_end = cb_at + timedelta(milliseconds=r["cb"])
        row = {"b": r["b"], "n": r["n"], "trig": r["trig"], "pre_cb_ms": ms(trig_at, cb_at),
               "cb_to_first_job": ms(cb_at, agg_start), "agg_span": ms(agg_start, agg_end),
               "gap_agg_to_orders": ms(agg_end, orders_first),
               "orders_save_job": ms(*span(saves[0])),
               "cb_end_to_trig_end": ms(cb_end, trig_at + timedelta(milliseconds=r["trig"])),
               "cls": r["cls"], "ow": r["ow"], "dw": r["dw"], "cb": r["cb"], "jobs": len(js)}
        if dedup:
            row["dedup_map_job"] = ms(*span(dedup[0]))
        if len(saves) == 2:
            row["gap_orders_to_dlq"] = ms(span(saves[0])[1], span(saves[1])[0])
            row["dlq_save_job"] = ms(*span(saves[1]))
        # 트리거 시간에서 job 구간(집계 span + 나머지 job)을 뺀 값 = job이 돌지 않은 driver 시간
        row["driver_total"] = r["trig"] - ms(agg_start, agg_end) - sum(ms(*span(j)) for j in rest)
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
