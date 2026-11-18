"""Spark 드라이버의 운영 지표를 ClickHouse ops 테이블에 HTTP INSERT한다.

근거: ClickHouse PRD '운영 지표 저장'. 지표 쓰기 실패는 경고 로그만 남기고 주문 배치를 실패시키지 않는다.
"""

import json
import logging
import math
import urllib.parse
import urllib.request

from streaming.config import Config
from streaming.sinks import CLICKHOUSE_USER
from streaming.transforms import DLQ, EXCLUDED, ORDER

log = logging.getLogger("streaming")

BATCH_ATTEMPTS_TABLE = "ops.spark_batch_attempts"
QUERY_PROGRESS_TABLE = "ops.spark_query_progress"
TIMEOUT_SECONDS = 5
# 작은 INSERT는 서버가 묶도록 async_insert를 쓰고, 오류를 알 수 있게 flush까지 기다린다.
INSERT_SETTINGS = {"async_insert": 1, "wait_for_async_insert": 1, "date_time_input_format": "best_effort"}
PROGRESS_DURATIONS = {"trigger_execution_ms": "triggerExecution", "add_batch_ms": "addBatch",
                      "latest_offset_ms": "latestOffset", "query_planning_ms": "queryPlanning",
                      "wal_commit_ms": "walCommit", "commit_offsets_ms": "commitOffsets"}


def insert_rows(config: Config, table: str, rows: list[dict]) -> bool:
    """행을 JSONEachRow로 INSERT한다. 실패하면 경고만 남기고 False를 돌려준다. 예외를 올리지 않는다."""
    try:
        query = urllib.parse.urlencode({"query": f"INSERT INTO {table} FORMAT JSONEachRow", **INSERT_SETTINGS})
        body = "\n".join(json.dumps(row, allow_nan=False) for row in rows).encode("utf-8")
        request = urllib.request.Request(
            f"{config.clickhouse_http_url}/?{query}", data=body, method="POST",
            headers={"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": config.clickhouse_password})
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            response.read()
        return True
    except Exception as error:  # 지표 손실 허용: 어떤 실패도 주문 처리로 전파하지 않는다.
        log.warning("ops metrics insert failed table=%s rows=%d error=%s", table, len(rows), error)
        return False


def _number(value) -> float:
    """측정하지 않은 값(None)·비유한 값은 0으로 둔다. 측정 여부는 건수·status 컬럼으로 구분한다."""
    return float(value) if value is not None and math.isfinite(value) else 0.0


def batch_attempt_row(metrics: dict, counts: dict) -> dict:
    """process_batch의 계측 dict와 분류별 건수를 ops.spark_batch_attempts 행으로 바꾼다."""
    latency = metrics.get("kafka_to_callback_start_ms") or {}
    return {
        "query_id": metrics.get("query_id", ""), "run_id": metrics.get("run_id", ""),
        "batch_id": metrics["batch_id"], "callback_started_at": metrics["callback_started_at"],
        "status": metrics["status"], "failed_stage": metrics.get("failed_stage") or "",
        "input_rows": sum(counts.values()), "order_rows": counts.get(ORDER, 0),
        "excluded_rows": counts.get(EXCLUDED, 0), "dlq_rows": counts.get(DLQ, 0),
        "classification_ms": _number(metrics.get("classification_and_metrics_action_ms")),
        "orders_write_ms": _number(metrics.get("orders_write_action_ms")),
        "dlq_write_ms": _number(metrics.get("dlq_write_action_ms")),
        "callback_total_ms": _number(metrics.get("callback_total_ms")),
        "kafka_to_callback_ms_p50": _number(latency.get("p50_ms")),
        "kafka_to_callback_ms_p95": _number(latency.get("p95_ms")),
        "kafka_to_callback_ms_p99": _number(latency.get("p99_ms")),
        "kafka_to_callback_ms_max": _number(latency.get("max_ms")),
        "kafka_to_callback_ms_count": latency.get("count") or 0,
        "kafka_ts_missing_count": latency.get("missing_timestamp_count") or 0,
        "kafka_ts_future_count": latency.get("future_timestamp_count") or 0,
    }


def _offsets(value) -> dict:
    """소스 offset JSON({topic: {partition: offset}})을 파티션→offset으로 합친다. 단일 토픽 구독 전제."""
    if isinstance(value, str):
        value = json.loads(value)
    merged = {}
    for partitions in (value or {}).values():
        merged.update({str(partition): offset for partition, offset in partitions.items()})
    return merged


def progress_row(progress: dict) -> dict:
    """StreamingQueryProgress JSON을 ops.spark_query_progress 행으로 바꾼다."""
    durations = progress.get("durationMs") or {}
    end_offsets, latest_offsets = {}, {}
    for source in progress.get("sources") or []:
        end_offsets.update(_offsets(source.get("endOffset")))
        latest_offsets.update(_offsets(source.get("latestOffset")))
    return {
        "query_id": progress["id"], "run_id": progress["runId"], "batch_id": progress["batchId"],
        "trigger_at": progress["timestamp"],
        **{column: durations.get(key, 0) for column, key in PROGRESS_DURATIONS.items()},
        "num_input_rows": progress.get("numInputRows", 0),
        "input_rows_per_sec": _number(progress.get("inputRowsPerSecond")),
        "processed_rows_per_sec": _number(progress.get("processedRowsPerSecond")),
        "end_offsets": end_offsets, "latest_offsets": latest_offsets,
    }
