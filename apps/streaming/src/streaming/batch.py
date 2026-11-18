"""Driver에서 마이크로배치의 분류·출력·캐시 해제와 실패 계측을 제어한다."""

import json
import logging
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.config import Config
from streaming.monitoring import summarize_batch
from streaming.ops_metrics import BATCH_ATTEMPTS_TABLE, batch_attempt_row, insert_rows
from streaming.sinks import write_dlq, write_orders
from streaming.transforms import (DLQ, EXCLUDED, ORDER, build_dlq_records, classify_batch, deduplicate_events,
                                  select_orders)

log = logging.getLogger("streaming")


def with_spark_columns(orders: DataFrame, metrics: dict, callback_started_at: datetime) -> DataFrame:
    """ClickHouse 추적 컬럼(쿼리·실행·배치 ID, 콜백 진입 시각)을 붙인다. 같은 배치의 운영 지표 행과 같은 값이다."""
    return orders.select(
        "*",
        F.lit(metrics.get("query_id", "")).alias("spark_query_id"),
        F.lit(metrics.get("run_id", "")).alias("spark_run_id"),
        F.lit(metrics["batch_id"]).cast("long").alias("spark_batch_id"),
        F.lit(callback_started_at).alias("spark_processed_at"))


def process_batch(batch: DataFrame, batch_id: int, config: Config, query_context: dict | None = None) -> None:
    """변환 → 중복 제거 → 저장 순서를 제어하고 분류·출력 action을 계측한다. 실패한 시도도 로그에 남기고 예외는 쿼리에 전달한다."""
    callback_started_at = datetime.now(timezone.utc)
    callback_clock = time.perf_counter()
    metrics = {**(query_context or {}), "batch_id": batch_id,
               "callback_started_at": callback_started_at.isoformat(timespec="milliseconds"),
               "status": "failed", "failed_stage": None,
               "classification_and_metrics_action_ms": None,
               "orders_write_action_ms": None, "dlq_write_action_ms": None,
               "kafka_to_callback_start_ms": None}
    classified = None
    counts = {}
    stage = "classification_and_metrics_action"
    try:
        action_clock = time.perf_counter()
        try:
            # 집계·정상 출력·DLQ 쓰기마다 UDF가 다시 실행되지 않도록 분류 결과를 캐시한다.
            classified = classify_batch(batch, callback_started_at).persist()
            counts, latency = summarize_batch(classified, callback_started_at, config.latency_metrics_enabled)
            metrics["kafka_to_callback_start_ms"] = latency
        finally:
            metrics["classification_and_metrics_action_ms"] = (time.perf_counter() - action_clock) * 1000
        for outcome, writer, frame_builder, stage in (
            (ORDER, write_orders,
             lambda frame: with_spark_columns(deduplicate_events(select_orders(frame)), metrics, callback_started_at),
             "orders_write_action"),
            (DLQ, write_dlq, build_dlq_records, "dlq_write_action"),
        ):
            if counts.get(outcome):
                action_clock = time.perf_counter()
                try:
                    writer(frame_builder(classified), config)
                finally:
                    metrics[f"{stage}_ms"] = (time.perf_counter() - action_clock) * 1000
        log.info("batch %s", json.dumps({"batch_id": batch_id, "input": sum(counts.values()),
                                          "order": counts.get(ORDER, 0), "excluded": counts.get(EXCLUDED, 0),
                                          "dlq": counts.get(DLQ, 0)}))
        metrics["status"] = "succeeded"
    except Exception:
        metrics["failed_stage"] = stage
        raise
    finally:
        try:
            if classified is not None:
                classified.unpersist()
        except Exception:
            metrics["status"] = "failed"
            metrics["failed_stage"] = "cache_cleanup"
            raise
        finally:
            metrics["callback_total_ms"] = (time.perf_counter() - callback_clock) * 1000
            log.info("latency %s", json.dumps(metrics, allow_nan=False))
            # 실패한 시도도 기록한다. insert_rows는 예외를 올리지 않으므로 원래 배치 예외를 가리지 않는다.
            insert_rows(config, BATCH_ATTEMPTS_TABLE, [batch_attempt_row(metrics, counts)])
