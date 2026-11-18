"""streaming.ops_metrics: 운영 지표 행 변환과 INSERT 실패 격리를 검증한다."""

import json
import types

from streaming import ops_metrics

# 3단계 지속 부하 측정(2026-11-17)의 실제 progress JSON에서 필요한 필드만 남겼다.
PROGRESS = {
    "id": "1d841388-b026-4329-adc3-f9b3ba1450a3", "runId": "a59d9926-1a3d-4eb5-b38d-db27a1459c69",
    "name": "orders-stream", "timestamp": "2026-11-17T05:41:24.971Z", "batchId": 5,
    "numInputRows": 49999, "processedRowsPerSecond": 5321.4,
    "durationMs": {"addBatch": 8900, "commitOffsets": 27, "getBatch": 0, "latestOffset": 6,
                   "triggerExecution": 9395, "walCommit": 35},
    "sources": [{"description": "KafkaV2[Subscribe[raw.ecommerce.orders.v1]]",
                 "endOffset": {"raw.ecommerce.orders.v1": {"0": 100, "1": 200, "2": 300}},
                 "latestOffset": {"raw.ecommerce.orders.v1": {"0": 150, "1": 200, "2": 330}}}],
}


def test_progress_row_변환():
    """durationMs 누락 키는 0, 비유한 처리율은 0(Spark JSON은 NaN 필드를 생략), offset은 파티션→offset Map으로 합친다."""
    progress = {**PROGRESS, "inputRowsPerSecond": float("nan")}
    row = ops_metrics.progress_row(progress)
    assert (row["batch_id"], row["trigger_at"], row["num_input_rows"]) == (5, "2026-11-17T05:41:24.971Z", 49999)
    assert (row["trigger_execution_ms"], row["add_batch_ms"], row["query_planning_ms"]) == (9395, 8900, 0)
    assert row["input_rows_per_sec"] == 0.0 and row["processed_rows_per_sec"] == 5321.4
    assert row["end_offsets"] == {"0": 100, "1": 200, "2": 300}
    assert row["latest_offsets"] == {"0": 150, "1": 200, "2": 330}
    json.dumps(row, allow_nan=False)  # INSERT 본문으로 직렬화할 수 있어야 한다.


def test_batch_attempt_row_미측정값은_0과_건수로_구분():
    """실행하지 않은 action·지연 집계 없음은 0이며, 실패 단계와 건수는 그대로 남는다."""
    metrics = {"batch_id": 3, "callback_started_at": "2026-11-17T05:41:26.735+00:00", "status": "failed",
               "failed_stage": "orders_write_action", "classification_and_metrics_action_ms": 12.5,
               "orders_write_action_ms": 3.0, "dlq_write_action_ms": None, "kafka_to_callback_start_ms": None,
               "callback_total_ms": 20.0}
    row = ops_metrics.batch_attempt_row(metrics, {"order": 4, "excluded": 1})
    assert (row["query_id"], row["status"], row["failed_stage"]) == ("", "failed", "orders_write_action")
    assert (row["input_rows"], row["order_rows"], row["excluded_rows"], row["dlq_rows"]) == (5, 4, 1, 0)
    assert (row["dlq_write_ms"], row["kafka_to_callback_ms_count"], row["kafka_to_callback_ms_p99"]) == (0.0, 0, 0.0)
    json.dumps(row, allow_nan=False)


def test_insert_실패는_예외없이_False(caplog):
    """연결 실패는 경고만 남기고 비밀번호를 로그에 쓰지 않는다."""
    config = types.SimpleNamespace(clickhouse_http_url="http://127.0.0.1:1", clickhouse_password="secret-value")
    with caplog.at_level("WARNING", logger="streaming"):
        assert ops_metrics.insert_rows(config, "ops.spark_batch_attempts", [{"batch_id": 1}]) is False
    assert "ops metrics insert failed" in caplog.text and "secret-value" not in caplog.text
