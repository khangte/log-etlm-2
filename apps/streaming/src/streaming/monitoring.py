"""배치 건수·지연을 집계하고 Driver에서 쿼리 진행 이벤트를 기록한다."""

import json
import logging
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

log = logging.getLogger("streaming")

def summarize_batch(classified: DataFrame, callback_started_at: datetime) -> tuple[dict, dict]:
    """기존 분류 건수 action에서 전체·분류별 지연을 집계한다. 드라이버에는 최대 4행만 전달한다."""
    # 콜백 진입 시각은 action 이전에 고정한다. Spark의 지연 실행 시각으로 대체하지 않는다.
    started_ms = int(callback_started_at.timestamp() * 1000)
    delay = F.lit(started_ms) - F.unix_millis("kafka_append_timestamp")
    valid = F.when(delay >= 0, delay)
    rows = (classified.rollup(F.col("result.outcome").alias("outcome"))
            .agg(F.count("*").alias("input_count"),
                 F.count(valid).alias("count"),
                 F.count(F.when(F.col("kafka_append_timestamp").isNull(), 1)).alias("missing_timestamp_count"),
                 F.count(F.when(delay < 0, 1)).alias("future_timestamp_count"),
                 F.min(valid).alias("min_ms"), F.max(valid).alias("max_ms"), F.avg(valid).alias("mean_ms"),
                 F.percentile_approx(valid, [0.5, 0.95, 0.99], 10000).alias("percentiles"))
            .collect())
    # 빈 입력의 rollup은 집계 행을 반환하지 않을 수 있다. 분위수에 가짜 0을 넣지 않는다.
    counts, latency = {}, {"by_outcome": {}, "count": 0, "missing_timestamp_count": 0,
                           "future_timestamp_count": 0, "min_ms": None, "max_ms": None, "mean_ms": None,
                           "p50_ms": None, "p95_ms": None, "p99_ms": None}
    for row in rows:
        stats = row.asDict()
        outcome = stats.pop("outcome")
        count = stats.pop("input_count")
        quantiles = stats.pop("percentiles") or [None, None, None]
        stats.update(zip(("p50_ms", "p95_ms", "p99_ms"), quantiles))
        if outcome is None:
            latency.update(stats)
        else:
            counts[outcome] = count
            latency["by_outcome"][outcome] = stats
    return counts, latency


class ProgressLogger(StreamingQueryListener):
    """쿼리 시작·진행·종료 정보를 한 줄 JSON으로 로그에 남긴다."""

    def __init__(self, query_context: dict | None = None):
        """단일 쿼리의 동기 시작 이벤트에서 콜백에 공유할 실행 식별자를 설정한다."""
        super().__init__()
        self.query_context = query_context if query_context is not None else {}

    def onQueryStarted(self, event):
        """쿼리 id·runId를 남긴다. 재시작 시 runId로 실행을 구분한다."""
        self.query_context.update(query_id=str(event.id), run_id=str(event.runId))
        log.info("query started %s", json.dumps(self.query_context))

    def onQueryProgress(self, event):
        """배치 ID·입력 건수·파티션별 offset 범위·처리속도·durationMs를 담은 진행 정보를 남긴다."""
        log.info("progress %s", event.progress.json)

    def onQueryIdle(self, event):
        """새 데이터가 없는 트리거는 기록하지 않는다."""

    def onQueryTerminated(self, event):
        """종료 원인 예외를 남긴다."""
        log.info("query terminated %s", json.dumps(
            {"id": str(event.id), "run_id": str(event.runId), "exception": event.exception}))
