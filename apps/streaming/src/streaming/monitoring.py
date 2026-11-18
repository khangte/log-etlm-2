"""배치 건수·지연을 집계하고 Driver에서 쿼리 진행 이벤트를 기록한다."""

import json
import logging
import os
import threading
import time
from collections.abc import Callable
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from streaming.config import Config
from streaming.ops_metrics import QUERY_PROGRESS_TABLE, insert_rows, progress_row

log = logging.getLogger("streaming")

def summarize_batch(classified: DataFrame, callback_started_at: datetime,
                    latency_enabled: bool = True) -> tuple[dict, dict | None]:
    """기존 분류 건수 action에서 전체·분류별 지연을 집계한다. 드라이버에는 최대 4행만 전달한다.

    latency_enabled가 False면 같은 action에서 분류별 건수만 집계하고 지연은 None으로 돌려준다.
    """
    if not latency_enabled:
        rows = classified.groupBy(F.col("result.outcome").alias("outcome")).count().collect()
        return {row["outcome"]: row["count"] for row in rows}, None
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

    def __init__(self, query_context: dict | None = None, config: Config | None = None):
        """단일 쿼리의 동기 시작 이벤트에서 콜백에 공유할 실행 식별자를 설정한다. config가 있으면 진행 정보를 ClickHouse에도 기록한다."""
        super().__init__()
        self.query_context = query_context if query_context is not None else {}
        self.config = config
        # 멈춤 감시 기준 시각. 배치 지표로 퍼지지 않도록 query_context와 분리한다.
        self.heartbeat = {"last_event_at": time.monotonic()}

    def onQueryStarted(self, event):
        """쿼리 id·runId를 남긴다. 재시작 시 runId로 실행을 구분한다."""
        ids = {"query_id": str(event.id), "run_id": str(event.runId)}
        self.query_context.update(ids)
        log.info("query started %s", json.dumps(ids))

    def onQueryProgress(self, event):
        """배치 ID·입력 건수·파티션별 offset 범위·처리속도·durationMs를 담은 진행 정보를 남긴다."""
        self.heartbeat["last_event_at"] = time.monotonic()
        log.info("progress %s", event.progress.json)
        if self.config is not None:
            try:
                row = progress_row(json.loads(event.progress.json))
            except Exception as error:  # 지표 변환 실패도 쿼리를 멈추지 않는다.
                log.warning("ops metrics progress conversion failed error=%s", error)
                return
            insert_rows(self.config, QUERY_PROGRESS_TABLE, [row])

    def onQueryIdle(self, event):
        """새 데이터가 없는 트리거는 기록하지 않는다. 멈춤 감시용 마지막 이벤트 시각만 갱신한다."""
        self.heartbeat["last_event_at"] = time.monotonic()

    def onQueryTerminated(self, event):
        """종료 원인 예외를 남긴다."""
        log.info("query terminated %s", json.dumps(
            {"id": str(event.id), "run_id": str(event.runId), "exception": event.exception}))


class StallWatchdog(threading.Thread):
    """진행·유휴 이벤트가 timeout초 동안 없으면 멈춤으로 보고 프로세스를 exit 1로 끝낸다.

    배치가 멈추면 진행 이벤트도 오지 않아 리스너만으로는 감지할 수 없다. 멈춘 태스크는
    query.stop()도 끝나지 않을 수 있으므로 정상 종료를 시도하지 않고 바로 종료하며,
    재시작은 Compose restart 정책에 맡긴다. 기준 시각은 ProgressLogger.heartbeat["last_event_at"]이다.
    """

    def __init__(self, heartbeat: dict, query_context: dict, timeout: float, interval: float = 5.0,
                 clock: Callable[[], float] = time.monotonic, exit_fn: Callable[[int], None] = os._exit):
        """감시 시작 시각을 첫 기준으로 둔다. clock·exit_fn은 테스트에서 바꿔 넣는다."""
        super().__init__(name="stall-watchdog", daemon=True)
        self.heartbeat, self.query_context, self.timeout, self.interval = heartbeat, query_context, timeout, interval
        self.clock, self.exit_fn = clock, exit_fn
        self.stopped = threading.Event()
        self.heartbeat["last_event_at"] = clock()

    def check(self) -> bool:
        """멈춤이면 오류 로그를 남기고 exit_fn(1)을 호출한 뒤 True를 돌려준다."""
        idle = self.clock() - self.heartbeat["last_event_at"]
        if idle <= self.timeout:
            return False
        log.error("query stalled: no progress or idle event for %.0fs (timeout %ss), exiting for restart %s",
                  idle, self.timeout, json.dumps(self.query_context))
        self.exit_fn(1)
        return True

    def run(self):
        """stop() 전까지 interval마다 확인한다."""
        while not self.stopped.wait(self.interval):
            if self.check():
                return

    def stop(self):
        """정상 종료 경로에서 감시를 끝낸다."""
        self.stopped.set()
