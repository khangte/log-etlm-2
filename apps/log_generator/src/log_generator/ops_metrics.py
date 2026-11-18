"""생성기·Producer 1초 구간 지표를 ClickHouse ops.producer_stats에 HTTP INSERT한다.

근거: ClickHouse PRD '생성기·Producer 지표'. Spark 지표와 같은 HTTP + async_insert 방식이다.
INSERT는 백그라운드 스레드에서 보내 생성 속도에 영향을 주지 않는다. 실패는 경고만 남기고 생성을 계속한다.
"""

import json
import queue
import threading
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from typing import TextIO

from log_generator.config import ClickHouseConfig
from log_generator.outputs.kafka_producer import percentiles

TABLE = "ops.producer_stats"
USER = "generator_writer"
TIMEOUT_SECONDS = 5
CLOSE_TIMEOUT_SECONDS = 10  # 종료 시 남은 행 전송을 기다리는 최대 시간
# 작은 INSERT는 서버가 묶도록 async_insert를 쓰고, 오류를 알 수 있게 flush까지 기다린다.
INSERT_SETTINGS = {"async_insert": 1, "wait_for_async_insert": 1, "date_time_input_format": "best_effort"}


def quantile_columns(prefix: str, histogram: Counter) -> dict:
    """히스토그램을 {prefix}_p50·_p95·_p99·_count 컬럼으로 바꾼다. 건수 0이면 분위수는 0이다."""
    values = percentiles(histogram)
    return {f"{prefix}_{q}": float(values[q] or 0) for q in ("p50", "p95", "p99")} | {
        f"{prefix}_count": histogram.total()}


def producer_stats_row(interval_start: float, run_id: str, target_eps: int, generated: int,
                       kafka_interval: dict, serialize_us: Counter) -> dict:
    """구간 시작 epoch 초·생성 건수·KafkaOutput.interval_metrics() 결과를 테이블 행으로 바꾼다."""
    started = datetime.fromtimestamp(interval_start, timezone.utc).isoformat(timespec="milliseconds")
    return {
        "interval_start": started, "run_id": run_id, "target_eps": target_eps, "generated": generated,
        "acked": kafka_interval["acked"], "failed": kafka_interval["failed"],
        "undelivered": kafka_interval["undelivered"], "queue_len_max": kafka_interval["queue_len_max"],
        **quantile_columns("ack_latency_ms", kafka_interval["ack_latency_ms"]),
        **quantile_columns("gen_to_kafka_ms", kafka_interval["gen_to_kafka_ms"]),
        **quantile_columns("gen_serialize_us", serialize_us),
    }


class ProducerStatsWriter:
    """행을 큐에 받아 백그라운드 스레드가 쌓인 만큼 한 번에 INSERT한다."""

    def __init__(self, config: ClickHouseConfig, err: TextIO):
        """접속 정보와 경고 출력 대상을 받고 전송 스레드를 시작한다."""
        self.config = config
        self.err = err
        self.failed_rows = 0  # 전송 실패로 잃은 행 수(최종 통계용)
        self._rows: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._loop, name="producer-stats-writer", daemon=True)
        self._thread.start()

    def submit(self, row: dict) -> None:
        """행을 전송 대기열에 넣는다. 생성 루프를 막지 않는다."""
        self._rows.put(row)

    def close(self) -> None:
        """남은 행을 보내고 스레드를 끝낸다. 최대 CLOSE_TIMEOUT_SECONDS만 기다린다."""
        self._rows.put(None)
        self._thread.join(CLOSE_TIMEOUT_SECONDS)

    def _loop(self) -> None:
        """대기열이 빌 때까지 모은 행을 INSERT 1회로 보낸다. None을 받으면 끝낸다."""
        while True:
            batch, stop = [self._rows.get()], False
            while True:
                try:
                    batch.append(self._rows.get_nowait())
                except queue.Empty:
                    break
            if None in batch:
                stop, batch = True, [row for row in batch if row is not None]
            if batch:
                self._insert(batch)
            if stop:
                return

    def _insert(self, rows: list[dict]) -> None:
        """JSONEachRow로 POST한다. 실패하면 경고 한 줄만 남긴다. 예외를 올리지 않는다."""
        try:
            query = urllib.parse.urlencode({"query": f"INSERT INTO {TABLE} FORMAT JSONEachRow", **INSERT_SETTINGS})
            body = "\n".join(json.dumps(row, allow_nan=False) for row in rows).encode("utf-8")
            request = urllib.request.Request(
                f"{self.config.http_url}/?{query}", data=body, method="POST",
                headers={"X-ClickHouse-User": USER, "X-ClickHouse-Key": self.config.password})
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                response.read()
        except Exception as error:  # 지표 손실 허용: 어떤 실패도 생성으로 전파하지 않는다.
            self.failed_rows += len(rows)
            self.err.write(json.dumps({"warning": "producer_stats insert failed", "rows": len(rows),
                                       "error": str(error)}, ensure_ascii=False) + "\n")
            self.err.flush()
