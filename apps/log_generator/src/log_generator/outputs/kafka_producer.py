"""confluent-kafka 비동기 Producer로 직렬화된 페이로드를 그대로 전송한다.

원본 bytes는 재직렬화하지 않고 LF 없이 메시지 값으로 보내며 key는 지정하지 않는다.
전송 성공은 큐 등록이 아니라 delivery 콜백의 성공(ACK)이다.
실패(delivery 최종 실패·fatal 오류·큐 포화 시간 초과)는 KafkaOutputError(OSError)로 알려
main.run이 기존 출력 오류와 같은 경로(종료 코드 1)로 처리하게 한다.
"""

import json
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Callable, TextIO

from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, KafkaError, Producer

from log_generator.config import KafkaConfig

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MS = timedelta(milliseconds=1)


class KafkaOutputError(OSError):
    """Kafka 전송을 계속할 수 없거나 종료 시 전달 실패·미전달이 남은 경우."""


def percentiles(counts: Counter) -> dict:
    """{값: 건수} 히스토그램의 nearest-rank 백분위. 음수도 0으로 보정하지 않는다. 값이 없으면 None."""
    total = counts.total()
    if not total:
        return {"p50": None, "p95": None, "p99": None}
    result, keys, seen = {}, iter(sorted(counts)), 0
    for q in (50, 95, 99):
        rank = max(1, -(-total * q // 100))  # ceil(total × q / 100)번째 값
        while seen < rank:
            value = next(keys)
            seen += counts[value]
        result[f"p{q}"] = value
    return result


def _event_ms(payload: bytes) -> int | None:
    """원본의 event_timestamp를 epoch ms로 읽는다. JSON을 해석할 수 없거나 값이 없으면 None."""
    try:
        parsed = datetime.fromisoformat(json.loads(payload)["event_timestamp"])
    except (ValueError, KeyError, TypeError):
        return None
    return (parsed - _EPOCH) // _MS


class KafkaOutput:
    def __init__(self, config: KafkaConfig, err: TextIO, producer_factory: Callable[[dict], Producer] = Producer):
        self.config = config
        self.err = err
        self.start = time.monotonic()
        self.generated = 0  # send() 호출 건수(큐 등록 실패 포함)
        self.queued = 0
        self.acked = 0
        self.failed = 0
        self.client_errors = 0  # fatal이 아닌 error_cb 건수
        self.retries = 0  # 통계 콜백의 브로커 요청 재시도 누적(메시지별 정확한 건수가 아니다)
        self.tx_errors = 0
        # 값 대신 {지연: 건수}만 둬서 무제한 실행에도 메모리가 실행 시간에 비례해 늘지 않는다.
        self.ack_latency_ms: Counter = Counter()  # 0.1ms 단위로 반올림
        self.append_delay_ms: Counter = Counter()  # 생성 -> Kafka(LogAppendTime) 지연(ms 정수). 음수 포함
        self.delay_unparseable = 0  # event_timestamp를 읽을 수 없어 지연 계산 불가
        self.delay_not_append_time = 0  # 콜백 timestamp 종류가 LogAppendTime이 아님
        self._abort: str | None = None
        self._last_status = (self.start, 0)
        self._producer = producer_factory({
            "bootstrap.servers": config.bootstrap_servers,
            # 단일 ISR에서도 브로커 확인을 성공 기준으로 하고 클라이언트 재시도 중복을 억제한다.
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": config.linger_ms,
            "compression.type": config.compression,
            "delivery.timeout.ms": config.delivery_timeout_ms,
            # 이 값이 없으면 통계 콜백이 호출되지 않는다.
            "statistics.interval.ms": 1000,
            "error_cb": self._on_error,
            "stats_cb": self._on_stats,
        })

    # --- 콜백 (poll/flush 중 호출된다) ---

    def _on_delivery(self, error: KafkaError | None, msg) -> None:
        latency = msg.latency()
        if latency is not None:
            self.ack_latency_ms[round(latency * 1000, 1)] += 1
        if error is not None:
            self.failed += 1
            self._abort = self._abort or f"delivery failed: {error}"
            return
        self.acked += 1
        ts_type, ts = msg.timestamp()
        if ts_type != TIMESTAMP_LOG_APPEND_TIME:
            self.delay_not_append_time += 1
            return
        event_ms = _event_ms(msg.value())
        if event_ms is None:
            self.delay_unparseable += 1
        else:
            self.append_delay_ms[ts - event_ms] += 1

    def _on_error(self, error: KafkaError) -> None:
        # 브로커 단절·fatal 오류는 개별 메시지가 아닌 이 콜백으로만 전달된다.
        if error.fatal():
            self._abort = self._abort or f"fatal error: {error}"
        else:
            self.client_errors += 1
        self.err.write(json.dumps({"kafka_error": str(error), "fatal": bool(error.fatal())}) + "\n")
        self.err.flush()

    def _on_stats(self, raw: str) -> None:
        brokers = json.loads(raw).get("brokers", {}).values()
        self.retries = sum(b.get("txretries", 0) for b in brokers)
        self.tx_errors = sum(b.get("txerrs", 0) for b in brokers)

    # --- 전송 ---

    def _check(self) -> None:
        if self._abort:
            raise KafkaOutputError(self._abort)

    def poll(self, timeout: float = 0) -> None:
        """delivery·오류·통계 콜백을 처리하고 중단 사유가 있으면 예외로 알린다."""
        self._producer.poll(timeout)
        self._check()

    def send(self, payload: bytes) -> None:
        self.generated += 1
        self._check()
        deadline = time.monotonic() + self.config.queue_full_timeout_sec
        while True:
            try:
                self._producer.produce(self.config.topic, payload, on_delivery=self._on_delivery)
                self.queued += 1
                return
            except BufferError:
                # 로컬 큐 포화: 콜백을 처리해 빈 자리를 만들며 최대 대기 시간까지 재시도한다.
                if time.monotonic() >= deadline:
                    raise KafkaOutputError(
                        f"producer queue full for {self.config.queue_full_timeout_sec}s"
                    ) from None
                self.poll(0.1)

    def flush(self) -> None:
        """최대 flush_timeout_sec 대기. 전달 실패나 미전달이 남으면 성공 종료로 보지 않는다."""
        self._producer.flush(self.config.flush_timeout_sec)
        self._check()
        undelivered = self.undelivered
        if undelivered:
            raise KafkaOutputError(f"{undelivered} messages undelivered after flush")

    @property
    def undelivered(self) -> int:
        return self.queued - self.acked - self.failed

    def metrics(self, detail: bool = False) -> dict:
        """상태 출력용 요약. detail=True(최종 통계)만 백분위를 계산한다(주기 호출 시 정렬 비용 회피).

        호출할 때마다 직전 호출 이후 구간의 ACK EPS를 갱신한다.
        """
        now = time.monotonic()
        last_time, last_acked = self._last_status
        self._last_status = (now, self.acked)
        elapsed = now - self.start
        result = {
            "generated": self.generated,
            "queued": self.queued,
            "acked": self.acked,
            "failed": self.failed,
            "undelivered": self.undelivered,
            "ack_eps": round(self.acked / elapsed, 1) if elapsed else 0.0,
            "interval_ack_eps": round((self.acked - last_acked) / (now - last_time), 1) if now > last_time else 0.0,
            "queue_len": len(self._producer),
        }
        if detail:
            result |= {
                "acks": "all",
                "linger_ms": self.config.linger_ms,
                "compression": self.config.compression,
                "ack_latency_ms": percentiles(self.ack_latency_ms),
                "gen_to_kafka_ms": percentiles(self.append_delay_ms),
                "gen_to_kafka_samples": self.append_delay_ms.total(),
                "gen_to_kafka_negative": sum(c for d, c in self.append_delay_ms.items() if d < 0),
                "gen_to_kafka_min": min(self.append_delay_ms, default=None),
                "gen_to_kafka_unparseable": self.delay_unparseable,
                "timestamp_not_log_append_time": self.delay_not_append_time,
                "client_errors": self.client_errors,
                "retries": self.retries,
                "tx_errors": self.tx_errors,
            }
        return result
