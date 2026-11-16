"""실행 진입점. stdout·file·kafka 출력을 지원한다.

stdout·파일에는 페이로드 + LF만 쓰고, Kafka에는 LF 없이 페이로드 bytes만 보낸다.
상태·최종 통계는 stderr에 JSON 한 줄로 쓴다(kafka 출력이면 "kafka" 키에 전송 지표 포함).
종료 코드: 0 정상 완료, 1 출력 오류, 2 설정 오류, 130 Ctrl+C.
"""

import json
import math
import random
import sys
import time
from collections import Counter
from typing import BinaryIO, TextIO

from confluent_kafka import KafkaException

from log_generator import pools
from log_generator.config import Config, ConfigError, load_config
from log_generator.generator import EventGenerator
from log_generator.outputs.file import open_file
from log_generator.outputs.kafka_producer import KafkaOutput

TICK_SEC = 0.005  # 5ms마다 밀린 만큼만 출력해 1초 분량을 한꺼번에 내보내지 않는다.


class Stats:
    """출력 완료(출력 버퍼 기록 성공) 기준 집계. 크기는 LF를 뺀 페이로드 바이트다.

    출력 오류로 종료하면 버퍼에 남아 전달되지 못한 건까지 포함해 실제보다 많을 수 있다.
    정상 완료·Ctrl+C는 flush 성공 후 집계하므로 실제 출력과 일치한다.
    """

    def __init__(self, target_eps: int):
        self.target_eps = target_eps
        self.start = time.monotonic()
        self.events = 0
        self.payload_bytes = 0
        self.max_payload = 0
        self.kind_counts = Counter()
        self.kind_bytes = Counter()
        self.kind_max = Counter()
        self.anomaly_counts = Counter()
        self.output_errors = 0
        self.last_status_time = self.start  # 직전 상태 출력 시점과 건수. 구간 EPS 계산용
        self.last_status_events = 0

    def record(self, kind: str, anomaly: str | None, size: int) -> None:
        self.events += 1
        self.payload_bytes += size
        self.max_payload = max(self.max_payload, size)
        self.kind_counts[kind] += 1
        self.kind_bytes[kind] += size
        self.kind_max[kind] = max(self.kind_max[kind], size)
        if anomaly:
            self.anomaly_counts[anomaly] += 1

    def progress(self) -> dict:
        """주기 상태용 요약. 직전 상태 출력 이후 구간의 건수·EPS를 포함한다."""
        now = time.monotonic()
        interval_events = self.events - self.last_status_events
        interval_sec = now - self.last_status_time
        self.last_status_time, self.last_status_events = now, self.events
        elapsed = now - self.start
        return {
            "status": "running",
            "elapsed_sec": round(elapsed, 3),
            "interval_events": interval_events,
            "interval_eps": round(interval_events / interval_sec, 1) if interval_sec else 0.0,
            "events_written": self.events,
            "actual_eps": round(self.events / elapsed, 1) if elapsed else 0.0,
        }

    def snapshot(self, status: str) -> dict:
        elapsed = time.monotonic() - self.start
        return {
            "status": status,
            "elapsed_sec": round(elapsed, 3),
            "target_eps": self.target_eps,
            "actual_eps": round(self.events / elapsed, 1) if elapsed else 0.0,
            "events_written": self.events,
            "payload_bytes": self.payload_bytes,
            "payload_bytes_per_sec": round(self.payload_bytes / elapsed, 1) if elapsed else 0.0,
            "payload_avg": round(self.payload_bytes / self.events, 1) if self.events else 0.0,
            "payload_max": self.max_payload,
            "kinds": {
                kind: {
                    "count": self.kind_counts[kind],
                    "avg": round(self.kind_bytes[kind] / self.kind_counts[kind], 1) if self.kind_counts[kind] else 0.0,
                    "max": self.kind_max[kind],
                }
                for kind in pools.EVENT_KINDS
            },
            "anomalies": {anomaly: self.anomaly_counts[anomaly] for anomaly in pools.ANOMALY_KINDS},
            "output_errors": self.output_errors,
        }


def _log(err: TextIO, data: dict) -> None:
    err.write(json.dumps(data, ensure_ascii=False) + "\n")
    err.flush()


def run(config: Config, out: BinaryIO | None, err: TextIO, kafka: KafkaOutput | None = None) -> tuple[Stats, str]:
    """설정 속도로 이벤트를 out(또는 kafka)에 기록한다. (통계, 종료 상태)를 반환한다.

    kafka가 있으면 out은 쓰지 않는다. Stats의 건수는 큐 등록 기준이고 ACK 성공 건수는 kafka 지표에 있다.
    """
    sink = kafka or out  # 둘 다 flush()를 제공하며 kafka의 flush는 실패·미전달 시 OSError를 낸다.
    generator = EventGenerator(random.Random(config.seed), pools.build_user_pool(config.user_pool_size))
    stats = Stats(config.eps)
    # 한 틱에 허용하는 최대 출력 수. 이보다 밀리면 기준 시각을 당겨 밀린 분량을 포기한다(몰아서 따라잡지 않음).
    burst = max(1, math.ceil(config.eps * TICK_SEC)) * 2
    schedule_start = stats.start
    next_tick = stats.start
    next_status = stats.start + config.status_interval_sec
    status = "completed"

    try:
        while config.max_events is None or stats.events < config.max_events:
            now = time.monotonic()
            if now < next_tick:
                # kafka는 대기 시간에 delivery 콜백을 처리한다.
                if kafka:
                    kafka.poll(next_tick - now)
                else:
                    time.sleep(next_tick - now)
                now = time.monotonic()
            next_tick += TICK_SEC
            if next_tick < now:  # 출력 지연으로 틱이 밀리면 다음 틱을 현재 기준으로 맞춘다.
                next_tick = now + TICK_SEC

            due = int((now - schedule_start) * config.eps) - stats.events
            if due > burst:
                schedule_start = now - (stats.events + burst) / config.eps
                due = burst
            if config.max_events is not None:
                due = min(due, config.max_events - stats.events)

            for _ in range(due):
                event = generator.next_event()
                if kafka:
                    kafka.send(event.payload)
                else:
                    out.write(event.payload + b"\n")
                stats.record(event.kind, event.anomaly, len(event.payload))

            if now >= next_status:
                _log(err, stats.progress() | ({"kafka": kafka.metrics()} if kafka else {}))
                next_status += config.status_interval_sec
                if next_status <= now:  # 출력 지연으로 여러 주기가 밀리면 몰아서 출력하지 않고 현재 기준으로 다시 센다.
                    next_status = now + config.status_interval_sec
        sink.flush()
    except KeyboardInterrupt:
        status = "interrupted"
        try:
            sink.flush()
        except OSError as exc:
            stats.output_errors += 1
            status = "output_error"
            _log(err, {"error": f"output flush failed: {exc}"})
    except OSError as exc:
        # 실패한 건은 record 전에 예외가 나므로 성공 건수에 포함되지 않는다.
        stats.output_errors += 1
        status = "output_error"
        _log(err, {"error": f"output write failed: {exc}"})
    _log(err, stats.snapshot(status) | ({"kafka": kafka.metrics(detail=True)} if kafka else {}))
    return stats, status


def main() -> int:
    try:
        config = load_config()
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    kafka = None
    if config.output == "kafka":
        out = None
        try:
            kafka = KafkaOutput(config.kafka, sys.stderr)
        except KafkaException as exc:  # 클라이언트 설정값을 librdkafka가 거부한 경우
            print(f"cannot create kafka producer: {exc}", file=sys.stderr)
            return 2
    elif config.output == "file":
        try:
            out = open_file(config.output_path)
        except OSError as exc:
            print(f"cannot create output file: {exc}", file=sys.stderr)
            return 2
    else:
        out = sys.stdout.buffer

    _, status = run(config, out, sys.stderr, kafka)
    if config.output == "file":
        try:
            out.close()
        except OSError:
            status = "output_error"
    if status == "output_error" and config.output == "stdout":
        # 파이프가 닫힌 경우 인터프리터 종료 시 flush 오류가 다시 나지 않도록 stdout을 버린다.
        sys.stdout = open("/dev/null", "w")
    return {"completed": 0, "output_error": 1, "interrupted": 130}[status]


if __name__ == "__main__":
    sys.exit(main())
