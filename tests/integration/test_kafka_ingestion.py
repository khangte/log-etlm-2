"""실제 Kafka 브로커로 원본 보존과 LogAppendTime timestamp를 검증한다.

브로커(compose의 kafka·kafka-init 기동 후)에 접속할 수 없으면 건너뛴다.
실행: cd apps/log_generator && uv run pytest ../../tests/integration -v
실행마다 테스트 메시지 8건(테스트 2개 × 4건)이 원본 토픽에 남는다. 실제 .env의 접속 주소·토픽을 사용한다.
"""

import io
import os
import random
import time
import unittest
import uuid

from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from dotenv import dotenv_values

from log_generator import pools
from log_generator.config import ENV_FILE, ConfigError, load_config
from log_generator.generator import EventGenerator, serialize
from log_generator.outputs.kafka_producer import KafkaOutput, _event_ms

CONSUME_TIMEOUT_SEC = 20


def _kafka_config():
    """.env와 환경변수에서 Kafka 설정을 읽는다. 읽을 수 없으면 건너뛴다."""
    try:
        env = {**(dotenv_values(ENV_FILE) if ENV_FILE and ENV_FILE.exists() else {}), **os.environ}
        return load_config(env | {"PRODUCER_OUTPUT": "kafka"}).kafka
    except ConfigError as exc:
        raise unittest.SkipTest(f"Kafka 설정을 읽을 수 없음: {exc}")


class KafkaIngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kafka = _kafka_config()
        try:
            meta = AdminClient({"bootstrap.servers": cls.kafka.bootstrap_servers}).list_topics(
                cls.kafka.topic, timeout=5)
            partitions = meta.topics[cls.kafka.topic].partitions
        except Exception as exc:  # 브로커 미기동·토픽 없음
            raise unittest.SkipTest(f"Kafka 브로커에 접속할 수 없음: {exc}")
        if not partitions:
            raise unittest.SkipTest(f"토픽이 없음: {cls.kafka.topic}")
        cls.consumer = Consumer({
            "bootstrap.servers": cls.kafka.bootstrap_servers,
            "group.id": f"it-{uuid.uuid4()}",
            "enable.auto.commit": False,
        })
        # 전송 전 끝 offset부터 읽어 다른 데이터와 섞이지 않게 한다.
        cls.consumer.assign([
            TopicPartition(cls.kafka.topic, p, cls.consumer.get_watermark_offsets(
                TopicPartition(cls.kafka.topic, p), timeout=5)[1])
            for p in partitions
        ])

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "consumer"):
            cls.consumer.close()

    def send_and_consume(self, payloads: list[bytes]):
        """KafkaOutput으로 보내고 같은 값을 가진 레코드를 소비해 {값: 레코드}로 반환한다."""
        out = KafkaOutput(self.kafka, io.StringIO())
        for payload in payloads:
            out.send(payload)
        out.flush()  # 전송 실패·미전달이면 예외
        wanted, found = set(payloads), {}
        deadline = time.monotonic() + CONSUME_TIMEOUT_SEC
        while wanted - found.keys() and time.monotonic() < deadline:
            msg = self.consumer.poll(1.0)
            if msg is not None and not msg.error() and msg.value() in wanted:
                found[msg.value()] = msg
        self.assertEqual(wanted - found.keys(), set(), "소비하지 못한 메시지가 있음")
        return out, found

    def sample_payloads(self) -> dict[str, bytes]:
        """정상·health_check·불량(JSON 문법 오류)·UTF-8이 아닌 raw bytes. 고유 event_id로 다른 데이터와 구분한다."""
        gen = EventGenerator(random.Random(), pools.build_user_pool(10))
        return {
            "normal": serialize(gen.order()),
            "health_check": serialize(gen.health_check()),
            "malformed": gen.invalid_order("malformed_json").payload,
            "raw": b"\xff\xfe not-json " + uuid.uuid4().bytes,
        }

    def test_original_bytes_preserved_without_lf(self):
        payloads = self.sample_payloads()
        _, found = self.send_and_consume(list(payloads.values()))
        for name, payload in payloads.items():
            with self.subTest(name):
                value = found[payload].value()
                self.assertEqual(value, payload)  # bytes 단위 일치
                self.assertNotIn(b"\n", value)  # 파일용 LF 구분자 없음
                self.assertIsNone(found[payload].key())  # key 미지정

    def test_log_append_time_timestamp_and_generation_delay(self):
        payloads = self.sample_payloads()
        out, found = self.send_and_consume(list(payloads.values()))
        for name in ("normal", "health_check"):
            ts_type, ts = found[payloads[name]].timestamp()
            self.assertEqual(ts_type, TIMESTAMP_LOG_APPEND_TIME, name)
            delay_ms = ts - _event_ms(payloads[name])  # 음수일 수 있어 부호는 단언하지 않는다.
            self.assertLess(abs(delay_ms), 60_000, f"{name} 지연 {delay_ms}ms")
        # 해석 불가 로그는 지연 계산 대상이 아니다.
        self.assertIsNone(_event_ms(payloads["malformed"]))
        # 성공 콜백의 msg.timestamp()도 LogAppendTime이므로 Producer가 지연을 집계한다.
        metrics = out.metrics(detail=True)
        self.assertEqual(metrics["gen_to_kafka_samples"], 2)
        self.assertEqual(metrics["timestamp_not_log_append_time"], 0)
        self.assertEqual(metrics["gen_to_kafka_unparseable"], 2)


if __name__ == "__main__":
    unittest.main()
