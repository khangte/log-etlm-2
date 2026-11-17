"""브로커 없이 대체 Producer로 Kafka 출력의 전송·오류·종료 동작을 검증한다."""

import io
import json
import signal
import unittest
from collections import Counter
from unittest.mock import patch

from confluent_kafka import KafkaError

from log_generator.config import KafkaConfig, load_config
from log_generator.main import main, run
from log_generator.outputs.kafka_producer import KafkaOutput, KafkaOutputError, percentiles
from test_generator import ENV_VALUES

KAFKA = KafkaConfig("localhost:9092", "raw.ecommerce.orders.v1", 5, "lz4", 30000, 30, 30)
APPEND = 2  # TIMESTAMP_LOG_APPEND_TIME
CREATE = 1  # TIMESTAMP_CREATE_TIME


def payload(event_ms: int) -> bytes:
    # 밀리초 UTC 문자열 형식의 event_timestamp만 필요한 최소 페이로드
    return json.dumps({"event_timestamp": "1970-01-01T00:00:%06.3fZ" % (event_ms / 1000)}).encode()


class FakeMsg:
    def __init__(self, value, ts_type=APPEND, ts=0, latency=0.01):
        self._value, self._ts, self._latency = value, (ts_type, ts), latency

    def value(self):
        return self._value

    def timestamp(self):
        return self._ts

    def latency(self):
        return self._latency


class FakeProducer:
    """produce 호출을 기록하고 poll/flush에서 정해진 결과로 delivery 콜백을 호출한다."""

    def __init__(self, conf, capacity=None, result=None):
        self.conf, self.capacity, self.result = conf, capacity, result
        self.produced, self.pending = [], []
        self.deliver_on_flush = True

    def produce(self, topic, value, **kwargs):
        if self.capacity is not None and len(self.pending) >= self.capacity:
            raise BufferError("queue full")
        self.produced.append((topic, value, kwargs))
        self.pending.append((value, kwargs["on_delivery"]))

    def _deliver(self):
        pending, self.pending = self.pending, []
        for value, callback in pending:
            if self.result is None:
                callback(None, FakeMsg(value, ts=1000))
            else:
                callback(self.result, FakeMsg(value))

    def poll(self, timeout=0):
        if self.capacity is None or timeout > 0:
            self._deliver()

    def flush(self, timeout=None):
        if self.deliver_on_flush:
            self._deliver()
        return len(self.pending)

    def __len__(self):
        return len(self.pending)


def make(**kwargs) -> tuple[KafkaOutput, FakeProducer, io.StringIO]:
    err, holder = io.StringIO(), {}

    def factory(conf):
        holder["p"] = FakeProducer(conf, **kwargs)
        return holder["p"]

    out = KafkaOutput(KAFKA, err, factory)
    return out, holder["p"], err


class KafkaOutputTests(unittest.TestCase):
    def test_sends_original_bytes_without_key_or_lf_and_sets_producer_config(self):
        out, producer, _ = make()
        data = b'{"broken":' + bytes([0xFF, 0xFE])  # 불량 bytes도 그대로 보낸다.
        out.send(data)
        topic, value, kwargs = producer.produced[0]
        self.assertEqual((topic, value), ("raw.ecommerce.orders.v1", data))
        self.assertEqual(set(kwargs), {"on_delivery"})  # key·headers·timestamp 미지정
        conf = producer.conf
        for key, expected in {"acks": "all", "enable.idempotence": True, "linger.ms": 5,
                              "compression.type": "lz4", "delivery.timeout.ms": 30000,
                              "statistics.interval.ms": 1000, "bootstrap.servers": "localhost:9092"}.items():
            self.assertEqual(conf[key], expected, key)
        self.assertTrue(callable(conf["error_cb"]) and callable(conf["stats_cb"]))

    def test_success_counts_and_latency_metrics(self):
        out, _, _ = make()
        for ms in (100, 200, 400):
            out.send(payload(ms))
        out.send(b"not json")
        self.assertEqual((out.generated, out.queued, out.acked), (4, 4, 0))  # 큐 등록은 성공이 아니다.
        out.poll()
        out.flush()
        m = out.metrics(detail=True)
        self.assertEqual((m["queued"], m["acked"], m["failed"], m["undelivered"]), (4, 4, 0, 0))
        # append 시각 1000ms 기준: 900·800·600ms. 파싱 불가 1건은 지연 계산에서 제외한다.
        self.assertEqual(m["gen_to_kafka_samples"], 3)
        self.assertEqual(m["gen_to_kafka_unparseable"], 1)
        self.assertEqual(m["gen_to_kafka_ms"]["p50"], 800)
        self.assertEqual(m["ack_latency_ms"]["p99"], 10.0)

    def test_negative_delay_is_kept_not_clamped(self):
        out, producer, _ = make()
        out.send(payload(1500))  # append(1000) - event(1500) = -500
        out.send(payload(900))
        out.flush()
        m = out.metrics(detail=True)
        self.assertEqual((m["gen_to_kafka_negative"], m["gen_to_kafka_min"]), (1, -500))
        self.assertEqual(m["gen_to_kafka_ms"]["p50"], -500)

    def test_non_log_append_time_timestamp_is_not_used_for_delay(self):
        out, _, _ = make()
        out.send(payload(0))
        msg = FakeMsg(payload(0), ts_type=CREATE, ts=5)
        out._on_delivery(None, msg)
        m = out.metrics(detail=True)
        self.assertEqual(m["timestamp_not_log_append_time"], 1)
        self.assertEqual(m["gen_to_kafka_samples"], 0)

    def test_queue_full_polls_until_space_then_sends(self):
        out, producer, _ = make(capacity=1)
        out.send(payload(1))
        out.send(payload(2))  # 큐 포화 -> poll(0.1)로 비운 뒤 재시도
        self.assertEqual(len(producer.produced), 2)
        self.assertEqual((out.generated, out.queued), (2, 2))

    def test_queue_full_gives_up_after_timeout(self):
        out, producer, _ = make(capacity=1)
        producer.deliver_on_flush = False
        producer.poll = lambda timeout=0: None  # 큐가 영원히 비지 않는다.
        out.send(payload(1))
        clock = iter(range(0, 1000, 10))  # 호출마다 10초 경과
        with patch("log_generator.outputs.kafka_producer.time") as fake_time:
            fake_time.monotonic.side_effect = lambda: next(clock)
            with self.assertRaisesRegex(KafkaOutputError, "queue full"):
                out.send(payload(2))
        self.assertEqual((out.generated, out.queued), (2, 1))  # 큐 등록 실패는 queued에 포함하지 않는다.

    def test_delivery_failure_aborts_on_next_call(self):
        error = KafkaError(KafkaError._MSG_TIMED_OUT, "timed out")
        out, _, _ = make(result=error)
        out.send(payload(1))
        with self.assertRaisesRegex(KafkaOutputError, "delivery failed"):
            out.poll()
        self.assertEqual((out.failed, out.acked), (1, 0))
        with self.assertRaises(KafkaOutputError):
            out.send(payload(2))

    def test_fatal_error_cb_aborts_and_non_fatal_only_counts(self):
        out, producer, err = make()
        error_cb = producer.conf["error_cb"]
        error_cb(KafkaError(KafkaError._ALL_BROKERS_DOWN, "all down"))
        out.send(payload(1))  # fatal이 아니므로 계속 전송한다.
        self.assertEqual(out.client_errors, 1)
        error_cb(KafkaError(KafkaError._FATAL, "fenced", fatal=True))
        with self.assertRaisesRegex(KafkaOutputError, "fatal error"):
            out.send(payload(2))
        self.assertEqual(sum('"fatal": true' in line for line in err.getvalue().splitlines()), 1)

    def test_flush_with_remaining_messages_is_failure_with_counts(self):
        out, producer, _ = make()
        producer.deliver_on_flush = False
        out.send(payload(1))
        out.send(payload(2))
        with self.assertRaisesRegex(KafkaOutputError, "2 messages undelivered"):
            out.flush()
        self.assertEqual(out.metrics()["undelivered"], 2)

    def test_stats_callback_aggregates_retries(self):
        out, producer, _ = make()
        producer.conf["stats_cb"](json.dumps({"brokers": {"a": {"txretries": 2, "txerrs": 1}, "b": {"txretries": 3}}}))
        self.assertEqual((out.retries, out.tx_errors), (5, 1))

    def test_percentiles_nearest_rank(self):
        self.assertEqual(percentiles(Counter(range(1, 101))), {"p50": 50, "p95": 95, "p99": 99})
        self.assertEqual(percentiles(Counter()), {"p50": None, "p95": None, "p99": None})
        # 중복 값은 건수로 합쳐도 리스트 정렬 방식과 같은 결과다.
        values = [-3, 5, 5, 5, 7, 7, 9, 100, 100, 250]
        ordered = sorted(values)
        expected = {f"p{q}": ordered[-(-len(values) * q // 100) - 1] for q in (50, 95, 99)}
        self.assertEqual(percentiles(Counter(values)), expected)


class RunWithKafkaTests(unittest.TestCase):
    def config(self, **overrides):
        env = ENV_VALUES | {"PRODUCER_OUTPUT": "kafka", "PRODUCER_EPS": "50000", "PRODUCER_MAX_EVENTS": "500",
                            "PRODUCER_SEED": "3"} | overrides
        return load_config(env)

    def test_run_sends_payloads_without_lf_and_reports_kafka_metrics(self):
        out, producer, _ = make()
        err = io.StringIO()
        stats, status = run(self.config(), None, err, out)
        self.assertEqual((status, stats.events), ("completed", 500))
        self.assertEqual(len(producer.produced), 500)
        self.assertTrue(all(b"\n" not in value for _, value, _ in producer.produced))
        final = json.loads(err.getvalue().splitlines()[-1])
        self.assertEqual((final["kafka"]["queued"], final["kafka"]["acked"]), (500, 500))
        self.assertEqual(final["kafka"]["acks"], "all")

    def test_sigint_finishes_enqueue_ack_and_flush_without_corrupting_counts(self):
        for stage in ("enqueue", "ack", "flush", "flush_remaining", "delivery_failure"):
            with self.subTest(stage=stage):
                out, producer, _ = make()
                err = io.StringIO()
                previous_handler = signal.getsignal(signal.SIGINT)

                def interrupt_then_call(original):
                    """실제 신호를 보내고 중단 없이 원래 처리가 끝나는지 확인한다."""
                    def call(*args, **kwargs):
                        signal.raise_signal(signal.SIGINT)
                        signal.raise_signal(signal.SIGINT)
                        return original(*args, **kwargs)
                    return call

                if stage == "enqueue":
                    original = producer.produce

                    def produce_then_interrupt(*args, **kwargs):
                        """큐 등록 성공과 queued 증가 사이에 SIGINT를 발생시킨다."""
                        original(*args, **kwargs)
                        signal.raise_signal(signal.SIGINT)

                    producer.produce = produce_then_interrupt
                elif stage == "flush_remaining":
                    producer.deliver_on_flush = False
                    producer.poll = lambda timeout=0: None
                    producer.flush = interrupt_then_call(producer.flush)
                elif stage == "delivery_failure":
                    producer.result = KafkaError(KafkaError._MSG_TIMED_OUT, "timed out")
                    producer.flush = interrupt_then_call(producer.flush)
                    producer.poll = lambda timeout=0: None
                elif stage == "flush":
                    producer.flush = interrupt_then_call(producer.flush)

                latency = FakeMsg.latency

                def latency_then_interrupt(msg):
                    """ACK 집계 전에 SIGINT를 발생시킨다."""
                    signal.raise_signal(signal.SIGINT)
                    return latency(msg)

                with patch("log_generator.main.load_config", return_value=self.config()), \
                     patch("log_generator.main.KafkaOutput", return_value=out), \
                     patch("log_generator.main.sys.stderr", err), \
                     patch.object(FakeMsg, "latency", latency_then_interrupt if stage == "ack" else latency):
                    code = main()

                self.assertEqual(signal.getsignal(signal.SIGINT), previous_handler)
                final = json.loads(err.getvalue().splitlines()[-1])
                if stage in ("flush_remaining", "delivery_failure"):
                    self.assertEqual((code, final["status"], final["output_errors"]), (1, "output_error", 1))
                    self.assertGreater(final["kafka"]["undelivered"] + final["kafka"]["failed"], 0)
                else:
                    self.assertEqual((code, final["status"]), (130, "interrupted"))
                    self.assertGreater(final["events_written"], 0)
                    self.assertEqual(final["events_written"], len(producer.produced))
                    self.assertEqual((out.generated, out.queued, out.acked), (final["events_written"],) * 3)
                    self.assertEqual((final["kafka"]["undelivered"], final["output_errors"]), (0, 0))
                    if stage == "enqueue":
                        self.assertEqual(final["events_written"], 1)

    def test_run_exits_with_output_error_on_delivery_failure(self):
        out, _, _ = make(result=KafkaError(KafkaError._MSG_TIMED_OUT, "timed out"))
        err = io.StringIO()
        stats, status = run(self.config(), None, err, out)
        self.assertEqual((status, stats.output_errors), ("output_error", 1))
        final = json.loads(err.getvalue().splitlines()[-1])
        self.assertGreaterEqual(final["kafka"]["failed"], 1)

    def test_run_reports_undelivered_when_flush_leaves_messages(self):
        out, producer, _ = make()
        producer.deliver_on_flush = False
        producer.poll = lambda timeout=0: None
        err = io.StringIO()
        stats, status = run(self.config(PRODUCER_MAX_EVENTS="100"), None, err, out)
        self.assertEqual(status, "output_error")
        self.assertEqual(json.loads(err.getvalue().splitlines()[-1])["kafka"]["undelivered"], 100)


if __name__ == "__main__":
    unittest.main()
