"""생성 로그의 계약(contracts/) 준수, 불량 주입, 비율, 직렬화·크기, 설정, 실행 통계를 검증한다.

로그 크기는 PRD상 조건이 아니므로 1KB 여부를 단언하지 않고, 바이트 실측 방식만 검증한다.
"""

import io
import json
import os
import random
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator, FormatChecker

from log_generator import pools
from log_generator.config import ClickHouseConfig, Config, ConfigError, load_config
from log_generator.generator import EventGenerator, serialize
from log_generator.main import run

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
VALIDATORS = {
    name: Draft202012Validator(
        json.loads((CONTRACTS / f"schemas/{name}.1.0.0.schema.json").read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    )
    for name in ("order_completed", "health_check")
}
PRODUCTS = {p.product_id: p for p in pools.PRODUCTS}
LOCATIONS = set(pools.LOCATIONS)


def contract_errors(payload: bytes) -> list[str]:
    """계약 검증 순서: 파싱 → event_name으로 스키마 선택 → 스키마 → 스키마 밖 의미 규칙."""
    try:
        obj = json.loads(payload)
    except json.JSONDecodeError:
        return ["parse_error"]
    validator = VALIDATORS.get(obj.get("event_name"))
    if validator is None:
        return ["unknown_event_name"]
    errors = [e.message for e in validator.iter_errors(obj)]
    if errors or obj["event_name"] != "order_completed":
        return errors
    props = obj["properties"]
    subtotal = sum(i["price"] * i["quantity"] for i in props["items"])
    if props["total_amount"] != subtotal - props["discount_amount"]:
        errors.append("amount_mismatch")
    for item in props["items"]:
        product = PRODUCTS.get(item["product_id"])
        if product is None or tuple(product) != (
            item["product_id"], item["product_name"], item["category"], item["price"]
        ):
            errors.append("product_mismatch")
    net = obj["context"]["network"]
    if (net["country"], net["region"], net["city"]) not in {tuple(loc) for loc in LOCATIONS}:
        errors.append("location_mismatch")
    return errors


def make_generator(seed: int = 7) -> EventGenerator:
    return EventGenerator(random.Random(seed), pools.build_user_pool(500))


class ContractTests(unittest.TestCase):
    def test_standard_samples_match_contract(self):
        expected = {
            "valid_order_completed.json": [],
            "valid_health_check.json": [],
            "invalid_malformed_json.json": ["parse_error"],
            "invalid_missing_order_id.json": ["'order_id' is a required property"],
            "invalid_amount_type.json": ["'invalid_amount' is not of type 'integer'"],
        }
        for name, errors in expected.items():
            raw = (CONTRACTS / "samples" / name).read_bytes()
            self.assertTrue(raw.endswith(b"\n") and raw.count(b"\n") == 1, name)
            self.assertEqual(contract_errors(raw[:-1]), errors, name)

    def test_generated_valid_events_pass_contract(self):
        gen = make_generator()
        for _ in range(2000):
            for obj in (gen.order(), gen.health_check()):
                payload = serialize(obj)
                self.assertEqual(contract_errors(payload), [], payload)
                self.assertNotIn(b"\n", payload)
        # health_check는 주문 필드 없이 공통 필드 4개만 가진다.
        self.assertEqual(set(gen.health_check()), {"event_id", "event_timestamp", "event_name", "schema_version"})

    def test_forced_anomalies_fail_only_as_intended(self):
        gen = make_generator()
        expected = {
            "malformed_json": ["parse_error"],
            "missing_order_id": ["'order_id' is a required property"],
            "invalid_amount_type": ["'invalid_amount' is not of type 'integer'"],
        }
        for anomaly, errors in expected.items():
            for _ in range(200):
                event = gen.invalid_order(anomaly)
                self.assertEqual((event.kind, event.anomaly), ("invalid_order", anomaly))
                self.assertEqual(contract_errors(event.payload), errors)
                event.payload.decode("utf-8")  # UTF-8 유효성
                self.assertNotIn(b"\n", event.payload)
        # 문법 오류는 마지막 '}' 한 바이트만 '!'로 바꾸므로, 복구하면 유효한 주문이 된다.
        broken = gen.invalid_order("malformed_json").payload
        self.assertEqual(broken[-1:], b"!")
        self.assertEqual(contract_errors(broken[:-1] + b"}"), [])
        with self.assertRaises(ValueError):
            gen.invalid_order("unknown")

    def test_no_personal_data_fields(self):
        payload = serialize(make_generator().order())
        # 개인정보 필드 키가 없고, 경로에 쿼리 문자열이 없어야 한다.
        for banned in (b'"name"', b"email", b"phone", b"address", b'"ip"', b"?"):
            self.assertNotIn(banned, payload)


class RatioTests(unittest.TestCase):
    def test_seeded_300k_ratio(self):
        gen = make_generator(seed=42)
        kinds, anomalies = Counter(), Counter()
        for _ in range(300_000):
            event = gen.next_event()
            kinds[event.kind] += 1
            if event.anomaly:
                anomalies[event.anomaly] += 1
        for kind, weight in zip(pools.EVENT_KINDS, pools.EVENT_WEIGHTS):
            self.assertAlmostEqual(kinds[kind] / 300_000, weight / 100, delta=0.005, msg=kind)
        invalid = kinds["invalid_order"]
        for anomaly in pools.ANOMALY_KINDS:
            self.assertAlmostEqual(anomalies[anomaly] / invalid, 1 / 3, delta=0.02, msg=anomaly)

    def test_seed_reproduces_choices(self):
        def strip_random_ids(payload: bytes) -> dict:
            obj = json.loads(payload)
            for key in ("event_id", "event_timestamp"):
                obj.pop(key)
            obj["context"].pop("session_id")
            obj["properties"].pop("order_id")
            return obj

        a, b = make_generator(seed=1), make_generator(seed=1)
        for _ in range(100):
            self.assertEqual(strip_random_ids(serialize(a.order())), strip_random_ids(serialize(b.order())))


class SerializationTests(unittest.TestCase):
    def test_compact_utf8_byte_size(self):
        obj = {"product_name": "무선 마우스", "price": 30000}
        payload = serialize(obj)
        self.assertEqual(payload, '{"product_name":"무선 마우스","price":30000}'.encode())
        self.assertNotIn(b"\\u", payload)
        # 크기는 글자 수가 아닌 UTF-8 바이트 수다.
        self.assertGreater(len(payload), len(payload.decode("utf-8")))


# 테스트 입력은 실제 .env와 분리해 모든 설정을 명시한다.
ENV_VALUES = {
    "PRODUCER_EPS": "5000",
    "PRODUCER_OUTPUT": "stdout",
    "PRODUCER_OUTPUT_PATH": "",
    "PRODUCER_MAX_EVENTS": "300000",
    "PRODUCER_USER_POOL_SIZE": "500",
    "PRODUCER_SEED": "",
    "PRODUCER_STATUS_INTERVAL_SEC": "1",
    "PRODUCER_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "PRODUCER_KAFKA_TOPIC": "raw.ecommerce.orders.v1",
    "PRODUCER_KAFKA_LINGER_MS": "5",
    "PRODUCER_KAFKA_COMPRESSION": "lz4",
    "PRODUCER_KAFKA_DELIVERY_TIMEOUT_MS": "30000",
    "PRODUCER_KAFKA_QUEUE_FULL_TIMEOUT_SEC": "30",
    "PRODUCER_KAFKA_FLUSH_TIMEOUT_SEC": "30",
    "PRODUCER_CLICKHOUSE_HTTP_URL": "",
    "CLICKHOUSE_GENERATOR_PASSWORD": "",
}


class ConfigTests(unittest.TestCase):
    def test_explicit_env_values(self):
        config = load_config(ENV_VALUES)
        self.assertEqual(config, Config(5000, "stdout", None, 300000, 500, None, 1, None, None))
        config = load_config(ENV_VALUES | {"PRODUCER_EPS": "100", "PRODUCER_SEED": "-3",
                                          "PRODUCER_MAX_EVENTS": "0"})
        self.assertEqual((config.eps, config.seed, config.max_events), (100, -3, None))

    def test_requires_every_key_and_rejects_empty_required_values(self):
        for key in ENV_VALUES:
            env = ENV_VALUES.copy()
            del env[key]
            with self.subTest(missing=key), self.assertRaisesRegex(ConfigError, key):
                load_config(env)
            if key not in ("PRODUCER_SEED", "PRODUCER_OUTPUT_PATH", "PRODUCER_CLICKHOUSE_HTTP_URL",
                           "CLICKHOUSE_GENERATOR_PASSWORD"):
                with self.subTest(empty=key), self.assertRaisesRegex(ConfigError, key):
                    load_config(ENV_VALUES | {key: " "})
        with self.assertRaises(ConfigError):
            load_config({})
        # Config 자체에도 암묵적인 설정 기본값이 없다.
        with self.assertRaises(TypeError):
            Config()

    def test_clickhouse_metrics_only_for_kafka_with_url_and_password(self):
        kafka = ENV_VALUES | {"PRODUCER_OUTPUT": "kafka"}
        self.assertIsNone(load_config(kafka).clickhouse)  # 빈 URL은 기록 끔
        url = {"PRODUCER_CLICKHOUSE_HTTP_URL": "http://localhost:8123/"}
        with self.assertRaisesRegex(ConfigError, "CLICKHOUSE_GENERATOR_PASSWORD"):
            load_config(kafka | url)
        config = load_config(kafka | url | {"CLICKHOUSE_GENERATOR_PASSWORD": "pw"})
        self.assertEqual(config.clickhouse, ClickHouseConfig("http://localhost:8123", "pw"))
        self.assertIsNone(load_config(ENV_VALUES | url).clickhouse)  # stdout 출력에는 Producer 지표가 없다.

    def test_loads_dotenv_independent_of_cwd_and_env_takes_precedence(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_file = Path(tmp) / ".env"
            env_file.write_text("\n".join(f"{k}={v}" for k, v in ENV_VALUES.items()), encoding="utf-8")
            with patch("log_generator.config.ENV_FILE", env_file), patch.dict(os.environ, {}, clear=True):
                original_cwd = Path.cwd()
                try:
                    os.chdir(tmp)
                    self.assertEqual(load_config().eps, 5000)
                    # 파일을 읽어도 프로세스 환경은 바뀌지 않는다.
                    self.assertNotIn("PRODUCER_EPS", os.environ)
                    env_file.write_text(env_file.read_text().replace("PRODUCER_EPS=5000", "PRODUCER_EPS=7000"))
                    self.assertEqual(load_config().eps, 7000)
                    with patch.dict(os.environ, {"PRODUCER_EPS": "123"}):
                        self.assertEqual(load_config().eps, 123)
                    with patch.dict(os.environ, {"PRODUCER_EPS": ""}):
                        with self.assertRaisesRegex(ConfigError, "PRODUCER_EPS"):
                            load_config()
                finally:
                    os.chdir(original_cwd)

    def test_missing_dotenv_uses_environment_only(self):
        # .env가 없거나(로컬) 루트를 계산할 수 없으면(얕은 컨테이너 경로) 환경변수만 쓴다.
        # 같은 위치의 .env.example은 템플릿이므로 읽지 않는다.
        with tempfile.TemporaryDirectory() as tmp:
            example = Path(tmp) / ".env.example"
            example.write_text("\n".join(f"{k}={v}" for k, v in ENV_VALUES.items()), encoding="utf-8")
            for env_file in (Path(tmp) / ".env", None):
                with self.subTest(env_file=env_file), patch("log_generator.config.ENV_FILE", env_file):
                    with patch.dict(os.environ, ENV_VALUES, clear=True):
                        self.assertEqual(load_config().eps, 5000)
                    with patch.dict(os.environ, {}, clear=True):
                        with self.assertRaises(ConfigError):
                            load_config()

    def test_explicit_mapping_does_not_read_dotenv(self):
        with patch("log_generator.config.dotenv_values", side_effect=AssertionError("실제 .env는 읽지 않음")):
            self.assertEqual(load_config(ENV_VALUES).eps, 5000)

    def test_rejects_invalid_values(self):
        for overrides in (
            {"PRODUCER_EPS": "0"},
            {"PRODUCER_EPS": "1.5"},
            {"PRODUCER_MAX_EVENTS": "-1"},
            {"PRODUCER_USER_POOL_SIZE": "abc"},
            {"PRODUCER_SEED": "invalid"},
            {"PRODUCER_STATUS_INTERVAL_SEC": "0"},
            {"PRODUCER_OUTPUT": "nats"},
            {"PRODUCER_OUTPUT": "kafka", "PRODUCER_KAFKA_COMPRESSION": "lz5"},
            {"PRODUCER_KAFKA_LINGER_MS": "-1"},
            {"PRODUCER_KAFKA_DELIVERY_TIMEOUT_MS": "0"},
            {"PRODUCER_KAFKA_QUEUE_FULL_TIMEOUT_SEC": "0"},
            {"PRODUCER_KAFKA_FLUSH_TIMEOUT_SEC": "x"},
            {"PRODUCER_OUTPUT": "file"},
            {"PRODUCER_OUTPUT": "file", "PRODUCER_OUTPUT_PATH": "/nonexistent-dir/x.jsonl"},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ConfigError):
                load_config(ENV_VALUES | overrides)

    def test_kafka_config_only_when_output_is_kafka(self):
        self.assertIsNone(load_config(ENV_VALUES).kafka)
        kafka = load_config(ENV_VALUES | {"PRODUCER_OUTPUT": "kafka"}).kafka
        self.assertEqual(
            (kafka.bootstrap_servers, kafka.topic, kafka.linger_ms, kafka.compression,
             kafka.delivery_timeout_ms, kafka.queue_full_timeout_sec, kafka.flush_timeout_sec),
            ("localhost:9092", "raw.ecommerce.orders.v1", 5, "lz4", 30000, 30, 30),
        )

    def test_rejects_existing_output_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.jsonl"
            env = ENV_VALUES | {"PRODUCER_OUTPUT": "file", "PRODUCER_OUTPUT_PATH": str(path)}
            self.assertEqual(load_config(env).output_path, path)
            path.write_bytes(b"")
            with self.assertRaises(ConfigError):
                load_config(env)


class RunTests(unittest.TestCase):
    def test_stats_match_output(self):
        out, err = io.BytesIO(), io.StringIO()
        stats, status = run(load_config(ENV_VALUES | {"PRODUCER_EPS": "50000", "PRODUCER_MAX_EVENTS": "3000", "PRODUCER_SEED": "3"}), out, err)
        lines = out.getvalue().split(b"\n")
        self.assertEqual(status, "completed")
        self.assertEqual(lines[-1], b"")  # 각 페이로드 뒤에 LF 1바이트
        self.assertEqual(stats.events, len(lines) - 1)
        self.assertEqual(stats.payload_bytes, len(out.getvalue()) - stats.events)
        self.assertEqual(stats.max_payload, max(len(line) for line in lines))
        final = json.loads(err.getvalue().splitlines()[-1])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(sum(k["count"] for k in final["kinds"].values()), 3000)
        self.assertEqual(sum(final["anomalies"].values()), final["kinds"]["invalid_order"]["count"])

    def test_rate_control_keeps_target_eps(self):
        # 1,000 EPS로 500건이면 약 0.5초. 속도 제어가 깨져 몰아서 출력하거나 멈추면 범위를 벗어난다.
        out, err = io.BytesIO(), io.StringIO()
        stats, status = run(load_config(ENV_VALUES | {"PRODUCER_EPS": "1000", "PRODUCER_MAX_EVENTS": "500"}), out, err)
        elapsed = json.loads(err.getvalue().splitlines()[-1])["elapsed_sec"]
        self.assertEqual((status, stats.events), ("completed", 500))
        self.assertGreaterEqual(elapsed, 0.45)
        self.assertLess(elapsed, 0.6)

    def test_output_error_not_counted(self):
        class FailingOut(io.BytesIO):
            def write(self, data):
                if self.tell() > 5000:
                    raise OSError("disk full")
                return super().write(data)

        out, err = FailingOut(), io.StringIO()
        stats, status = run(load_config(ENV_VALUES | {"PRODUCER_EPS": "50000", "PRODUCER_MAX_EVENTS": "1000", "PRODUCER_SEED": "3"}), out, err)
        self.assertEqual(status, "output_error")
        self.assertEqual(stats.output_errors, 1)
        self.assertEqual(stats.events, out.getvalue().count(b"\n"))


if __name__ == "__main__":
    unittest.main()
