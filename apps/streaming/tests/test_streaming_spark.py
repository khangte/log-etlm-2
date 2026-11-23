"""streaming 기능별 모듈의 Spark 함수 테스트. 정적 DataFrame 과 local 세션을 사용한다(Kafka 불필요)."""

import base64
import dataclasses
import datetime as dt
import json
import types

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BinaryType, BooleanType, IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

from conftest import dump, sample_bytes
from streaming import batch, schemas, transforms
from streaming import monitoring
from streaming.schemas import ORDER_COLUMNS
from streaming.config import load_config

TOPIC = "raw.ecommerce.orders.v1"
INPUT_SCHEMA = StructType([
    StructField("value", BinaryType()), StructField("topic", StringType()), StructField("partition", IntegerType()),
    StructField("offset", LongType()), StructField("timestamp", TimestampType()),
    StructField("timestampType", IntegerType()),
])
TS = dt.datetime(2026, 11, 16, 6, 8, 1, 456000)  # 세션 시간대 UTC 기준 naive
# 이벤트 시각 범위 규칙의 기준 시각. 표준 샘플(2026-11-16T06:08:00.123Z)이 범위 안에 들도록 고정한다.
NOW = dt.datetime(2026, 11, 16, 6, 8, 2, tzinfo=dt.timezone.utc)


def classify_batch(source, callback_started_at=NOW):
    """기준 시각을 NOW로 고정한 transforms.classify_batch."""
    return transforms.classify_batch(source, callback_started_at)


def at(delta: dt.timedelta) -> str:
    """NOW+delta를 계약 형식(밀리초, Z) 문자열로 만든다."""
    return (NOW + delta).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@pytest.fixture(scope="module")
def spark():
    """UTC 시간대·셔플 3 의 local 세션. 운영과 같은 설정으로 만든다."""
    s = (SparkSession.builder.master("local[2]").appName("streaming-tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "3")
         .config("spark.ui.enabled", "false").config("spark.driver.memory", "1g").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def frame(spark, values, ts_type=1, partition=0, start=0):
    """값 목록을 Kafka 소스와 같은 컬럼의 DataFrame 으로 만든다."""
    rows = [(None if v is None else bytes(v), TOPIC, partition, start + i, TS, ts_type)
            for i, v in enumerate(values)]
    return spark.createDataFrame(rows, INPUT_SCHEMA)


def mixed_values():
    """정상 2(서로 다른 event_id)·제외 1·불량 5(null·빈·깨진 UTF-8·malformed·필수 누락)."""
    o = json.loads(sample_bytes("valid_order_completed.json"))
    o2 = json.loads(sample_bytes("valid_order_completed.json"))
    o2["event_id"] = "660e8400-e29b-41d4-a716-446655440000"
    return [dump(o), dump(o2), sample_bytes("valid_health_check.json"), None, b"", b"\xff\xfe",
            sample_bytes("invalid_malformed_json.json"), sample_bytes("invalid_missing_order_id.json")]


class TestClassifyAndSelect:
    """classify_batch·select_orders·deduplicate_events."""

    def test_classify_batch_컬럼(self, spark):
        """원문·추적 컬럼과 result 구조체."""
        df = classify_batch(frame(spark, [sample_bytes("valid_order_completed.json")]))
        assert df.columns == ["raw_value", "kafka_topic", "kafka_partition", "kafka_offset",
                              "kafka_append_timestamp", "result"]
        types = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(types["raw_value"], BinaryType)
        assert isinstance(types["kafka_partition"], IntegerType)
        assert isinstance(types["kafka_offset"], LongType)
        assert isinstance(types["kafka_append_timestamp"], TimestampType)

    def test_select_orders_35컬럼_타입(self, spark):
        """31개 이벤트 컬럼 + 4개 Kafka 컬럼, PRD 매핑 표의 타입."""
        orders = transforms.select_orders(classify_batch(frame(spark, [sample_bytes("valid_order_completed.json")],
                                                              partition=2, start=77)))
        assert orders.columns == list(ORDER_COLUMNS) + list(transforms.KAFKA_COLUMNS) and len(orders.columns) == 35
        types = {f.name: f.dataType for f in orders.schema.fields}
        for c in ("total_amount", "discount_amount", "price", "quantity", "kafka_offset"):
            assert isinstance(types[c], LongType), c
        assert isinstance(types["event_timestamp"], TimestampType)
        assert isinstance(types["kafka_append_timestamp"], TimestampType)
        assert isinstance(types["is_logged_in"], BooleanType)
        assert isinstance(types["kafka_partition"], IntegerType)
        for c in set(orders.columns) - {"total_amount", "discount_amount", "price", "quantity", "kafka_offset",
                                        "event_timestamp", "kafka_append_timestamp", "is_logged_in",
                                        "kafka_partition"}:
            assert isinstance(types[c], StringType), c

    def test_select_orders_값_보존(self, spark):
        """표준 주문 값과 밀리초·Kafka 위치 보존."""
        rows = transforms.select_orders(classify_batch(frame(spark, [sample_bytes("valid_order_completed.json")],
                                                           partition=2, start=77))).collect()
        assert len(rows) == 1
        r = rows[0].asDict()
        assert (r["total_amount"], r["discount_amount"], r["price"], r["quantity"]) == (27000, 3000, 15000, 2)
        assert r["event_timestamp"].replace(tzinfo=None) == dt.datetime(2026, 11, 16, 6, 8, 0, 123000)
        assert r["os_version"] == "18.1.1" and r["is_logged_in"] is True
        assert (r["kafka_topic"], r["kafka_partition"], r["kafka_offset"]) == (TOPIC, 2, 77)
        assert r["kafka_append_timestamp"].replace(tzinfo=None) == TS

    def test_event_timestamp_UTC_문자열_표현(self, spark):
        """세션 시간대 UTC 에서 문자열화한 값이 원본 시각과 같다."""
        df = transforms.select_orders(classify_batch(frame(spark, [sample_bytes("valid_order_completed.json")])))
        s = df.select(F.date_format("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("t")).first()["t"]
        assert s == "2026-11-16T06:08:00.123Z"

    def test_배치_내_event_id_중복_제거(self, spark):
        """같은 event_id 3건 → 1행. 다른 id 는 유지."""
        v = sample_bytes("valid_order_completed.json")
        o2 = json.loads(v)
        o2["event_id"] = "660e8400-e29b-41d4-a716-446655440000"
        selected = transforms.select_orders(classify_batch(frame(spark, [v, v, dump(o2), v])))
        assert selected.count() == 4  # 중복 제거는 select_orders 가 아니라 deduplicate_events 책임
        orders = transforms.deduplicate_events(selected)
        assert orders.count() == 2
        assert orders.select("event_id").distinct().count() == 2

    def test_정상_없는_배치(self, spark):
        """불량·제외만 있어도 select_orders 는 0행."""
        orders = transforms.select_orders(classify_batch(frame(spark, [None, sample_bytes("valid_health_check.json")])))
        assert orders.count() == 0 and len(orders.columns) == 35

    def test_건수_등식(self, spark):
        """입력 = 정상 + 제외 + 불량."""
        vals = mixed_values()
        c = classify_batch(frame(spark, vals)).persist()
        counts = {r["o"]: r["count"] for r in c.groupBy(F.col("result.outcome").alias("o")).count().collect()}
        assert counts == {"order": 2, "excluded": 1, "dlq": 5}
        assert sum(counts.values()) == len(vals)
        c.unpersist()

    def test_빈_입력(self, spark):
        """0건 DataFrame 도 예외 없이 처리."""
        c = classify_batch(frame(spark, []))
        assert c.count() == 0

    def test_timestampType이_LogAppendTime이_아니면_실패(self, spark):
        """CreateTime(0) 이면 raise_error 로 작업이 실패한다."""
        c = classify_batch(frame(spark, [sample_bytes("valid_order_completed.json")], ts_type=0))
        with pytest.raises(Exception, match="not LogAppendTime"):
            c.persist().collect()

    @pytest.mark.parametrize("ts_type", [0, -1, 2])
    def test_select_orders_경로도_timestampType_검사(self, spark, ts_type):
        """정상 출력 경로(select_orders.collect)에서도 실패."""
        c = classify_batch(frame(spark, [sample_bytes("valid_order_completed.json")], ts_type=ts_type))
        with pytest.raises(Exception, match="not LogAppendTime"):
            transforms.select_orders(c).collect()

    def test_timestampType_불량_행이_불량_로그에도_실패(self, spark):
        """불량 입력에서도 timestampType 가 틀리면 DLQ 경로에서 실패."""
        c = classify_batch(frame(spark, [b"{"], ts_type=0))
        with pytest.raises(Exception, match="not LogAppendTime"):
            transforms.build_dlq_records(c).collect()

    def test_timestampType이_섞이면_배치_전체가_실패(self, spark):
        """한 건이라도 LogAppendTime 이 아니면 배치 실패."""
        rows = [(sample_bytes("valid_order_completed.json"), TOPIC, 0, 0, TS, 1),
                (sample_bytes("valid_health_check.json"), TOPIC, 0, 1, TS, 0)]
        c = classify_batch(spark.createDataFrame(rows, INPUT_SCHEMA))
        with pytest.raises(Exception, match="not LogAppendTime"):
            c.persist().count()

    def test_단독_서로게이트_주문은_UDF를_통과한다(self, spark):
        """JSON \\ud800 단독 서로게이트가 있어도 배치가 죽지 않는다(값은 U+FFFD 로 바뀐다: 관찰 기록용)."""
        raw = sample_bytes("valid_order_completed.json").replace(b'"os":"iOS"', b'"os":"\\ud800"')
        rows = transforms.select_orders(classify_batch(frame(spark, [raw]))).select("os").collect()
        assert len(rows) == 1


class TestDlqRecords:
    """build_dlq_records."""

    def collect(self, spark, values, **kw):
        """DLQ 레코드를 key → 파싱된 value 로 수집한다."""
        df = transforms.build_dlq_records(classify_batch(frame(spark, values, **kw)))
        assert df.columns == ["key", "value"]
        assert isinstance(df.schema["key"].dataType, StringType) and isinstance(df.schema["value"].dataType, StringType)
        return {r["key"]: json.loads(r["value"]) for r in df.collect()}, df

    def test_키와_필드_목록(self, spark):
        """키 topic:partition:offset, 값 필드 14개 모두 존재(null 포함)."""
        vals = mixed_values()
        recs, _ = self.collect(spark, vals, partition=1, start=10)
        assert set(recs) == {f"{TOPIC}:1:{10 + i}" for i in (3, 4, 5, 6, 7)}
        expected = {"dlq_schema_version", "source_topic", "source_partition", "source_offset", "source_timestamp",
                    "raw_value_base64", "error_stage", "error_code", "error_path", "error_message", "event_name",
                    "schema_version", "detected_at", "processor_version"}
        for k, v in recs.items():
            assert set(v) == expected, (k, set(v) ^ expected)

    def test_필드_값(self, spark):
        """표준 불량 3종의 기대 단계·코드·경로와 위치·시각 형식."""
        vals = [sample_bytes("invalid_malformed_json.json"), sample_bytes("invalid_missing_order_id.json"),
                sample_bytes("invalid_amount_type.json")]
        recs, _ = self.collect(spark, vals, partition=2, start=5)
        a, b, c = (recs[f"{TOPIC}:2:{i}"] for i in (5, 6, 7))
        assert (a["error_stage"], a["error_code"], a["error_path"]) == ("parse", "INVALID_JSON", None)
        assert (b["error_stage"], b["error_code"], b["error_path"]) == ("schema", "SCHEMA_REQUIRED",
                                                                          "properties.order_id")
        assert (c["error_stage"], c["error_code"], c["error_path"]) == ("schema", "SCHEMA_TYPE",
                                                                          "properties.total_amount")
        assert (a["event_name"], a["schema_version"]) == (None, None)
        assert (b["event_name"], b["schema_version"]) == ("order_completed", "1.0.0")
        assert a["dlq_schema_version"] == "1.0.0" and a["processor_version"] == transforms.PROCESSOR_VERSION
        assert (a["source_topic"], a["source_partition"], a["source_offset"]) == (TOPIC, 2, 5)
        assert isinstance(a["source_partition"], int) and isinstance(a["source_offset"], int)
        assert a["source_timestamp"] == "2026-11-16T06:08:01.456Z"
        import re
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", a["detected_at"]), a["detected_at"]
        assert a["error_message"]

    def test_원문_복원(self, spark):
        """Base64 디코딩이 원문 bytes 와 정확히 같다(깨진 UTF-8 포함)."""
        vals = [sample_bytes("invalid_malformed_json.json"), b"\xff\xfe\x00\x80", b"{", b"\x00"]
        recs, _ = self.collect(spark, vals)
        for i, v in enumerate(vals):
            assert base64.b64decode(recs[f"{TOPIC}:0:{i}"]["raw_value_base64"]) == v

    def test_Base64는_표준_형식_줄바꿈없음(self, spark):
        """PRD: raw_value_base64 는 표준 Base64. 76자마다 CRLF 를 넣는 MIME 청킹이면 안 된다."""
        recs, _ = self.collect(spark, [sample_bytes("invalid_malformed_json.json")])
        text = recs[f"{TOPIC}:0:0"]["raw_value_base64"]
        assert "\r" not in text and "\n" not in text, repr(text[:100])
        assert base64.b64decode(text, validate=True) == sample_bytes("invalid_malformed_json.json")

    def test_null_과_빈값_구분(self, spark):
        """null value → raw_value_base64 null, 빈 bytes → \"\"."""
        recs, _ = self.collect(spark, [None, b""])
        assert recs[f"{TOPIC}:0:0"]["raw_value_base64"] is None
        assert recs[f"{TOPIC}:0:0"]["error_code"] == "NULL_VALUE"
        assert recs[f"{TOPIC}:0:1"]["raw_value_base64"] == ""
        assert recs[f"{TOPIC}:0:1"]["error_code"] == "EMPTY_VALUE"

    def test_정상과_제외는_DLQ에_없다(self, spark):
        """정상 주문·health_check 는 DLQ 대상이 아니다."""
        recs, _ = self.collect(spark, [sample_bytes("valid_order_completed.json"),
                                       sample_bytes("valid_health_check.json")])
        assert recs == {}

    def test_value는_JSON_문자열(self, spark):
        """value 는 UTF-8 JSON 객체 문자열이다."""
        _, df = self.collect(spark, [b"{"])
        text = df.first()["value"]
        assert isinstance(json.loads(text), dict)

    def test_메시지_특수문자와_한글_사유(self, spark):
        """오류 사유에 비ASCII 가 있어도 JSON 이 유효하다."""
        raw = sample_bytes("valid_order_completed.json").replace(b'"os":"iOS"', '"os":"한글","x":"\\u00e9"'.encode())
        recs, _ = self.collect(spark, [raw])
        v = next(iter(recs.values()))
        assert v["error_code"] == "SCHEMA_ADDITIONALPROPERTIES"

    def test_같은_배치_키_고유(self, spark):
        """위치가 다르면 키가 모두 다르다."""
        recs, df = self.collect(spark, [b"{"] * 20)
        assert len(recs) == 20 and df.count() == 20


class TestProcessBatch:
    """process_batch: 쓰기 함수는 대체하고 건수·실패 전파를 확인한다."""

    @pytest.fixture(autouse=True)
    def fixed_clock(self, monkeypatch):
        """콜백 진입 시각(datetime.now)을 NOW로 고정해 고정 시각 샘플이 이벤트 시각 범위 안에 들게 한다."""
        monkeypatch.setattr(batch, "datetime", types.SimpleNamespace(now=lambda tz=None: NOW))

    @pytest.fixture(autouse=True)
    def ops_rows(self, monkeypatch):
        """운영 지표 INSERT를 대체하고 기록된 행을 모은다."""
        rows = []
        monkeypatch.setattr(batch, "insert_rows", lambda cfg, table, new: rows.append((table, new)))
        return rows

    @pytest.fixture
    def orders_out(self, monkeypatch):
        """정상 주문 JDBC 쓰기를 대체하고 쓰려던 행·컬럼을 모은다."""
        out = {}
        def capture(orders, cfg):
            """DataFrame을 소비해 행과 컬럼 이름을 남긴다."""
            out["columns"], out["rows"] = orders.columns, orders.collect()
        monkeypatch.setattr(batch, "write_orders", capture)
        return out

    @pytest.fixture
    def config(self, tmp_path):
        """ClickHouse 접속 없이 콜백만 실행하는 설정."""
        return load_config({
            "SPARK_KAFKA_BOOTSTRAP_SERVERS": "kafka:19092", "KAFKA_TOPIC": TOPIC,
            "KAFKA_DLQ_TOPIC": "dlq.ecommerce.orders.v1", "SPARK_STARTING_OFFSETS": "earliest",
            "SPARK_MAX_OFFSETS_PER_TRIGGER": "100", "SPARK_TRIGGER_INTERVAL": "1 seconds",
            "SPARK_SHUFFLE_PARTITIONS": "3", "SPARK_CHECKPOINT_DIR": str(tmp_path / "ckpt"),
            "SPARK_LATENCY_METRICS_ENABLED": "true", "SPARK_STALL_TIMEOUT_SECONDS": "90",
            "SPARK_CLICKHOUSE_JDBC_URL": "jdbc:clickhouse:http://clickhouse:8123",
            "SPARK_CLICKHOUSE_HTTP_URL": "http://clickhouse:8123", "CLICKHOUSE_SPARK_PASSWORD": "x",
            "SPARK_JDBC_BATCH_SIZE": "50000", "SPARK_JDBC_NUM_PARTITIONS": "3"})

    def test_건수_로그와_출력(self, spark, config, monkeypatch, caplog, orders_out, ops_rows):
        """배치 로그 JSON 의 input = order + excluded + dlq, 출력 행수·DLQ 건수 일치, 중복 제거, 추적 컬럼."""
        captured = {}
        monkeypatch.setattr(batch, "write_dlq", lambda records, cfg: captured.setdefault("dlq", records.collect()))
        vals = mixed_values() + [sample_bytes("valid_order_completed.json")]  # 중복 event_id 1건 추가
        context = {"query_id": "q-1", "run_id": "r-1"}
        with caplog.at_level("INFO", logger="streaming"):
            batch.process_batch(frame(spark, vals), 7, config, context)
        msg = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("batch "))
        log = json.loads(msg[len("batch "):])
        assert log == {"batch_id": 7, "input": 9, "order": 3, "excluded": 1, "dlq": 5}
        assert log["input"] == log["order"] + log["excluded"] + log["dlq"] == len(vals)
        assert len(orders_out["rows"]) == 2  # 배치 내 event_id 중복 제거: 3 → 2
        assert len(orders_out["columns"]) == 39  # 이벤트 31 + Kafka 4 + Spark 추적 4
        assert orders_out["columns"][-4:] == ["spark_query_id", "spark_run_id", "spark_batch_id", "spark_processed_at"]
        metrics = json.loads(next(r.getMessage()[8:] for r in caplog.records if r.getMessage().startswith("latency ")))
        row = orders_out["rows"][0]
        assert (row["spark_query_id"], row["spark_run_id"], row["spark_batch_id"]) == ("q-1", "r-1", 7)
        assert row["spark_processed_at"].isoformat(timespec="milliseconds") == metrics["callback_started_at"][:23]
        assert len(captured["dlq"]) == 5
        [(table, [attempt])] = ops_rows
        assert table == "ops.spark_batch_attempts"
        assert (attempt["status"], attempt["input_rows"], attempt["order_rows"], attempt["dlq_rows"]) == (
            "succeeded", 9, 3, 5)
        assert (attempt["query_id"], attempt["batch_id"], attempt["failed_stage"]) == ("q-1", 7, "")

    def test_지연_계측_끄면_건수와_출력은_같고_지연은_null(self, spark, config, monkeypatch, caplog, orders_out, ops_rows):
        """끔 설정은 지연 집계만 생략한다. 건수 로그·정상 출력·DLQ 건수는 켬과 같다."""
        captured = {}
        monkeypatch.setattr(batch, "write_dlq", lambda records, cfg: captured.setdefault("dlq", records.collect()))
        off = dataclasses.replace(config, latency_metrics_enabled=False)
        with caplog.at_level("INFO", logger="streaming"):
            batch.process_batch(frame(spark, mixed_values()), 7, off)
        messages = [r.getMessage() for r in caplog.records]
        log = json.loads(next(m for m in messages if m.startswith("batch "))[6:])
        metrics = json.loads(next(m for m in messages if m.startswith("latency "))[8:])
        assert log == {"batch_id": 7, "input": 8, "order": 2, "excluded": 1, "dlq": 5}
        assert metrics["status"] == "succeeded" and metrics["kafka_to_callback_start_ms"] is None
        assert metrics["classification_and_metrics_action_ms"] >= 0
        assert len(orders_out["rows"]) == 2
        assert len(captured["dlq"]) == 5
        [(_, [attempt])] = ops_rows
        assert attempt["kafka_to_callback_ms_count"] == 0 and attempt["kafka_to_callback_ms_p95"] == 0.0

    def test_DLQ_없으면_DLQ_쓰기_생략(self, spark, config, monkeypatch, orders_out):
        """불량이 없으면 write_dlq 를 호출하지 않는다."""
        calls = []
        monkeypatch.setattr(batch, "write_dlq", lambda *a: calls.append(a))
        batch.process_batch(frame(spark, [sample_bytes("valid_order_completed.json")]), 1, config)
        assert calls == []

    def test_정상_없으면_정상_쓰기_생략(self, spark, config, monkeypatch, orders_out):
        """정상이 없으면 write_orders 를 호출하지 않는다."""
        monkeypatch.setattr(batch, "write_dlq", lambda *a: None)
        batch.process_batch(frame(spark, [sample_bytes("valid_health_check.json"), b"{"]), 1, config)
        assert orders_out == {}

    def test_DLQ_쓰기_실패는_배치_실패로_전파(self, spark, config, monkeypatch, caplog, ops_rows):
        """write_dlq 예외가 숨겨지지 않고 올라가며 성공 건수 로그를 남기지 않는다."""
        def boom(*a):
            raise RuntimeError("dlq down")
        monkeypatch.setattr(batch, "write_dlq", boom)
        with caplog.at_level("INFO", logger="streaming"):
            with pytest.raises(RuntimeError, match="dlq down"):
                batch.process_batch(frame(spark, [b"{"]), 1, config)
        assert not any(r.getMessage().startswith("batch ") for r in caplog.records)
        metrics = json.loads(next(r.getMessage()[8:] for r in caplog.records
                                  if r.getMessage().startswith("latency ")))
        assert metrics["status"] == "failed" and metrics["failed_stage"] == "dlq_write_action"
        assert metrics["dlq_write_action_ms"] >= 0
        assert metrics["orders_write_action_ms"] is None
        assert metrics["callback_total_ms"] >= metrics["dlq_write_action_ms"]
        [(_, [attempt])] = ops_rows  # 실패한 시도도 운영 지표에 남는다.
        assert (attempt["status"], attempt["failed_stage"], attempt["orders_write_ms"]) == (
            "failed", "dlq_write_action", 0.0)

    def test_정상_쓰기_실패는_전파(self, spark, config, monkeypatch):
        """write_orders 예외 전파."""
        def boom(*a):
            raise RuntimeError("sink down")
        monkeypatch.setattr(batch, "write_orders", boom)
        with pytest.raises(RuntimeError, match="sink down"):
            batch.process_batch(frame(spark, [sample_bytes("valid_order_completed.json")]), 1, config)

    def test_timestampType_오류는_배치_실패(self, spark, config, monkeypatch):
        """CreateTime 입력이면 쓰기 전에 배치가 실패하고 출력이 없다."""
        monkeypatch.setattr(batch, "write_dlq", lambda *a: None)
        with pytest.raises(Exception, match="not LogAppendTime"):
            batch.process_batch(frame(spark, [sample_bytes("valid_order_completed.json")], ts_type=0), 1, config)

    def test_timestampType_오류_health_check만_있는_배치도_실패(self, spark, config, monkeypatch):
        """제외만 있는 배치에서도 메타데이터 오류는 드러나야 한다."""
        monkeypatch.setattr(batch, "write_dlq", lambda *a: None)
        with pytest.raises(Exception, match="not LogAppendTime"):
            batch.process_batch(frame(spark, [sample_bytes("valid_health_check.json")], ts_type=0), 1, config)

    def test_빈_배치(self, spark, config, caplog):
        """0건 배치는 건수 0 로그만 남긴다."""
        with caplog.at_level("INFO", logger="streaming"):
            batch.process_batch(frame(spark, []), 3, config)
        msg = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("batch "))
        assert json.loads(msg[6:]) == {"batch_id": 3, "input": 0, "order": 0, "excluded": 0, "dlq": 0}

    def test_100시간_넘게_퍼진_배치는_범위_밖만_DLQ(self, spark, config, monkeypatch, caplog, orders_out):
        """0~119시간 전 주문 120건: 범위 안 25건(0~24시간)만 적재, 95건은 DLQ, 등식 유지, 적재 파티션 26개 이하."""
        captured = {}
        monkeypatch.setattr(batch, "write_dlq", lambda records, cfg: captured.setdefault("dlq", records.collect()))
        doc = json.loads(sample_bytes("valid_order_completed.json"))
        vals = [dump({**doc, "event_id": f"00000000-0000-4000-8000-{k:012d}", "event_timestamp": at(-dt.timedelta(hours=k))})
                for k in range(120)]
        with caplog.at_level("INFO", logger="streaming"):
            batch.process_batch(frame(spark, vals), 1, config)
        log = json.loads(next(r.getMessage()[6:] for r in caplog.records if r.getMessage().startswith("batch ")))
        assert log == {"batch_id": 1, "input": 120, "order": 25, "excluded": 0, "dlq": 95}
        assert len(orders_out["rows"]) == 25
        assert len({r["event_timestamp"].replace(minute=0, second=0, microsecond=0) for r in orders_out["rows"]}) <= 26
        codes = {json.loads(r["value"])["error_code"] for r in captured["dlq"]}
        assert codes == {"SEMANTIC_TIMESTAMP_OUT_OF_RANGE"} and len(captured["dlq"]) == 95


class TestEventTimeWindow:
    """이벤트 시각 범위 규칙: [NOW − 24시간, NOW + 5분] 밖 주문은 DLQ."""

    def outcomes(self, spark, values):
        """값 목록을 분류해 offset 순 result 구조체 dict 목록을 돌려준다."""
        return [r["result"].asDict() for r in classify_batch(frame(spark, values)).orderBy("kafka_offset").collect()]

    @pytest.mark.parametrize("delta, outcome", [
        (-dt.timedelta(hours=24), "order"),
        (-dt.timedelta(hours=24, milliseconds=1), "dlq"),
        (-dt.timedelta(hours=25), "dlq"),
        (dt.timedelta(minutes=5), "order"),
        (dt.timedelta(minutes=5, milliseconds=1), "dlq"),
        (dt.timedelta(0), "order"),
    ])
    def test_경계값(self, spark, mutate, delta, outcome):
        """하한·상한은 포함하고 1ms라도 벗어나면 DLQ. 25시간 전 주문은 DLQ."""
        [r] = self.outcomes(spark, [mutate(lambda d: d.update(event_timestamp=at(delta)))])
        assert r["outcome"] == outcome
        if outcome == "dlq":
            assert (r["error_stage"], r["error_code"], r["error_path"]) == (
                "semantic", "SEMANTIC_TIMESTAMP_OUT_OF_RANGE", "event_timestamp")

    def test_health_check는_범위_밖이어도_제외(self, spark):
        """health_check는 저장하지 않으므로 규칙을 적용하지 않는다."""
        doc = json.loads(sample_bytes("valid_health_check.json"))
        [r] = self.outcomes(spark, [dump({**doc, "event_timestamp": at(-dt.timedelta(hours=48))})])
        assert r["outcome"] == "excluded" and r["error_code"] is None

    def test_다른_불량_사유가_우선(self, spark, mutate):
        """이미 DLQ인 주문은 원래 오류 코드를 유지한다."""
        raw = mutate(lambda d: d.update(event_timestamp=at(-dt.timedelta(hours=48)))
                     or d["properties"].update(total_amount=1))
        [r] = self.outcomes(spark, [raw])
        assert r["error_code"] == "SEMANTIC_AMOUNT_MISMATCH"

    def test_DLQ_레코드_필드(self, spark, mutate):
        """새 코드의 DLQ 메시지: 단계·코드·경로·이벤트 식별·사유·원문."""
        ts = at(-dt.timedelta(hours=25))
        raw = mutate(lambda d: d.update(event_timestamp=ts))
        [row] = transforms.build_dlq_records(classify_batch(frame(spark, [raw], partition=1, start=9))).collect()
        v = json.loads(row["value"])
        assert row["key"] == f"{TOPIC}:1:9"
        assert (v["error_stage"], v["error_code"], v["error_path"]) == (
            "semantic", "SEMANTIC_TIMESTAMP_OUT_OF_RANGE", "event_timestamp")
        assert (v["event_name"], v["schema_version"]) == ("order_completed", "1.0.0")
        assert v["error_message"] == (f"event_timestamp {ts} is outside "
                                      "[2026-11-15T06:08:02.000+00:00, 2026-11-16T06:13:02.000+00:00]")
        assert base64.b64decode(v["raw_value_base64"]) == raw


def test_CLASSIFICATION_SCHEMA_가_classify_키와_일치():
    """UDF 반환 스키마의 필드 이름이 classify() 결과 키와 같다."""
    from streaming.transforms import classify
    assert [f.name for f in schemas.CLASSIFICATION_SCHEMA.fields] == list(classify(None))


class TestLatency:
    """고정 시각으로 지연 모집단과 분위수·경계 입력을 검증한다."""

    def test_전체와_분류별_지연_음수_누락_중복(self, spark):
        """정상 중복도 원본 2건으로 집계하며 미래·null은 분위수에서 제외한다."""
        order = sample_bytes("valid_order_completed.json")
        values = [order, order, sample_bytes("valid_health_check.json"), b"{", b"{", b"{"]
        rows = frame(spark, values).collect()
        timestamps = [TS - dt.timedelta(milliseconds=x) if x is not None else None
                      for x in (1000, 1000, 500, 0, -1, None)]
        source = spark.createDataFrame([tuple(r[:4]) + (ts, r[5]) for r, ts in zip(rows, timestamps)], INPUT_SCHEMA)
        c = classify_batch(source).persist()
        try:
            counts, stats = monitoring.summarize_batch(c, TS.replace(tzinfo=dt.timezone.utc))
            assert counts == {"order": 2, "excluded": 1, "dlq": 3}
            assert stats["count"] == 4 and stats["mean_ms"] == 625
            assert stats["missing_timestamp_count"] == stats["future_timestamp_count"] == 1
            assert [stats[k] for k in ("min_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms")] == [0, 500, 1000, 1000, 1000]
            assert stats["by_outcome"]["order"]["count"] == 2
            # 消費待ちだけを 2 秒増やす。分류 건수는 바뀌지 않고 모든 유효 지연이 2초 증가한다.
            _, later = monitoring.summarize_batch(c.filter(F.col("kafka_append_timestamp") <= F.lit(TS)),
                                           (TS + dt.timedelta(seconds=2)).replace(tzinfo=dt.timezone.utc))
            assert later["p50_ms"] == stats["p50_ms"] + 2000
            assert later["p99_ms"] == stats["p99_ms"] + 2000
        finally:
            c.unpersist()

    @pytest.mark.parametrize("values", [[], [b"{"]])
    def test_끄면_지연_집계를_생략하고_건수는_같다(self, spark, values):
        """latency_enabled=False는 켬과 같은 분류 건수와 None 지연을 돌려준다."""
        c = classify_batch(frame(spark, mixed_values() + values)).persist()
        try:
            started = TS.replace(tzinfo=dt.timezone.utc)
            on_counts, _ = monitoring.summarize_batch(c, started)
            off_counts, off_latency = monitoring.summarize_batch(c, started, latency_enabled=False)
            assert off_counts == on_counts and off_latency is None
        finally:
            c.unpersist()

    @pytest.mark.parametrize("values", [[], [b"{"]])
    def test_유효시차가_없으면_null(self, spark, values):
        """빈 입력과 미래 timestamp만 있는 입력은 분위수에 가짜 0을 만들지 않는다."""
        c = classify_batch(frame(spark, values))
        counts, stats = monitoring.summarize_batch(c, (TS - dt.timedelta(seconds=1)).replace(tzinfo=dt.timezone.utc))
        assert sum(counts.values()) == len(values)
        assert stats["count"] == 0 and stats["future_timestamp_count"] == len(values)
        assert all(stats[k] is None for k in ("min_ms", "max_ms", "mean_ms", "p50_ms", "p95_ms", "p99_ms"))


def test_스트리밍_지연주입과_실행키_연결(spark, tmp_path, monkeypatch, caplog):
    """별도 파일 소스·임시 체크포인트에서 실제 콜백·진행 이벤트와 출력 지연을 확인한다."""
    import threading
    import time

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    # 실제 시계로 콜백을 돌리므로 이벤트 시각을 현재로 바꿔 이벤트 시각 범위 안에 둔다.
    now = dt.datetime.now(dt.timezone.utc)
    value = dump({**json.loads(sample_bytes("valid_order_completed.json")),
                  "event_timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"})
    record = {"value": base64.b64encode(value).decode(), "topic": TOPIC, "partition": 0, "offset": 0,
              "timestamp": TS.isoformat(), "timestampType": 1}
    (input_dir / "one.json").write_text(json.dumps(record) + "\n")
    context, progresses = {}, []
    arrived = threading.Event()

    class Listener(monitoring.ProgressLogger):
        """진행 이벤트 도착을 기다려 비동기 리스너의 검증을 동기화한다."""

        def onQueryProgress(self, event):
            """실제 진행 JSON을 모으고 대기 중인 테스트를 깨운다."""
            super().onQueryProgress(event)
            progresses.append(json.loads(event.progress.json))
            arrived.set()

    listener = Listener(context)
    spark.streams.addListener(listener)

    def slow_write(orders, config):
        """출력 action에만 대기를 주입하며 실제 DataFrame도 소비한다."""
        assert orders.count() == 1
        time.sleep(0.15)

    monkeypatch.setattr(batch, "write_orders", slow_write)
    monkeypatch.setattr(batch, "insert_rows", lambda *a: None)
    # 쓰기 함수를 대체했으므로 콜백이 읽는 설정은 지연 계측 여부뿐이다.
    stub_config = types.SimpleNamespace(latency_metrics_enabled=True)
    source = spark.readStream.schema(INPUT_SCHEMA).json(str(input_dir))
    query = None
    try:
        with caplog.at_level("INFO", logger="streaming"):
            query = (source.writeStream
                     .foreachBatch(lambda df, bid: batch.process_batch(df, bid, stub_config, context))
                     .option("checkpointLocation", str(tmp_path / "checkpoint"))
                     .trigger(availableNow=True).start())
            assert query.awaitTermination(60)
            assert arrived.wait(10)
        metrics = json.loads(next(r.getMessage()[8:] for r in caplog.records
                                  if r.getMessage().startswith("latency ")))
        progress = progresses[0]
        assert metrics["query_id"] == progress["id"] == str(query.id)
        assert metrics["run_id"] == progress["runId"] == str(query.runId)
        assert metrics["batch_id"] == progress["batchId"] == 0
        assert metrics["status"] == "succeeded" and metrics["failed_stage"] is None
        assert metrics["orders_write_action_ms"] >= 150
        assert metrics["dlq_write_action_ms"] is None
        assert metrics["callback_total_ms"] >= (metrics["orders_write_action_ms"] +
                                                metrics["classification_and_metrics_action_ms"])
        started = dt.datetime.fromisoformat(metrics["callback_started_at"])
        trigger = dt.datetime.fromisoformat(progress["timestamp"].replace("Z", "+00:00"))
        assert started >= trigger
        expected = int(started.timestamp() * 1000) - int(TS.replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
        assert metrics["kafka_to_callback_start_ms"]["count"] == 1
        assert metrics["kafka_to_callback_start_ms"]["p50_ms"] == expected
        assert progress["durationMs"]["triggerExecution"] > 0
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.streams.removeListener(listener)


def test_원본_JSON_전체_평탄화_값_보존(spark):
    """from_json과 select가 모든 원본 경로의 값·UTC 밀리초를 보존한다."""
    raw = sample_bytes("valid_order_completed.json")
    doc = json.loads(raw)
    order = transforms.select_orders(classify_batch(frame(spark, [raw]))).first().asDict()
    for name, parts in ORDER_COLUMNS.items():
        value = doc
        for part in parts:
            value = value[part]
        expected = dt.datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None) if name == "event_timestamp" else value
        actual = order[name].replace(tzinfo=None) if name == "event_timestamp" else order[name]
        assert actual == expected, name


def test_원본_Spark_스키마가_계약_필드_타입과_대응한다():
    """중첩 필드 추가·삭제·타입 변경 시 명시적 Spark 스키마와의 불일치를 찾는다."""
    from streaming.transforms import VALIDATORS
    contract = VALIDATORS[("order_completed", "1.0.0")].schema

    def check(schema, spark_type):
        """계약의 로컬 참조를 풀고 중첩 객체·배열의 필드와 타입을 비교한다."""
        if "$ref" in schema:
            schema = contract["$defs"][schema["$ref"].split("/")[-1]]
        kind = schema.get("type", "string")
        if kind == "object":
            assert isinstance(spark_type, StructType)
            assert set(spark_type.fieldNames()) == set(schema["properties"])
            for field in spark_type:
                check(schema["properties"][field.name], field.dataType)
        elif kind == "array":
            from pyspark.sql.types import ArrayType
            assert isinstance(spark_type, ArrayType)
            check(schema["items"], spark_type.elementType)
        else:
            assert isinstance(spark_type, {"integer": LongType, "boolean": BooleanType, "string": StringType}[kind])

    check(contract, schemas.ORDER_EVENT_SCHEMA)


@pytest.mark.parametrize("timestamp,text,quantity,shift", [
    ("2028-02-29T23:59:59.999Z", "아이폰🍎", 2, dt.timedelta(0)),
    ("0001-01-01T00:00:00.000Z", "아이", 2, dt.timedelta(hours=24)),
    ("9999-12-31T23:59:59.999Z", "iOS", (2**63 - 1) // 15000, -dt.timedelta(minutes=5)),
])
def test_Spark_변환_날짜_정수_유니코드_경계(spark, timestamp, text, quantity, shift):
    """Python 검증을 통과한 날짜·거대 정수·유니코드가 Spark에서도 손실 없이 변환된다."""
    doc = json.loads(sample_bytes("valid_order_completed.json"))
    doc["event_timestamp"] = timestamp
    doc["context"]["device"]["os"] = text
    doc["properties"]["items"][0]["quantity"] = quantity
    doc["properties"].update(total_amount=15000 * quantity, discount_amount=0)
    # 기준 시각을 이벤트 시각 근처로 둔다. shift는 datetime 범위(1~9999년)를 넘지 않게 범위 끝에 맞춘다.
    event_time = dt.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=dt.timezone.utc)
    orders = transforms.select_orders(classify_batch(frame(spark, [dump(doc)]), event_time + shift))
    row = orders.select("os", "quantity", "total_amount",
                        F.date_format("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("time")).first()
    assert (row.os, row.quantity, row.total_amount, row.time) == (text, quantity, 15000 * quantity, timestamp)


class TestArrowUdf:
    """Arrow 분류 UDF와 pickle 분류 UDF·Python classify의 결과 동일성(PRD 03 'Spark 튜닝' S1)."""

    def values(self):
        """생성기 출력(정상·제외·불량 유형 전부)과 Kafka 경계 입력(null·빈·깨진 UTF-8·서로게이트)을 모은다."""
        import random

        from log_generator import pools
        from log_generator.generator import EventGenerator

        gen = EventGenerator(random.Random(11), pools.build_user_pool(500))
        surrogate = sample_bytes("valid_order_completed.json").replace(b'"os":"iOS"', b'"os":"\\ud800"')
        return [gen.next_event().payload for _ in range(3000)] + mixed_values() + [surrogate]

    def test_세_경로의_분류_결과가_같다(self, spark):
        """Arrow UDF 결과 = pickle UDF 결과 = 드라이버의 classify 직접 호출 결과."""
        values = self.values()
        df = frame(spark, values)
        fields = schemas.CLASSIFICATION_SCHEMA.fieldNames()

        def collect(use_arrow):
            udf = F.udf(transforms.classify, schemas.CLASSIFICATION_SCHEMA, useArrow=use_arrow)
            rows = df.select("offset", udf("value").alias("r")).orderBy("offset").collect()
            return [tuple(row["r"][f] for f in fields) for row in rows]

        direct = [tuple(transforms.classify(v)[f] for f in fields) for v in values]
        assert collect(True) == collect(False) == direct
        assert {r[0] for r in direct} == {transforms.ORDER, transforms.EXCLUDED, transforms.DLQ}

    def test_운영_UDF는_Arrow다(self):
        """transforms.classify_udf가 Arrow 최적화 UDF로 등록됐다."""
        from pyspark.util import PythonEvalType

        assert transforms.classify_udf.evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF
