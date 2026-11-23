"""원문 파싱·검증·분류와 Spark DataFrame 필터·구조화·평탄화·DLQ 변환을 담당한다."""

import functools
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jsonschema_rs
from jsonschema import Draft202012Validator, validators
from jsonschema.exceptions import ValidationError, best_match

from log_generator.pools import LOCATIONS, PRODUCTS

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from streaming import __version__
from streaming.schemas import CLASSIFICATION_SCHEMA, ORDER_COLUMNS, ORDER_EVENT_SCHEMA

# 저장소 루트(src/streaming의 4단계 위). 컨테이너도 /opt/app 아래에 같은 구조로 마운트한다.
CONTRACTS_DIR = Path(__file__).resolve().parents[4] / "contracts"

ORDER = "order"
EXCLUDED = "excluded"
DLQ = "dlq"

LONG_MIN, LONG_MAX = -(2**63), 2**63 - 1

# Spark LongType으로 표현 가능한지 검사할 원본 경로. 평탄화 매핑에서 가져온다.
LONG_PATHS = {name: ORDER_COLUMNS[name] for name in ("total_amount", "discount_amount", "price", "quantity")}


@functools.lru_cache(maxsize=None)
def _ecma_pattern(pattern: str) -> re.Pattern:
    """ECMA-262처럼 `$`가 문자열 끝에서만 일치하도록 `$` 앵커를 `\\Z`로 바꿔 컴파일한다."""
    # shortcut: 문자 클래스 안의 `$`(예: `[$]`)도 바뀐다. 계약 pattern에는 없으며, 생기면 정규식 파서로 바꾼다.
    return re.compile(re.sub(r"(?<!\\)\$", r"\\Z", pattern))


def _pattern(validator, pattern, instance, schema):
    """pattern 키워드 재정의. Python `$`가 끝 개행 앞에서도 일치해 `"usr_00001\\n"`이 통과하는 것을 막는다."""
    if validator.is_type(instance, "string") and not _ecma_pattern(pattern).search(instance):
        yield ValidationError(f"{instance!r} does not match {pattern!r}")


# JSON Schema의 integer는 1.0 같은 소수점 표기도 허용한다. 원래 JSON 정수만 허용하도록 바꾼다.
_StrictValidator = validators.extend(
    Draft202012Validator,
    validators={"pattern": _pattern},
    type_checker=Draft202012Validator.TYPE_CHECKER.redefine(
        "integer", lambda _checker, value: isinstance(value, int) and not isinstance(value, bool)
    ),
)
_FORMAT_CHECKER = Draft202012Validator.FORMAT_CHECKER
# rfc3339-validator가 없으면 date-time 검사가 조용히 통과하므로 기동 시 실패시킨다.
if "date-time" not in _FORMAT_CHECKER.checkers:
    raise RuntimeError("rfc3339-validator is required for format: date-time")


def _load_schema(file_name: str) -> dict:
    """contracts/schemas의 계약 파일을 읽는다."""
    return json.loads((CONTRACTS_DIR / "schemas" / file_name).read_text(encoding="utf-8"))


def _inline_refs(schema: dict) -> dict:
    """`{"$ref": "#/$defs/..."}`만 있는 하위 스키마를 대상 정의로 바꾼 사본을 반환한다.

    jsonschema가 검증 중 `$ref`마다 참조 해석(referencing)을 반복하는 비용을 없앤다. 형제 키워드가 없는
    로컬 `$ref`는 대상 스키마와 같게 평가되므로 판정·오류 경로가 같다(원본 스키마 검증기와 대조 테스트).
    다른 형태의 `$ref`는 의미가 달라질 수 있어 기동 시 실패시킨다.
    """
    defs = schema.get("$defs", {})

    def walk(node):
        """하위 스키마를 재귀로 복사하며 `$ref`를 펼친다."""
        if isinstance(node, list):
            return [walk(item) for item in node]
        if not isinstance(node, dict):
            return node
        if "$ref" in node:
            ref = node["$ref"]
            if len(node) != 1 or not ref.startswith("#/$defs/") or ref.removeprefix("#/$defs/") not in defs:
                raise RuntimeError(f"unsupported $ref form for inlining: {node!r}")
            return walk(defs[ref.removeprefix("#/$defs/")])
        return {key: walk(value) for key, value in node.items() if key != "$defs"}

    return walk(schema)


_CONTRACT_FILES = {
    ("order_completed", "1.0.0"): "order_completed.1.0.0.schema.json",
    ("health_check", "1.0.0"): "health_check.1.0.0.schema.json",
}
# 지원 계약: (event_name, schema_version) → 엄격 검증기. 판정과 DLQ 오류 내용의 기준이다.
VALIDATORS = {key: _StrictValidator(_inline_refs(_load_schema(name)), format_checker=_FORMAT_CHECKER)
              for key, name in _CONTRACT_FILES.items()}
# 1단 빠른 검증기(Rust). "유효" 판정만 신뢰하고 무효면 엄격 검증기로 다시 판정한다.
# 소수점 표기 정수(15000.0)를 integer로 허용하므로 소수가 있는 문서에는 쓰지 않는다.
# date-time 형식은 연도 0000·초 60을 허용하므로 event_timestamp는 기존 형식 검사기로 다시 확인한다.
FAST_VALIDATORS = {key: jsonschema_rs.Draft202012Validator(_load_schema(name), validate_formats=True)
                   for key, name in _CONTRACT_FILES.items()}


def _fast_valid(key: tuple[str, str], doc: dict) -> bool:
    """1단 판정. True면 엄격 검증기도 유효로 판정해야 한다(대조 테스트로 확인)."""
    # shortcut: 계약의 format 필드가 event_timestamp 하나라고 가정한다. format 필드가 늘면 함께 재확인한다.
    return FAST_VALIDATORS[key].is_valid(doc) and _FORMAT_CHECKER.conforms(doc["event_timestamp"], "date-time")


_PRODUCTS = {p.product_id: (p.product_name, p.category, p.price) for p in PRODUCTS}
_LOCATIONS = frozenset(LOCATIONS)


def _result(outcome, *, event_name=None, schema_version=None, stage=None, code=None, path=None, message=None) -> dict:
    """분류 결과와 오류 메타데이터를 반환한다. Spark 스키마는 schemas.py에 정의한다."""
    return {
        "outcome": outcome,
        "event_name": event_name,
        "schema_version": schema_version,
        "error_stage": stage,
        "error_code": code,
        "error_path": path,
        "error_message": message,
    }


def _path(parts) -> str:
    """JSON 경로 요소를 `properties.items[0].price` 형식으로 만든다. 루트는 `$`."""
    text = ""
    for part in parts:
        text += f"[{part}]" if isinstance(part, int) else (f".{part}" if text else part)
    return text or "$"


def _reject_constant(name: str):
    """NaN·Infinity 같은 비표준 JSON 상수를 거부한다."""
    raise ValueError(f"non-standard JSON constant: {name}")


def _reject_duplicates(pairs) -> dict:
    """마지막 값으로 덮어쓰는 대신 중복 키를 거부한다. 오류 메시지는 처음 다시 나온 키를 가리킨다."""
    obj = dict(pairs)  # C 구현으로 먼저 만들고, 키 수가 줄었을 때만 중복 키를 찾는다.
    if len(obj) != len(pairs):
        seen = set()
        for key, _ in pairs:
            if key in seen:
                raise ValueError(f"duplicate key: {key!r}")
            seen.add(key)
    return obj


def _schema_error(error, event_name: str, schema_version: str) -> dict:
    """jsonschema 오류를 DLQ 분류로 바꾼다. required·additionalProperties는 해당 필드까지 경로에 붙인다."""
    parts = list(error.absolute_path)
    if error.validator == "required":
        parts.append(next(name for name in error.validator_value if name not in error.instance))
    elif error.validator == "additionalProperties":
        parts.append(sorted(set(error.instance) - set(error.schema.get("properties", {})))[0])
    return _result(DLQ, event_name=event_name, schema_version=schema_version, stage="schema",
                   code=f"SCHEMA_{error.validator.upper()}", path=_path(parts), message=error.message)


def _semantic_error(doc: dict) -> tuple[str, str, str] | None:
    """스키마 밖 의미 규칙(contracts/README.md)을 검사해 (코드, 경로, 사유) 또는 None을 반환한다."""
    props = doc["properties"]
    # Python 정수라 곱셈·합계 오버플로가 없다. LongType 범위는 변환 단계에서 검사한다.
    subtotal = sum(item["price"] * item["quantity"] for item in props["items"])
    if props["discount_amount"] > subtotal:
        return ("SEMANTIC_DISCOUNT_EXCEEDS", "properties.discount_amount",
                f"discount_amount {props['discount_amount']} > item subtotal {subtotal}")
    if props["total_amount"] != subtotal - props["discount_amount"]:
        return ("SEMANTIC_AMOUNT_MISMATCH", "properties.total_amount",
                f"total_amount {props['total_amount']} != subtotal {subtotal} - discount {props['discount_amount']}")
    for index, item in enumerate(props["items"]):
        if _PRODUCTS.get(item["product_id"]) != (item["product_name"], item["category"], item["price"]):
            return ("SEMANTIC_PRODUCT_MISMATCH", f"properties.items[{index}]",
                    f"product {item['product_id']!r} does not match the product pool")
    network = doc["context"]["network"]
    if (network["country"], network["region"], network["city"]) not in _LOCATIONS:
        return ("SEMANTIC_LOCATION_MISMATCH", "context.network",
                f"location {network['country']}/{network['region']}/{network['city']} is not in the location pool")
    return None


def _conversion_error(doc: dict) -> tuple[str, str, str] | None:
    """Spark 변환 전에 원본 시각과 정수 표현 범위를 검증한다. 주문 행은 만들지 않는다."""
    # 계약 pattern이 `YYYY-MM-DDTHH:MM:SS.mmmZ` 형식을 이미 보장하므로 strptime 대신 C 구현 fromisoformat으로
    # 연도 0000·초 60처럼 Python datetime이 표현할 수 없는 값만 거른다(Python 3.10은 끝 Z를 받지 않아 뗀다).
    try:
        datetime.fromisoformat(doc["event_timestamp"][:-1])
    except ValueError:
        return "CONVERSION_TIMESTAMP", "event_timestamp", "event_timestamp cannot be represented as a Spark timestamp"
    for column, parts in LONG_PATHS.items():
        value = doc
        for part in parts:
            value = value[part]
        if not LONG_MIN <= value <= LONG_MAX:
            return "CONVERSION_LONG_RANGE", _path(parts), f"{column} cannot be represented as a Spark long"
    return None


def classify(value: bytes | bytearray | None) -> dict:
    """Kafka value 1건을 정상 주문(order)·제외(excluded)·불량(dlq) 중 하나로 분류한다.

    반환 dict의 키는 outcome, event_name, schema_version, error_stage, error_code, error_path,
    error_message이다. 구조화·타입 변환·평탄화는 아래 DataFrame 변환 함수가 담당한다.
    """
    if value is None:
        return _result(DLQ, stage="parse", code="NULL_VALUE", message="Kafka value is null")
    if len(value) == 0:
        return _result(DLQ, stage="parse", code="EMPTY_VALUE", message="Kafka value is empty")
    try:
        text = bytes(value).decode("utf-8")
    except UnicodeDecodeError as exc:
        return _result(DLQ, stage="parse", code="INVALID_UTF8", message=str(exc))
    try:
        floats = []  # 소수 리터럴이 있으면 1단 빠른 검증을 건너뛴다.
        doc = json.loads(text, parse_constant=_reject_constant, object_pairs_hook=_reject_duplicates,
                         parse_float=lambda s: floats.append(s) or float(s))
    except (ValueError, RecursionError) as exc:
        return _result(DLQ, stage="parse", code="INVALID_JSON", message=str(exc))

    if not isinstance(doc, dict):
        return _result(DLQ, stage="schema", code="SCHEMA_TYPE", path="$",
                       message=f"root must be a JSON object, got {type(doc).__name__}")
    event_name, schema_version = doc.get("event_name"), doc.get("schema_version")
    known = {
        "event_name": event_name if isinstance(event_name, str) else None,
        "schema_version": schema_version if isinstance(schema_version, str) else None,
    }
    for field in ("event_name", "schema_version"):
        if known[field] is None:
            reason = "is missing" if field not in doc else "must be a string"
            return _result(DLQ, **known, stage="contract", code="CONTRACT_FIELD_INVALID", path=field,
                           message=f"{field} {reason}")
    validator = VALIDATORS.get((event_name, schema_version))
    if validator is None:
        return _result(DLQ, **known, stage="contract", code="CONTRACT_UNSUPPORTED",
                       message=f"unsupported event_name/schema_version: {event_name!r}/{schema_version!r}")

    fast_valid = not floats and _fast_valid((event_name, schema_version), doc)
    error = None if fast_valid else best_match(validator.iter_errors(doc))
    if error is not None:
        return _schema_error(error, event_name, schema_version)
    if event_name == "health_check":
        return _result(EXCLUDED, **known)

    semantic = _semantic_error(doc)
    if semantic is not None:
        code, path, message = semantic
        return _result(DLQ, **known, stage="semantic", code=code, path=path, message=message)
    conversion = _conversion_error(doc)
    if conversion is not None:
        code, path, message = conversion
        return _result(DLQ, **known, stage="conversion", code=code, path=path, message=message)
    return _result(ORDER, **known)


PROCESSOR_VERSION = f"streaming {__version__}"
DLQ_SCHEMA_VERSION = "1.0.0"
LOG_APPEND_TIME = 1  # Kafka 소스 timestampType 값
KAFKA_COLUMNS = ("kafka_topic", "kafka_partition", "kafka_offset", "kafka_append_timestamp")


# Arrow 직렬화로 Python 워커 입출력 비용을 줄인다(PRD 03 'Spark 튜닝' S1). 분류 결과는 pickle UDF와 같다(대조 테스트).
classify_udf = F.udf(classify, CLASSIFICATION_SCHEMA, useArrow=True)

# 이벤트 시각 허용 범위(PRD 03 '이벤트 시각 범위 규칙'). 하한은 silver.order_events TTL과 같아야 한다.
EVENT_TIME_MAX_AGE = timedelta(hours=24)
EVENT_TIME_MAX_AHEAD = timedelta(minutes=5)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _event_time_window(result: Column, raw_value: Column, callback_started_at: datetime) -> Column:
    """order 결과 중 event_timestamp가 허용 범위 밖이면 DLQ 결과로 바꾼 result 표현식을 반환한다."""
    lower, upper = callback_started_at - EVENT_TIME_MAX_AGE, callback_started_at + EVENT_TIME_MAX_AHEAD
    text = F.get_json_object(raw_value.cast("string"), "$.event_timestamp")
    # order 행은 classify가 형식·변환 가능을 이미 확인했다. 다른 행에서 ANSI 파싱 오류가 나지 않게 try_를 쓴다.
    event_time = F.try_to_timestamp(text, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    # F.lit(datetime)은 1582년 이전 시각을 율리우스력으로 바꿔 어긋나므로 epoch 마이크로초로 넘긴다.
    lower_ts, upper_ts = (F.timestamp_micros(F.lit((t - _EPOCH) // timedelta(microseconds=1))) for t in (lower, upper))
    out_of_range = (result["outcome"] == ORDER) & ~event_time.between(lower_ts, upper_ts)
    window = f"[{lower.isoformat(timespec='milliseconds')}, {upper.isoformat(timespec='milliseconds')}]"
    rejected = F.struct(
        F.lit(DLQ).alias("outcome"), result["event_name"].alias("event_name"),
        result["schema_version"].alias("schema_version"), F.lit("semantic").alias("error_stage"),
        F.lit("SEMANTIC_TIMESTAMP_OUT_OF_RANGE").alias("error_code"), F.lit("event_timestamp").alias("error_path"),
        F.concat(F.lit("event_timestamp "), text, F.lit(f" is outside {window}")).alias("error_message"))
    return F.when(out_of_range, rejected).otherwise(result)


def classify_batch(source: DataFrame, callback_started_at: datetime) -> DataFrame:
    """Kafka 소스 DataFrame에 원문·추적 컬럼과 분류 결과(result 구조체)를 붙인다.

    timestampType이 LogAppendTime이 아니면 이벤트 불량이 아닌 설정 오류이므로 raise_error로 작업을 실패시킨다.
    callback_started_at(UTC aware)은 이벤트 시각 범위 규칙의 기준 시각이다. classify()는 시계를 쓰지 않는다.
    """
    classified = source.select(
        F.col("value").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.when(F.col("timestampType") == LOG_APPEND_TIME, F.col("timestamp"))
        .otherwise(F.raise_error(F.format_string(
            "Kafka timestampType %s is not LogAppendTime at %s:%s:%s",
            F.col("timestampType"), F.col("topic"), F.col("partition"), F.col("offset"))))
        .alias("kafka_append_timestamp"),
        classify_udf(F.col("value")).alias("result"),
    )
    return classified.withColumn("result", _event_time_window(F.col("result"), F.col("raw_value"), callback_started_at))


def select_orders(classified: DataFrame) -> DataFrame:
    """정상 주문을 필터링·구조화·평탄화해 이벤트 31개·Kafka 추적 4개 출력 컬럼을 선택한다."""
    orders = classified.filter(F.col("result.outcome") == ORDER).withColumn(
        "event", F.from_json(F.col("raw_value").cast("string"), ORDER_EVENT_SCHEMA, {"mode": "FAILFAST"}))
    columns = []
    for name, parts in ORDER_COLUMNS.items():
        value = F.col("event")
        for part in parts:
            value = value[part]
        if name == "event_timestamp":
            value = F.to_timestamp(value, "yyyy-MM-dd'T'HH:mm:ss.SSSX")
        columns.append(value.alias(name))
    return orders.select(*columns, *KAFKA_COLUMNS)


def deduplicate_events(events: DataFrame) -> DataFrame:
    """배치 안의 같은 event_id 행을 하나만 남긴다. 배치 간 중복은 PRD에 따라 처리하지 않는다."""
    return events.dropDuplicates(["event_id"])


def build_dlq_records(classified: DataFrame) -> DataFrame:
    """불량 레코드를 Kafka writer용 key·value로 만든다. 스키마는 PRD 'DLQ 메시지 스키마'를 따른다."""
    position = F.concat_ws(":", "kafka_topic", "kafka_partition", "kafka_offset")
    message = F.struct(
        F.lit(DLQ_SCHEMA_VERSION).alias("dlq_schema_version"),
        F.col("kafka_topic").alias("source_topic"),
        F.col("kafka_partition").alias("source_partition"),
        F.col("kafka_offset").alias("source_offset"),
        F.col("kafka_append_timestamp").alias("source_timestamp"),
        F.base64("raw_value").alias("raw_value_base64"),
        F.col("result.error_stage").alias("error_stage"),
        F.col("result.error_code").alias("error_code"),
        F.col("result.error_path").alias("error_path"),
        F.col("result.error_message").alias("error_message"),
        F.col("result.event_name").alias("event_name"),
        F.col("result.schema_version").alias("schema_version"),
        F.current_timestamp().alias("detected_at"),
        F.lit(PROCESSOR_VERSION).alias("processor_version"),
    )
    # 세션 시간대가 UTC이므로 XXX는 Z로 출력된다. null 필드도 생략하지 않는다.
    options = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "ignoreNullFields": "false"}
    return classified.filter(F.col("result.outcome") == DLQ).select(
        position.alias("key"), F.to_json(message, options).alias("value"))
