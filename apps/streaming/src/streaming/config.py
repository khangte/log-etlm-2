"""Spark 스트리밍 설정을 환경변수에서 기본값 없이 읽고 검증한다.

컨테이너에서는 Compose가 루트 .env 값을 환경변수로 주입한다.
SPARK_MASTER·SPARK_DRIVER_MEMORY는 JVM 기동 전에 필요하므로 Compose가 spark-submit 인자로 넘기고 여기서는 읽지 않는다.
테스트에서 매핑을 전달하면 프로세스 환경을 읽지 않는다.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

STARTING_OFFSETS = ("earliest", "latest")
BOOLEANS = {"true": True, "false": False}


class ConfigError(ValueError):
    """쿼리 시작 전에 거부해야 하는 설정 오류."""


@dataclass(frozen=True)
class Config:
    """Kafka 소비·출력·트리거 설정. 값의 근거는 Spark PRD '실행 설정과 자원 예산'을 따른다."""

    bootstrap_servers: str
    topic: str
    dlq_topic: str
    starting_offsets: str
    max_offsets_per_trigger: int
    trigger_interval: str  # Spark processingTime 문자열. 예: "5 seconds"
    shuffle_partitions: int
    checkpoint_dir: str
    latency_metrics_enabled: bool  # false면 지연 분위수 집계를 생략한다(계측 오버헤드 대조군).
    clickhouse_jdbc_url: str  # 정상 주문 JDBC 적재 주소. 동기 INSERT 서버 설정을 포함한다.
    clickhouse_http_url: str  # 운영 지표 HTTP INSERT 주소
    clickhouse_password: str = field(repr=False)  # spark_writer 계정 비밀번호. 로그·repr에 남기지 않는다.
    jdbc_batch_size: int  # 가장 큰 쓰기 파티션 행 수 이상이면 쓰기 파티션당 INSERT 1회다.
    jdbc_num_partitions: int  # ClickHouse 동시 JDBC 연결 수
    stall_timeout_seconds: int  # 진행·유휴 이벤트가 이 시간 동안 없으면 멈춤으로 보고 exit 1


def _value(environ: Mapping[str, str], key: str) -> str:
    """키 누락과 빈 값을 거부한다."""
    value = (environ.get(key) or "").strip()
    if not value:
        raise ConfigError(f"{key} is required")
    return value


def _positive_int(environ: Mapping[str, str], key: str) -> int:
    """양의 정수 설정을 읽는다."""
    raw = _value(environ, key)
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{key} must be an integer: {raw!r}") from None
    if value < 1:
        raise ConfigError(f"{key} must be >= 1: {raw!r}")
    return value


def _bool(environ: Mapping[str, str], key: str) -> bool:
    """소문자 true/false만 허용하는 불리언 설정을 읽는다."""
    raw = _value(environ, key)
    if raw not in BOOLEANS:
        raise ConfigError(f"{key} must be one of {tuple(BOOLEANS)}: {raw!r}")
    return BOOLEANS[raw]


def load_config(environ: Mapping[str, str] | None = None) -> Config:
    """환경변수에서 설정을 읽는다. 코드 기본값은 제공하지 않는다."""
    env = os.environ if environ is None else environ
    starting_offsets = _value(env, "SPARK_STARTING_OFFSETS")
    if starting_offsets not in STARTING_OFFSETS:
        raise ConfigError(f"SPARK_STARTING_OFFSETS must be one of {STARTING_OFFSETS}: {starting_offsets!r}")
    return Config(
        bootstrap_servers=_value(env, "SPARK_KAFKA_BOOTSTRAP_SERVERS"),
        topic=_value(env, "KAFKA_TOPIC"),
        dlq_topic=_value(env, "KAFKA_DLQ_TOPIC"),
        starting_offsets=starting_offsets,
        max_offsets_per_trigger=_positive_int(env, "SPARK_MAX_OFFSETS_PER_TRIGGER"),
        trigger_interval=_value(env, "SPARK_TRIGGER_INTERVAL"),
        shuffle_partitions=_positive_int(env, "SPARK_SHUFFLE_PARTITIONS"),
        checkpoint_dir=_value(env, "SPARK_CHECKPOINT_DIR"),
        latency_metrics_enabled=_bool(env, "SPARK_LATENCY_METRICS_ENABLED"),
        clickhouse_jdbc_url=_value(env, "SPARK_CLICKHOUSE_JDBC_URL"),
        clickhouse_http_url=_value(env, "SPARK_CLICKHOUSE_HTTP_URL").rstrip("/"),
        clickhouse_password=_value(env, "CLICKHOUSE_SPARK_PASSWORD"),
        jdbc_batch_size=_positive_int(env, "SPARK_JDBC_BATCH_SIZE"),
        jdbc_num_partitions=_positive_int(env, "SPARK_JDBC_NUM_PARTITIONS"),
        stall_timeout_seconds=_positive_int(env, "SPARK_STALL_TIMEOUT_SECONDS"),
    )
