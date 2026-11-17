"""Spark 스트리밍 설정을 환경변수에서 기본값 없이 읽고 검증한다.

컨테이너에서는 Compose가 루트 .env 값을 환경변수로 주입한다.
SPARK_MASTER·SPARK_DRIVER_MEMORY는 JVM 기동 전에 필요하므로 Compose가 spark-submit 인자로 넘기고 여기서는 읽지 않는다.
테스트에서 매핑을 전달하면 프로세스 환경을 읽지 않는다.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass

STARTING_OFFSETS = ("earliest", "latest")


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
    output_dir: str  # 3단계 정상 주문 Parquet 경로. 4단계 JDBC 전환 시 제거한다.


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
        output_dir=_value(env, "SPARK_OUTPUT_DIR"),
    )
