"""루트 .env와 환경변수로부터 기본값 없이 설정을 읽고 검증한다.

루트 .env가 없으면 환경변수만 사용한다. .env.example은 템플릿이며 읽지 않는다.
이미 설정된 환경변수가 파일보다 우선한다.
테스트에서 매핑을 전달하면 .env와 프로세스 환경을 읽지 않는다.
"""

import csv
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path

from dotenv import dotenv_values

OUTPUTS = ("stdout", "file", "kafka")
# librdkafka compression.type 중 허용하는 값. 오타로 인한 기동 후 오류를 설정 단계에서 막는다.
COMPRESSIONS = ("none", "gzip", "snappy", "lz4", "zstd")
# 실행 디렉터리에 따라 다른 .env를 읽지 않도록 프로젝트 루트(src/log_generator의 4단계 위)를 기준으로 한다.
# 컨테이너 /app/src/log_generator처럼 경로가 얕으면 루트가 없으므로 파일 없이 환경변수만 쓴다.
_PARENTS = Path(__file__).resolve().parents
ROOT = _PARENTS[4] if len(_PARENTS) > 4 else None
ENV_FILE = ROOT / ".env" if ROOT is not None else None
PROFILE_HEADER = ["duration_sec", "eps", "label"]


class ConfigError(ValueError):
    """생성 시작 전에 거부해야 하는 설정 오류."""


@dataclass(frozen=True)
class KafkaConfig:
    """PRODUCER_OUTPUT=kafka일 때만 사용한다. acks=all·idempotence 등 고정값은 kafka_producer.py에 둔다."""

    bootstrap_servers: str
    topic: str
    linger_ms: int
    compression: str
    delivery_timeout_ms: int
    queue_full_timeout_sec: int  # 로컬 큐가 가득 찼을 때 poll하며 기다리는 최대 시간
    flush_timeout_sec: int  # 종료 시 flush 최대 대기 시간


@dataclass(frozen=True)
class ClickHouseConfig:
    """ops.producer_stats 기록용 HTTP 접속 정보. 계정 이름은 init.sh에 고정된 generator_writer다."""

    http_url: str
    password: str


@dataclass(frozen=True)
class Segment:
    """부하 프로파일의 한 구간. duration_sec 동안 eps를 고정으로 유지한다."""

    duration_sec: int
    eps: int
    label: str


@dataclass(frozen=True)
class LoadProfile:
    """PRODUCER_LOAD_PROFILE CSV에서 읽은 구간 목록. 마지막 구간이 끝나면 생성을 종료한다."""

    path: Path
    segments: tuple[Segment, ...]


@dataclass(frozen=True)
class Config:
    eps: int
    output: str
    output_path: Path | None
    max_events: int | None  # None이면 무제한
    user_pool_size: int
    seed: int | None
    status_interval_sec: int
    kafka: KafkaConfig | None  # output이 kafka일 때만 값이 있다.
    clickhouse: ClickHouseConfig | None  # kafka 출력이고 URL이 있을 때만 생성기 지표를 기록한다.
    profile: LoadProfile | None  # 있으면 eps 대신 구간별 eps를 쓴다.
    workers: int  # 생성 프로세스 수. 2 이상이면 EPS·최대 건수를 나눠 worker 프로세스로 실행한다.


def _value(environ: Mapping[str, str | None], key: str, *, allow_empty: bool = False) -> str:
    """키 누락을 거부한다. 빈 값은 의미가 명시된 설정에만 허용한다."""
    if key not in environ or environ[key] is None:
        raise ConfigError(f"{key} is required")
    value = environ[key].strip()
    if not value and not allow_empty:
        raise ConfigError(f"{key} must not be empty")
    return value


def _int(environ: Mapping[str, str | None], key: str, minimum: int | None) -> int:
    """필수 정수를 읽고 최솟값을 검증한다."""
    raw = _value(environ, key)
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{key} must be an integer: {raw!r}") from None
    if minimum is not None and value < minimum:
        raise ConfigError(f"{key} must be >= {minimum}: {raw!r}")
    return value


def load_profile(raw_path: str) -> LoadProfile:
    """프로파일 CSV를 읽고 검증한다. 상대 경로는 프로젝트 루트(없으면 실행 디렉터리) 기준이다."""
    path = Path(raw_path)
    if not path.is_absolute() and ROOT is not None:
        path = ROOT / path
    try:
        with path.open(encoding="utf-8", newline="") as f:
            rows = list(csv.reader(f))
    except (OSError, UnicodeError) as exc:
        raise ConfigError(f"cannot read PRODUCER_LOAD_PROFILE: {exc}") from exc
    if not rows or rows[0] != PROFILE_HEADER:
        raise ConfigError(f"PRODUCER_LOAD_PROFILE header must be {','.join(PROFILE_HEADER)}: {path}")
    segments = []
    for line_no, row in enumerate(rows[1:], start=2):
        if len(row) != len(PROFILE_HEADER):
            raise ConfigError(f"PRODUCER_LOAD_PROFILE line {line_no}: expected 3 columns: {row!r}")
        try:
            duration, eps = int(row[0]), int(row[1])
        except ValueError:
            raise ConfigError(f"PRODUCER_LOAD_PROFILE line {line_no}: duration_sec and eps must be integers: {row!r}") from None
        label = row[2].strip()
        if duration < 1 or eps < 1 or not label:
            raise ConfigError(f"PRODUCER_LOAD_PROFILE line {line_no}: duration_sec, eps >= 1 and non-empty label required: {row!r}")
        segments.append(Segment(duration, eps, label))
    if not segments:
        raise ConfigError(f"PRODUCER_LOAD_PROFILE has no segments: {path}")
    return LoadProfile(path, tuple(segments))


def load_config(environ: Mapping[str, str] | None = None) -> Config:
    """환경변수 > .env 순서로 읽는다. 코드 기본값은 제공하지 않는다."""
    if environ is None:
        try:
            file_values = dotenv_values(ENV_FILE) if ENV_FILE is not None and ENV_FILE.exists() else {}
            # 프로세스 환경을 변경하지 않아 반복 호출에도 파일 변경값을 반영한다.
            env = {**file_values, **os.environ}
        except (OSError, UnicodeError) as exc:
            raise ConfigError(f"cannot read .env: {exc}") from exc
    else:
        env = environ

    output = _value(env, "PRODUCER_OUTPUT")
    if output not in OUTPUTS:
        raise ConfigError(f"PRODUCER_OUTPUT must be one of {OUTPUTS}: {output!r}")

    raw_path = _value(env, "PRODUCER_OUTPUT_PATH", allow_empty=True)
    output_path = None
    if output == "file":
        if not raw_path:
            raise ConfigError("PRODUCER_OUTPUT_PATH is required when PRODUCER_OUTPUT=file")
        output_path = Path(raw_path)
        # 기존 파일 덮어쓰기 금지. 실제 생성 시에도 'xb' 모드로 한 번 더 막는다.
        if output_path.exists():
            raise ConfigError(f"output file already exists: {output_path}")
        if not output_path.parent.is_dir():
            raise ConfigError(f"output directory does not exist: {output_path.parent}")

    # Kafka 키는 출력 대상과 무관하게 모두 명시해야 하지만 값 검증과 사용은 kafka 출력에만 적용한다.
    compression = _value(env, "PRODUCER_KAFKA_COMPRESSION")
    kafka = KafkaConfig(
        bootstrap_servers=_value(env, "PRODUCER_KAFKA_BOOTSTRAP_SERVERS"),
        topic=_value(env, "PRODUCER_KAFKA_TOPIC"),
        linger_ms=_int(env, "PRODUCER_KAFKA_LINGER_MS", minimum=0),
        compression=compression,
        delivery_timeout_ms=_int(env, "PRODUCER_KAFKA_DELIVERY_TIMEOUT_MS", minimum=1),
        queue_full_timeout_sec=_int(env, "PRODUCER_KAFKA_QUEUE_FULL_TIMEOUT_SEC", minimum=1),
        flush_timeout_sec=_int(env, "PRODUCER_KAFKA_FLUSH_TIMEOUT_SEC", minimum=1),
    )
    if output == "kafka" and compression not in COMPRESSIONS:
        raise ConfigError(f"PRODUCER_KAFKA_COMPRESSION must be one of {COMPRESSIONS}: {compression!r}")

    # 빈 URL은 생성기 지표 기록 끔을 명시적으로 뜻한다. URL이 있으면 비밀번호가 필요하다.
    clickhouse_url = _value(env, "PRODUCER_CLICKHOUSE_HTTP_URL", allow_empty=True).rstrip("/")
    clickhouse_password = _value(env, "CLICKHOUSE_GENERATOR_PASSWORD", allow_empty=True)
    clickhouse = None
    if output == "kafka" and clickhouse_url:
        if not clickhouse_password:
            raise ConfigError("CLICKHOUSE_GENERATOR_PASSWORD is required when PRODUCER_CLICKHOUSE_HTTP_URL is set")
        clickhouse = ClickHouseConfig(clickhouse_url, clickhouse_password)

    # 빈 seed는 무작위를, 최대 건수 0은 무제한을 명시적으로 뜻한다.
    seed = _value(env, "PRODUCER_SEED", allow_empty=True)
    # 빈 프로파일 경로는 PRODUCER_EPS 고정 속도를 뜻한다.
    raw_profile = _value(env, "PRODUCER_LOAD_PROFILE", allow_empty=True)
    config = Config(
        eps=_int(env, "PRODUCER_EPS", minimum=1),
        output=output,
        output_path=output_path,
        max_events=_int(env, "PRODUCER_MAX_EVENTS", minimum=0) or None,
        user_pool_size=_int(env, "PRODUCER_USER_POOL_SIZE", minimum=1),
        seed=_int(env, "PRODUCER_SEED", minimum=None) if seed else None,
        status_interval_sec=_int(env, "PRODUCER_STATUS_INTERVAL_SEC", minimum=1),
        kafka=kafka if output == "kafka" else None,
        clickhouse=clickhouse,
        profile=load_profile(raw_profile) if raw_profile else None,
        workers=_int(env, "PRODUCER_WORKERS", minimum=1),
    )
    _check_workers(config)
    return config


def _check_workers(config: Config) -> None:
    """worker 수가 2 이상이면 출력 대상과, 나눴을 때 0이 되는 EPS·최대 건수를 거부한다."""
    n = config.workers
    if n == 1:
        return
    if config.output != "kafka":
        raise ConfigError(f"PRODUCER_WORKERS >= 2 requires PRODUCER_OUTPUT=kafka: {config.output!r}")
    rates = [s.eps for s in config.profile.segments] if config.profile else [config.eps]
    if min(rates) < n:
        raise ConfigError(f"every EPS must be >= PRODUCER_WORKERS ({n}): min {min(rates)}")
    if config.max_events is not None and config.max_events < n:
        raise ConfigError(f"PRODUCER_MAX_EVENTS must be 0 or >= PRODUCER_WORKERS ({n}): {config.max_events}")


def _share(total: int, n: int, index: int) -> int:
    """total을 n개로 나눈 index번째 몫. 나머지는 앞 번호부터 1씩 더한다."""
    return total // n + (1 if index < total % n else 0)


def worker_config(config: Config, index: int) -> Config:
    """index번째 worker 설정. EPS·구간 eps·최대 건수를 나누고 seed를 index만큼 바꾼다."""
    n = config.workers
    profile = config.profile
    if profile:
        profile = replace(profile, segments=tuple(replace(s, eps=_share(s.eps, n, index)) for s in profile.segments))
    return replace(
        config,
        eps=_share(config.eps, n, index),
        max_events=_share(config.max_events, n, index) if config.max_events else None,
        seed=config.seed + index if config.seed is not None else None,
        profile=profile,
        workers=1,
    )
