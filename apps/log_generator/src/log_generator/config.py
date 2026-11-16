"""루트 .env와 환경변수로부터 기본값 없이 설정을 읽고 검증한다.

루트 .env가 없으면 환경변수만 사용한다. .env.example은 템플릿이며 읽지 않는다.
이미 설정된 환경변수가 파일보다 우선한다.
테스트에서 매핑을 전달하면 .env와 프로세스 환경을 읽지 않는다.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

OUTPUTS = ("stdout", "file")
# 실행 디렉터리에 따라 다른 .env를 읽지 않도록 프로젝트 루트(src/log_generator의 4단계 위)를 기준으로 한다.
# 컨테이너 /app/src/log_generator처럼 경로가 얕으면 루트가 없으므로 파일 없이 환경변수만 쓴다.
_PARENTS = Path(__file__).resolve().parents
ENV_FILE = _PARENTS[4] / ".env" if len(_PARENTS) > 4 else None


class ConfigError(ValueError):
    """생성 시작 전에 거부해야 하는 설정 오류."""


@dataclass(frozen=True)
class Config:
    eps: int
    output: str
    output_path: Path | None
    max_events: int | None  # None이면 무제한
    user_pool_size: int
    seed: int | None
    status_interval_sec: int


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

    # 빈 seed는 무작위를, 최대 건수 0은 무제한을 명시적으로 뜻한다.
    seed = _value(env, "PRODUCER_SEED", allow_empty=True)
    return Config(
        eps=_int(env, "PRODUCER_EPS", minimum=1),
        output=output,
        output_path=output_path,
        max_events=_int(env, "PRODUCER_MAX_EVENTS", minimum=0) or None,
        user_pool_size=_int(env, "PRODUCER_USER_POOL_SIZE", minimum=1),
        seed=_int(env, "PRODUCER_SEED", minimum=None) if seed else None,
        status_interval_sec=_int(env, "PRODUCER_STATUS_INTERVAL_SEC", minimum=1),
    )
