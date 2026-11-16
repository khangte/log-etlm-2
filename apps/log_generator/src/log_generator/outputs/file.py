"""기존 파일을 덮어쓰지 않고 바이너리 로그 파일을 연다."""

from pathlib import Path
from typing import BinaryIO


def open_file(path: Path) -> BinaryIO:
    """기존과 같은 1MB 버퍼와 배타적 생성 모드를 사용한다."""
    return open(path, "xb", buffering=1024 * 1024)
