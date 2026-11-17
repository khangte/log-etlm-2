"""테스트 공용 헬퍼: 표준 샘플 로더와 주문 변형 도구."""

import copy
import json
from pathlib import Path

import pytest

SAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "samples"


def sample_bytes(name: str) -> bytes:
    """표준 샘플 파일의 페이로드 bytes(끝 LF 제외)를 읽는다."""
    return (SAMPLES / name).read_bytes().rstrip(b"\n")


def dump(doc) -> bytes:
    """문서를 공백 없는 UTF-8 JSON bytes로 직렬화한다."""
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


@pytest.fixture
def order_doc() -> dict:
    """표준 정상 주문 샘플을 dict로 돌려준다. 호출마다 새 복사본이다."""
    return json.loads(sample_bytes("valid_order_completed.json"))


@pytest.fixture
def mutate(order_doc):
    """표준 주문에 변경을 적용해 bytes로 직렬화하는 함수를 돌려준다.

    사용: mutate(lambda d: d["properties"].pop("order_id"))
    """

    def _apply(fn) -> bytes:
        doc = copy.deepcopy(order_doc)
        fn(doc)
        return dump(doc)

    return _apply
