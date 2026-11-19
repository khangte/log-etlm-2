"""정상 주문·health_check·불량 주문 로그를 직렬화된 UTF-8 bytes로 생성한다.

계약: contracts/schemas/*.1.0.0.schema.json. 무작위 선택은 주입된 random.Random만 사용하므로
seed로 선택을 재현할 수 있다. UUID와 현재 시각은 재현 대상이 아니다.
"""

import json
import random
import uuid
from datetime import datetime, timezone
from itertools import accumulate
from typing import NamedTuple

from log_generator import pools

SCHEMA_VERSION = "1.0.0"

# choices(weights=...)는 호출마다 누적합을 다시 만든다. 한 번만 계산해 재사용하며 선택 결과는 동일하다.
EVENT_CUM_WEIGHTS = tuple(accumulate(pools.EVENT_WEIGHTS))
LOCATION_CUM_WEIGHTS = tuple(accumulate(pools.LOCATION_WEIGHTS))
PRODUCT_CUM_WEIGHTS = tuple(accumulate(pools.PRODUCT_WEIGHTS))
USER_TIER_CUM_WEIGHTS = tuple(accumulate(pools.USER_TIER_WEIGHTS))
PAYMENT_CUM_WEIGHTS = tuple(accumulate(pools.PAYMENT_WEIGHTS))

# 기기·유입 정보는 1단계에서 고정 합성값이다. 직렬화만 하고 변경하지 않으므로 이벤트 간 공유한다.
DEVICE = {
    "os": "iOS",
    "os_version": "18.1.1",
    "app_version": "4.12.0",
    "device_model": "iPhone16,1",
    "device_type": "mobile",
}
PAGE = {
    "current_url": "/checkout/complete",
    "referrer_url": "/cart",
    "utm_source": "direct",
    "utm_medium": "none",
}


class GeneratedEvent(NamedTuple):
    """kind는 pools.EVENT_KINDS, anomaly는 불량일 때만 pools.ANOMALY_KINDS 중 하나다.

    event_timestamp는 payload에 넣은 문자열 그대로다. 생성 → Kafka 지연 계산용이며
    JSON으로 해석할 수 없는 payload(malformed_json)는 None이다.
    """

    kind: str
    anomaly: str | None
    payload: bytes
    event_timestamp: str | None


def serialize(obj: dict) -> bytes:
    """공백 없는 한 줄 JSON, 한글은 이스케이프 없이 UTF-8로 인코딩한다."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def utc_timestamp() -> str:
    """밀리초 고정 UTC 시각. 예: 2026-11-16T06:08:00.123Z"""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class EventGenerator:
    def __init__(self, rng: random.Random, user_pool: tuple[str, ...]):
        self.rng = rng
        self.user_pool = user_pool

    def next_event(self) -> GeneratedEvent:
        """설정 비율에 따라 이벤트 유형을 고르고 생성한다."""
        kind = self.rng.choices(pools.EVENT_KINDS, cum_weights=EVENT_CUM_WEIGHTS)[0]
        if kind == "normal_order":
            doc = self.order()
            return GeneratedEvent(kind, None, serialize(doc), doc["event_timestamp"])
        if kind == "health_check":
            doc = self.health_check()
            return GeneratedEvent(kind, None, serialize(doc), doc["event_timestamp"])
        return self.invalid_order(self.rng.choice(pools.ANOMALY_KINDS))

    def health_check(self) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": utc_timestamp(),
            "event_name": "health_check",
            "schema_version": SCHEMA_VERSION,
        }

    def order(self) -> dict:
        """모든 주문 제약을 만족하는 정상 주문."""
        rng = self.rng
        event_uuid = uuid.uuid4()
        location = rng.choices(pools.LOCATIONS, cum_weights=LOCATION_CUM_WEIGHTS)[0]
        product = rng.choices(pools.PRODUCTS, cum_weights=PRODUCT_CUM_WEIGHTS)[0]
        quantity = rng.choice(pools.QUANTITIES)
        subtotal = product.price * quantity
        # 가격이 1,000원 단위라 10% 할인도 정수로 떨어진다. 정수 나눗셈으로 float 오차를 피한다.
        discount = subtotal * rng.choice(pools.DISCOUNT_RATES) // 100
        return {
            "event_id": str(event_uuid),
            "event_timestamp": utc_timestamp(),
            "event_name": "order_completed",
            "schema_version": SCHEMA_VERSION,
            "context": {
                "user_id": rng.choice(self.user_pool),
                "session_id": "sess_" + uuid.uuid4().hex,
                "is_logged_in": True,
                "user_tier": rng.choices(pools.USER_TIERS, cum_weights=USER_TIER_CUM_WEIGHTS)[0],
                "device": DEVICE,
                "network": {
                    "carrier": "synthetic",
                    "country": location.country,
                    "region": location.region,
                    "city": location.city,
                },
                "page": PAGE,
            },
            "properties": {
                "order_id": "ord_" + event_uuid.hex,
                "payment_method": rng.choices(pools.PAYMENT_METHODS, cum_weights=PAYMENT_CUM_WEIGHTS)[0],
                "currency": "KRW",
                "total_amount": subtotal - discount,
                "discount_amount": discount,
                "items": [
                    {
                        "product_id": product.product_id,
                        "product_name": product.product_name,
                        "category": product.category,
                        "price": product.price,
                        "quantity": quantity,
                    }
                ],
            },
        }

    def invalid_order(self, anomaly: str) -> GeneratedEvent:
        """정상 주문에 지정한 오류 하나만 주입한다. 테스트에서 유형별 강제 생성에도 사용한다."""
        order = self.order()
        if anomaly == "malformed_json":
            payload = serialize(order)
            # 최종 닫는 중괄호를 '!'로 교체: 길이·UTF-8·한 줄 구조를 유지한 채 파싱만 실패한다.
            return GeneratedEvent("invalid_order", anomaly, payload[:-1] + b"!", None)
        if anomaly == "missing_order_id":
            del order["properties"]["order_id"]
        elif anomaly == "invalid_amount_type":
            order["properties"]["total_amount"] = "invalid_amount"
        else:
            raise ValueError(f"unknown anomaly: {anomaly}")
        return GeneratedEvent("invalid_order", anomaly, serialize(order), order["event_timestamp"])
