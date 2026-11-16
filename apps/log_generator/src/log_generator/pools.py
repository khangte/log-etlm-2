"""주문 이벤트용 합성 데이터 풀. 가중치는 실제 통계가 아닌 검증용 백분율이다.

사용자 풀은 실행마다 한 번 생성하고 재사용한다.
값 선택에는 호출자가 관리하는 random.Random 인스턴스를 사용한다:
    rng.choice(user_ids)
    rng.choices(PRODUCTS, weights=PRODUCT_WEIGHTS, k=1)[0]
"""

from typing import NamedTuple


class Product(NamedTuple):
    """같은 상품 ID에 대응하는 이름·카테고리·가격을 묶는다."""

    product_id: str
    product_name: str
    category: str
    price: int


class Location(NamedTuple):
    """국가·지역·도시를 묶어 선택 시 잘못된 조합을 방지한다."""

    country: str
    region: str
    city: str


def build_user_pool(size: int) -> tuple[str, ...]:
    """실제 회원과 연결되지 않는 합성 사용자 ID 목록을 반환한다. size는 config에서 양의 정수로 검증된다."""
    return tuple(f"usr_{index:05d}" for index in range(1, size + 1))


PRODUCTS = (
    Product("prod_0001", "USB-C Cable", "electronics", 15000),
    Product("prod_0002", "Wireless Mouse", "electronics", 30000),
    Product("prod_0003", "Keyboard", "electronics", 70000),
    Product("prod_0004", "Earbuds", "electronics", 90000),
    Product("prod_0005", "T-Shirt", "fashion", 20000),
    Product("prod_0006", "Cap", "fashion", 25000),
    Product("prod_0007", "Socks", "fashion", 5000),
    Product("prod_0008", "Mug", "home", 10000),
    Product("prod_0009", "Towel", "home", 8000),
    Product("prod_0010", "Desk Lamp", "home", 40000),
)
PRODUCT_WEIGHTS = (25, 20, 15, 10, 8, 7, 5, 4, 3, 3)

LOCATIONS = (
    Location("KR", "Seoul", "Geumcheon-gu"),
    Location("KR", "Seoul", "Gwanak-gu"),
    Location("KR", "Gyeonggi", "Gwangmyeong"),
    Location("KR", "Gyeonggi", "Anyang"),
    Location("KR", "Incheon", "Yeonsu-gu"),
    Location("KR", "Busan", "Haeundae-gu"),
    Location("KR", "Chungcheongnam-do", "Cheonan-si")
)
LOCATION_WEIGHTS = (20, 20, 15, 15, 15, 10, 5)

PAYMENT_METHODS = ("credit_card", "kakao_pay", "naver_pay", "bank_transfer")
PAYMENT_WEIGHTS = (45, 30, 15, 10)
USER_TIERS = ("STANDARD", "VIP")
USER_TIER_WEIGHTS = (90, 10)
EVENT_KINDS = ("normal_order", "health_check", "invalid_order")
EVENT_WEIGHTS = (85, 10, 5)
ANOMALY_KINDS = ("malformed_json", "missing_order_id", "invalid_amount_type")
QUANTITIES = (1, 2, 3)
DISCOUNT_RATES = (0, 10)
