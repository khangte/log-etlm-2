"""원본 주문 JSON의 중첩 구조·타입, 분류 메타데이터의 Spark 스키마와 정상 주문 평탄화 매핑을 정의한다.

contracts/schemas/order_completed.1.0.0.schema.json의 필드와 대응한다.
required·pattern·enum·추가 필드 금지 등 계약 제약은 transforms.py에서 별도로 검증한다.
"""

from pyspark.sql.types import ArrayType, BooleanType, LongType, StringType, StructField, StructType

# 원본 시각은 문자열로 파싱하고 transforms.py에서 UTC TimestampType으로 변환한다.
ORDER_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("event_name", StringType()),
    StructField("schema_version", StringType()),
    StructField("context", StructType([
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("is_logged_in", BooleanType()),
        StructField("user_tier", StringType()),
        StructField("device", StructType([
            StructField(name, StringType())
            for name in ("os", "os_version", "app_version", "device_model", "device_type")
        ])),
        StructField("network", StructType([
            StructField(name, StringType()) for name in ("carrier", "country", "region", "city")
        ])),
        StructField("page", StructType([
            StructField(name, StringType())
            for name in ("current_url", "referrer_url", "utm_source", "utm_medium")
        ])),
    ])),
    StructField("properties", StructType([
        StructField("order_id", StringType()),
        StructField("payment_method", StringType()),
        StructField("currency", StringType()),
        StructField("total_amount", LongType()),
        StructField("discount_amount", LongType()),
        StructField("items", ArrayType(StructType([
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("price", LongType()),
            StructField("quantity", LongType()),
        ]))),
    ])),
])

# UDF는 검증 결과만 전달한다. 정상 주문 데이터는 from_json으로 구조화한다.
CLASSIFICATION_SCHEMA = StructType([
    StructField(name, StringType()) for name in (
        "outcome", "event_name", "schema_version", "error_stage", "error_code", "error_path", "error_message")
])

# 정상 주문 평탄화 매핑(PRD 표 순서). 출력 컬럼 → 원본 JSON 경로.
ORDER_COLUMNS = {
    "event_id": ("event_id",),
    "event_timestamp": ("event_timestamp",),
    "event_name": ("event_name",),
    "schema_version": ("schema_version",),
    "user_id": ("context", "user_id"),
    "session_id": ("context", "session_id"),
    "is_logged_in": ("context", "is_logged_in"),
    "user_tier": ("context", "user_tier"),
    "os": ("context", "device", "os"),
    "os_version": ("context", "device", "os_version"),
    "app_version": ("context", "device", "app_version"),
    "device_model": ("context", "device", "device_model"),
    "device_type": ("context", "device", "device_type"),
    "carrier": ("context", "network", "carrier"),
    "country": ("context", "network", "country"),
    "region": ("context", "network", "region"),
    "city": ("context", "network", "city"),
    "current_url": ("context", "page", "current_url"),
    "referrer_url": ("context", "page", "referrer_url"),
    "utm_source": ("context", "page", "utm_source"),
    "utm_medium": ("context", "page", "utm_medium"),
    "order_id": ("properties", "order_id"),
    "payment_method": ("properties", "payment_method"),
    "currency": ("properties", "currency"),
    "total_amount": ("properties", "total_amount"),
    "discount_amount": ("properties", "discount_amount"),
    "product_id": ("properties", "items", 0, "product_id"),
    "product_name": ("properties", "items", 0, "product_name"),
    "category": ("properties", "items", 0, "category"),
    "price": ("properties", "items", 0, "price"),
    "quantity": ("properties", "items", 0, "quantity"),
}
