-- ClickHouse 스키마: 저장 계층(silver·gold·ops)과 테이블·뷰.
-- 근거: docs/prd/04-clickhouse-storage.md (저장 계층, 컬럼 타입, 압축 코덱, 중복 처리, 운영 지표 저장)
-- init.sh가 계정(gold_definer 포함)을 만든 뒤 실행한다. 모든 문은 IF NOT EXISTS라 `up`마다 다시 실행해도 결과가 같다.
-- IF NOT EXISTS는 기존 테이블의 컬럼·설정을 바꾸지 않는다. 변경은 아래에 ALTER 문을 추가하거나 볼륨을 다시 만든다.

CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS ops;

-- 정상 주문 이벤트. 이벤트당 1행이며 재시도 중복 행은 조회 시 FINAL로 합친다.
-- 컬럼 순서는 Spark 평탄화 매핑(streaming.schemas.ORDER_COLUMNS) → Kafka 추적 4개 → Spark 식별 4개 → 서버 적재 시각.
CREATE TABLE IF NOT EXISTS silver.order_events
(
    event_id               String                                  CODEC(ZSTD(1)),
    event_timestamp        DateTime64(3, 'UTC')                    CODEC(Delta, ZSTD(1)),
    event_name             LowCardinality(String)                  CODEC(ZSTD(1)),
    schema_version         LowCardinality(String)                  CODEC(ZSTD(1)),
    user_id                String                                  CODEC(ZSTD(1)),
    session_id             String                                  CODEC(ZSTD(1)),
    is_logged_in           Bool                                    CODEC(ZSTD(1)),
    user_tier              LowCardinality(String)                  CODEC(ZSTD(1)),
    os                     LowCardinality(String)                  CODEC(ZSTD(1)),
    os_version             LowCardinality(String)                  CODEC(ZSTD(1)),
    app_version            LowCardinality(String)                  CODEC(ZSTD(1)),
    device_model           LowCardinality(String)                  CODEC(ZSTD(1)),
    device_type            LowCardinality(String)                  CODEC(ZSTD(1)),
    carrier                LowCardinality(String)                  CODEC(ZSTD(1)),
    country                LowCardinality(String)                  CODEC(ZSTD(1)),
    region                 LowCardinality(String)                  CODEC(ZSTD(1)),
    city                   LowCardinality(String)                  CODEC(ZSTD(1)),
    current_url            String                                  CODEC(ZSTD(3)),
    referrer_url           String                                  CODEC(ZSTD(3)),
    utm_source             LowCardinality(String)                  CODEC(ZSTD(1)),
    utm_medium             LowCardinality(String)                  CODEC(ZSTD(1)),
    order_id               String                                  CODEC(ZSTD(1)),
    payment_method         LowCardinality(String)                  CODEC(ZSTD(1)),
    currency               LowCardinality(String)                  CODEC(ZSTD(1)),
    total_amount           Int64                                   CODEC(T64, ZSTD(1)),
    discount_amount        Int64                                   CODEC(T64, ZSTD(1)),
    product_id             LowCardinality(String)                  CODEC(ZSTD(1)),
    product_name           LowCardinality(String)                  CODEC(ZSTD(1)),
    category               LowCardinality(String)                  CODEC(ZSTD(1)),
    price                  Int64                                   CODEC(T64, ZSTD(1)),
    quantity               Int64                                   CODEC(T64, ZSTD(1)),
    kafka_topic            LowCardinality(String)                  CODEC(ZSTD(1)),
    kafka_partition        Int32                                   CODEC(T64, ZSTD(1)),
    kafka_offset           Int64                                   CODEC(T64, ZSTD(1)),
    kafka_append_timestamp DateTime64(3, 'UTC')                    CODEC(Delta, ZSTD(1)),
    spark_query_id         LowCardinality(String)                  CODEC(ZSTD(1)),
    spark_run_id           LowCardinality(String)                  CODEC(ZSTD(1)),
    spark_batch_id         Int64                                   CODEC(T64, ZSTD(1)),
    spark_processed_at     DateTime64(3, 'UTC')                    CODEC(Delta, ZSTD(1)),
    ingested_at            DateTime64(3, 'UTC') DEFAULT now64(3)   CODEC(Delta, ZSTD(1))
)
ENGINE = ReplacingMergeTree()
-- TTL 24시간은 Spark streaming.transforms.EVENT_TIME_MAX_AGE와 같아야 한다(PRD 03 '이벤트 시각 범위 규칙').
PARTITION BY toStartOfHour(event_timestamp)
ORDER BY (event_timestamp, event_id)
TTL toDateTime(event_timestamp) + INTERVAL 24 HOUR
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600;

-- 대시보드·정확성 검증용 중복 제거 조회 경로. 조회에는 항상 event_timestamp 범위 조건을 붙인다.
CREATE VIEW IF NOT EXISTS gold.orders_deduplicated
DEFINER = gold_definer SQL SECURITY DEFINER AS
SELECT * FROM silver.order_events FINAL;

-- 이벤트 단위 지연(생성 → Kafka, Kafka → Spark) 조회 경로. 재처리 시도 행을 합치지 않으므로 FINAL을 쓰지 않는다.
CREATE VIEW IF NOT EXISTS gold.order_event_latency
DEFINER = gold_definer SQL SECURITY DEFINER AS
SELECT event_id, event_timestamp, kafka_append_timestamp, spark_processed_at, spark_run_id, spark_batch_id
FROM silver.order_events;

-- 운영 지표: 원천별 테이블 3개. 분위수 건수가 0이면 측정 불가이며 분위수 값은 0으로 둔다.

-- 로그 생성기 1초 구간당 1행.
CREATE TABLE IF NOT EXISTS ops.producer_stats
(
    interval_start       DateTime64(3, 'UTC'),
    run_id               LowCardinality(String),
    target_eps           UInt32,
    generated            UInt64,
    acked                UInt64,
    failed               UInt64,
    undelivered          UInt64,
    queue_len_max        UInt64,
    ack_latency_ms_p50   Float64,
    ack_latency_ms_p95   Float64,
    ack_latency_ms_p99   Float64,
    ack_latency_ms_count UInt64,
    gen_to_kafka_ms_p50  Float64,
    gen_to_kafka_ms_p95  Float64,
    gen_to_kafka_ms_p99  Float64,
    gen_to_kafka_ms_count UInt64,
    gen_serialize_us_p50 Float64,
    gen_serialize_us_p95 Float64,
    gen_serialize_us_p99 Float64,
    gen_serialize_us_count UInt64
)
ENGINE = MergeTree
ORDER BY interval_start
TTL toDateTime(interval_start) + INTERVAL 24 HOUR;

-- foreachBatch 콜백 시도당 1행. 실패한 시도 포함. 실행하지 않은 action 시간은 0이며 status·failed_stage로 구분한다.
CREATE TABLE IF NOT EXISTS ops.spark_batch_attempts
(
    query_id                  LowCardinality(String),
    run_id                    LowCardinality(String),
    batch_id                  Int64,
    callback_started_at       DateTime64(3, 'UTC'),
    status                    LowCardinality(String),
    failed_stage              LowCardinality(String),
    input_rows                UInt64,
    order_rows                UInt64,
    excluded_rows             UInt64,
    dlq_rows                  UInt64,
    classification_ms         Float64,
    orders_write_ms           Float64,
    dlq_write_ms              Float64,
    callback_total_ms         Float64,
    kafka_to_callback_ms_p50  Float64,
    kafka_to_callback_ms_p95  Float64,
    kafka_to_callback_ms_p99  Float64,
    kafka_to_callback_ms_max  Float64,
    kafka_to_callback_ms_count UInt64,
    kafka_ts_missing_count    UInt64,
    kafka_ts_future_count     UInt64
)
ENGINE = MergeTree
ORDER BY callback_started_at
TTL toDateTime(callback_started_at) + INTERVAL 24 HOUR;

-- onQueryProgress 배치당 1행. backlog는 조회 시 latest_offsets − end_offsets로 계산한다.
CREATE TABLE IF NOT EXISTS ops.spark_query_progress
(
    query_id               LowCardinality(String),
    run_id                 LowCardinality(String),
    batch_id               Int64,
    trigger_at             DateTime64(3, 'UTC'),
    trigger_execution_ms   UInt64,
    add_batch_ms           UInt64,
    latest_offset_ms       UInt64,
    query_planning_ms      UInt64,
    wal_commit_ms          UInt64,
    commit_offsets_ms      UInt64,
    num_input_rows         UInt64,
    input_rows_per_sec     Float64,
    processed_rows_per_sec Float64,
    end_offsets            Map(Int32, Int64),
    latest_offsets         Map(Int32, Int64)
)
ENGINE = MergeTree
ORDER BY trigger_at
TTL toDateTime(trigger_at) + INTERVAL 24 HOUR;
