"""Spark 배치 단계별 비용 분해 벤치(PRD 03 'Spark 튜닝').

운영 배치와 같은 크기의 고정 offset 구간을 batch 모드로 읽어, 분류·정상 변환·DLQ 변환을 noop 출력으로 측정한다.
JDBC·Kafka 쓰기 시간은 포함하지 않는다. 스트리밍 컨테이너를 멈추고 같은 이미지의 1회성 컨테이너에서 실행한다.

사용법: spark-submit --master local[3] --driver-memory 2g bench_spark_stages.py <ends_json> <per_partition> <mode>
  ends_json: {"0": 끝 offset, ...}. 각 파티션의 [끝 - per_partition, 끝) 구간을 읽는다.
  mode: breakdown(단계 분해) | pickle | arrow(운영 경로만 반복). 변형 비교는 셸에서 컨테이너를 번갈아 실행한다.
"""

import json
import statistics
import sys
import time
from datetime import timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming import transforms as T
from streaming.batch import with_spark_columns
from streaming.monitoring import summarize_batch
from streaming.schemas import CLASSIFICATION_SCHEMA

TOPIC, WARMUP, REPEAT = "raw.ecommerce.orders.v1", 3, 5
ENDS, PER_PART, MODE = json.loads(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
STARTS = {p: e - PER_PART for p, e in ENDS.items()}

spark = (SparkSession.builder.appName("bench").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "3").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def source():
    """고정 구간 Kafka 소스 DataFrame을 만든다."""
    return (spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:19092").option("subscribe", TOPIC)
            .option("startingOffsets", json.dumps({TOPIC: STARTS}))
            .option("endingOffsets", json.dumps({TOPIC: ENDS})).load())


def noop(df):
    """모든 컬럼을 계산하고 버린다."""
    df.write.format("noop").mode("overwrite").save()


def run(name, fn):
    """JIT 워밍업 WARMUP회 후 fn을 REPEAT회 실행해 중앙값(ms)을 출력한다."""
    for _ in range(WARMUP):
        fn()
    times = []
    for _ in range(REPEAT):
        clock = time.perf_counter()
        fn()
        times.append((time.perf_counter() - clock) * 1000)
    print(f"BENCH {MODE} {name} median={statistics.median(times):.0f}ms all={[round(x) for x in times]}", flush=True)


def production(udf):
    """운영 경로(쓰기 제외): 분류+persist+건수·지연 집계, 정상 변환, DLQ 변환. 단계별 시간도 출력한다."""
    T.classify_udf = udf
    classified = T.classify_batch(source(), REF).persist()
    t0 = time.perf_counter()
    counts, _ = summarize_batch(classified, REF, True)
    t1 = time.perf_counter()
    noop(with_spark_columns(T.deduplicate_events(T.select_orders(classified)), {"batch_id": 0}, REF))
    t2 = time.perf_counter()
    noop(T.build_dlq_records(classified))
    t3 = time.perf_counter()
    classified.unpersist()
    print(f"  stages classify={1000 * (t1 - t0):.0f} orders={1000 * (t2 - t1):.0f} dlq={1000 * (t3 - t2):.0f}"
          f" counts={counts}", flush=True)


# 이벤트 시각 범위 규칙이 과거 구간을 DLQ로 바꾸지 않도록 기준 시각을 구간 마지막 append 시각 + 1초로 둔다.
REF = source().agg(F.max("timestamp")).first()[0].replace(tzinfo=timezone.utc) + timedelta(seconds=1)
PICKLE_UDF = F.udf(T.classify, CLASSIFICATION_SCHEMA, useArrow=False)
print("rows", source().count(), "ref", REF, flush=True)

if MODE == "breakdown":
    run("read_only", lambda: noop(source().select("value", "timestamp")))
    run("udf_pickle_only", lambda: noop(source().select(PICKLE_UDF("value"))))
    run("from_json_only", lambda: noop(source().select(
        F.from_json(F.col("value").cast("string"), T.ORDER_EVENT_SCHEMA).alias("e")).select("e.*")))
    run("production", lambda: production(PICKLE_UDF))
else:
    udf = F.udf(T.classify, CLASSIFICATION_SCHEMA, useArrow=MODE == "arrow")
    run("udf_only", lambda: noop(source().select(udf("value"))))
    run("production", lambda: production(udf))
spark.stop()
