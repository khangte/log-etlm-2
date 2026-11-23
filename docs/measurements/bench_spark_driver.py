"""Spark 배치 콜백의 driver 쪽 시간(job이 돌지 않는 구간) 분해 벤치(PRD 03 'Spark 튜닝').

운영 배치와 같은 크기의 고정 offset 구간을 batch 모드로 읽고 분류 결과를 캐시한 뒤, 출력 action 앞에서
driver가 쓰는 시간을 단계별로 잰다. ClickHouse에는 연결·메타데이터 조회만 하고 쓰지 않는다.

사용법: spark-submit --master local[3] --driver-memory 1536m bench_spark_driver.py <ends_json> <per_partition>
  ends_json: {"0": 끝 offset, ...}. 각 파티션의 [끝 - per_partition, 끝) 구간을 읽는다.
"""

import json
import statistics
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming.batch import with_spark_columns
from streaming.config import load_config
from streaming.monitoring import summarize_batch
from streaming.schemas import ORDER_EVENT_SCHEMA
from streaming.sinks import CLICKHOUSE_USER, ORDERS_TABLE
from streaming.transforms import ORDER, build_dlq_records, classify_batch, deduplicate_events, select_orders

TOPIC, WARMUP, REPEAT = "raw.ecommerce.orders.v1", 3, 7
ENDS, PER_PART = json.loads(sys.argv[1]), int(sys.argv[2])
STARTS = {p: e - PER_PART for p, e in ENDS.items()}
config = load_config()
spark = (SparkSession.builder.appName("bench-driver").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions)).getOrCreate())
spark.sparkContext.setLogLevel("WARN")
jvm = spark.sparkContext._jvm


def ms(fn) -> float:
    """fn 실행 시간을 ms로 돌려준다."""
    t = time.perf_counter()
    fn()
    return (time.perf_counter() - t) * 1000


def jdbc_connect_and_check() -> None:
    """Spark JDBC append 앞 단계처럼 연결을 열고 테이블 존재·스키마 조회(WHERE 1=0)를 한 뒤 닫는다."""
    props = jvm.java.util.Properties()
    props.setProperty("user", CLICKHOUSE_USER)
    props.setProperty("password", config.clickhouse_password)
    jvm.java.lang.Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    conn = jvm.java.sql.DriverManager.getConnection(config.clickhouse_jdbc_url, props)
    try:
        for sql in (f"SELECT 1 FROM {ORDERS_TABLE} WHERE 1=0", f"SELECT * FROM {ORDERS_TABLE} WHERE 1=0"):
            stmt = conn.createStatement()
            stmt.executeQuery(sql).getMetaData().getColumnCount()
            stmt.close()
    finally:
        conn.close()


def one_round() -> dict:
    """한 배치 분량으로 driver 쪽 단계 시간을 잰다."""
    source = (spark.read.format("kafka").option("kafka.bootstrap.servers", config.bootstrap_servers)
              .option("subscribe", TOPIC).option("startingOffsets", json.dumps({TOPIC: STARTS}))
              .option("endingOffsets", json.dumps({TOPIC: ENDS})).load())
    started = datetime.now(timezone.utc)
    r = {}
    holder = {}
    r["classify_build"] = ms(lambda: holder.update(c=classify_batch(source, started).persist()))
    classified = holder["c"]
    # 분류 구성 세부: 식 구성(classify_batch)과 캐시 등록(persist)을 나눈다. 같은 식을 새로 만들어 캐시 적중을 피한다.
    split_started = datetime.now(timezone.utc)
    r["classify_build_expr"] = ms(lambda: holder.update(e=classify_batch(source, split_started)))
    r["classify_build_persist"] = ms(lambda: holder["e"].persist())
    holder["e"].unpersist()
    r["summarize_action"] = ms(lambda: summarize_batch(classified, started, True))
    metrics = {"query_id": "bench", "run_id": "bench", "batch_id": 0}
    r["orders_build"] = ms(lambda: holder.update(o=with_spark_columns(deduplicate_events(select_orders(classified)),
                                                                      metrics, started)))
    # 정상 출력 구성 세부: 필터+from_json, 컬럼 선택, 배치 내 중복 제거, 추적 컬럼
    r["orders_build_filter_json"] = ms(lambda: holder.update(f=classified.filter(F.col("result.outcome") == ORDER).withColumn(
        "event", F.from_json(F.col("raw_value").cast("string"), ORDER_EVENT_SCHEMA, {"mode": "FAILFAST"}))))
    r["orders_build_select_orders"] = ms(lambda: holder.update(s=select_orders(classified)))
    r["orders_build_dedup"] = ms(lambda: holder.update(dd=deduplicate_events(holder["s"])))
    r["orders_build_spark_columns"] = ms(lambda: with_spark_columns(holder["dd"], metrics, started))
    qe = holder["o"]._jdf.queryExecution()
    r["orders_analyze"] = ms(lambda: qe.analyzed())
    r["orders_optimize"] = ms(lambda: qe.optimizedPlan())
    r["orders_physical"] = ms(lambda: qe.executedPlan())
    r["jdbc_connect_check"] = ms(jdbc_connect_and_check)
    r["dlq_build"] = ms(lambda: holder.update(d=build_dlq_records(classified)))
    dqe = holder["d"]._jdf.queryExecution()
    r["dlq_plan"] = ms(lambda: dqe.executedPlan())
    classified.unpersist()
    return r


for _ in range(WARMUP):
    one_round()
rounds = [one_round() for _ in range(REPEAT)]
print(json.dumps({"per_partition": PER_PART, "repeat": REPEAT,
                  "median_ms": {k: round(statistics.median(x[k] for x in rounds), 1) for k in rounds[0]},
                  "rounds": rounds}, ensure_ascii=False, indent=1))
spark.stop()
