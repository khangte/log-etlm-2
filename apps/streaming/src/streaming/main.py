"""설정과 Spark 세션·Kafka 소스를 연결하고 스트리밍 쿼리를 시작한다.

실행: spark-submit --master ... --driver-memory ... main.py (Compose의 spark 서비스)."""

import logging
from functools import partial

from pyspark.sql import SparkSession

from streaming.batch import process_batch
from streaming.config import Config, load_config
from streaming.monitoring import ProgressLogger
from streaming.sources import read_kafka


def build_spark(config: Config) -> SparkSession:
    """세션 시간대 UTC·셔플 파티션 수를 고정한 세션을 만든다. master·driver 메모리는 spark-submit 인자로 정한다."""
    spark = (
        SparkSession.builder.appName("orders-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    """설정을 읽고 스트리밍 쿼리를 시작해 종료까지 기다린다. 쿼리 실패 시 예외로 비정상 종료한다."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config()
    spark = build_spark(config)
    query_context = {}
    listener = ProgressLogger(query_context, config)
    query = None
    try:
        spark.streams.addListener(listener)
        source = read_kafka(spark, config)
        query = (
            source.writeStream
            .queryName("orders-stream")
            .outputMode("append")
            .foreachBatch(partial(process_batch, config=config, query_context=query_context))
            .option("checkpointLocation", config.checkpoint_dir)
            .trigger(processingTime=config.trigger_interval)
            .start()
        )
        query.awaitTermination()
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            try:
                spark.streams.removeListener(listener)
            finally:
                spark.stop()


if __name__ == "__main__":
    main()
