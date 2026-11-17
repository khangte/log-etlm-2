"""Kafka 원본 토픽을 공식 Structured Streaming 소스로 연결한다."""

from pyspark.sql import DataFrame, SparkSession

from streaming.config import Config


def read_kafka(spark: SparkSession, config: Config) -> DataFrame:
    """토픽·시작 offset·트리거 입력 상한을 적용하며 offset 소실은 실패로 처리한다."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", config.starting_offsets)
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .option("failOnDataLoss", "true")
        .load()
    )
