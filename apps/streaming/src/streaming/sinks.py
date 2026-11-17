"""정상 주문은 Parquet에, 불량 레코드는 Kafka DLQ에 기록한다."""

from pyspark.sql import DataFrame

from streaming.config import Config


def write_orders(orders: DataFrame, config: Config) -> None:
    """3단계 정상 출력: Parquet append. 4단계에서 이 함수만 Spark JDBC writer로 교체한다."""
    orders.write.mode("append").parquet(config.output_dir)


def write_dlq(records: DataFrame, config: Config) -> None:
    """DLQ 토픽에 기록한다. 실패하면 예외가 그대로 올라가 배치가 성공 처리되지 않는다."""
    (records.write.format("kafka")
     .option("kafka.bootstrap.servers", config.bootstrap_servers)
     .option("kafka.acks", "all")
     .option("topic", config.dlq_topic)
     .save())
