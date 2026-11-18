"""정상 주문은 ClickHouse에 JDBC로, 불량 레코드는 Kafka DLQ에 기록한다."""

from pyspark.sql import DataFrame

from streaming.config import Config

CLICKHOUSE_USER = "spark_writer"  # 계정 이름은 infra/clickhouse/init.sh에 고정한다.
ORDERS_TABLE = "silver.order_events"


def write_orders(orders: DataFrame, config: Config) -> None:
    """정상 주문을 Spark JDBC writer로 append한다. 근거: ClickHouse PRD 'JDBC 쓰기 설정'.

    쓰기 파티션마다 연결 1개를 열고 batchsize 행마다 INSERT 1회를 보낸다. 실패하면 예외가 그대로 올라간다.
    """
    (orders.write.format("jdbc").mode("append")
     .option("url", config.clickhouse_jdbc_url)
     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
     .option("dbtable", ORDERS_TABLE)
     .option("user", CLICKHOUSE_USER)
     .option("password", config.clickhouse_password)
     .option("batchsize", config.jdbc_batch_size)
     .option("numPartitions", config.jdbc_num_partitions)
     .save())


def write_dlq(records: DataFrame, config: Config) -> None:
    """DLQ 토픽에 기록한다. 실패하면 예외가 그대로 올라가 배치가 성공 처리되지 않는다."""
    (records.write.format("kafka")
     .option("kafka.bootstrap.servers", config.bootstrap_servers)
     .option("kafka.acks", "all")
     .option("topic", config.dlq_topic)
     .save())
