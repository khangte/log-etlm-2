# 공식 Spark 이미지(Spark 4.2.1, Scala 2.13, Java 17, Python 3.10)에 Kafka 커넥터·ClickHouse JDBC Driver와 검증·테스트 패키지를 더한다.
# 앱 코드·계약·풀은 Compose가 읽기 전용으로 마운트하므로 이미지에 복사하지 않는다.
FROM apache/spark:4.2.1-scala2.13-java17-python3-ubuntu

USER root

# spark-sql-kafka POM의 compile 의존성. lz4·snappy·zstd·slf4j는 Spark 기본 jars에 있다.
# 실행 시 --packages 해석(네트워크)을 하지 않도록 빌드 시 받아 둔다.
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.1/spark-sql-kafka-0-10_2.13-4.2.1.jar \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.1/spark-token-provider-kafka-0-10_2.13-4.2.1.jar \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar \
    ${MAVEN}/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar \
    /opt/spark/jars/

# ClickHouse 공식 JDBC Driver. all classifier는 의존성을 포함한 단일 jar다(ClickHouse PRD '버전').
ADD --chmod=644 ${MAVEN}/com/clickhouse/clickhouse-jdbc/0.11.2/clickhouse-jdbc-0.11.2-all.jar /opt/spark/jars/

# rfc3339-validator가 없으면 jsonschema가 format: date-time을 검사하지 않는다.
# 버전은 pyproject.toml과 같게 유지한다.
RUN pip3 install --no-cache-dir jsonschema==4.26.0 jsonschema-rs==0.61.2 rfc3339-validator==0.1.4 pytest==9.2.0

# Spark 4.2의 base64()는 기본값으로 76자마다 CRLF를 넣는다. DLQ raw_value_base64를 표준 Base64로 만들기 위해
# 컨테이너의 모든 세션(spark-submit 잡·pytest 세션)이 읽는 spark-defaults.conf에서 끈다.
RUN mkdir -p /opt/spark/conf && echo "spark.sql.chunkBase64String.enabled false" > /opt/spark/conf/spark-defaults.conf

# named volume은 처음 마운트될 때 이미지 디렉터리의 소유자를 따른다. spark 사용자가 쓸 수 있게 미리 만든다.
RUN mkdir -p /opt/spark/checkpoints && chown spark:spark /opt/spark/checkpoints

USER spark
