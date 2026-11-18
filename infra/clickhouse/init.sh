#!/bin/bash
# ClickHouse 계정 → 스키마 → 권한 순서로 적용한다. `up`마다 다시 실행해도 결과가 같다.
# 계정은 CREATE USER OR REPLACE로 만들어 .env의 비밀번호 변경을 반영하고, 권한은 매번 다시 부여한다.
# 필요 환경변수: CLICKHOUSE_USER, CLICKHOUSE_PASSWORD(관리자),
#   CLICKHOUSE_SPARK_PASSWORD, CLICKHOUSE_GENERATOR_PASSWORD, CLICKHOUSE_GRAFANA_PASSWORD (compose.yaml이 주입한다.)
# 근거: docs/prd/04-clickhouse-storage.md '계정과 권한', 'DDL 관리'
set -euo pipefail

# 관리자 계정으로 SQL을 실행한다. 여러 문은 --multiquery로 한 번에 보낸다.
run() {
  clickhouse-client --host clickhouse --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery "$@"
}

# 비밀번호를 SQL 문자열 리터럴로 쓸 수 있게 작은따옴표·역슬래시를 이스케이프한다.
quote() {
  local value="${1//\\/\\\\}"
  printf "'%s'" "${value//\'/\\\'}"
}

# 1. 계정: Gold 뷰의 DEFINER가 스키마 생성 전에 있어야 한다.
run --query "
CREATE USER OR REPLACE spark_writer IDENTIFIED WITH sha256_password BY $(quote "$CLICKHOUSE_SPARK_PASSWORD");
CREATE USER OR REPLACE generator_writer IDENTIFIED WITH sha256_password BY $(quote "$CLICKHOUSE_GENERATOR_PASSWORD");
CREATE USER OR REPLACE grafana_reader IDENTIFIED WITH sha256_password BY $(quote "$CLICKHOUSE_GRAFANA_PASSWORD")
  SETTINGS readonly = 1, max_execution_time CHANGEABLE_IN_READONLY;
CREATE USER OR REPLACE gold_definer HOST NONE;
"

# 2. 스키마
run --queries-file /scripts/schema.sql

# 3. 권한: Spark JDBC는 append 전에 SELECT ... WHERE 1=0으로 테이블을 확인하므로 SELECT도 준다.
run --query "
GRANT INSERT, SELECT ON silver.order_events TO spark_writer;
GRANT INSERT, SELECT ON ops.spark_batch_attempts TO spark_writer;
GRANT INSERT, SELECT ON ops.spark_query_progress TO spark_writer;
GRANT INSERT ON ops.producer_stats TO generator_writer;
GRANT SELECT ON gold.* TO grafana_reader;
GRANT SELECT ON ops.* TO grafana_reader;
GRANT SELECT ON system.query_log TO grafana_reader;
GRANT SELECT ON system.part_log TO grafana_reader;
GRANT SELECT ON silver.order_events TO gold_definer;
"

echo "clickhouse-init: schema and accounts applied"
