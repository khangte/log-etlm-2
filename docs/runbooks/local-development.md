# 로컬 개발 runbook

- 상태: 구현 중
- 적용 범위: 제품 v1 / 로그 생성기 및 Kafka 로컬 실행 환경

명령은 WSL Ubuntu에서 실행한다. 단계가 추가되면 이 문서를 갱신한다.

## 1. 환경 준비

```bash
cp .env.example .env            # 프로젝트 루트. Producer·Compose가 함께 읽는 실행 설정 파일
cd apps/log_generator
uv sync                         # .venv 생성, 실행 의존성(python-dotenv, confluent-kafka)과 dev 의존성(jsonschema, pytest) 포함
```

## 2. 테스트

```bash
cd apps/log_generator
uv run pytest                   # 단위 테스트(브로커 불필요)
uv run python -m unittest discover -s tests -v   # 같은 테스트를 unittest로도 실행 가능
```

## 3. 생성기 실행

프로젝트 루트 `.env`를 자동으로 읽는다. 셸에서 지정한 환경변수가 `.env`보다 우선하며, 코드 기본값은 없다. 모든 설정 키를 명시해야 하고 누락·필수값 공백은 종료 코드 2로 거부한다. seed는 빈 값으로 무작위를, stdout 출력 경로는 빈 값으로 미사용을 지정한다. `.env.example`은 실행 시 읽지 않는다. `.env`가 없어도 모든 키를 환경변수로 제공하면 실행할 수 있다.

```bash
cd apps/log_generator

# stdout, 기본 300,000건
uv run log-generator

# 파일 출력. 기존 파일이 있으면 종료 코드 2로 거부한다.
PRODUCER_OUTPUT=file PRODUCER_OUTPUT_PATH=/tmp/events.jsonl uv run log-generator

# 무제한 실행. Ctrl+C로 종료하면 최종 통계를 stderr에 쓴다.
PRODUCER_MAX_EVENTS=0 uv run log-generator > /dev/null
```

stderr에는 상태 주기마다 `"status": "running"` JSON이, 종료 시 최종 통계 JSON이 한 줄씩 기록된다. 주기 상태의 `interval_events`·`interval_eps`는 직전 상태 출력 이후 구간의 건수·속도다.

```bash
# 로그는 버리고 1초마다 생성 건수만 확인
PRODUCER_MAX_EVENTS=0 PRODUCER_STATUS_INTERVAL_SEC=1 uv run log-generator > /dev/null
```

## 4. 성능 측정 (PRD 기준 6)

```bash
cd apps/log_generator
PRODUCER_OUTPUT=file PRODUCER_OUTPUT_PATH=/tmp/perf.jsonl PRODUCER_MAX_EVENTS=0 PRODUCER_STATUS_INTERVAL_SEC=5 \
  timeout -s INT 67 /usr/bin/time -v .venv/bin/log-generator 2> /tmp/perf.err
```

- 첫 번째 `running` 상태(약 5초)와 13번째 상태(약 65초)의 `events_written` 차이를 경과 시간 차이로 나눠 측정 EPS를 구한다.
- `/usr/bin/time` 출력에서 CPU 사용률과 `Maximum resident set size`를 기록한다.
- `timeout`이 SIGINT를 보내므로 셸의 종료 코드는 124다. 생성기 자체의 종료 코드는 130이다.

## 5. 데이터 초기화

생성 파일은 지정한 출력 경로에만 생긴다. 다시 실행하기 전에 해당 파일을 지운다.

```bash
rm -f /tmp/events.jsonl /tmp/perf.jsonl /tmp/perf.err
```

## 6. Kafka 실행 (2단계)

Docker Desktop의 Settings → Resources → WSL Integration에서 현재 Ubuntu 배포판을 활성화한다. Docker Desktop이 실행 중이어야 하며, WSL에서 `docker version`과 `docker compose version`이 성공해야 한다.

기존 `.env`가 있으면 덮어쓰지 않고 `.env.example`의 2단계 키(`PRODUCER_KAFKA_*`, `KAFKA_HOST_PORT`, `KAFKA_TOPIC`, `KAFKA_DLQ_TOPIC`, `KAFKA_UI_HOST_PORT`)를 추가한다. 이전의 `KAFKA_TOPIC=application-logs`는 `raw.ecommerce.orders.v1`로 바꾼다(이미 만든 `application-logs` 토픽은 볼륨에 남지만 사용하지 않는다). 아래 명령은 프로젝트 루트에서 실행한다.

```bash
docker compose config --quiet
docker compose up -d
docker compose ps -a
docker compose logs kafka-init
```

`kafka`가 healthy, `kafka-init`이 Exited (0)이면 토픽 초기화 완료다. `kafka-ui`는 프로필 `tools`에 있어 기본 `up`에서는 실행하지 않는다.

### 토픽 확인

```bash
# 원본: 파티션 3, retention.ms=3600000, retention.bytes=8589934592, LogAppendTime
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic raw.ecommerce.orders.v1
# DLQ: 파티션 1, retention.ms=21600000, retention.bytes=12884901888, LogAppendTime
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic dlq.ecommerce.orders.v1
# 파티션별 끝 offset
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic raw.ecommerce.orders.v1
# 표본 확인(timestamp 종류 포함)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic raw.ecommerce.orders.v1 --from-beginning --max-messages 3 --property print.timestamp=true
```

### Kafka UI

필요할 때만 실행하고, 확인 후 멈춘다.

```bash
docker compose --profile tools up -d kafka-ui
docker compose --profile tools stop kafka-ui
```

`http://localhost:8080`(`KAFKA_UI_HOST_PORT`로 변경)에서 클러스터 `local`의 브로커 상태, 두 토픽의 파티션·설정, offset 범위, 메시지 표본을 확인한다. 이미지는 `ghcr.io/kafbat/kafka-ui:v1.5.0`으로 고정했다. 인증은 없고 루프백에만 게시한다. 성능 측정 중에는 UI 메시지 탐색을 하지 않고 UI 실행 여부를 기록한다.

### 생성기 Kafka 출력

`PRODUCER_KAFKA_BOOTSTRAP_SERVERS`는 `localhost:${KAFKA_HOST_PORT}`와 같아야 한다.

```bash
cd apps/log_generator
# 약 10초 실행(무제한 + SIGINT). 최종 통계의 "kafka" 키에 ACK EPS, ACK 지연 p50/p95/p99, 생성->Kafka 지연이 있다.
PRODUCER_OUTPUT=kafka PRODUCER_MAX_EVENTS=0 timeout -s INT 10 .venv/bin/log-generator 2> /tmp/kafka-run.err
# 60초 성능 측정(PRD 완료 기준 3): 5초 준비 후 60초. 상태 로그의 interval_ack_eps와 최종 통계를 사용한다.
PRODUCER_OUTPUT=kafka PRODUCER_MAX_EVENTS=0 timeout -s INT 66 .venv/bin/log-generator 2> /tmp/kafka-perf.err
# 측정 중 Kafka·UI 사용량
docker stats --no-stream
```

종료 코드는 정상 완료 0, 전송 실패·fatal 오류·큐 포화 시간 초과·flush 후 미전달 1, 설정 오류 2, Ctrl+C 130이다(실패 또는 미전달이 있으면 Ctrl+C도 1).

### 통합 테스트

실제 브로커로 원본 bytes 보존(LF 없음·key 없음)과 LogAppendTime timestamp를 검증한다. 브로커에 접속할 수 없으면 건너뛴다. 원본 토픽에 테스트 메시지 몇 건이 남는다.

```bash
cd apps/log_generator
uv run pytest ../../tests/integration -v
```

### 재기동·초기화

```bash
# 데이터 유지 종료 및 재기동. get-offsets로 기존 데이터 유지 확인
docker compose down
docker compose up -d

# 데이터 초기화: Kafka 볼륨의 모든 로그와 메타데이터를 삭제하므로 필요한 경우에만 실행
docker compose down --volumes
```

보관 기간은 원본 1시간·DLQ 6시간, 보관 크기는 파티션당 원본 8GiB·DLQ 12GiB다. 둘 중 먼저 적용되는 조건에 따라 오래된 세그먼트를 삭제하므로 정확한 보관 시간·크기는 보장하지 않는다. 토픽 설정은 기존 토픽에 자동 반영되지 않는다.

### 2단계 실행 검증 결과 (2026-11-16)

- 토픽: 원본(3파티션)·DLQ(1파티션)가 PRD 설정대로 생성됨을 `--describe`로 확인. `down` 후 `up -d`에서 offset(합계 49,612)이 유지됨.
- Kafka UI: `v1.5.0`, 클러스터 `local` ONLINE·브로커 1·토픽 2 확인. 메모리 약 248MiB(제한 1GiB), Kafka 약 329MiB(제한 2GiB, 유휴·부하 직후).
- 10초 실행: ACK EPS 약 4,993, ACK 지연 p95 약 104ms(준비 구간 포함), 실패·미전달 0. 60초 성능·브로커 중단·지연 주입은 미검증이다.
- 볼륨 마운트는 `/var/lib/kafka/data`다. 기존 `/tmp/kraft-combined-logs` 마운트는 권한 오류로 브로커가 기동하지 못해 변경했다.

## 7. Spark Structured Streaming 실행 (3단계)

Spark 서비스는 `compose.yaml`의 `spark`다. Kafka(6절)가 실행 중이고 `kafka-init`·`clickhouse-init`(8절)이 완료돼야 시작한다. 4단계부터 정상 주문은 Parquet 대신 ClickHouse `silver.order_events`에 JDBC로 적재한다. `.env`에는 `.env.example`의 `SPARK_*` 키를 모두 추가한다. 누락하면 `docker compose`가 설정 오류로 거부한다. 기본값은 `SPARK_MASTER=local[3]`, `SPARK_DRIVER_MEMORY=2g`, `SPARK_TRIGGER_INTERVAL=5 seconds`, `SPARK_MAX_OFFSETS_PER_TRIGGER=50000`, `SPARK_SHUFFLE_PARTITIONS=3`, `SPARK_STARTING_OFFSETS=earliest`, 체크포인트 `/opt/spark/checkpoints/orders`, `SPARK_JDBC_BATCH_SIZE=50000`, `SPARK_JDBC_NUM_PARTITIONS=3`이다. `SPARK_LATENCY_METRICS_ENABLED`(기본 `true`)는 `.env`에 없으면 Compose가 `true`로 두며, `false`는 계측 오버헤드 대조군 측정에만 쓴다. 체크포인트는 볼륨 `spark-checkpoints`에 저장되며 컨테이너를 지워도 남는다. 3단계 Parquet 출력 볼륨 `spark-output`은 Compose에서 제거했다. 남은 볼륨은 `docker volume rm log-etlm-2_spark-output`으로 지운다. 명령은 프로젝트 루트에서 실행한다.

### 기동·로그·중지

```bash
docker compose up -d kafka kafka-init       # 브로커·토픽
docker compose up -d --build spark          # 스트리밍 쿼리 시작
docker compose logs -f spark                # 진행·건수 로그
docker compose stop spark                   # 중지. 체크포인트 볼륨은 유지
```

실행 중에는 `http://localhost:4040`(루프백)에서 Spark UI를 볼 수 있다.

### 같은 체크포인트로 재시작

`docker compose stop spark` 뒤 `docker compose up -d spark`로 다시 시작하면 같은 `SPARK_CHECKPOINT_DIR`을 사용한다. spark 서비스는 `restart: on-failure`(횟수 상한 없음)라 쿼리 실패·멈춤 감시 종료(exit 1) 때 자동으로 같은 체크포인트로 재시작하며, `docker compose stop`으로 직접 멈춘 경우는 재시작하지 않는다. 지속 실패는 간격 최대 1분으로 무한 재시작되므로 멈추려면 `docker compose stop spark`, 횟수는 `docker inspect -f '{{.RestartCount}}' log-etlm-2-spark-1`로 확인한다. 체크포인트가 있으면 `SPARK_STARTING_OFFSETS`는 무시되고 마지막 커밋 offset부터 이어 읽는다. 커밋되지 않은 배치는 같은 `batch_id`·offset 범위로 다시 실행된다. 쿼리가 실패해 컨테이너가 종료한 경우에도 원인을 해결한 뒤 같은 방법으로 재시작한다.

### 테스트

코드가 읽기 전용 마운트라 pytest 캐시를 끈다. 브로커·ClickHouse는 필요 없다(JDBC·지표 쓰기는 테스트에서 대체한다).

```bash
docker compose run --rm --no-deps spark python3 -m pytest -p no:cacheprovider /opt/app/apps/streaming/tests -q
```

### 측정용 별도 체크포인트 실행

운영 체크포인트와 섞이지 않도록 경로를 바꿔 별도 컨테이너로 실행한다. 적재 대상 테이블은 같으므로 측정 행은 `spark_run_id`·`spark_batch_id`로 구분한다. 최초 기동 시점 이후의 입력만 처리하려면 `SPARK_STARTING_OFFSETS=latest`로 지정한다. `<이름>`과 `<경로>`는 실행마다 새로 정한다.

```bash
docker compose run -d --no-deps --name <이름> \
  -e SPARK_CHECKPOINT_DIR=/opt/spark/checkpoints/<경로> \
  -e SPARK_STARTING_OFFSETS=latest spark
```

- 같은 이름의 컨테이너가 이미 있으면 실행할 수 없다. 측정마다 이름을 바꾼다.
- 중단 뒤 같은 체크포인트로 이어 가려면 같은 `<경로>`와 같은 설정으로 다시 실행한다.
- 측정이 끝난 경로는 볼륨에 남는다. 지울 때는 다음의 체크포인트 초기화 주의를 먼저 읽는다.

### 장애와 조치

쿼리는 정상 출력·DLQ 쓰기에 실패하면 배치를 성공 처리하지 않고 실패로 종료한다(컨테이너 종료 코드 1, 로그 `terminated with error`). 지연 로그에는 `status=failed`와 `failed_stage`가 남는다. 해당 배치의 `commits/<batch_id>` 파일은 만들어지지 않는다.

| 증상 | 의미 | 조치 |
| --- | --- | --- |
| `failed_stage=orders_write_action`, 예: JDBC 연결·인증 오류 | 정상 적재 실패. DLQ는 시도하지 않았다. 일부 쓰기 파티션은 이미 INSERT됐을 수 있다 | ClickHouse 상태·계정을 고친 뒤 같은 체크포인트·설정으로 재시작한다. 재INSERT 중복은 `gold.orders_deduplicated`(`FINAL`)에서 합쳐진다 |
| `failed_stage=dlq_write_action`, 예: `MESSAGE_TOO_LARGE` | 정상 출력은 기록됐고 DLQ 쓰기가 실패했다 | DLQ 토픽·브로커 상태를 고친 뒤 같은 체크포인트로 재시작한다. 정상 출력이 해당 배치 정상 건수만큼 중복된다. Compose `restart: on-failure`가 같은 체크포인트로 자동 재시작한다. 실패 행은 DLQ producer `delivery.timeout.ms`(30초) 뒤에 남는다. 장애가 계속되면 간격 최대 1분으로 재시작이 무한 반복되므로 `docker compose stop spark`로 멈추고, 원인을 고친 뒤 `docker compose up -d spark`로 다시 시작한다 |
| 로그에 `splitting and retrying ... MESSAGE_TOO_LARGE`가 이어진 뒤 `Uncaught exception in thread 'kafka-producer-network-thread ...': java.lang.StackOverflowError`, 이후 새 `batch`·`progress` 로그 없음, 컨테이너는 실행 중 | DLQ producer 스레드가 죽어 DLQ 쓰기 태스크가 무기한 대기한다. 장애를 고쳐도 풀리지 않고 쿼리도 종료하지 않는다 | 멈춤 감시가 `SPARK_STALL_TIMEOUT_SECONDS`(90초) 뒤 `ERROR streaming query stalled: ...`를 남기고 exit 1로 끝내며, Compose가 같은 체크포인트로 자동 재시작한다. 정상 출력은 위 행과 같이 중복되고, 멈추기 전 기록된 일부 DLQ 메시지는 키로 중복 판별한다 |
| 콜백은 성공했으나 체크포인트 `commits`의 `Permission denied` | 출력은 모두 기록됐고 커밋만 실패했다 | 체크포인트 권한을 고친 뒤 같은 체크포인트로 재시작한다. 정상 출력과 DLQ가 해당 배치 건수만큼 중복된다 |
| `KafkaIllegalStateException: Some data may have been lost because they are not available in Kafka any more; ...` (원인 `OffsetOutOfRangeException`) | 체크포인트의 다음 offset이 보관 기간·크기 제한으로 이미 삭제됐다. 종료 코드 1이며 소실 구간을 건너뛰고 커밋하지 않는다 | 아래 offset 소실 조치를 따른다 |

재시작 뒤 같은 배치는 같은 `batch_id`와 offset 범위로 다시 실행되고 커밋되며 유실은 없다. 중복은 제거하지 않는다. 같은 원본 위치는 정상 출력의 `kafka_topic`·`kafka_partition`·`kafka_offset`, DLQ의 메시지 키(`source_topic:source_partition:source_offset`)로 찾는다. 정상 출력 중복은 ClickHouse 조회 뷰 `gold.orders_deduplicated`에서 제거한다. 검증 결과는 [Spark PRD](../prd/03-stream-processing.md#출력-장애와-동일-체크포인트-재시작-검증)를 따른다.

#### offset 소실 조치

원본 토픽의 보관 기간(1시간)이나 크기 제한을 넘겨 Spark를 오래 중지하면 마지막 커밋 offset 이후 데이터가 삭제될 수 있다. 이때 `failOnDataLoss=true`로 쿼리가 실패하며 마지막 커밋 batch·offset과 정상 출력은 실패 전과 같다. 같은 체크포인트로 다시 시작해도 같은 오류가 반복된다.

1. 오류 문장의 `Could not read records in offset [시작, 끝) for topic partition ...`으로 소실 구간을 확인한다. 현재 offset 범위는 `kafka-get-offsets.sh`(6절)로 확인한다.
2. 소실 구간의 데이터는 복구할 수 없다. 이를 허용할 때만 새 체크포인트·출력 경로로 시작한다(아래 체크포인트 초기화 주의).
3. 재발을 막으려면 Spark 중지 시간을 원본 보관 기간 안으로 유지한다.

이 조치 절차 자체는 실행·검증한 것이 아니다. 검증된 것은 같은 체크포인트 재시작이 위 오류로 실패한다는 사실까지다(PRD 03의 offset 소실 시 실패 결과).

### 체크포인트 초기화 시 주의

- 체크포인트는 소비 위치와 배치 계획을 저장한다. 이를 삭제하거나 다른 경로로 바꾸면 `SPARK_STARTING_OFFSETS`부터 다시 시작한다. `earliest`는 보관 중인 모든 원본을 다시 읽어 같은 출력 경로에 중복을 만들고, `latest`는 중지 중에 들어온 데이터를 건너뛰어 누락을 만든다.
- 체크포인트만 지우고 `earliest`로 시작하면 보관 중인 원본이 다시 적재된다. 같은 `event_id`라 조회 뷰에서는 합쳐지지만 물리 행과 병합 부하는 늘어난다.
- 측정·검증에는 운영 경로 대신 별도 경로를 쓴다(위 측정용 실행).
- 실행 중인 쿼리의 체크포인트 경로를 직접 지우거나 권한을 바꾸지 않는다. 쿼리가 실패한다.

### DLQ 조회·원문 복원

DLQ 메시지에는 원문 bytes가 Base64(`raw_value_base64`)로 있고, 키는 원본 위치 `source_topic:source_partition:source_offset`이다. 조회·복원·원본 대조 절차와 재실행 명령은 [DLQ 조회·원문 bytes 복원 검증](../measurements/2026-11-17-dlq-lookup-restoration.md)과 `docs/measurements/verify_dlq_lookup.py`를 따른다. 이 도구는 `assign`으로 명시한 offset만 읽고 소비 offset을 커밋하지 않는다. 이미 기록된 DLQ 데이터의 수정·재투입·자동 재처리는 지원하지 않는다.

## 8. ClickHouse 실행 (4단계)

서비스는 `compose.yaml`의 `clickhouse`(서버)와 `clickhouse-init`(일회성 계정·스키마·권한 적용)이다. `.env`에 `.env.example`의 4단계 키를 모두 추가하고 비밀번호 4개(`CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SPARK_PASSWORD`, `CLICKHOUSE_GENERATOR_PASSWORD`, `CLICKHOUSE_GRAFANA_PASSWORD`)를 직접 정한다. 누락하면 `docker compose`가 설정 오류로 거부한다. 설계 근거는 [ClickHouse PRD](../prd/04-clickhouse-storage.md)를 따른다.

```bash
docker compose up -d clickhouse clickhouse-init   # 서버 기동 후 init이 Exited (0)이면 적용 완료
docker compose logs clickhouse-init               # "clickhouse-init: schema and accounts applied"
docker compose run --rm clickhouse-init           # 스키마·계정 재적용(반복 실행 안전)
```

- DDL은 `infra/clickhouse/schema.sql`, 계정·권한은 `infra/clickhouse/init.sh`, 저메모리 설정은 `infra/clickhouse/config.d/low-memory.xml`이다. `IF NOT EXISTS`라 기존 테이블의 컬럼·설정은 바뀌지 않는다. 바꾸려면 `schema.sql`에 `ALTER`를 추가하거나 볼륨 `clickhouse-data`를 지우고 다시 만든다.
- 데이터는 볼륨 `clickhouse-data`에 저장한다. 주문은 24시간, 운영 지표는 24시간 TTL이다.
- HTTP 포트는 `127.0.0.1:${CLICKHOUSE_HTTP_HOST_PORT}`에만 게시한다.

### 조회

관리자 계정으로 컨테이너 안에서 조회한다. 비밀번호는 `.env`에서 읽는다.

```bash
docker compose exec clickhouse sh -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD"'
```

```sql
-- 최근 15분 주문 수·매출(중복 제거 경로)
SELECT count(), sum(total_amount) FROM gold.orders_deduplicated WHERE event_timestamp >= now() - INTERVAL 15 MINUTE;
-- 생성기 1초 구간 지표(최근 실행). 분위수는 구간별 값이며 여러 행을 평균하지 않는다.
SELECT interval_start, generated, acked, failed, undelivered, queue_len_max, ack_latency_ms_p99, gen_to_kafka_ms_p99, gen_serialize_us_p99
FROM ops.producer_stats WHERE run_id = (SELECT run_id FROM ops.producer_stats ORDER BY interval_start DESC LIMIT 1)
ORDER BY interval_start;
-- 배치별 단계 시간과 Kafka → 콜백 지연
SELECT batch_id, status, failed_stage, order_rows, classification_ms, orders_write_ms, dlq_write_ms, kafka_to_callback_ms_p95
FROM ops.spark_batch_attempts ORDER BY callback_started_at DESC LIMIT 10;
-- 트리거 시간과 offset backlog
SELECT batch_id, trigger_execution_ms, num_input_rows,
       if(empty(latest_offsets), NULL, arraySum(mapValues(latest_offsets)) - arraySum(mapValues(end_offsets))) AS backlog
FROM ops.spark_query_progress ORDER BY trigger_at DESC LIMIT 10;
-- Spark JDBC INSERT 요청별 서버 처리시간
SELECT event_time, written_rows, query_duration_ms FROM system.query_log
WHERE user = 'spark_writer' AND query_kind = 'Insert' AND type = 'QueryFinish' AND has(tables, 'silver.order_events')
ORDER BY event_time DESC LIMIT 10;
-- Spark INSERT 실패. 권한·인증 거부는 tables가 비어 있으므로 tables로 거르지 않는다.
SELECT event_time, type, exception_code, written_rows, substring(exception, 1, 120) FROM system.query_log
WHERE user = 'spark_writer' AND type != 'QueryStart' AND exception_code != 0
ORDER BY event_time DESC LIMIT 10;
-- 재INSERT로 생긴 물리 중복 수(실행 단위). 실패 INSERT의 written_rows는 실제 남은 행 수가 아니다.
SELECT count() - uniqExact(event_id) AS duplicates FROM silver.order_events WHERE spark_query_id = '<query_id>';
-- 같은 event_id에 다른 event_timestamp가 있는 충돌 건수(최근 1시간). 0이 아니면 주문 집계가 부풀 수 있다.
-- 구간 밖 충돌은 WHERE를 빼고 전체 구간으로 낮은 빈도로 점검한다.
SELECT count() FROM (
  SELECT event_id FROM silver.order_events
  WHERE event_timestamp >= now() - INTERVAL 1 HOUR
  GROUP BY event_id HAVING uniqExact(event_timestamp) > 1
);
```

지표 해석 주의(근거: [ClickHouse PRD 장애 주입 측정 결과](../prd/04-clickhouse-storage.md#장애-주입-측정-결과-2026-11-18)):

- backlog는 배치 계획 시점 값이라 배치 입력이 `SPARK_MAX_OFFSETS_PER_TRIGGER`보다 적으면 0이다. 처리가 느려지면 먼저 트리거 시간 5초 초과, 배치 입력 증가, Kafka → 콜백 p95 증가로 나타난다.
- 재시작 후 첫 진행 정보는 `latest_offsets`가 비어 있어 backlog를 계산할 수 없다(위 조회에서 NULL).
- `orders_write_action` 실패의 원인은 `query_log` 예외 유형으로 가른다. `ExceptionWhileProcessing`이면 ClickHouse가 처리 중 실패해 일부 행이 남았을 수 있고(재시작 시 중복), `ExceptionBeforeStart`(예: 497 권한)이면 저장되지 않았다.
- `orders_write_ms`와 `query_log` INSERT 시간이 함께 늘면 ClickHouse 쪽, `orders_write_ms`만 늘면 Spark 태스크·네트워크 쪽이다.
- DLQ(Kafka) 쓰기 장애는 ClickHouse `query_log`에 나타나지 않는다. 최신 `ops.spark_query_progress` 행이 트리거 주기(5초)보다 오래 갱신되지 않으면 콜백이 끝나지 않은 것이다. `failed_stage=dlq_write_action` 행은 약 30초 뒤에 남거나, producer 스레드가 죽으면 남지 않고 90초 뒤 멈춤 감시 종료·자동 재시작된다. 재시작 여부는 `docker inspect -f '{{.RestartCount}}' log-etlm-2-spark-1`과 `ops.spark_query_progress`의 `run_id` 변경으로 본다(근거: [DLQ 쓰기 실패 주입 결과](../prd/04-clickhouse-storage.md#dlq-쓰기-실패-주입-결과-2026-11-18)).
- 재INSERT 중복은 백그라운드 병합이 곧 합치므로 위 물리 중복 조회는 시간이 지나면 0이 될 수 있다. 재INSERT 양은 같은 시간 범위 `query_log` 주문 INSERT `written_rows` 합계와 고유 수의 차이로 본다.

### 운영 지표 쓰기 실패

Spark의 `ops.*` INSERT 실패는 `WARN streaming ops metrics insert failed table=...` 로그만 남기고 주문 처리는 계속한다. 해당 구간 지표는 비어 있으므로 Spark 로그의 `latency`·`progress` JSON으로 대조한다.

생성기의 `ops.producer_stats` INSERT 실패는 stderr에 `{"warning": "producer_stats insert failed", ...}` 한 줄을 남기고 생성은 계속한다. 최종 통계의 `producer_stats.failed_rows`가 잃은 행 수다. 기록을 끄려면 `PRODUCER_CLICKHOUSE_HTTP_URL=`로 실행한다. kafka 출력에서만 기록한다.


## 9. Grafana 실행 (5단계)

서비스는 `compose.yaml`의 `grafana`다. `clickhouse-init`(8절)이 완료돼야 시작한다. `.env`에 `.env.example`의 5단계 키(`GRAFANA_HOST_PORT`, `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD`)를 추가하고 관리자 비밀번호를 직접 정한다. 데이터 소스는 4단계 `CLICKHOUSE_GRAFANA_PASSWORD`(`grafana_reader`)를 쓴다. 설계 근거는 [Grafana PRD](../prd/05-grafana-dashboard.md)를 따른다.

```bash
docker compose up -d grafana     # 첫 기동은 ClickHouse 플러그인을 내려받는다(grafana.com 접속 필요)
docker compose logs -f grafana   # 기동·프로비저닝 로그
```

- 접속: `http://localhost:${GRAFANA_HOST_PORT}`(루프백), 관리자 계정으로 로그인한다.
- 대시보드: `주문 현황`(uid `orders`, 1분 갱신), `파이프라인 운영`(uid `pipeline`, 5초 갱신). 경보 규칙은 `log-etlm` 폴더에 5개가 있으며 외부 알림은 보내지 않는다. 상태는 경보 화면과 대시보드의 경보 패널에서 본다.
- 원본 파일: 데이터 소스 `infra/grafana/provisioning/datasources/clickhouse.yaml`, 대시보드 `infra/grafana/dashboards/*.json`, 경보 `infra/grafana/provisioning/alerting/rules.yaml`. 화면에서 고친 대시보드는 저장되지 않는다(`allowUiUpdates: false`). 바꾸려면 화면에서 편집한 뒤 JSON 모델을 내보내 파일에 반영한다. 대시보드 파일 변경은 기동 중에도 약 10초 안에 반영되며, 데이터 소스·경보 파일 변경은 `docker compose restart grafana`로 반영한다.
- 내부 DB(사용자·경보 상태 이력)는 볼륨 `grafana-data`다. 지워도 프로비저닝 파일로 같은 대시보드·경보가 다시 만들어진다.
- 기동 로그의 `Failed to read plugin provisioning files ... /etc/grafana/provisioning/plugins`는 플러그인 앱 프로비저닝 디렉터리가 없다는 뜻이며 무시한다.

### 상태 확인

```bash
set -a; . ./.env; set +a
G=http://127.0.0.1:${GRAFANA_HOST_PORT}; A="$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD"
curl -s -u "$A" $G/api/datasources/uid/clickhouse/health          # {"status":"OK"}
curl -s -u "$A" "$G/api/search?type=dash-db"                        # 대시보드 2개
curl -s -u "$A" $G/api/prometheus/grafana/api/v1/rules              # 경보 규칙 health·state
```

### 경보 대응

| 경보 | 확인할 것 |
| --- | --- |
| Spark 진행 공백 | `docker compose logs spark`에서 쿼리 상태와 `ops metrics insert failed` 경고. 지표 쓰기만 실패해도 공백이 생긴다. 90초 뒤 멈춤 감시가 종료·재시작한다(7절) |
| Spark 재시작 | `docker inspect -f '{{.RestartCount}}' log-etlm-2-spark-1`과 종료 원인. 지속 실패면 `docker compose stop spark` 후 원인을 고친다(7절 장애와 조치) |
| Spark 배치 실패 | 파이프라인 운영 대시보드의 '실패 시도' 표에서 `failed_stage` 확인 후 7절 장애와 조치 |
| ClickHouse INSERT 실패 | 8절 조회의 Spark INSERT 실패 쿼리로 예외 유형 확인 |
| ID 충돌(1시간) | 8절 조회의 충돌 건수 쿼리. 충돌 구간의 주문 집계는 부풀 수 있다 |
