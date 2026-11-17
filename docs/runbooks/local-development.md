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

`kafka`가 healthy, `kafka-init`이 Exited (0)이면 토픽 초기화 완료다. `kafka-ui`는 `kafka` healthy 후 시작한다.

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

Spark 서비스는 `compose.yaml`의 `spark`다. Kafka(6절)가 실행 중이고 `kafka-init`이 완료돼야 시작한다. `.env`에는 `.env.example`의 `SPARK_*` 키를 모두 추가한다. 누락하면 `docker compose`가 설정 오류로 거부한다. 기본값은 `SPARK_MASTER=local[3]`, `SPARK_DRIVER_MEMORY=2g`, `SPARK_TRIGGER_INTERVAL=5 seconds`, `SPARK_MAX_OFFSETS_PER_TRIGGER=50000`, `SPARK_SHUFFLE_PARTITIONS=3`, `SPARK_STARTING_OFFSETS=earliest`, 체크포인트 `/opt/spark/checkpoints/orders`, 출력 `/opt/spark/output/orders`다. `SPARK_LATENCY_METRICS_ENABLED`(기본 `true`)는 `.env`에 없으면 Compose가 `true`로 두며, `false`는 계측 오버헤드 대조군 측정에만 쓴다. 체크포인트·출력은 볼륨 `spark-checkpoints`·`spark-output`에 저장되며 컨테이너를 지워도 남는다. 명령은 프로젝트 루트에서 실행한다.

### 기동·로그·중지

```bash
docker compose up -d kafka kafka-init       # 브로커·토픽
docker compose up -d --build spark          # 스트리밍 쿼리 시작
docker compose logs -f spark                # 진행·건수 로그
docker compose stop spark                   # 중지. 체크포인트·출력 볼륨은 유지
```

실행 중에는 `http://localhost:4040`(루프백)에서 Spark UI를 볼 수 있다.

### 같은 체크포인트로 재시작

`docker compose stop spark` 뒤 `docker compose up -d spark`로 다시 시작하면 같은 `SPARK_CHECKPOINT_DIR`을 사용한다. 체크포인트가 있으면 `SPARK_STARTING_OFFSETS`는 무시되고 마지막 커밋 offset부터 이어 읽는다. 커밋되지 않은 배치는 같은 `batch_id`·offset 범위로 다시 실행된다. 쿼리가 실패해 컨테이너가 종료한 경우에도 원인을 해결한 뒤 같은 방법으로 재시작한다.

### 테스트

코드가 읽기 전용 마운트라 pytest 캐시를 끈다. 브로커는 필요 없다.

```bash
docker compose run --rm --no-deps spark python3 -m pytest -p no:cacheprovider /opt/app/apps/streaming/tests -q
```

### 측정용 별도 체크포인트·출력 경로 실행

운영 체크포인트·출력과 섞이지 않도록 경로를 바꿔 별도 컨테이너로 실행한다. 최초 기동 시점 이후의 입력만 처리하려면 `SPARK_STARTING_OFFSETS=latest`로 지정한다. `<이름>`과 `<경로>`는 실행마다 새로 정한다.

```bash
docker compose run -d --no-deps --name <이름> \
  -e SPARK_CHECKPOINT_DIR=/opt/spark/checkpoints/<경로> \
  -e SPARK_OUTPUT_DIR=/opt/spark/output/<경로> \
  -e SPARK_STARTING_OFFSETS=latest spark
```

- 같은 이름의 컨테이너가 이미 있으면 실행할 수 없다. 측정마다 이름을 바꾼다.
- 중단 뒤 같은 체크포인트로 이어 가려면 같은 `<경로>`와 같은 설정으로 다시 실행한다.
- 측정이 끝난 경로는 볼륨에 남는다. 지울 때는 다음의 체크포인트 초기화 주의를 먼저 읽는다.

### 장애와 조치

쿼리는 정상 출력·DLQ 쓰기에 실패하면 배치를 성공 처리하지 않고 실패로 종료한다(컨테이너 종료 코드 1, 로그 `terminated with error`). 지연 로그에는 `status=failed`와 `failed_stage`가 남는다. 해당 배치의 `commits/<batch_id>` 파일은 만들어지지 않는다.

| 증상 | 의미 | 조치 |
| --- | --- | --- |
| `failed_stage=orders_write_action`, 예: `Mkdirs failed to create .../_temporary/...` | 정상 출력 쓰기 실패. DLQ는 시도하지 않았다 | 출력 경로 권한·용량을 고친 뒤 같은 체크포인트·출력 경로·설정으로 재시작한다. 중복은 생기지 않는다 |
| `failed_stage=dlq_write_action`, 예: `MESSAGE_TOO_LARGE` | 정상 출력은 기록됐고 DLQ 쓰기가 실패했다 | DLQ 토픽·브로커 상태를 고친 뒤 같은 체크포인트로 재시작한다. 정상 출력이 해당 배치 정상 건수만큼 중복된다 |
| 콜백은 성공했으나 체크포인트 `commits`의 `Permission denied` | 출력은 모두 기록됐고 커밋만 실패했다 | 체크포인트 권한을 고친 뒤 같은 체크포인트로 재시작한다. 정상 출력과 DLQ가 해당 배치 건수만큼 중복된다 |
| `KafkaIllegalStateException: Some data may have been lost because they are not available in Kafka any more; ...` (원인 `OffsetOutOfRangeException`) | 체크포인트의 다음 offset이 보관 기간·크기 제한으로 이미 삭제됐다. 종료 코드 1이며 소실 구간을 건너뛰고 커밋하지 않는다 | 아래 offset 소실 조치를 따른다 |

재시작 뒤 같은 배치는 같은 `batch_id`와 offset 범위로 다시 실행되고 커밋되며 유실은 없다. 중복은 제거하지 않는다. 같은 원본 위치는 정상 출력의 `kafka_topic`·`kafka_partition`·`kafka_offset`, DLQ의 메시지 키(`source_topic:source_partition:source_offset`)로 찾는다. 정상 출력 중복의 저장 단계 처리는 4단계(ClickHouse) 범위다. 검증 결과는 [Spark PRD](../prd/03-stream-processing.md#출력-장애와-동일-체크포인트-재시작-검증)를 따른다.

#### offset 소실 조치

원본 토픽의 보관 기간(1시간)이나 크기 제한을 넘겨 Spark를 오래 중지하면 마지막 커밋 offset 이후 데이터가 삭제될 수 있다. 이때 `failOnDataLoss=true`로 쿼리가 실패하며 마지막 커밋 batch·offset과 정상 출력은 실패 전과 같다. 같은 체크포인트로 다시 시작해도 같은 오류가 반복된다.

1. 오류 문장의 `Could not read records in offset [시작, 끝) for topic partition ...`으로 소실 구간을 확인한다. 현재 offset 범위는 `kafka-get-offsets.sh`(6절)로 확인한다.
2. 소실 구간의 데이터는 복구할 수 없다. 이를 허용할 때만 새 체크포인트·출력 경로로 시작한다(아래 체크포인트 초기화 주의).
3. 재발을 막으려면 Spark 중지 시간을 원본 보관 기간 안으로 유지한다.

이 조치 절차 자체는 실행·검증한 것이 아니다. 검증된 것은 같은 체크포인트 재시작이 위 오류로 실패한다는 사실까지다(PRD 03의 offset 소실 시 실패 결과).

### 체크포인트 초기화 시 주의

- 체크포인트는 소비 위치와 배치 계획을 저장한다. 이를 삭제하거나 다른 경로로 바꾸면 `SPARK_STARTING_OFFSETS`부터 다시 시작한다. `earliest`는 보관 중인 모든 원본을 다시 읽어 같은 출력 경로에 중복을 만들고, `latest`는 중지 중에 들어온 데이터를 건너뛰어 누락을 만든다.
- 체크포인트만 지우고 출력은 두면 정상 출력이 중복된다. 반대로 출력만 지우고 체크포인트를 두면 이미 커밋된 구간은 다시 만들어지지 않는다. 체크포인트와 출력 경로는 짝으로 다룬다.
- 측정·검증에는 운영 경로 대신 별도 경로를 쓴다(위 측정용 실행).
- 실행 중인 쿼리의 체크포인트·출력 경로를 직접 지우거나 권한을 바꾸지 않는다. 쿼리가 실패한다.

### DLQ 조회·원문 복원

DLQ 메시지에는 원문 bytes가 Base64(`raw_value_base64`)로 있고, 키는 원본 위치 `source_topic:source_partition:source_offset`이다. 조회·복원·원본 대조 절차와 재실행 명령은 [DLQ 조회·원문 bytes 복원 검증](../measurements/2026-11-17-dlq-lookup-restoration.md)과 `docs/measurements/verify_dlq_lookup.py`를 따른다. 이 도구는 `assign`으로 명시한 offset만 읽고 소비 offset을 커밋하지 않는다. 이미 기록된 DLQ 데이터의 수정·재투입·자동 재처리는 지원하지 않는다.
