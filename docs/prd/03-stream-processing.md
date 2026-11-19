# Spark 스트림 처리

- 상태: 완료 (5,000 EPS 지속 부하 충족(로컬 Parquet 1차, ClickHouse 경로는 4단계에서 재측정·통과), 출력 장애·재시작·offset 소실·DLQ 조회 검증 완료, 지연 주입 부분 통과 후 판정 정리, 계측 오버헤드 1회 측정. [3단계 마무리 검증 계획](#3단계-마무리-검증-계획-2026-11-17) 종료)
- 적용 범위: 제품 v1

## 목표

Kafka에서 로그를 읽어 주문 로그를 검증하고 변환한다.

## Spark UI

Compose의 `spark` 서비스에서 내장 Spark UI의 4040 포트를 `127.0.0.1:4040`으로 게시한다. 실행 중인 애플리케이션의 작업·스테이지·실행기 상태는 `http://localhost:4040`에서 확인한다. UI는 애플리케이션 실행 중에만 제공한다.

참고: [Spark 모니터링 공식 문서](https://spark.apache.org/docs/latest/monitoring).

## 스택별 역할과 적재 방식

제품 v1은 Spark Structured Streaming의 마이크로배치 모드와 `foreachBatch`를 사용한다. 정상 주문은 콜백에 전달된 배치 DataFrame을 Spark JDBC writer로 쓰고, ClickHouse 공식 JDBC Driver를 통해 적재한다. 아래는 선택한 설계이며 구현 완료를 의미하지 않는다.

```text
로그 생성기 → Kafka 원본 토픽 → Spark Structured Streaming
                              └─ JSON 파싱·검증·필터링·평탄화
                                 └─ foreachBatch(batch_df, batch_id)
                                    ├─ 정상 주문 → Spark JDBC writer
                                    │              → ClickHouse JDBC Driver → ClickHouse
                                    └─ 불량 로그 → Spark Kafka writer → Kafka DLQ

Grafana → ClickHouse 조회 → 대시보드 표시
```

| 구성 요소 | 역할 |
| --- | --- |
| 로그 생성기 | 입력 계약에 맞는 합성 로그와 검증용 불량 로그를 생성하고 원본 bytes를 Kafka로 전송한다. |
| Kafka | 원본 로그를 보관하고 Spark가 offset 기준으로 읽거나 재처리할 수 있게 한다. 불량 로그는 별도 DLQ 토픽에 보관한다. |
| Spark Structured Streaming | Kafka 소비, 트리거 실행, 분산 변환, 체크포인트 기반 진행 상태 복구를 담당한다. 변환 규칙은 애플리케이션에서 정의한다. |
| `foreachBatch` | 마이크로배치마다 배치 DataFrame과 `batch_id`를 받는 출력 진입점이다. 정상 주문 적재와 DLQ 기록을 호출하고 실패를 상위 쿼리에 전달한다. 자체적으로 DB 연결이나 중복 제거를 제공하지 않는다. |
| Spark JDBC writer | 콜백의 정상 주문 DataFrame을 `write.format("jdbc").mode("append")` 또는 `write.jdbc`로 적재한다. 파티션별 쓰기와 JDBC 배치 크기·동시 연결 수를 제어한다. |
| ClickHouse JDBC Driver | Spark executor에서 ClickHouse에 연결하고 Spark JDBC writer가 요청한 INSERT를 실행한다. 드라이버 클래스는 `com.clickhouse.jdbc.ClickHouseDriver`를 사용한다. |
| ClickHouse | 정제 데이터를 저장하고 SQL 집계를 제공한다. 테이블 구조와 중복 처리 방식은 Spark 출력 계약과 함께 설계한다. |
| Grafana | ClickHouse를 조회해 주문·매출 등의 지표를 시각화한다. |

### 쓰기 보장과 장애 경계

- `foreachBatch`의 기본 쓰기 보장은 최소 한 번(at-least-once)이다. 체크포인트와 `batch_id`만으로 ClickHouse의 중복 적재가 자동 방지되지는 않는다.
- 마이크로배치 하나는 여러 Spark 파티션과 JDBC INSERT로 나뉠 수 있다. JDBC의 배치 크기는 스트리밍 트리거의 입력 건수와 별개다. 마이크로배치 전체를 하나의 ClickHouse 트랜잭션으로 간주하지 않는다.
- 일부 JDBC 쓰기 성공 후 태스크 또는 쿼리를 재실행하면 중복이 발생할 수 있다. Kafka 원본 위치(topic·partition·offset)에 따른 재처리 중복과 `event_id`가 같은 이벤트의 재발행을 구분해 정책을 정한다.
- 정상 적재와 DLQ 기록은 원자적으로 완료되지 않는다. 어느 한쪽이라도 실패하면 예외를 숨기지 않고 쿼리를 실패시키며, 복구 후 같은 체크포인트로 재시작한다. 이미 성공한 출력의 중복 처리도 필요하다.
- 실패는 빨리 드러내고 재시작은 외부 관리자가 맡는다(4단계 추가). 앱은 스스로 재시도·재시작하지 않는다. 구현·검증: [멈춤 감시·자동 재시작 검증](04-clickhouse-storage.md#멈춤-감시자동-재시작-검증-2026-11-18).
  - 멈춤 감시: 실패가 예외로 끝나지 않고 멈추는 경우가 있다([4단계 DLQ 쓰기 실패 주입](04-clickhouse-storage.md#dlq-쓰기-실패-주입-결과-2026-11-18)에서 producer 네트워크 스레드가 죽어 `flush`가 무기한 대기). 배치가 멈추면 진행 이벤트도 오지 않으므로 리스너만으로는 알 수 없다. Driver 감시 스레드가 마지막 진행·유휴 이벤트 시각을 보고, `SPARK_STALL_TIMEOUT_SECONDS` 동안 없으면 로그를 남기고 프로세스를 exit 1로 끝낸다. 멈춘 태스크는 정상 종료(`query.stop()`)도 끝나지 않을 수 있어 바로 종료한다.
  - 자동 재시작: Compose `restart: on-failure`(횟수 상한 없음)가 같은 체크포인트로 다시 띄운다. 예외로 끝난 실패(exit 1)도 같이 재시작된다. 상한을 두지 않는 이유: Docker의 `on-failure:N` 횟수는 컨테이너 수명 동안 누적되고 정상 가동 뒤에도 초기화되지 않아, 무관한 장애가 쌓이면 Spark가 영구 정지한다. 지속 실패(독 배치, 보존 기간 지난 Kafka offset 등)는 재시작 간격 최대 1분(100ms에서 두 배씩 증가, 10초 이상 가동하면 초기화)으로 계속 반복되므로, 반복 실패 감지는 `RestartCount` 증가 경보(5단계 Grafana)로 한다.
  - DLQ 전송 타임아웃: DLQ producer는 `delivery.timeout.ms` 30초·`request.timeout.ms` 15초로 둔다(기본 120초·30초). 전송 실패가 30초 안에 예외로 드러나 시도 행(`dlq_write_action`)이 남는다. 네트워크 스레드가 죽은 경우는 타임아웃이 동작하지 않으므로 멈춤 감시가 필요하다.
  - 비교한 대안: DLQ 쓰기를 앱 안 재시도로 감싸기(실패를 숨김), 사람이 재시작(4단계 관찰에서 3.8분간 감지 못 함), DLQ 실패를 무시하고 배치 성공 처리(유실).
- 적재 방식은 Spark의 범용 JDBC writer와 ClickHouse JDBC Driver 조합으로 고정한다. Spark·Java·JDBC Driver 버전과 타입 매핑은 구현 전에 호환성을 확인해 고정한다.

참고: [Spark foreachBatch](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#using-foreach-and-foreachbatch), [Spark JDBC 설정](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html), [ClickHouse Spark JDBC 연동](https://clickhouse.com/docs/integrations/apache-spark/spark-jdbc).

## 다음 설계·개발 순서

아래 항목은 남은 작업이며 각 항목의 완료 결과를 보고한 뒤 사용자 요청에 따라 다음 항목을 진행한다. 요구사항이 바뀌면 관련 PRD를 먼저 갱신한다.

1. **조회 요구사항 정의:** 초기 대시보드 지표와 조회 조건을 정한다. 주문 수·매출·상품 판매량 등의 계산 기준, 시간 기준, 집계 차원을 명시한다.
2. **파싱·평탄화 및 출력·저장 계약 정의:** 아래 파싱·분류 규칙과 평탄화 매핑을 기준으로 Spark 출력을 정의한다. 이후 ClickHouse 타입·테이블 초안과 운영 지표 저장 구조를 [ClickHouse PRD](04-clickhouse-storage.md)에 정의한다.
3. **검증·실패·중복 정책 정의:** 정상·제외·불량의 판별 규칙, DLQ 스키마·오류 코드, 재시작·재처리 방식, 부분 적재 후 중복 처리 정책을 확정한다. ClickHouse 엔진과 조회 시 중복 처리에 필요한 결정도 함께 기록한다.
4. **실행 환경과 검증 기준 확정:** Spark·Java·JDBC Driver 버전, 실행 자원, 트리거 주기, 체크포인트 경로, JDBC 배치 크기·쓰기 병렬도, 처리량·지연 목표를 정한다. 실행 자원·트리거·체크포인트·JDBC 쓰기 초기값은 [실행 설정과 자원 예산](#실행-설정과-자원-예산)에 계산값으로 정의했고 버전과 실측 검증이 남았다. 3단계에서 사용할 정상 출력 검증 경로와 4단계 실제 적재 검증의 경계도 명시한다.
5. **3단계 구현·검증:** Kafka 소비, 검증·변환, `foreachBatch` 출력 경로, DLQ와 계측을 구현한다. 표준 샘플, 건수 대조, 체크포인트 복구, 실패·재처리 동작을 검증하고 결과와 남은 작업을 보고한다.
6. **4단계 ClickHouse 연동·검증:** 사용자 요청 후 실제 테이블·JDBC 적재를 구성하고 타입 매핑, 부분 실패·중복, 집계 정확성, 조회 가능 지연을 검증한다.

조회 요구사항에는 [주문 현황과 파이프라인 운영 대시보드](05-grafana-dashboard.md)를 모두 포함한다. 출력·저장 계약을 정할 때 단계별 운영 지표의 수집·저장 경로도 함께 정의한다.

## 범위 및 후속 정의 사항

health_check를 필터링하고 주문 필수 필드를 검증한다. 정상 주문은 아래 매핑에 따라 평탄화한다. v1은 items 길이가 1인 주문만 허용하며 정상 주문 이벤트 1건을 1행으로 출력한다. 불량 로그는 원문과 오류 사유를 포함해 `dlq.ecommerce.orders.v1` 토픽으로 격리한다. 이벤트 기준으로 처리 건수를 확인한다. DLQ 상세 스키마와 오류 코드는 이 단계에서 확정한다. 이번 구현 범위는 DLQ 기록·조회·원문 복원 검증까지이며, 자동 재처리와 수정 데이터 재투입 기능은 후속 작업으로 보류한다.

입력 계약은 [로그 생성기](01-log-generator.md)와 [데이터 계약](../../contracts/README.md)의 중첩 스키마를 따른다. JSON 파싱 후 `event_name`과 `schema_version`으로 계약을 선택해 검증한다. 정상 health_check만 제외하며 주문 스키마 검증을 적용하지 않는다. health_check에는 `context`·`properties`가 없다.

## 파싱·이벤트 분류 규칙

아래는 확정한 기능 요구사항이며 `apps/streaming`에 구현했다. 정적 DataFrame·파일 스트림 검증 결과는 아래 지연 계측 검증 결과를 참고한다. Kafka 실환경의 일부 검증은 남아 있다. 원본 이벤트 스키마와 버전은 변경하지 않는다.

1. **원본 확보:** Kafka `value`를 `raw_value`(BinaryType)로 보존하고 topic·partition·offset·timestamp를 확보한다. 파싱 실패 시에도 원문 bytes와 원본 위치를 추적할 수 있어야 한다. Kafka null value는 정상 이벤트로 간주하지 않으며 null과 빈 bytes를 구분해 격리한다.
2. **JSON 파싱:** UTF-8 JSON 객체 1개를 입력으로 받는다. 잘못된 UTF-8, 빈 값, JSON 문법 오류는 파싱 실패로 분류하고, 객체가 아닌 유효 JSON은 스키마 위반으로 분류한다. 파서의 관대한 확장 문법으로 잘못된 JSON을 정상 수용하지 않는다.
3. **계약 선택:** 지원 조합은 `order_completed` / `1.0.0`, `health_check` / `1.0.0`이다. 분류 필드 누락·타입 오류와 미지원 이벤트·버전은 불량으로 격리한다.
4. **계약 검증:** 해당 JSON Schema의 필수값·원래 JSON 타입·null 금지·허용 필드·형식·enum·범위·배열 길이를 검증한다. 중첩 객체에도 `additionalProperties=false`를 적용한다. `pattern`은 JSON Schema 규정대로 ECMA-262 의미로 해석해 값 전체가 일치해야 하며, 끝에 개행이 붙은 값(예: `"usr_00001\n"`)은 거부한다. `event_name=health_check`라는 이유만으로 검증 없이 제외하지 않는다.
5. **의미 검증:** 정상 주문 후보에는 데이터 계약의 금액 계산, 할인 상한, 상품 풀 일관성과 지역·도시 조합 규칙을 적용한다. 풀 기준과 배포 방식은 [3단계 구현 결정](#3단계-구현-결정)을 따른다. 이와 별도로 저장 제약에서 온 [이벤트 시각 범위 규칙](#이벤트-시각-범위-규칙)을 적용한다.
6. **변환·평탄화:** 검증을 통과한 주문만 아래 명시적 Spark 타입으로 변환한다. 변환 불가·표현 범위 초과는 변환 실패로 격리하고, 기본값 대입·절삭·반올림·대소문자 보정으로 오류를 숨기지 않는다.
7. **출력 분류:** 정상 주문, 정상 health_check 제외, 불량 격리 중 하나로만 분류한다. 분류 자체는 입력 이벤트마다 한 번이며 출력 재시도의 중복 문제와 구분한다.

Spark 파싱 스키마는 명시적 `StructType`으로 정의하며 실행 데이터의 스키마 추론에 의존하지 않는다. `from_json` 등 타입 파싱 결과만으로 JSON Schema 검증이 완료됐다고 가정하지 않는다. 문자열·숫자·불리언의 자동 변환과 누락 필드의 null 처리로 원래 타입 오류를 놓치지 않도록 원본 기준 검증을 함께 수행한다. 엄격한 검증의 구현 방식은 [3단계 구현 결정](#3단계-구현-결정)을 따른다.

### 정상·제외·불량 분류

| 입력 | 분류 | 처리 |
| --- | --- | --- |
| 계약·의미 규칙·타입 변환을 통과한 주문 | 정상 주문 | 평탄화 결과를 정상 출력에 전달 |
| health_check 계약을 통과한 이벤트 | 제외 | 제외 건수만 집계하고 정상 출력·DLQ에는 보내지 않음 |
| UTF-8·JSON 파싱 실패 | 불량 / 파싱 | 원문과 Kafka 위치를 보존해 DLQ 대상으로 전달 |
| 필수값·타입·형식·허용값·배열 길이 위반 | 불량 / 스키마 검증 | 실패 경로와 사유를 보존해 DLQ 대상으로 전달 |
| 미지원 이벤트·버전 | 불량 / 계약 선택 | 식별 가능한 이벤트·버전과 사유를 보존 |
| 금액·상품·지역 의미 규칙 위반, 이벤트 시각 범위 밖 주문 | 불량 / 의미 검증 | 위반 규칙과 사유를 보존 |
| timestamp·정수 변환 실패 또는 범위 초과 | 불량 / 타입 변환 | 원래 값과 실패 경로를 원문에서 추적 가능하게 보존 |

실패 단계·필드 경로·사유를 식별할 수 있게 한다. DLQ 오류 코드와 메시지 스키마는 [3단계 구현 결정](#3단계-구현-결정)을 따른다. JDBC 연결 실패 등 인프라 장애는 이벤트 불량으로 분류하지 않는다.

### 이벤트 시각 범위 규칙

- 규칙: 정상 주문 후보의 `event_timestamp`가 `callback_started_at − 24시간 ≤ event_timestamp ≤ callback_started_at + 5분`(양 끝 포함)을 벗어나면 `semantic`/`SEMANTIC_TIMESTAMP_OUT_OF_RANGE`/경로 `event_timestamp`로 DLQ에 보낸다. 기준 시각 `callback_started_at`은 배치 콜백 진입 시각이며 같은 배치의 `spark_processed_at`·운영 지표와 같은 값이다.
- 이유 1(독성 배치): `silver.order_events`는 `toStartOfHour(event_timestamp)` 파티션이고 ClickHouse는 INSERT 블록 1개가 파티션 100개를 넘으면 기본값(`max_partitions_per_insert_block=100`, `throw_on_max_partitions_per_insert_block=1`)으로 INSERT를 거부한다. 계약은 `event_timestamp` 형식만 검사하므로, 시각이 100시간 넘게 퍼진 배치는 같은 offset을 재시도할 때마다 실패해 뒤의 모든 배치를 막는다. 허용 구간은 24시간 5분이라 한 배치가 걸칠 수 있는 시간 파티션은 최대 26개다.
- 이유 2(조용한 손실): TTL이 `event_timestamp + 24시간`이므로 하한보다 오래된 주문은 INSERT 시점에 이미 만료돼 다음 TTL 병합에서 사라진다. 적재 성공으로 세면서 데이터가 없어지는 대신 원문을 DLQ에 남긴다. 장애 후 24시간 넘은 backlog를 재처리하는 경우도 같다.
- 경계값: 하한 24시간은 [ClickHouse TTL](04-clickhouse-storage.md#이벤트-식별과-테이블-기준)과 같은 값이다. 상한 5분은 생성기·Spark 시계 차이 허용분이다(로컬은 같은 호스트 시계). 환경 변수로 두지 않고 `transforms.EVENT_TIME_MAX_AGE`·`EVENT_TIME_MAX_AHEAD` 상수로 둔다. TTL을 바꾸면 `infra/clickhouse/schema.sql`과 이 상수를 함께 바꾼다(Spark 컨테이너에 `infra/`가 마운트되지 않아 자동 대조 테스트는 두지 않고 양쪽 주석으로 서로를 가리킨다).
- 적용 위치: `classify()`는 시계를 쓰지 않는 순수 함수로 유지한다. `classify_batch(source, callback_started_at)`가 UDF 결과가 `order`인 행에만 이 규칙을 DataFrame 표현식으로 적용해 `result`를 DLQ로 덮어쓴다. `persist()`와 건수·지연 집계 전이므로 건수, 분류별 지연, 정상 적재, DLQ 쓰기가 같은 분류를 보며 `input = order + excluded + dlq`가 유지된다. 다른 불량 사유가 있으면 그 사유가 먼저 기록된다(오류 1건만 기록).
- health_check는 적용하지 않는다. ClickHouse에 저장하지 않아 파티션·TTL 제약이 없으며 계속 제외로 센다.
- 제약: 같은 offset을 재시도하면 기준 시각이 바뀌므로 경계 근처 주문은 시도마다 분류가 달라질 수 있다. 최소 한 번 쓰기 전제에서 허용한다.

### 학습 노트: `filter()`와 `select()`의 차이

| 함수 | 대상 | 역할 | 이 프로젝트의 예시 |
| --- | --- | --- | --- |
| `filter(조건)` | 행(이벤트) | 조건이 참인 행을 남긴 새 DataFrame을 반환한다. | 검증을 마친 이벤트에서 주문 행을 선택하고 정상 health_check는 제외 |
| `select(컬럼 또는 표현식)` | 컬럼(필드) | 지정한 컬럼·표현식으로 구성된 새 DataFrame을 반환한다. | 중첩 필드를 꺼내 이름을 지정하고 평탄화 |

`filter()`의 조건은 제외할 조건이 아니라 **남길 조건**이다. 조건 결과가 false 또는 null인 행은 남지 않으므로, 분류 전에 필터링하면 잘못된 이벤트가 DLQ 없이 사라질 수 있다. 먼저 정상·제외·불량을 분류하고 각 경로의 건수를 집계한다.

```python
from pyspark.sql import functions as F

# 예시의 validated_df는 계약·의미 규칙 검증과 타입 변환을 통과한 이벤트만 포함한다.
# 정상 주문만 남긴다. 정상 health_check 제외 건수와 불량 경로는 별도로 관리한다.
orders_df = validated_df.filter(F.col("event_name") == "order_completed")

# 중첩 필드를 꺼내 최상위 컬럼 이름을 붙인다. 단순 컬럼 선택은 행을 제외하지 않는다.
example_flat_df = orders_df.select(
    F.col("event_id"),
    F.col("context.device.os_version").alias("os_version"),
    F.col("properties.order_id").alias("order_id"),
)
```

위 코드는 두 함수의 차이를 설명하기 위한 일부 컬럼 예시이며 실제 출력 계약이 아니다. 실제 평탄화에서는 아래 매핑의 모든 이벤트 컬럼과 Kafka 추적 컬럼을 선택한다. `select()`는 컬럼 선택뿐 아니라 이름 지정·계산·타입 변환 표현식에도 사용할 수 있다. 배열을 펼치는 표현식 등은 행 수도 바꿀 수 있으므로 단순 컬럼 선택과 구분한다.

두 함수는 원본 DataFrame을 직접 변경하지 않는다. 또한 위 변환을 선언하는 것만으로 쓰기가 실행되지는 않으며, 실제 실행은 출력 action이나 스트리밍 쿼리 실행 경로에서 이루어진다. 원본 Kafka 로그를 삭제하는 작업도 아니다.

현재 필터링 대상은 계약을 통과한 health_check다. 정상 주문의 매출 0원·할인 0원·`utm_source=direct`·`utm_medium=none`과 특정 지역·결제수단·회원 등급은 노이즈로 제외하지 않는다. 불량은 DLQ, 중복은 별도 중복 정책으로 처리한다.

참고: [Spark DataFrame.filter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html), [Spark DataFrame.select](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html).

## 정상 주문 평탄화 매핑

정상 주문 이벤트 1건당 1행이다. 모든 원본 리프 필드를 우선 보존하며 컬럼 축소는 별도 요구사항 변경으로 관리한다. 아래 컬럼은 정상 출력에서 필수이며 원본에 null을 허용하지 않는다. Spark 파서의 nullable 설정만으로 필수값 검증을 대신하지 않는다.

| 원본 JSON 경로 | 출력 컬럼 | Spark 타입 |
| --- | --- | --- |
| `event_id` | `event_id` | StringType |
| `event_timestamp` | `event_timestamp` | TimestampType |
| `event_name` | `event_name` | StringType |
| `schema_version` | `schema_version` | StringType |
| `context.user_id` | `user_id` | StringType |
| `context.session_id` | `session_id` | StringType |
| `context.is_logged_in` | `is_logged_in` | BooleanType |
| `context.user_tier` | `user_tier` | StringType |
| `context.device.os` | `os` | StringType |
| `context.device.os_version` | `os_version` | StringType |
| `context.device.app_version` | `app_version` | StringType |
| `context.device.device_model` | `device_model` | StringType |
| `context.device.device_type` | `device_type` | StringType |
| `context.network.carrier` | `carrier` | StringType |
| `context.network.country` | `country` | StringType |
| `context.network.region` | `region` | StringType |
| `context.network.city` | `city` | StringType |
| `context.page.current_url` | `current_url` | StringType |
| `context.page.referrer_url` | `referrer_url` | StringType |
| `context.page.utm_source` | `utm_source` | StringType |
| `context.page.utm_medium` | `utm_medium` | StringType |
| `properties.order_id` | `order_id` | StringType |
| `properties.payment_method` | `payment_method` | StringType |
| `properties.currency` | `currency` | StringType |
| `properties.total_amount` | `total_amount` | LongType |
| `properties.discount_amount` | `discount_amount` | LongType |
| `properties.items[0].product_id` | `product_id` | StringType |
| `properties.items[0].product_name` | `product_name` | StringType |
| `properties.items[0].category` | `category` | StringType |
| `properties.items[0].price` | `price` | LongType |
| `properties.items[0].quantity` | `quantity` | LongType |

- `items` 길이가 1임을 검증한 뒤 유일한 상품을 꺼낸다. 빈 배열·null·2건 이상을 첫 요소 추출이나 행 누락으로 숨기지 않고 스키마 위반으로 분류한다.
- `event_timestamp`는 UTC·밀리초 3자리·`Z` 형식과 실제 달력 시각을 검증한 뒤 변환한다. Spark 세션 시간대는 UTC로 고정하고 원래 밀리초 정밀도를 보존한다. 화면의 한국 시간 변환은 Grafana 표시에서 수행한다.
- 금액은 원 단위 정수이며 부동소수점으로 변환하지 않는다. `LongType` 표현 범위를 넘는 값은 원본 스키마 위반과 구분되는 변환 실패다. 금액 계산 검증에서도 곱셈·합계의 오버플로로 잘못 통과하지 않도록 한다.
- 문자열 식별자와 버전은 숫자로 변환하지 않는다. 문자열 공백 제거·불리언 강제 변환 등 원본 의미를 바꾸는 보정을 수행하지 않는다.
- 업무 분석용 데이터 보강(파생 컬럼·외부 데이터 조인)은 추후 ClickHouse·Grafana 조회 쿼리 설계 시 필요성을 검토한다. Kafka 추적 컬럼과 Spark 배치 식별자는 운영·추적 목적이므로 별도로 유지한다. 중복 제거 정책은 파싱·평탄화와 별도 정의한다. 원본 bytes는 분류·DLQ·추적을 위해 유지하되 정상 적재 컬럼에는 포함하지 않는다.

### 데이터 타입 변환과 포맷 표준화

평탄화는 중첩 경로를 최상위 컬럼으로 옮기는 작업이고, 타입 변환은 그 값을 분석·저장에 사용할 Spark 타입으로 표현하는 작업이다. 원본부터 형식이 통일돼 있으므로 잘못된 값을 임의로 고치지 않고 계약 검증 후 아래 기준을 적용한다.

| 출력 컬럼 | 원본 JSON 타입 → Spark 타입 | 표준화·검증 기준 |
| --- | --- | --- |
| `event_timestamp` | 문자열 → TimestampType | UTC·밀리초 3자리·`Z` 형식과 실제 시각을 검증한 뒤 변환. 밀리초 정밀도 유지 |
| `total_amount`, `discount_amount` | 정수 → LongType | KRW 원 단위, 0 이상. 소수점·반올림·단위 변환 없이 유지 |
| `price` | 정수 → LongType | KRW 원 단위, 1 이상. 부동소수점으로 변환하지 않음 |
| `quantity` | 정수 → LongType | 상품 수량, 1 이상. 소수 수량을 절삭하지 않음 |
| `is_logged_in` | 불리언 → BooleanType | 원본 true·false 유지. 문자열이나 숫자로부터 강제 변환하지 않음 |
| `os_version`, `app_version`, `schema_version` | 문자열 → StringType | 버전 구분자·자릿수 보존. `18.1.1` 같은 값을 숫자로 변환하지 않음 |
| `event_id`, `order_id`, `user_id`, `session_id`, `product_id` | 문자열 → StringType | 접두사·선행 0·UUID 형식 보존 |
| `currency`, `country`, `user_tier` 등 계약상 코드 | 문자열 → StringType | `KRW`, 국가 코드 형식, `STANDARD`·`VIP` 등의 계약 기준 검증. 임의 대소문자 보정 없음 |

- `"27000"`, `"true"`처럼 계약과 다른 JSON 타입은 자동 변환으로 수용하지 않고 스키마 위반으로 격리한다. 원본 계약에는 맞지만 Spark 타입으로 표현할 수 없는 값은 타입 변환 실패로 구분한다.
- TimestampType은 시각 값이며 문자열 출력 포맷과 구분한다. 한국 시간 표시는 Grafana에서 수행하고 UTC 저장값을 덮어쓰지 않는다.
- 금액의 최솟값, 수량 범위와 코드 허용값의 기준은 기존 JSON Schema를 따른다. 이 절은 원본 계약이나 버전을 변경하지 않는다.

### Kafka 원본 추적 컬럼

| Kafka 원본 | 출력 컬럼 | Spark 타입 | 처리 기준 |
| --- | --- | --- | --- |
| `topic` | `kafka_topic` | StringType | 모든 분류에서 원래 값 보존 |
| `partition` | `kafka_partition` | IntegerType | 모든 분류에서 원래 값 보존 |
| `offset` | `kafka_offset` | LongType | 모든 분류에서 원래 값 보존 |
| `timestamp` | `kafka_append_timestamp` | TimestampType | LogAppendTime임을 확인한 경우에만 이 의미로 사용 |

Kafka timestamp 종류도 분류 과정에서 확인한다. LogAppendTime이 아니거나 메타데이터가 유효하지 않으면 인프라·설정 오류로 드러내고 해당 값을 append 시각으로 사용하지 않는다. Spark 쿼리·실행·배치 식별자는 `foreachBatch` 출력 설계에서 추가하며 이번 원본 필드 매핑과 구분한다.

### 파싱·평탄화 검증 기준

- 표준 정상 주문 샘플은 위 31개 이벤트 컬럼과 4개 Kafka 추적 컬럼을 가진 1행으로 출력한다. `total_amount=27000`, `discount_amount=3000`, `price=15000`, `quantity=2`와 원본 식별자·시간·집계 차원 값이 보존돼야 한다.
- 표준 정상 health_check는 제외 1건이며 정상 주문 출력과 DLQ 대상은 0건이다.
- 표준 불량 3종은 각각 JSON 파싱 실패, `properties.order_id` 누락, `properties.total_amount` 타입 오류로 분류한다. 실패 경로와 원문 bytes·Kafka 위치를 복원할 수 있어야 한다.
- 추가 경계 입력으로 잘못된 UTF-8·빈 값·null value, 비객체 JSON, 미지원 이벤트·버전, 잘못된 health_check, 정의 외 필드·null, 잘못된 날짜, items 0건·2건, 숫자 문자열·불리언, 의미 규칙 위반과 정수 범위 초과를 검증한다.
- 같은 입력 처리 시도에서 `입력 건수 = 정상 주문 + health_check 제외 + 불량 격리 대상`을 만족해야 한다. 재시도별 건수와 최종 성공 건수는 분리한다.

참고: [Spark from_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html), [Spark JSON 옵션](https://spark.apache.org/docs/latest/sql-data-sources-json.html), [Spark LongType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.LongType.html).

## 실행 설정과 자원 예산

Spark는 단일 컨테이너의 local 모드로 실행한다. 아래 값은 PRD 부하 기준의 계산으로 정한 초기 설정이다. `.env.example`과 `compose.yaml`에 같은 이름으로 반영했으며 실측 결과는 아래 지속 부하 측정 결과를 따른다.

### 계산 전제

- 입력: 전체 5,000 EPS, 평균 793B, 약 3.97MB/s. 원본 토픽 파티션 3개. 정상 주문 4,250건/s.
- 자원: [개발환경](00-development-environment.md)의 WSL 메모리 12GB·CPU 8개. Kafka 2GiB·CPU 2개, Kafka UI 1GiB·CPU 1개가 이미 배정되어 있다.
- 반영 지연: [대시보드 조회 조합 비교](04-clickhouse-storage.md#중복-제거조회-조합-비교) 기준으로 대시보드 갱신 대기 60초가 가장 큰 항이다.

### 환경변수와 초기값

| 환경변수 | 초기값 | 근거 |
| --- | --- | --- |
| `SPARK_MASTER` | `local[3]` | 원본 파티션 3개와 태스크를 1:1로 맞춘다. 늘려도 Kafka 읽기 병렬도는 파티션 수 3에서 막힌다. |
| `SPARK_DRIVER_MEMORY` | `2g` | local 모드에서는 driver heap이 실행 메모리 전체다. 배치 최대 원문 39.7MB(50,000 × 793B)를 파싱·컬럼 전개로 10배 잡아도 약 400MB다. |
| 컨테이너 `mem_limit` / `cpus` | `3g` / `3` | heap 2g에 JVM 비heap·네이티브 약 1g를 더한다. Kafka 2 + UI 1 + Spark 3 = 6GiB이며 ClickHouse·Grafana·개발 도구에 약 6GB가 남는다. |
| `SPARK_TRIGGER_INTERVAL` | `5 seconds` | 배치당 약 25,000건(19.8MB). 반영 지연에 최대 5초를 더하며 60초 갱신 대기보다 작다. |
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | `50000` | 배치당 입력 상한은 평상시 예상 25,000건의 2배다. 5초 내 처리 시에만 최대 10,000 EPS에 해당하며 처리속도를 보장하지 않는다. 첫 지속 부하 실측에서는 약 50,000건 처리에 평균 16.05초가 걸렸고, 2단 검증 적용 후 재측정에서는 배치당 약 25,000건을 평균 2.04초에 처리했다. |
| `SPARK_SHUFFLE_PARTITIONS` | `3` | `local[3]` 태스크 슬롯 수와 같게 두어 셔플 후 태스크가 한 번에 모두 실행된다. 슬롯보다 많으면 태스크가 여러 차례로 나뉘어 실행될 뿐 병렬도는 늘지 않고, 배치당 Parquet 파일 수만 늘어난다. 4단계 `SPARK_JDBC_NUM_PARTITIONS`와 같은 값이다. 체크포인트에 기록되므로 변경하려면 체크포인트를 새로 만든다. |
| `SPARK_JDBC_BATCH_SIZE` (4단계 추가) | `50000` | `maxOffsetsPerTrigger`와 같게 두어 파티션 편중과 무관하게 쓰기 파티션당 INSERT 1회로 만든다. |
| `SPARK_JDBC_NUM_PARTITIONS` (4단계 추가) | `3` | ClickHouse 동시 연결 3개. 배치당 part 3개, 약 0.6 part/s다. |
| `SPARK_CHECKPOINT_DIR` | `/opt/spark/checkpoints/orders` | 볼륨에 마운트한다. Git에서 제외한다. |
| `SPARK_OUTPUT_DIR` (4단계 제거) | - | 3단계 정상 주문 검증용 Parquet 경로였다. 4단계 JDBC writer 교체와 함께 제거했다. |
| `SPARK_CLICKHOUSE_JDBC_URL` (4단계 추가) | `jdbc:clickhouse:http://clickhouse:8123?clickhouse_setting_async_insert=0` | 동기 INSERT를 서버 설정으로 명시한다. 근거는 [JDBC 쓰기 설정](04-clickhouse-storage.md#jdbc-쓰기-설정). |
| `SPARK_CLICKHOUSE_HTTP_URL` (4단계 추가) | `http://clickhouse:8123` | 운영 지표 HTTP INSERT 주소. |
| `CLICKHOUSE_SPARK_PASSWORD` (4단계 추가) | `.env`에만 설정 | `spark_writer` 계정 비밀번호. 계정 이름은 코드·`init.sh`에 고정한다. |
| `SPARK_STARTING_OFFSETS` | `earliest` | [Kafka PRD](02-kafka-ingestion.md)의 최초 실행 기준. 재시작은 체크포인트 위치가 우선한다. |
| `SPARK_KAFKA_BOOTSTRAP_SERVERS` | `kafka:19092` | Compose 내부 주소. |
| `SPARK_LATENCY_METRICS_ENABLED` | `true` | `false`면 지연 분위수 집계를 생략하고 건수만 집계한다. [계측 오버헤드 대조군](#3-계측-오버헤드-대조군) 전용이다. 값은 `true`·`false`만 허용하며, 기존 로컬 `.env`와 호환되도록 Compose가 기본값 `true`를 넣는다. |
| `SPARK_STALL_TIMEOUT_SECONDS` (4단계 추가) | `90` | 진행·유휴 이벤트가 이 시간 동안 없으면 멈춤으로 보고 exit 1([쓰기 보장과 장애 경계](#쓰기-보장과-장애-경계)). 정상 실패가 예외로 드러나는 최대 시간(DLQ 전송 30초, Kafka 메타데이터 대기 기본 60초)보다 길고, 평시 배치 2~5초·유휴 이벤트 간격 약 10초보다 충분히 길다. 기존 로컬 `.env`와 호환되도록 Compose가 기본값 `90`을 넣는다. |

- 토픽 이름은 기존 `KAFKA_TOPIC`·`KAFKA_DLQ_TOPIC`을 재사용한다. JDBC 배치 크기·쓰기 병렬도는 3단계에서 사용하지 않으므로 ClickHouse JDBC URL·계정과 함께 4단계에서 추가한다.
- `SPARK_MASTER`·`SPARK_DRIVER_MEMORY`는 Compose가 `spark-submit`의 `--master`·`--driver-memory`로 전달한다. 나머지는 `config.py`가 기본값 없이 읽는다.
- Spark·Java·PySpark·Kafka 커넥터 버전은 [3단계 구현 결정](#3단계-구현-결정)에 고정했다. JDBC Driver 버전은 4단계에서 확정한다.

### 값 선택 이유

- **`maxOffsetsPerTrigger`는 필수다.** 없으면 `earliest` 첫 배치가 보관분 전체를 읽는다. 1시간 보관분만 약 1,800만 건·14.3GB로 heap 2g에서 처리할 수 없다.
- **JDBC 배치 크기 기본값 1,000을 쓰지 않는다.** 쓰기 파티션당 약 7,083행이 INSERT 8회로 나뉘어 초당 약 4.8개 part가 생긴다. 50,000이면 약 0.6 part/s이고 INSERT당 7,000행 이상이 되어 ClickHouse 병합 부담이 줄어든다.
- **트리거를 5초보다 줄이지 않는다.** 화면 반영 지연은 60초 갱신 대기가 지배하므로 트리거 단축 이득이 작고 배치·part 수만 늘어난다.

### 확인 필요

- 메모리 10배 전개율은 가정이다. 엄격 검증에 Python UDF를 사용하므로 Python worker(최대 3개)가 heap 밖 약 1g 여유를 함께 쓴다. 부하 측정에서 `docker stats`로 컨테이너 메모리를 기록하고, 2.7g(90%)을 넘으면 `mem_limit`을 먼저 조정한다.
- CPU는 Kafka 2 + UI 1 + Spark 3 + 생성기 1에 ClickHouse가 더해지면 8개를 넘어 경합한다. 5,000 EPS 유지 여부를 실측한다.
- `foreachBatch` 안에서 AQE가 셔플 파티션을 합쳐 쓰기 파티션이 3보다 줄어드는지 확인한다.
- 배치 처리시간이 트리거 주기 5초를 넘지 않는지, backlog가 해소되는지 실측한다.
- 결과(2026-11-19 정리):
  - 메모리: 60분 진단 실행에서 Spark 컨테이너 최대 약 2.34GiB/3GiB로 2.7g(90%) 미만이었다. `mem_limit` 조정은 필요 없다.
  - CPU 경합·5,000 EPS 유지, 트리거 5초·backlog 해소: ClickHouse까지 함께 띄운 4단계 지속 부하(12분·25분)에서 [4단계 완료 기준](04-clickhouse-storage.md#완료-기준)을 통과했다.
  - AQE 파티션 병합: 별도로 확인하지 않았다. 6단계 상세 세팅 조정에서 쓰기 파티션 수를 확인한다.

## 3단계 구현 결정

3단계 구현에서 확정한 값이다. 변경 시 이 절을 먼저 갱신한다. 단위·로컬 스트림 검증과 Kafka 지속 부하 검증을 구분한다.

### 버전

| 구성 | 버전 | 근거 |
| --- | --- | --- |
| Spark | 4.2.1 (Scala 2.13) | 2026-10-27 유지보수 릴리스. 4.3.0(2026-10-30)은 새 기능 릴리스라 안정화를 지켜본 뒤 검토한다. |
| 이미지 | `apache/spark:4.2.1-scala2.13-java17-python3-ubuntu` | 공식 이미지. Ubuntu 22.04, 사용자 `spark`(uid 185). |
| Java | OpenJDK 17 (이미지 내장 17.0.20.1) | Spark 4.2 공식 지원 Java 17/21/25 중 LTS 최소 버전. |
| Python / PySpark | 3.10.12 (이미지 내장) / 4.2.1 (`/opt/spark/python`) | Spark 4.2는 Python 3.10 이상을 요구한다. 생성기(3.12)와 프로세스가 분리되므로 이미지 버전을 그대로 쓴다. |
| Kafka 커넥터 | `spark-sql-kafka-0-10_2.13:4.2.1`, `spark-token-provider-kafka-0-10_2.13:4.2.1`, `kafka-clients:3.9.2`, `commons-pool2:2.13.1` | 커넥터 POM의 compile 의존성. lz4·snappy·zstd·slf4j는 Spark 기본 jar를 쓴다. 이미지 빌드 시 Maven Central에서 `/opt/spark/jars`로 받아 실행 시 네트워크 해석을 하지 않는다. kafka-clients 3.9는 브로커 4.4가 지원하는 클라이언트 범위(2.1 이상)다. |
| 검증 라이브러리 | `jsonschema==4.26.0`, `rfc3339-validator==0.1.4` | 계약 JSON Schema(Draft 2020-12)를 그대로 사용한다. `rfc3339-validator`가 없으면 `format: date-time`을 검사하지 않으므로 함께 고정하고 모듈 로드 시 존재를 확인한다. |
| 테스트 | `pytest==9.2.0` | 생성기와 같은 버전. 컨테이너 안에서 실행한다. |

참고: [Spark 4.2.1 문서](https://spark.apache.org/docs/4.2.1/), [Kafka 연동](https://spark.apache.org/docs/4.2.1/streaming/structured-streaming-kafka-integration.html).

### 엄격 검증 구현 방식

- 원본 주문의 중첩 구조·타입은 `schemas.ORDER_EVENT_SCHEMA`에 명시하고 `from_json`으로 구조화한다. `event_timestamp`는 원본 문자열로 읽은 뒤 `to_timestamp`로 변환하고 `select`로 31개 이벤트 컬럼을 평탄화한다. JSON Schema의 필수값·추가 필드·정규식 제약은 `StructType`으로 대체하지 않는다.
- `transforms.classify(value)`는 원본 bytes의 엄격한 계약·의미·표현 범위 검증만 담당하며 주문 행을 만들지 않는다. Spark는 오류 메타데이터만 `CLASSIFICATION_SCHEMA`의 Python UDF로 받는다. 검증을 통과한 주문만 `from_json(mode=FAILFAST)`에 전달한다. 불량 입력은 기존 DLQ로 보내고, 검증 통과 후 Spark 파싱 실패는 코드·스키마 불일치로 쿼리를 실패시킨다.
- JSON 파싱은 Python 표준 `json`으로 하되 `NaN`·`Infinity` 등 비표준 상수와 중복 키를 파싱 실패로 거부한다.
- 계약 검증은 `contracts/schemas`의 파일을 `Draft202012Validator`와 형식 검사기로 실행한다. 성능을 위해 `jsonschema-rs`를 1단으로 먼저 실행하고 유효로 판정된 문서만 바로 통과시킨다. 1단 무효·소수 포함 문서는 아래 엄격 검증기로 판정하므로 DLQ 오류 내용은 엄격 검증기 기준이다([처리량 병목](#처리량-병목-분류-단계-계약-검증-2026-11-17)). `integer`는 원래 JSON 정수만 허용하도록 재정의해 `27000.0`처럼 소수점 표기 숫자도 거부한다(JSON Schema 기본은 허용). `pattern`도 재정의한다. Python `re.search`의 `$`는 끝 개행 앞에서도 일치하므로 검증기가 `$` 앵커를 `\Z`로 바꿔 ECMA-262와 같이 전체 일치시키고 끝 개행을 `SCHEMA_PATTERN`으로 거부한다. 계약 스키마 파일은 바꾸지 않는다. 오류가 여러 개면 `best_match` 1건을 기록한다.
- 의미 검증의 상품·지역 풀은 생성기 `log_generator.pools`를 단일 기준으로 사용한다. Spark 컨테이너에 `apps/log_generator/src`를 읽기 전용으로 마운트하고 `PYTHONPATH`로 가져온다. 풀을 바꾸면 생성기와 Spark를 함께 재배포한다.
- 금액 계산은 Python 정수로 수행해 오버플로가 없고, 이후 타입 변환 단계에서 `LongType` 범위를 검사한다.
- 메모리 영향은 [확인 필요](#확인-필요)에 기록했다. 분류 결과는 배치에서 `persist`해 UDF가 집계·정상 출력·DLQ 쓰기마다 다시 실행되지 않게 한다.

### DLQ 메시지 스키마

키는 `{topic}:{partition}:{offset}` 문자열(UTF-8), 값은 아래 필드를 모두 포함하는 UTF-8 JSON 객체다. 값이 없는 필드는 생략하지 않고 `null`로 기록한다.

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| `dlq_schema_version` | string | `1.0.0` |
| `source_topic` / `source_partition` / `source_offset` | string / int / long | 원본 Kafka 위치 |
| `source_timestamp` | string | 원본 LogAppendTime. UTC `yyyy-MM-ddTHH:mm:ss.SSSZ` |
| `raw_value_base64` | string \| null | 원문 bytes의 표준 Base64(RFC 4648, 줄바꿈 없음). Kafka null value는 `null`, 빈 bytes는 `""`. Spark 4.2의 `base64()`는 기본값으로 76자마다 CRLF를 넣으므로 이미지의 `spark-defaults.conf`에 `spark.sql.chunkBase64String.enabled false`를 두어 컨테이너의 모든 세션(잡·테스트)에 적용한다. |
| `error_stage` | string | `parse`·`contract`·`schema`·`semantic`·`conversion` |
| `error_code` | string | 아래 오류 코드 |
| `error_path` | string \| null | 실패 필드 경로. 예: `properties.order_id`, `properties.items[0].price`, 루트는 `$` |
| `error_message` | string | 사람이 읽는 사유 |
| `event_name` / `schema_version` | string \| null | 원문에서 문자열로 식별된 경우만 기록 |
| `detected_at` | string | DLQ를 기록한 배치 쓰기의 시각(UTC). 이벤트별 감지 시각이 아니다. |
| `processor_version` | string | 처리기 버전. 예: `streaming 0.1.0` |

원문 복원은 `raw_value_base64`를 Base64 디코딩한다. 배치 재시도 시 같은 키의 DLQ 메시지가 중복 기록될 수 있으며 조회 시 키로 구분한다.

### 오류 코드

| 단계 | 코드 | 조건 |
| --- | --- | --- |
| parse | `NULL_VALUE` | Kafka value가 null |
| parse | `EMPTY_VALUE` | 0바이트 값 |
| parse | `INVALID_UTF8` | UTF-8 디코딩 실패(BOM 포함 시 JSON 실패) |
| parse | `INVALID_JSON` | JSON 문법 오류, 비표준 상수, 중복 키 |
| schema | `SCHEMA_TYPE` (경로 `$`) | 유효 JSON이지만 객체가 아님 |
| contract | `CONTRACT_FIELD_INVALID` | `event_name`·`schema_version` 누락 또는 문자열이 아님 |
| contract | `CONTRACT_UNSUPPORTED` | 지원하지 않는 이벤트·버전 조합 |
| schema | `SCHEMA_<키워드>` | JSON Schema 위반. 키워드를 대문자로 붙인다. 예: `SCHEMA_REQUIRED`(경로는 누락 필드), `SCHEMA_TYPE`, `SCHEMA_ADDITIONALPROPERTIES`(경로는 정의 외 필드), `SCHEMA_PATTERN`, `SCHEMA_ENUM`, `SCHEMA_CONST`, `SCHEMA_FORMAT`, `SCHEMA_MINIMUM`, `SCHEMA_MINITEMS`, `SCHEMA_MAXITEMS`, `SCHEMA_MINLENGTH` |
| semantic | `SEMANTIC_DISCOUNT_EXCEEDS` | 할인 > 상품 합계 |
| semantic | `SEMANTIC_AMOUNT_MISMATCH` | `total_amount ≠ 상품 합계 − 할인` |
| semantic | `SEMANTIC_PRODUCT_MISMATCH` | 상품 풀에 없는 ID 또는 이름·카테고리·가격 불일치 |
| semantic | `SEMANTIC_LOCATION_MISMATCH` | 국가·지역·도시 조합이 지역 풀에 없음 |
| semantic | `SEMANTIC_TIMESTAMP_OUT_OF_RANGE` (경로 `event_timestamp`) | 주문 `event_timestamp`가 [이벤트 시각 범위](#이벤트-시각-범위-규칙) `[콜백 진입 − 24시간, 콜백 진입 + 5분]` 밖 |
| conversion | `CONVERSION_TIMESTAMP` | 형식 검사를 통과했으나 시각으로 변환 불가. 방어용이며 잘못된 날짜·윤초 `60`·연도 `0000`은 `SCHEMA_FORMAT`이 먼저 거부한다. |
| conversion | `CONVERSION_LONG_RANGE` | 금액·가격·수량이 `LongType` 범위 초과 |

표준 불량 3종의 기대값은 `parse`/`INVALID_JSON`, `schema`/`SCHEMA_REQUIRED`/`properties.order_id`, `schema`/`SCHEMA_TYPE`/`properties.total_amount`다.

Kafka timestamp 종류가 LogAppendTime(`timestampType=1`)이 아니면 이벤트 불량이 아닌 설정 오류로 보고 `raise_error`로 쿼리를 실패시킨다.

### 3단계 정상 출력 검증 경로

- ClickHouse는 4단계이므로 3단계는 정상 주문(31개 이벤트 컬럼 + Kafka 추적 4개, 배치 내 `dropDuplicates(["event_id"])` 적용 후)을 `SPARK_OUTPUT_DIR`의 Parquet에 append한다. 경로는 체크포인트(`spark-checkpoints`)와 다른 named volume `spark-output`이며 Git 밖에 있다.
- 쓰기 보장은 최소 한 번이다. 배치 재시도 시 같은 `event_id` 행이 중복될 수 있으므로 검증은 `event_id` 고유 건수로 한다.
- 4단계 경계: `streaming.sinks.write_orders` 한 함수만 Spark JDBC writer로 교체하고 `SPARK_OUTPUT_DIR`·`spark-output` 볼륨을 제거한다. 분류·평탄화·DLQ·계측 로직은 바꾸지 않는다.
  - 4단계 구현(2026-11-17): 위 경계대로 교체했다. 추가로 [ClickHouse 컬럼 타입](04-clickhouse-storage.md#order_events-컬럼-타입)에 따라 정상 주문에 `spark_query_id`·`spark_run_id`·`spark_batch_id`·`spark_processed_at`(콜백 진입 시각)을 붙이고, 콜백 계측 dict와 진행 정보를 `ops.spark_batch_attempts`·`ops.spark_query_progress`에 HTTP INSERT한다(`streaming.ops_metrics`). 기존 로그 출력은 유지한다.

### 진행 정보와 건수 계측

- `StreamingQueryListener`가 쿼리 시작·종료(id·runId·예외)와 매 진행 정보(`StreamingQueryProgress.json`: 배치 ID, 입력 건수, 파티션별 offset 범위, 처리속도, `durationMs`)를 한 줄 JSON으로 로그에 남긴다.
- `foreachBatch`가 배치마다 `batch_id`·입력·정상 주문·제외·DLQ 건수를 로그에 남긴다. 입력 건수 = 정상 + 제외 + DLQ이다. 지연 계측의 세부 항목과 구현 경계는 [처리지연 측정](#처리지연-측정)을 따른다.

### 파일 구조와 실행·테스트

원문 파싱·검증·분류와 DataFrame 변환을 `transforms.py` 하나에 모은다. `validation.py`·`classify.py`는 별도로 두지 않는다. `classify()`는 원문 검증과 정상·제외·불량 분류를 수행하고, `classify_batch()`는 이를 UDF로 DataFrame에 적용한다. `select_orders()`는 정상 필터·구조화·평탄화·출력 컬럼 선택, `deduplicate_events()`는 배치 내 `event_id` 중복 제거, `build_dlq_records()`는 불량 필터·DLQ 메시지 구성을 담당한다. `batch.py`의 `process_batch()`는 변환 → 중복 제거 → 저장 순서를 제어한다. 중첩 필드 전개에는 `select()`를 사용한다. 처리 순서·출력·계측 계약은 유지한다.

`schemas.py`는 원본 이벤트·분류 메타데이터의 Spark 스키마와 정상 주문 평탄화 매핑(`ORDER_COLUMNS`: 출력 컬럼 → 원본 경로)을 정의한다. `transforms.py`에서 DataFrame 실행 계획을 구성하며 실제 연산은 태스크와 Python worker에서 실행된다. 실행 진입점은 `main.py`로 유지한다.

```text
apps/streaming/
├── Dockerfile                 # 공식 Spark 이미지 + Kafka 커넥터 jar + 검증·테스트 패키지
├── src/streaming/
│   ├── config.py              # 환경변수 기반 설정(기본값 없음)
│   ├── schemas.py             # 입력·분류 결과 StructType·평탄화 출력 컬럼 매핑
│   ├── sources.py             # Kafka 입력
│   ├── transforms.py          # 파싱·검증·분류·필터링·평탄화
│   ├── monitoring.py          # 처리 건수·지연·쿼리 진행 정보
│   ├── sinks.py               # 정상 데이터·DLQ 출력
│   ├── batch.py               # foreachBatch 처리 조율
│   └── main.py                # 세션 생성·쿼리 시작·종료
├── tests/
└── pyproject.toml             # 의존성 명세. 실행은 Dockerfile 이미지와 Compose PYTHONPATH를 사용한다
```

Kafka 입력과 출력은 각각 `sources.py`·`sinks.py`에서 공식 DataStreamReader·DataFrameWriter API로 연결한다. `main.py`는 명시적 append 모드로 쿼리를 시작하고 종료 시 쿼리·리스너·세션을 정리한다. 다중 출력의 `foreachBatch`·persist/unpersist·최소 한 번 쓰기 보장·체크포인트·출력 계약은 유지한다. 이 변경은 성능 최적화가 아닌 역할 분리이며 기존 부하 측정은 변경 전 근거다. `from_json`·평탄화·타임스탬프 변환은 정상 출력 action에서 실행되므로 정상 출력 시간에 포함된다. 분류·계측 시간에는 원본 검증·분류·캐시·집계가 포함된다.

2026-11-17 변환 모듈 통합 검증: 원문 검증·분류를 `transforms.py`로 통합한 상태에서 Compose Spark 테스트 **276개 통과**(34.93초). `git diff --check` 통과. Kafka 실환경 재실행·부하 재측정은 수행하지 않았다.

2026-11-17 구조 정리 검증: Compose Spark 컨테이너에서 스트리밍 테스트 **276개 통과**. 원본 계약과 StructType의 중첩 필드·타입 대응, 전체 평탄화 값 보존, UTC 밀리초·윤년·1년/9999년·Long 범위·유니코드, 기존 DLQ·배치 실패·계측, 입력/쿼리 시작/실행 실패 시 자원 정리를 검증했다. 1년 시각은 Driver의 Python datetime 수집 시 시스템 시간대에 따른 범위 오류를 피하기 위해 Spark `date_format` 결과로 확인했다. Python 구문 검사와 `git diff --check`도 통과했다. Kafka 실환경 재실행·부하 재측정은 이번 구조 정리 범위에 포함하지 않았다.


참고: [from_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html), [JSON 파싱 옵션](https://spark.apache.org/docs/latest/sql-data-sources-json.html), [foreachBatch와 다중 출력](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#using-foreach-and-foreachbatch), [Kafka 소스·싱크](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html).

```bash
docker compose up -d kafka kafka-init                          # 브로커·토픽
docker compose up -d --build spark                             # 스트리밍 쿼리 시작
docker compose logs -f spark                                   # 진행·건수 로그
# 테스트(apps/streaming/tests). 코드는 읽기 전용 마운트라 pytest 캐시를 끈다.
docker compose run --rm --no-deps spark python3 -m pytest -p no:cacheprovider /opt/app/apps/streaming/tests
docker compose stop spark                                      # 정지. 체크포인트·출력 볼륨은 유지
```

구조 분리 검증(2026-11-17): 위 컨테이너 테스트 명령으로 **268개 통과**. 정적 DataFrame 변환·정상 출력·DLQ 구성·실패 전파·파일 스트림 콜백 및 지연 계측을 검증했다. Compose 진입점을 `main.py`로 변경했으며, Kafka 실환경 재실행과 지속 부하 재측정은 이번 구조 변경 검증에 포함하지 않았다. 과거 측정 JSON의 코드 해시는 당시 실행 근거로 보존한다.

체크포인트·출력 초기화는 Spark를 정지한 뒤 `docker volume rm log-etlm-2_spark-checkpoints log-etlm-2_spark-output`로 수행한다.

## 입력·DLQ 및 장애 처리

### 입력 중복과 JDBC 적재 중복의 구분

- `foreachBatch` 안의 `batch_df.dropDuplicates(["event_id"])`는 해당 배치 안의 중복만 제거한다. 콜백 이전의 스트리밍 DataFrame에서 상태를 유지하는 중복 제거를 적용하면 배치 간 입력 중복도 처리할 수 있으나 v1에서는 도입하지 않는다. Producer 멱등 전송과 주문별 새 ID 발급으로 입력 중복이 사실상 없고, 마이크로배치 재실행 중복은 상태 중복 제거로 막지 못하기 때문이다. 비교 근거는 [중복 제거·조회 조합 비교](04-clickhouse-storage.md#중복-제거조회-조합-비교)를 따른다.
- JDBC 일부 적재 후 재시도는 입력 중복 제거만으로 방지하지 못한다. [ClickHouse 적재 중복 정책](04-clickhouse-storage.md#jdbc-재시도와-적재-중복-처리)에 따라 `ReplacingMergeTree()` / `ORDER BY (event_timestamp, event_id)`로 저장하고 `FINAL` 기반 조회 뷰에서 논리 중복을 제거한다.
- 동일 ID 재전송은 동일 업무 내용이라는 전제를 따른다. 원본 ID와 `event_timestamp`를 유지하고 Kafka 위치·Spark 배치 식별자를 중복 키로 추가하지 않는다. 같은 ID에 다른 `event_timestamp`가 들어오면 저장 단계에서 합쳐지지 않으며 ClickHouse 충돌 점검으로 드러낸다. 그 밖의 다른 내용 충돌 감지는 별도 설계한다.
- 이 정책은 정상 주문 조회의 중복 합산을 막는 기준이다. 물리 INSERT 한 번 보장이나 정상 출력·DLQ의 원자성을 의미하지 않는다. DLQ 상세 스키마·오류 코드는 [3단계 구현 결정](#3단계-구현-결정)을 따르며, DLQ 데이터 재처리 정책과 기능은 후속 작업으로 보류한다.

[Kafka 연동](02-kafka-ingestion.md)의 원본·DLQ 계약을 따른다. 아래는 3단계 구현 요구사항이며 구현했다. 정적 DataFrame·파일 스트림 검증 결과는 아래 지연 계측 검증 결과를 참고한다. Kafka 실환경의 일부 검증은 남아 있다.

- 원본 `raw.ecommerce.orders.v1`의 3개 파티션을 읽고, 파싱·주문 검증 실패는 별도 토픽 `dlq.ecommerce.orders.v1`의 1개 파티션으로 보낸다. 원본과 DLQ 파티션을 일대일 대응시키지 않는다.
- 정상 health_check는 제외 건수로 집계하고 DLQ에 보내지 않는다. 정상 주문은 정상 출력 경로로 전달한다(3단계 Parquet, 4단계 ClickHouse).
- DLQ에는 원문 bytes를 Base64로 보존하고 원본 topic·partition·offset·Kafka timestamp, 오류 코드·설명, 실패 단계, 감지 시각, 처리기 버전을 포함한다.
- 원본 topic·partition·offset의 조합을 DLQ 메시지 키 및 중복 판별 기준으로 사용한다. 키 지정 자체가 중복을 제거하지 않으며, 재시도에 따른 DLQ 중복과 정상 적재 중복을 고려해 처리한다.
- ClickHouse 일시 장애는 배치 실패·재시도로 복구한다. DLQ 기록 실패도 해당 배치를 성공 처리하지 않고 재시도한다. 여러 출력의 원자적 완료는 가정하지 않는다.
- 최초 실행은 `startingOffsets=earliest`, 재시작은 체크포인트 위치를 따른다. `failOnDataLoss=true`로 필요한 offset의 소실을 드러낸다.
- 원본 소비 쿼리는 DLQ를 구독하지 않는다. DLQ에 기록된 불량 데이터를 자동으로 원본 토픽에 재투입하지 않는다.

### DLQ 기록 이후의 처리 범위

- **이번 단계:** 불량 데이터의 DLQ 기록, Kafka UI 또는 별도 소비 도구를 통한 메시지·오류 사유 조회, Base64 디코딩에 의한 원문 bytes 복원과 원본 위치 추적을 구현·검증한다. 별도 조회 애플리케이션은 요구하지 않는다.
- **보류:** 자동 재처리, 오류 데이터 자동 수정, 수정 데이터 재투입 기능은 이번 단계에서 구현하지 않는다. DLQ 상세 스키마·오류 코드 정의와 달리 재처리 기능은 이번 단계의 완료 조건에 포함하지 않는다.
- **후속 처리 원칙:** 오류 원인을 조사하고 복구 가능 여부를 판단한다. 처리기 버그는 코드를 수정하고, 입력 오류는 올바른 값을 확보한 뒤 선택 재처리한다. 누락된 식별자나 금액을 임의로 생성하지 않는다. 재처리 기능을 도입할 때 별도 경로·체크포인트, 대상 선택, 원본 위치 연결, 중복 처리, 성공·실패 결과 기록 방식을 PRD에 먼저 정의한다.
- **보관 한계:** DLQ는 현재 6시간 또는 파티션당 12GiB의 보관 조건 중 먼저 적용되는 조건으로 정리된다. 장기 조사가 필요한 데이터는 삭제 전에 원문과 오류 메타데이터를 별도로 보관해야 한다. 자동 장기 보관 기능은 이번 구현 범위에 포함하지 않는다.
- **장애 재시도와 구분:** DLQ 쓰기 실패나 정상 출력 장애로 인한 배치 실패·동일 체크포인트 재시작은 이번 단계의 검증 대상이다. 이는 이미 DLQ에 격리된 불량 데이터의 수정·재투입과 별개다.

### 출력 장애와 동일 체크포인트 재시작 검증

- 상태: 완료 (2026-11-17, 3개 시나리오 판정 기준 통과, 코드 결함 없음)
- 목적: 정상 출력·DLQ 쓰기 실패가 배치를 성공 처리하지 않고 쿼리를 실패시키는지, 같은 체크포인트로 재시작하면 미완료 배치가 같은 `batch_id`·offset 범위로 다시 실행돼 유실 없이 완료되는지, 재실행으로 생긴 중복을 원본 위치로 판별할 수 있는지 확인한다.
- 범위 밖: 이미 DLQ에 기록된 데이터의 수정·재투입·자동 재처리, offset 소실 시 실패, ClickHouse 적재 경로(4단계).

#### 실패 지점과 재실행 시 기대 중복

`process_batch`는 분류 → 정상 출력 → DLQ 쓰기 순서로 실행하고, 콜백이 끝난 뒤 Spark가 `commits/<batch_id>`를 기록한다. 커밋이 없는 배치는 재시작 시 `offsets/<batch_id>`의 같은 범위로 다시 실행된다.

| 시나리오 | 주입 방법 (코드 변경 없음) | 실패 시점 출력 상태 | 재시작 후 기대 중복 |
| --- | --- | --- | --- |
| A. 정상 출력 실패 | 쿼리 실행 중 정상 출력 디렉터리를 쓰기 불가로 바꾼다(`chmod a-w`) | 정상 없음(Parquet 작업 중단), DLQ 미시도 | 없음 |
| B. DLQ 쓰기 실패 | DLQ 토픽 `max.message.bytes`를 100으로 바꿔 브로커가 `MESSAGE_TOO_LARGE`로 거부하게 한다. 확인 후 동적 설정을 삭제해 되돌린다 | 정상 기록 완료, DLQ 없음 | 정상 출력만 해당 배치 정상 건수만큼 |
| C. 출력 완료 후 커밋 실패 | 체크포인트 `commits` 디렉터리를 쓰기 불가로 바꾼다 | 정상·DLQ 모두 기록 완료 | 정상·DLQ 모두 해당 배치 건수만큼 |

- 시나리오마다 별도 체크포인트·정상 출력 경로를 쓰고 `startingOffsets=latest`로 시작한다. 시작 offset 저장을 확인한 뒤 생성기로 고정 건수를 넣는다. 원본·DLQ 토픽은 삭제·초기화하지 않는다.
- 실패 후 주입을 되돌리고, 같은 체크포인트·출력 경로·설정으로 재시작한다. 최종 커밋 offset이 원본 최신 offset에 도달하면 중지한다.

#### 판정 기준

- 실패 시도: 해당 배치 지연 로그가 `status=failed`이고 `failed_stage`가 실패 지점과 같다(A `orders_write_action`, B `dlq_write_action`, C는 콜백 성공 후 커밋 실패). 그 배치의 `commits` 파일이 없고 쿼리가 실패로 종료한다.
- 재시작: 미완료 배치가 같은 `batch_id`와 같은 offset 범위로 다시 실행되고 커밋된다. 최종 커밋 offset이 원본 최신 offset과 같다.
- 유실 없음: 입력 범위의 원본 위치 수 = 고유 위치 기준 정상 + 제외 + DLQ.
- 중복 판별: 정상 출력은 `kafka_topic`·`kafka_partition`·`kafka_offset`, DLQ는 메시지 키(`topic:partition:offset`)로 같은 원본 위치를 찾는다. 중복 건수와 위치가 위 표의 기대와 같고, 같은 위치의 중복 행은 업무 내용이 같다(DLQ의 `detected_at` 등 처리 시각 제외).
- 중복 제거는 이 단계에서 구현하지 않는다. 정상 출력 중복은 4단계 ClickHouse 정책([입력 중복과 JDBC 적재 중복의 구분](#입력-중복과-jdbc-적재-중복의-구분))으로, DLQ 중복은 소비 측이 키로 판별한다.

#### 검증 결과 (2026-11-17)

- 코드: `93e861b` 기준 `apps/streaming` 변경 없음. 설정은 compose 기본값(`local[3]`, 트리거 5초 등)에 체크포인트·출력 경로만 시나리오별로 바꿨다.
- 입력: 시나리오마다 생성기 시드 7로 4,000건(정상 주문 3,344, health_check 438, 불량 218). 생성기 전송 실패·미전달 0.
- 세 시나리오 모두 입력 4,000건이 batch 1 하나로 들어갔다. batch 0은 `latest` 시작 시의 빈 배치로 성공·커밋됐다.

| 항목 | A. 정상 출력 실패 | B. DLQ 쓰기 실패 | C. 커밋 실패 |
| --- | --- | --- | --- |
| 실패 원인(로그) | `Mkdirs failed to create .../fault-a/_temporary/...` | `MESSAGE_TOO_LARGE` | `commits/.1.<uuid>.tmp` `Permission denied` |
| 1차 시도 batch 1 | `failed` / `orders_write_action` | `failed` / `dlq_write_action` | 콜백 `succeeded`, 커밋 실패 |
| 쿼리·컨테이너 | `terminated with error`, exit 1 | 같음 | 같음 |
| 재시작 전 체크포인트 | offsets [0, 1], commits [0] | 같음 | 같음 |
| 재시작 batch 1 | 같은 offset 범위 4,000행, `succeeded`, 커밋 | 같음 | 같음 |
| 최종 커밋 offset = 원본 최신 offset | 예 | 예 | 예 |
| 고유 위치: 정상 + 제외 + DLQ | 3,344 + 438 + 218 = 4,000 | 같음 | 같음 |
| 정상 출력 행 / 고유 위치 / 중복 위치 | 3,344 / 3,344 / **0** | 6,688 / 3,344 / **3,344** | 6,688 / 3,344 / **3,344** |
| DLQ 메시지 / 고유 키 / 중복 키 | 218 / 218 / **0** | 218 / 218 / **0** | 436 / 218 / **218** |
| 중복 위치가 실패 배치 범위 안 | 해당 없음 | 예 | 예 |
| 같은 위치 중복의 내용 일치 | 해당 없음 | 예(35개 컬럼 전체) | 예(정상 35개 컬럼, DLQ `detected_at` 제외 전체) |

- 판정: 세 시나리오 모두 기대 중복표와 같다. 실패 시 배치를 성공 처리하지 않고 쿼리를 실패시키며, 같은 체크포인트 재시작으로 유실 없이 복구한다. 재실행 중복은 정상 출력의 원본 위치 컬럼과 DLQ 키로 모두 판별된다. 정상 출력은 `event_id`도 위치와 1:1(고유 3,344)이다.
- B 1차 시도 기록: 처음 계획한 `min.insync.replicas=2`는 Kafka 4.4.0 단일 브로커에서 `acks=all` DLQ 쓰기를 거부하지 않아 배치가 성공했다(원인 미확인). 이 시도는 판정에서 제외했고, 설정은 1로 복원했다. 해당 구간 DLQ offset 476561~476778(218건)은 정상 기록이다.
- 검증 중 DLQ offset 476343~477432를 사용했다. 이 중 476997~477432(시나리오 C)의 키 218개 2회 기록은 의도된 중복이다. DLQ 토픽 설정은 검증 후 원래 상태(`max.message.bytes` 동적 설정 없음, `min.insync.replicas=1`)로 확인했다.
- 별도 체크포인트·출력(`/opt/spark/checkpoints/fault-{a,b,b2,c}`, `/opt/spark/output/fault-{a,b,b2,c}`)은 볼륨에 남겨 두었다. `fault-b`는 판정 제외한 1차 시도다.
- [검증 근거 JSON](../measurements/2026-11-17-spark-output-fault-restart.json)
- 남은 작업: offset 소실 시 실패 검증, 정상 출력 중복의 저장 단계 처리(4단계 ClickHouse).

## 처리지연 측정

[공통 측정 원칙](01-log-generator.md#공통-측정-원칙)을 따른다. 초기 측정 단위는 마이크로배치이며 이벤트별 순수 연산시간은 측정한다고 가정하지 않는다.

| 기준점·지표 | 기록 위치·계산 | 의미 |
| --- | --- | --- |
| 배치 시작 (`batch_started_at`) | Structured Streaming의 트리거 시작 시각 | 배치 실행 기준점이며 개별 이벤트의 실제 수신 시각이 아님 |
| 배치 처리시간 | `StreamingQueryProgress.durationMs`의 `triggerExecution` 등 | 트리거 전체 및 제공되는 세부 구간. sink·체크포인트 등 포함 범위를 함께 표시 |
| ClickHouse 쓰기 시간 | 실제 INSERT 요청 직전부터 성공 응답까지 | 전체 배치 시간과 분리해 관찰하되 중복 합산하지 않음 |
| Kafka → 콜백 진입 지연 | `callback_started_at - kafka_append_timestamp` | 해당 배치가 읽은 이벤트의 소비 대기 근사치. 트리거 시작부터 콜백 진입까지의 준비 시간도 포함하며 실제 수신 지연과 구분 |
| 분류·계측 action 시간 | 분류 결과의 건수·지연 집계 action 호출부터 반환까지 | Kafka 읽기·Python UDF 검증·캐시 구성·집계·스케줄링 포함. 순수 정제 시간으로 표시하지 않음 |
| 정상 출력·DLQ 출력 action 시간 | 각 출력 함수 호출부터 반환 또는 예외까지 | 정상은 3단계 Parquet, 4단계 JDBC. 개별 INSERT 요청 시간이 아님 |
| 콜백 전체 시간 | 콜백 진입부터 캐시 해제 호출 종료까지 | 계측·출력·정리 시간을 포함. 트리거 전체 시간의 일부이며 서로 합산하지 않음 |

- 콜백 진입 시각보다 늦은 Kafka timestamp가 있는 경우 해당 차이를 대기시간으로 사용하지 않는다. 범위 불일치 건수로 구분하며 0으로 보정하지 않는다. 정확한 수신 지연은 실제 읽기 경계에 별도 계측이 있을 때만 제공한다.
- 진행 정보는 `StreamingQueryListener` 또는 `lastProgress` 등으로 수집한다. 배치 ID, 입력 건수, topic·partition별 offset 범위, 처리속도와 실패·재시도를 함께 기록한다. 재시작 시 배치 ID가 충돌하지 않도록 쿼리·실행 식별자도 남긴다.
- 변환 코드 선언 전후의 시간 차이를 실제 처리시간으로 사용하지 않는다. Spark는 지연 실행하므로 실제 action과 진행 정보에서 측정한다.
- JDBC write action 호출부터 반환까지는 선행 변환·스케줄링 등이 포함될 수 있는 출력 action 소요시간으로 기록한다. 개별 INSERT 요청 시간이나 순수 정제 시간으로 표시하지 않는다. 요청별 JDBC 계측 방식은 구현 전에 확정한다.
- `current_timestamp()`를 변환 전후에 추가해 처리시간을 계산하지 않는다. 같은 쿼리에서 같은 값을 반환하므로 연산별 완료 시각이 아니다.
- `event_timestamp`, Kafka 추가 시각·topic·partition·offset을 후속 저장 컬럼으로 보존한다. items 분리 이후에도 지연 분포는 원본 이벤트 기준으로 집계해 상품 개수에 따른 중복 가중을 피한다.
- health_check 제외·정상 주문·불량 격리 건수를 구분한다. 파싱 실패는 Kafka 메타데이터로 추적하고 생성 기준 지연 계산 불가 건수를 기록한다.

### 3단계 지연 로그 계약

- `batch_started_at`은 진행 JSON의 `timestamp`(트리거 시작), `callback_started_at`은 Python 콜백 진입 시 UTC 시각이다. 두 값을 대체하지 않는다. Kafka → 트리거 시작 분포는 이번 구현에서 제공하지 않으며 Kafka → 콜백 진입 분포를 제공한다.
- 배치마다 `latency` JSON 로그에 `query_id`·`run_id`·`batch_id`, UTC 콜백 시작 시각, 성공/실패와 실패 구간, 각 action 및 콜백 전체 소요시간(ms)을 남긴다. 구간 시간은 `time.perf_counter()`로 측정한다. 실행하지 않은 출력 시간은 null, 실패한 출력 시간은 예외까지의 시간이다. 실패 예외는 그대로 전파한다.
- Kafka → 콜백 진입 지연은 중복 제거·health_check 제외 전 전체 입력과 분류별(정상·제외·DLQ)로 집계한다. 입력 레코드가 모집단이며 재시도는 별도 실행 시도로 취급한다. 이벤트 전체를 드라이버에 수집하지 않고 기존 분류 건수 집계 action에 통합한다.
- `count`(유효 비음수 시차), `missing_timestamp_count`, `future_timestamp_count`, min/max/평균 및 p50/p95/p99(ms)를 기록한다. 표본 추출 없이 전건에 `percentile_approx` accuracy=10000(순위 상대 오차 1/10000)을 적용한다. 유효값이 없으면 분위수와 min/max/평균은 null이다. 분류별 분위수를 평균 내 전체 분위수를 만들지 않는다.
- 원본 offset 범위는 동일 쿼리·실행·배치 키의 `progress` 로그에서 연결한다. 건수 로그의 정상 건수는 배치 내 중복 제거 전이다. 성공한 콜백도 이후 체크포인트 실패로 트리거가 실패할 수 있으므로 콜백 성공과 배치 커밋 성공을 구분한다.
- 생성 → Kafka 지연은 기존 생성기 계측을 사용한다. ClickHouse INSERT·조회 가능 지연과 지표 영속 저장·Grafana 표시는 후속 단계다. 5,000 EPS 지속 처리와 offset 소실 검증은 이 작업과 별도로 남는다.
- 검증: 고정 시각 입력의 지연·분위수·누락·미래 시각·빈 배치, 중복 제거 전 모집단, 소비 대기 증가, 출력 지연 주입 및 실패 시간·예외 전파, 실제 스트리밍 진행 정보와의 키 연결을 확인한다.

참고: [percentile_approx](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html), [Spark 진행 정보](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#monitoring-streaming-queries), [current_timestamp 의미](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_timestamp.html).

### 지연 계측 검증 결과 (2026-11-17)

- 검증 결과: Spark 전체 테스트 **268개 통과**(28.36초), `git diff --check` 통과.
- 실행 환경: 기존 Compose Spark 이미지에서 `docker compose run --rm --no-deps spark python3 -m pytest -p no:cacheprovider /opt/app/apps/streaming/tests -q`. 기존 Kafka 서비스·토픽·체크포인트를 변경하지 않고 테스트별 임시 경로를 사용했다.
- 고정 시각 DataFrame: 정상·제외·DLQ별 건수와 전체 지연 집계, 동일 event_id 원본 2건의 계측 유지, 누락·미래 timestamp 분리, 빈 배치의 null 분위수, 소비 대기 기준 2초 증가 시 분위수 2초 증가를 확인했다.
- 실제 Structured Streaming: Kafka 형태의 메타데이터를 가진 파일 소스와 임시 체크포인트로 콜백을 실행한다. 정상 출력 action에 150ms 대기를 주입해 출력 시간에 반영되고, query_id·run_id·batch_id가 실제 진행 JSON과 연결되며 콜백 진입 시각이 트리거 시작 이후임을 확인했다.
- 실패 경로: DLQ 출력 예외가 전파되고 성공 건수 로그를 남기지 않으며, 실패 구간·실패까지의 action 시간과 콜백 전체 시간이 기록됨을 확인했다.
- 검증 경계: 이 절은 정적 DataFrame·파일 소스 검증 결과다. Kafka 실환경의 최신·완료 offset 연결과 지속 부하는 아래 별도 결과를 따른다. 소비 지연의 통제된 주입, 계측을 끈 대조군을 통한 오버헤드 분리 측정, offset 소실 시 실패, 3단계 runbook 보완은 남아 있다. ClickHouse INSERT·조회 가능 지연은 4단계다.

### 지속 부하 측정 계획 (2026-11-17)

- 현재 구현과 자원 제한을 유지한 채 목표 5,000 EPS로 워밍업 120초 + 본 측정 600초를 연속 실행한다. 초기 지연이 안정되지 않아도 임의로 워밍업을 늘려 결과에서 제외하지 않는다.
- 기존 체크포인트·정상 출력은 보존한다. 이번 실행은 별도 체크포인트·Parquet 출력 하위 경로를 사용하고 `startingOffsets=latest`로 기존 보관분을 제외한다. 원본·DLQ 토픽은 기존 토픽을 사용하며 삭제·초기화하지 않는다. Kafka UI는 실행하지 않는다.
- 생성기 실제 ACK EPS(본 측정 구간 누적 ACK 차이 / 경과 시간), 전송 실패·미전달, Spark 커밋된 offset과 Kafka 최신 offset의 차이, 입력·정상·제외·DLQ 건수, 배치 처리시간, Kafka → 콜백 진입 지연, 컨테이너 CPU·메모리를 기록한다. offset 차이는 레코드 건수의 근사치이며 서로 다른 시각에 수집한 값임을 명시한다.
- 워밍업/본 측정 구간은 생성기 실행 시각 기준이다. 경계를 가로지르는 배치는 별도 표시하고, 본 측정의 배치 통계는 콜백 시작 시각 기준으로 집계한다. 배치별 p95/p99를 평균 내 전체 이벤트 분위수로 표시하지 않는다.
- 목표 입력의 95% 이상(4,750 ACK EPS), 출력·쿼리 오류 없음, 본 측정 후반에 backlog와 소비 대기 지연이 지속 증가하지 않는지를 확인한다. 배치 처리시간 5초 초과 빈도도 별도로 보고한다. 이는 로컬 Parquet 경로의 1차 측정 기준이며 장시간 안정성이나 ClickHouse 성능을 보장하지 않는다.
- 입력 중단 후 최대 180초 동안 최종 Kafka offset까지 Spark 커밋이 도달하는지 확인한다. 쿼리 실패·OOM 등으로 계속 측정할 수 없으면 조기 종료하고 실제 측정 길이와 원인을 기록한다. 값 조정이나 코드 최적화는 이번 측정 중 수행하지 않는다.

### 지속 부하 측정 결과 (2026-11-17)

**판정: 현재 설정과 Parquet 정상 출력 경로는 5,000 EPS 지속 처리 기준을 충족하지 못했다.** 초기 워밍업 이후에도 backlog와 소비 대기 지연이 증가했다. 이번 작업에서는 코드·자원을 조정하지 않았다.

- 실행: 12:51:48~13:03:48 KST에 입력 720초(워밍업 120초 + 본 측정 600초), 입력 중단 후 182.8초 관찰(180초 목표 + 수집 오버헤드). Spark는 `local[3]`·CPU 3·heap 2g·컨테이너 3GiB, 트리거 5초·최대 50,000 offsets·shuffle 3. Kafka UI는 꺼 두었다.
- 기존 토픽·체크포인트를 삭제하지 않았다. 별도 체크포인트 `/opt/spark/checkpoints/load-20261117`, 정상 출력 `/opt/spark/output/load-20261117`, 최초 `latest`를 사용했다. 초기 offset이 저장된 것을 확인한 뒤 입력을 시작했다.

| 항목 | 결과 |
| --- | --- |
| 본 측정 실제 ACK EPS | **4,947.65**. 누적 ACK 2,968,142건 / 실제 상태 구간 599.910초(생성기 elapsed 120.007~719.917초) |
| 본 측정 입력 배치 | 36개, 입력 1,799,944건. 콜백 진입 시각 기준이며 마지막 배치는 입력 중단 경계를 넘어 완료 |
| 배치 전체 처리시간 | 평균 **16.05초**, p50 15.46초, p95 19.19초, p99·최대 20.32초. 36개 모두 5초 초과 |
| 분류·계측 action | 배치 평균 **14.88초**. Kafka 읽기·검증·캐시·집계를 포함하므로 순수 Python 정제 시간으로 단정하지 않음 |
| 정상 Parquet / DLQ 출력 action | 배치 평균 **0.83초 / 0.25초** |
| Kafka → 콜백 대기 | 본 측정의 배치별 p95 **82.26~288.95초**, p99 **83.11~289.68초**. 전체 이벤트 분위수가 아님. 유효 이벤트 가중 평균 188.02초 |
| offset 차이 추이 | 약 121초 시점 283,696 → 약 713초 시점 1,459,956. 입력 중단 직후 1,455,979 |
| 입력 중단 후 관찰 | 182.8초 후 **805,996 offsets가 남음**, 완전 해소 실패 |
| Spark 메모리 | `docker stats` 표본 최대 **1.544GiB / 제한 3GiB**. 계획 종료 전 OOM·자발적 쿼리 종료 없음 |
| 생성기 종료 | **exit 1**, `output flush failed: 1 messages undelivered after flush`. ACK 최종 실패·재시도·클라이언트 오류는 각각 0. 종료 정상성 기준 미충족 |

#### 건수 대조와 종료 처리

- 전체 입력 큐 등록 3,564,479건, ACK 3,564,478건, 원본 Kafka offset 증가 3,564,479. ACK 통계와 offset 증가량의 1건 차이는 종료 경로 조사 대상이며 실제 데이터 유실로 단정하지 않는다.
- Spark 커밋 batch 56까지 처리한 입력 2,758,483 = 정상 2,345,282 + 제외 276,117 + DLQ 137,084. 체크포인트의 커밋 offset 증가량과 입력 로그가 일치한다. Parquet를 별도 Spark 세션에서 읽은 실제 행수는 **2,345,282**, 컬럼은 35개로 정상 출력 로그와 일치한다. DLQ 수치는 성공한 출력 콜백의 로그 집계이며 이번 측정에서 토픽 전체 메시지를 다시 세지는 않았다.
- 관찰 종료 후 측정용 Spark에 `docker stop`을 실행했다. 진행 중이던 batch 57은 SparkContext 종료 중 분류·계측 action에서 취소됐고 정상·DLQ 출력은 시작하지 않았다. 이 취소는 의도한 종료로 구분하며 부하 중 자발적 쿼리 실패로 집계하지 않는다. 마지막 커밋은 그대로 보존했다.
- [측정 근거 JSON](../measurements/2026-11-17-spark-sustained-load.json)에 설정·코드 해시·생성기 최종 통계·배치별 지표·offset 및 자원 표본을 보관했다. 원시 로그와 측정 스크립트는 `/tmp/log-etlm-2-load-20261117/`에 있다.

#### 해석과 남은 작업

- 50,000 offsets 상한은 처리량이 아니다. 평균 16.05초에 약 5만 건을 처리하므로 현재 배치 처리능력은 약 3,100건/s 수준이고 목표 입력보다 낮다. 워밍업만으로 설명할 수 없는 지속 backlog 증가다.
- 분류·계측 action이 시간의 대부분을 차지한다. 다음 성능 개선에서는 해당 action의 읽기·Python 검증·집계·스케줄링 비용을 구분해 조사해야 한다. 메모리 부족이나 ClickHouse 쓰기를 병목 원인으로 확정할 근거는 없다.
- 생성기 SIGINT 종료 시 flush/ACK 집계 불일치, 처리량 개선 후 동일 조건 재측정, offset 소실 시 실패 검증, 3단계 runbook 보완이 남았다. 코드 최적화·자원 조정·장애 검증은 사용자 요청 후 진행한다.

### 처리량 병목: 분류 단계 계약 검증 (2026-11-17)

- 상태: 수정·재측정 완료. 5,000 EPS 지속 처리 기준 충족([재측정 결과](#2단-검증-적용-후-지속-부하-재측정-2026-11-17))
- 문제: 지속 부하에서 배치 평균 16.05초 중 분류·계측 action이 14.88초다. 배치 처리능력 약 3,100건/s로 5,000 EPS에 미달한다.
- 측정 경계: 첫 지속 부하 측정 JSON의 코드 해시는 리팩터링 전(`job.py`·`classify.py`) 기준이다. 아래 근거 수치는 현재 구조(`transforms.classify`)를 Spark 컨테이너 단독으로 측정한 값이다. 현재 구조의 수정 전 Kafka 지속 부하는 [적용 전 측정](#2단-검증-적용-전-지속-부하-측정-2026-11-17)을 따른다.

#### 근거 수치

생성기 `EventGenerator(seed=1)` 20,000건을 Spark 이미지의 Python으로 `classify()`에 직접 넣어 측정했다. 수정 전 컨테이너 테스트는 276개 통과했다.

| 구간 | 주문 1건당 |
| --- | --- |
| `classify()` 전체 | 485µs (코어당 약 2,060건/s) |
| Python `jsonschema` 계약 검증 (`best_match(validator.iter_errors(doc))`) | 496µs |
| `json.loads` | 15µs |
| 타입 변환 검사 `_conversion_error` | 8.6µs |
| 의미 검사 `_semantic_error` | 1.6µs |
| 참고: `jsonschema-rs` `is_valid` | 1.5µs |

- 원인: 모든 레코드에 수행하는 Python `jsonschema` 검증이 분류 시간의 약 97%를 차지한다. `local[3]`에서 분류만으로 이론상 약 6,200건/s이며, Kafka 읽기·UDF 직렬화·캐시·집계를 더하면 목표에 못 미친다.
- 구간별 측정은 개별 호출 반복이라 합이 `classify()` 전체와 정확히 맞지 않는다.

#### 규칙 불일치

`jsonschema-rs` `Draft202012Validator(schema, validate_formats=True)`와 기존 `_StrictValidator`의 판정을 대조했다.

| 사례 | jsonschema-rs | 기존 | 판정 |
| --- | --- | --- | --- |
| `context.user_id` 끝 `\n` | 무효 | 무효 | 일치 |
| `event_timestamp` 형식 오류 | 무효 | 무효 | 일치 |
| `price` `15000.0` | **유효** | 무효 | **불일치** |
| `event_timestamp` 연도 `0000` | **유효** | 무효 | **불일치** (구현 중 대조 테스트로 발견) |
| `event_timestamp` 초 `60` | **유효** | 무효 | **불일치** (구현 중 추가 대조로 발견) |

- `jsonschema-rs`는 JSON Schema 기본대로 소수점 표기 정수를 `integer`로 허용한다. 그대로 대체하면 현재 DLQ로 가는 레코드가 정상으로 통과한다.
- `jsonschema-rs`의 `date-time` 형식 검사는 연도 `0000`과 윤초 `60`을 허용하고 `rfc3339-validator`는 거부한다. 그 밖의 시각 경계(2월 29일, 31일 없는 달, `24:00`, 소문자 `t`·`z`, 오프셋 등)는 두 검증기가 일치했다.

#### 수정 방향과 범위

- 2단 검증: 1단은 `jsonschema-rs` `is_valid`로 유효 레코드만 빠르게 통과시킨다. 1단에서 무효이거나 소수(float)가 포함된 레코드는 2단 기존 `_StrictValidator` + `best_match`로 판정한다. DLQ 오류 코드·경로·메시지는 바뀌지 않는다.
- 소수 감지: `json.loads(parse_float=...)`로 소수 존재를 표시하고, 소수가 있으면 1단을 건너뛴다.
- 시각 재확인: 1단이 유효로 판정하면 `event_timestamp`만 기존 형식 검사기(`FORMAT_CHECKER.conforms`)로 다시 검사한다. 계약의 `format` 필드가 이 하나뿐이라는 가정이며, 늘어나면 함께 재확인한다.
- 안전 조건: 1단은 "유효" 판정만 신뢰한다. 1단이 유효로 보고 기존이 무효로 보는 사례가 생기면 불량이 정상으로 통과하므로, `test_transforms_validation.py`의 모든 `classify` 입력과 생성기 이벤트 3,000건에서 1단 "유효"마다 기존 검증기도 유효인지 대조한다(autouse fixture).
- 변경 범위: `transforms.py`의 계약 검증·`json.loads` 호출부, Spark Dockerfile·`pyproject.toml`에 `jsonschema-rs==0.61.2` 고정, 대조 테스트.

#### 수정 결과 (2026-11-17)

- 컨테이너 테스트 **280개 통과**(기존 276 + 2단 검증 4).
- 같은 단독 벤치(생성기 20,000건): `classify()` **485µs → 54µs/건**, 코어당 약 2,060 → **18,535건/s**. 남은 시간은 `json.loads`·변환 검사와 1단 무효로 2단을 다시 타는 불량 레코드 비용이다.
- Kafka 지속 부하 재측정 결과는 아래 절을 따른다.
- 미확인: 분류 외 구간(Kafka 읽기·UDF 직렬화·캐시·`percentile_approx` 집계) 비용은 이전 측정 기준 배치당 약 6초로 추정되며 분류 개선 후 다음 병목 후보다. 수정 후 동일 조건(워밍업 120초 + 본 측정 600초)으로 재측정한다.

### 2단 검증 적용 전 지속 부하 측정 (2026-11-17)

**판정: 폴더 구조 리팩터링 후·2단 검증 적용 전 코드는 5,000 EPS 지속 처리 기준을 충족하지 못했다.** 입력은 목표를 유지했지만 모든 배치가 5초를 넘었고 backlog와 소비 대기 지연이 계속 늘었다. 적용 후 측정의 비교 기준이다.

- 실행: 14:59:02~15:11:02 KST 입력 720초(워밍업 120초 + 본 측정 600초), 입력 중단 후 183.6초 관찰. 자원·설정은 적용 후 측정과 같다.
- 코드: HEAD `e9184c6`(진입점 `main.py`, `transforms.classify`, Python `jsonschema` 단독 검증)을 별도 git worktree로 마운트했다. 같은 코드로 컨테이너 테스트 276개 통과. 이미지에는 `jsonschema-rs`가 설치돼 있지만 이 코드는 import하지 않는다.
- 별도 체크포인트 `/opt/spark/checkpoints/load-20261117c`, 정상 출력 `/opt/spark/output/load-20261117c`, 최초 `latest`. 시작 offset이 적용 후 측정의 마지막 커밋 offset과 같아 입력이 겹치지 않는다.

| 항목 | 결과 |
| --- | --- |
| 본 측정 실제 ACK EPS | **4,998.60** (ACK 2,998,591건 / 599.886초) |
| 본 측정 배치 수·입력 | 43개, 2,149,935건(배치당 약 50,000건 상한) |
| 배치 전체 처리시간 | 평균 **14.12초**, p50 14.13초, p95 14.78초, 최대 14.88초. **43개 모두 5초 초과**. 처리능력 약 3,540건/s |
| 분류·계측 action | 평균 **12.75초**, p95 13.35초 |
| 정상 Parquet / DLQ 출력 action | 평균 1.11초 / 0.18초 |
| Kafka → 콜백 대기 | 배치별 p95 **49.20~225.15초**, p99 49.60~226.20초. 유효 이벤트 가중 평균 133.64초 |
| offset 차이 추이 | 약 123초 288,185 → 약 664초 1,045,806 |
| 입력 중단 후 관찰 | 183.6초 후 **434,526 offsets가 남음**, 완전 해소 실패 |
| Spark 메모리·CPU | 최대 1.626GiB / 제한 3GiB, CPU 평균 275%·최대 297% |

- 워밍업 배치 8개도 모두 5초를 넘었다(평균 14.97초).
- 건수 대조: 원본 offset 증가 3,598,128. Spark 커밋 batch 64까지 입력 3,163,602 = 정상 2,689,333 + 제외 316,404 + DLQ 157,865. Parquet 실제 행수 **2,689,333**, 컬럼 35개. 미처리 434,526은 남은 offset 차이와 같다.
- 관찰 종료 후 `docker stop`으로 진행 중이던 batch 65가 Python worker 종료로 실패했다. 의도한 종료로 구분하며 부하 중 자발적 실패로 집계하지 않는다. 마지막 커밋은 보존했다.
- 생성기는 **exit 1**(`output flush failed: -1 messages undelivered after flush`)로 끝났다. generated·acked 3,598,128, queued 3,598,127로 큐 등록 집계가 1 적고, 원본 offset 증가는 ACK와 같다. 적용 후 측정의 `1 messages undelivered`와 방향이 반대인 집계 불일치로, 생성기 종료 경로 조사 대상이다.
- [측정 근거 JSON](../measurements/2026-11-17-spark-sustained-load-pre-2stage.json)

### 2단 검증 적용 후 지속 부하 재측정 (2026-11-17)

**판정: 5,000 EPS 지속 처리 기준을 충족했다.** 적용 전 측정과의 차이는 2단 계약 검증 하나다. 본 측정 ACK EPS 4,998.79(목표의 95% 이상), 쿼리·출력 오류 없음, 본 측정 중 소비 대기 지연이 일정하고 입력 중단 후 12초 안에 backlog가 0이 됐다. [측정 계획](#지속-부하-측정-계획-2026-11-17)과 같은 조건이며 로컬 Parquet 경로의 1차 기준이다.

- 실행: 14:41:25~14:53:26 KST 입력 720초(워밍업 120초 + 본 측정 600초), 입력 중단 후 관찰. 측정 순서는 적용 후가 먼저다. 자원·설정은 적용 전 측정과 같다(`local[3]`·CPU 3·heap 2g·컨테이너 3GiB, 트리거 5초·최대 50,000 offsets·shuffle 3, Kafka UI 꺼짐).
- 코드: `e9184c6`에 [2단 계약 검증](#처리량-병목-분류-단계-계약-검증-2026-11-17) 미커밋 변경을 더한 상태. 진입점 `main.py`. 파일별 해시는 측정 JSON `runtime.code_sha256`에 있다.
- 별도 체크포인트 `/opt/spark/checkpoints/load-20261117b`, 정상 출력 `/opt/spark/output/load-20261117b`, 최초 `latest`. 기존 토픽·체크포인트는 삭제하지 않았다.

| 항목 | 적용 전 (리팩터링 후) | 적용 후 |
| --- | --- | --- |
| 본 측정 실제 ACK EPS | 4,998.60 | **4,998.79** (ACK 3,002,327건 / 600.611초) |
| 본 측정 배치 수·입력 | 43개, 2,149,935건 | 120개, 2,998,431건 |
| 배치 전체 처리시간 | 평균 14.12초, p95 14.78초, 43개 모두 5초 초과 | 평균 **2.04초**, p50 2.00초, p95 2.32초, 최대 2.73초. **5초 초과 0개** |
| 분류·계측 action | 평균 12.75초 | 평균 **1.12초**, p95 1.32초 |
| 정상 Parquet / DLQ 출력 action | 평균 1.11초 / 0.18초 | 평균 0.71초 / 0.14초 |
| Kafka → 콜백 대기 | 배치별 p95 49.20~225.15초 | 배치별 p95 **4.77~4.81초**, p99 4.97~5.01초. 유효 이벤트 가중 평균 2.54초 |
| offset 차이 추이 | 288,185 → 1,045,806 | 약 140초 22,212 → 약 700초 23,865 |
| 입력 중단 후 | 183.6초 후 434,526 남음 | **약 12초 후 0** |
| Spark 메모리 | 최대 1.626GiB | 최대 1.643GiB / 제한 3GiB, CPU 평균 216%·최대 285% |

- 워밍업 120초는 첫 배치 기동 비용으로 배치 19개 중 6개가 5초를 넘었고(최대 13.85초) 약 80초 이후 offset 차이가 약 22,000으로 안정됐다. 워밍업은 판정에서 제외했지만 결과에서 숨기지 않는다.
- offset 차이는 본 측정 560초 동안 약 1,650(0.3%) 늘었다. 트리거 1회분(약 25,000건) 미만이고 배치별 대기 p95가 일정해 backlog 누적으로 보지 않는다. 장시간 실행에서의 추세는 확인하지 않았다.

#### 건수 대조

- 원본 offset 증가 3,581,314 = 생성기 큐 등록 3,581,314 = Spark 커밋 offset 증가 3,581,314 (batch 140까지).
- Spark 입력 3,581,314 = 정상 3,044,517 + 제외 357,955 + DLQ 178,842. 생성기 종류별 건수(정상 주문 3,044,517, health_check 357,955, 불량 주문 178,842)와 정확히 일치한다.
- Parquet를 별도 Spark 세션에서 읽은 실제 행수 **3,044,517**, 컬럼 35개.
- 생성기는 이전과 같이 **exit 1**(`output flush failed: 1 messages undelivered after flush`, ACK 3,581,313)로 끝났다. 원본 offset 증가량이 큐 등록 수와 같으므로 이 1건은 Kafka에 기록됐고 ACK 집계만 누락된 것으로 보인다. 생성기 종료 경로 조사는 남아 있다.
- 측정용 Spark는 offset 차이 0을 확인한 뒤 `docker stop`으로 종료했다. 종료 전 상태는 running, OOM 없음, 지연 계측 실패 배치 없음.
- [측정 근거 JSON](../measurements/2026-11-17-spark-sustained-load-2stage.json)에 설정·코드 해시·생성기 최종 통계·배치별 지표·offset 및 자원 표본을 보관했다.

#### 남은 작업

- 생성기 SIGINT 종료 시 flush/ACK 집계 불일치, offset 소실 시 실패 검증, 3단계 runbook 보완.
- 이 결과는 로컬 Parquet 출력 기준이다. ClickHouse 적재 경로의 처리량은 4단계에서 다시 측정한다.

### 3단계 마무리 검증 계획 (2026-11-17)

- 상태: 종료. 1 통과, 2 부분 통과 후 [판정 정리](#2-판정-정리-2026-11-19)로 종료, 3 측정 완료(켬−끔 차이 기록, 1회 측정), 4 완료
- 대상: offset 소실 시 실패, 소비·처리 지연 주입, 계측 오버헤드 대조군, 3단계 runbook. 아래 순서로 하나씩 실행하고 결과를 각 항목 아래에 기록한다.
- 공통: 공유 원본·DLQ 토픽은 삭제·초기화하지 않는다. 기존 체크포인트·출력은 보존하고 항목마다 별도 경로를 쓴다. 부하·지연 측정은 다른 Spark 측정과 동시에 실행하지 않는다.

#### 1. offset 소실 시 실패

- 목적: 체크포인트가 가리키는 다음 offset이 Kafka에서 삭제됐을 때 `failOnDataLoss=true`로 쿼리가 실패하고, 소실 구간을 건너뛰어 커밋하지 않는지 확인한다.
- 환경: 시험 전용 토픽 `test.offset-loss.raw.v1`·`test.offset-loss.dlq.v1`(파티션 1, `message.timestamp.type=LogAppendTime`)을 만들고 `KAFKA_TOPIC`·`KAFKA_DLQ_TOPIC`만 바꿔 실행한다. 시험 후 이 두 토픽은 삭제한다.
- 절차: 별도 체크포인트·`startingOffsets=earliest`로 기동 → 1,000건 입력·커밋 확인 → Spark 중지 → 1,000건 추가 입력 → `kafka-delete-records.sh`로 log start offset을 마지막 커밋 offset + 500으로 옮긴다 → 같은 체크포인트로 재시작한다.
- 판정: 재시작한 쿼리가 실패로 종료하고(종료 코드 0 아님), 오류가 데이터 소실(offset 범위 밖)을 가리킨다. 마지막 커밋 batch·offset과 정상 출력 행수가 재시작 전과 같다.
- 결과 (2026-11-17): 통과. 1,000건 입력 후 batch 1 커밋(offset 1000, 정상 출력 838행)했고, 1,000건 추가 입력(끝 offset 2000) 뒤 log start offset을 1500으로 옮겨 같은 체크포인트로 재시작하니 종료 코드 1로 실패했다. 오류는 `KafkaIllegalStateException: Some data may have been lost because they are not available in Kafka any more; ... Could not read records in offset [1000, 2000) for topic partition test.offset-loss.raw.v1-0`(원인 `OffsetOutOfRangeException`)이다. 재시작 후 마지막 커밋은 batch 1·offset 1000, 정상 출력은 838행으로 같고 처리된 배치 로그는 없다(체크포인트 `offsets/2`에 계획만 기록되고 `commits/2`는 없음). 근거: [../measurements/2026-11-17-spark-offset-loss.json](../measurements/2026-11-17-spark-offset-loss.json)

#### 2. 소비 지연·처리 지연 주입

- 목적: [처리지연 측정](#처리지연-측정)의 지표가 소비 지연과 처리 지연을 구분해 보여 주는지 확인한다. 코드는 바꾸지 않는다.
- 소비 지연: 생성기를 일정 EPS로 계속 보내는 중에 Spark를 60초 중지했다가 같은 체크포인트로 재시작한다. 기대: 재시작 직후 배치의 Kafka → 콜백 대기가 중지 시간만큼 커지고, 분류·출력 action 시간은 입력 건수 증가분 이상으로 늘지 않으며, 이후 기준 수준으로 돌아온다.
- 처리 지연: 같은 입력 중에 실행 중인 Spark 컨테이너 CPU를 `docker update --cpus`로 60초 낮췄다가 원래 값(3)으로 되돌린다. 기대: 분류·계측 action과 트리거 시간이 먼저 늘고, 처리능력이 입력보다 낮아지면 소비 대기가 뒤따라 늘며, 원복 후 회복한다.
- 판정: 두 주입의 지표 변화 순서와 크기가 위 기대와 같다. 주입 구간의 배치에 지연 로그 결측·계측 실패가 없고 건수 대조가 맞는다.
- 결과 (2026-11-17): **부분 통과.** 지표는 소비 지연(Kafka → 콜백 대기만 크게 증가)과 처리 지연(트리거·분류 action 먼저 증가, 대기는 뒤따라 소폭 증가)을 구분해 보여 줬고, 순서·회복·계측 무결성·건수 대조는 기대와 같다. 다만 재시작 직후 2개 배치의 분류 action이 입력 증가분보다 크게 늘었고, CPU 원복 후 원인 미확인 2차 지연이 있었다(아래 기대와 다른 점).
- 결과 — 실행 설계: 15:51:36~16:06:44 KST, 5,000 EPS 연속 입력 900초(실제 4,980.2 EPS). 체크포인트 `/opt/spark/checkpoints/delay-20261117`, 출력 `/opt/spark/output/delay-20261117`, 최초 `latest`. 생성기 경과 기준 워밍업 0~120초, 소비 기준 120~240초, 240초에 Spark 중지 60초, 재시작 후 회복, 처리 기준 480~600초, 600초에 CPU 3 → 1, 660초에 3으로 원복, 900초 입력 중단. 근거: 이전 측정에서 워밍업이 약 80초에 안정됐고, 기준 구간은 배치 24개(트리거 5초)를 확보하는 길이다. 중지는 배치 커밋 직후(진행 로그 확인 즉시)에 요청해 콜백 중단·재처리를 피했다. CPU 1은 기준 CPU 평균 약 216%로 보아 처리능력이 입력의 절반 수준이 될 것으로 추정해 골랐다.
- 결과 — 한 실행 선택 이유: 두 주입이 같은 입력·같은 체크포인트·같은 기준선에서 비교되고, 중지·재시작 후 CPU 기준 구간이 소비 지연 회복을 함께 확인한다. 중지에는 컨테이너 교체가 필요해 컨테이너는 a(중지 전)·b(재시작 후) 2개다.

| 구간 (콜백 시작 기준) | 배치 | 배치당 입력 | 트리거 평균(최대) | 분류·계측 action 평균 (1만 건당) | 대기 배치별 p95 |
| --- | --- | --- | --- | --- | --- |
| 소비 기준 120~241.6초 | 24 | 24,982 | 2.30초 (2.75) | 1.25초 (0.50초) | 4.77~4.81초 |
| 중지 241.6~312.9초 | 0 | — | — | — | offset 차이 약 2만 → 32만(재시작 요청 시) |
| 재시작 직후 b49·b50 | 2 | 49,999 | 14.97 / 7.33초 | 7.38 / 4.04초 (1.48 / 0.81초) | 73.66 / 77.36초 (p50 69.12 / 72.87초) |
| 백로그 소진 b51~b64 | 14 | 약 50,000 (상한) | 3.46~5.22초 | 2.09~2.79초 (0.42~0.56초) | 74.66 → 9.39초, 배치마다 약 5초 감소 |
| 처리 기준 480~600초 | 24 | 24,980 | 2.24초 (2.73) | 1.21초 (0.49초) | 4.77~4.82초 |
| CPU 1 첫 배치 b105 | 1 | 24,949 | 5.12초 | 2.79초 | 4.79초 (변화 없음) |
| CPU 1 b106~b113 | 8 | 25,631 → 33,421 | 6.0~7.9초 (모두 5초 초과) | 평균 3.87초 (1.29초) | 5.04 → 6.47초 |
| 원복 직후 b114~b116 | 3 | 39,466 → 17,721 | 4.31 → 2.06초 | 3.00 → 1.04초 | 7.58 → 3.41초, b117(673.8초)부터 4.80초 |
| 2차 지연 b119~b128 | 10 | 19,647~27,668 | 2.6~5.9초 | 1.2~3.6초 | 최대 5.81초(b124) |
| 안정 b129~b162 | 34 | 약 25,000 | 2.16~3.58초 | — | 4.76~4.81초 |

- 결과 — 소비 지연: 마지막 기준 콜백(238.8초)부터 재시작 첫 콜백(312.9초)까지 74.1초 동안 콜백이 없었고(중지 60.1초 + 중지·기동 약 14초), 첫 배치 대기 p50이 기준 2.54초 → 69.12초로 그 공백만큼 커졌다. 분류 action은 b51부터 1만 건당 0.42~0.56초로 기준(0.50초)과 같아 입력 증가분만큼만 늘었다. 50,000 offsets 상한 배치 16개로 백로그를 소진하고 b65(403.8초, 재시작 요청 후 약 101초)에 대기 p95 4.78초로 돌아왔다.
- 결과 — 처리 지연: CPU 1 직후 b105는 트리거·분류 action이 약 2.3배로 늘었지만 대기는 그대로였고, b106부터 트리거가 5초를 넘으며 배치 입력과 대기 p95가 뒤따라 늘었다. 원복 후 3개 배치(약 13초) 안에 대기가 기준으로 돌아왔다. CPU 1 동안 컨테이너 CPU는 94~97%로 제한에 묶였다.
- 결과 — 기대와 다른 점: (1) 재시작 직후 b49·b50은 분류 action이 1만 건당 1.48·0.81초로 기준의 3.0·1.6배였다. 최초 기동 워밍업 b1~b3과 같은 JVM·Python worker 기동 비용으로 보이며, 이 두 배치에서는 "입력 증가분 이상으로 늘지 않음"을 충족하지 못했다. (2) CPU 1의 실제 처리능력은 예상(입력의 절반)보다 높았다. 배치가 약 3만 건으로 커지면서 9개 배치 270,790건을 약 57초에 처리해 약 4,750건/s였고, offset 차이는 약 3.4만 → 5.1~5.5만에서 멈췄다. 그래서 대기 증가는 p95 +1.7초로 작고 무한 증가하지 않았다. (3) 원복 약 24초 뒤 683.8~728.8초에 트리거 최대 5.92초·대기 p95 최대 5.81초의 2차 지연이 있었다. 이 구간 `docker stats` 표본에 CPU 351%·344%(제한 3 초과)가 찍혔고 원인은 확인하지 못했다(호스트 부하는 측정하지 않음). b129부터 다시 안정됐다.
- 결과 — 무결성·건수: 배치 0~163, 164개 지연 로그가 모두 `succeeded`, 입력 있는 배치의 대기 집계 결측 0, 누락·미래 timestamp 0, 재처리 batch 없음, Spark 로그 ERROR·Exception 0, OOM 없음. 원본 offset 증가 4,482,286 = 생성기 큐 등록·ACK 4,482,286 = Spark 커밋 offset 증가 = 입력 합. 정상 3,810,403 + 제외 448,055 + DLQ 223,828이 생성기 종류별 건수와 같고, Parquet 실제 행수 3,810,403·컬럼 35개. 생성기는 SIGINT 종료 코드 130, undelivered 0으로 이전 ACK 집계 불일치는 재현되지 않았다. 입력 중단 후 약 7초 안에 offset 차이 0.
- 결과 — 정리: 컨테이너 `log-etlm-2-spark-delay-20261117-a`·`-b`는 삭제 전 CPU 제한 3(NanoCpus 3000000000)을 확인하고 삭제했다. 체크포인트·출력 경로는 남겼다. 토픽 설정은 바꾸지 않았다. 근거: [../measurements/2026-11-17-spark-delay-injection.json](../measurements/2026-11-17-spark-delay-injection.json)

#### 2. 판정 정리 (2026-11-19)

추가 측정 없이 기대와 다른 점 세 가지를 아래처럼 정리하고 2번을 종료한다. 목적인 "소비 지연과 처리 지연을 지표로 구분"은 충족했다.

- (1) 재시작 직후 b49·b50의 분류 action 증가: 기동 비용(JVM·Python worker 기동)으로 보고 판정에서 제외한다. 최초 기동 b1~b3에도 같은 모양이 있었고, 4단계 완료 기준도 트리거 시간에서 "워밍업·재시작 직후"를 제외한다. 2개 배치 뒤에는 1만 건당 시간이 기준과 같아졌다.
- (2) CPU 1의 처리능력이 예상보다 높음: 기대 문장이 틀린 것이다(예상치를 실측 없이 정함). 지표의 변화 순서·회복은 기대와 같으므로 판정에 영향이 없다.
- (3) 원복 뒤 2차 지연: 원인을 확정하지 못했다. 이후 [Spark 지연 급증 진단](../measurements/2026-11-19-spark-latency-spike-diagnosis.md)에서 같은 로컬 환경의 지연 급증이 WSL2 VM 전체 정지로 판정됐다. 이 구간은 당시 동시 관측 도구가 없어 같은 원인인지 확인할 수 없다. 로컬 환경 요인 후보로 기록하고 판정에서 제외한다.

#### 3. 계측 오버헤드 대조군

- 목적: 지연 분위수 집계가 분류·계측 action에 더하는 시간을 측정한다.
- 구현: 환경변수 `SPARK_LATENCY_METRICS_ENABLED`(기본 `true`)를 추가한다. `false`면 지연 분위수 집계를 생략하고 건수만 집계하며, 지연 로그의 `kafka_to_callback_start_ms`는 `null`이다. 분류·출력·DLQ 동작과 건수 로그는 같다. `.env.example`·`compose.yaml`·설정 검증·테스트를 함께 갱신한다.
- 측정: 지속 부하와 같은 자원·설정, 5,000 EPS, 워밍업 120초 + 본 측정 300초를 켬·끔 각 1회 실행하고 순서를 기록한다.
- 판정: 기준 통과·실패가 아니라 차이를 기록한다. 분류·계측 action 평균·p95와 배치 처리시간의 켬−끔 차이를 남기고, 1회씩 측정이라 잡음 범위를 구분하지 못한다는 한계를 함께 적는다. 끔 상태에서도 건수 대조가 맞아야 한다.
- 결과 (2026-11-17): 지연 분위수 집계를 켜면 분류·계측 action이 평균 198ms(+17.9%), 배치 처리시간이 평균 319ms(+15.2%) 늘었다. 두 실행 모두 트리거 5초를 넘은 배치가 없고, 끔 상태에서도 건수 대조가 맞았다. 1회씩 측정이라 차이 중 실행 간 잡음이 얼마인지는 구분하지 못한다.
- 결과 — 실행: 켬 → 끔 순서로 이어서 실행했다. 켬 16:13:50~16:21:02 KST(컨테이너 `log-etlm-2-spark-overhead-20261117-on`), 끔 16:21:19~16:28:33 KST(`-off`). 각각 별도 체크포인트 `/opt/spark/checkpoints/overhead-20261117-{on,off}`·출력 `/opt/spark/output/overhead-20261117-{on,off}`, `latest`, CPU 3·메모리 3g로 실행했다. 생성기 5,000 EPS 입력 420초(워밍업 120초 + 본 측정 300초) 후 중단했다. 아래 값은 생성기 경과 120~420초에 콜백이 시작된 입력 배치 기준이다.

| 지표 (본 측정) | 켬 | 끔 | 켬−끔 |
| --- | --- | --- | --- |
| 입력 배치 수 / 입력 합 | 60 / 1,498,461 | 60 / 1,499,013 | 0 / −552 |
| 분류·계측 action 평균 / p95 | 1,307.6 / 1,560.9ms | 1,109.2 / 1,444.0ms | +198.4 (+17.9%) / +116.9ms |
| 배치 처리시간(triggerExecution) 평균 / p95 | 2,427.6 / 2,793ms | 2,108.2 / 2,659ms | +319.4 (+15.2%) / +134ms |
| 정상 쓰기 action 평균 / p95 (참고) | 872.5 / 1,074.8ms | 772.9 / 956.4ms | +99.6 / +118.4ms |
| DLQ 쓰기 action 평균 / p95 (참고) | 171.7 / 261.3ms | 157.0 / 225.1ms | +14.7 / +36.1ms |
| 5초 초과 배치 | 0 | 0 | 0 |
| ACK EPS (120~420초) | 4,996.7 | 4,998.4 | −1.7 |

- 결과 — 해석: 분류·계측 action 차이 +198ms는 같은 action 안의 `percentile_approx`·min/max/mean 집계 비용으로 본다. 다만 지연 집계와 관계없는 정상 쓰기 action도 켬이 평균 +100ms 컸고, 배치 처리시간 차이(+319ms)는 세 action 차이 합(약 +313ms)과 비슷하다. 따라서 +198ms 중 일부는 실행 간 변동(호스트 부하, 순서 효과)일 수 있다. 확정하려면 순서를 바꿔 반복 측정해야 한다. 켬 상태에서도 배치 처리시간 평균은 트리거 주기의 약 49%라 3단계 기본값은 켬으로 유지한다.
- 결과 — 건수·무결성: 켬은 원본 offset 증가 2,096,331 = 생성기 ACK = 커밋 입력 = 정상 1,782,346 + 제외 209,669 + DLQ 104,316, Parquet 1,782,346행·35컬럼. 끔은 2,105,852 = 생성기 ACK = 커밋 입력 = 정상 1,789,978 + 제외 210,438 + DLQ 105,436, Parquet 1,789,978행·35컬럼. 두 실행 모두 지연 로그 전부 `succeeded`, Spark 로그 ERROR·Exception 0, OOM 없음, 생성기 undelivered 0, 입력 중단 후 약 11~12초 안에 offset 차이 0. 끔은 지연 로그 87개의 `kafka_to_callback_start_ms`가 모두 `null`이고 켬은 결측 0이다.
- 결과 — Compose 기본값: 다른 변수와 달리 `compose.yaml`에서 `${SPARK_LATENCY_METRICS_ENABLED:-true}`로 기본값을 둔다. 이 변수가 없는 기존 로컬 `.env`로도 Compose가 깨지지 않게 하기 위해서다. `config.py`는 다른 값처럼 기본값 없이 필수로 읽고 `true`·`false` 외 값은 오류로 거부한다.
- 결과 — 정리: 컨테이너 2개는 CPU 제한 3(NanoCpus 3000000000)·메모리 3g를 확인하고 삭제했다. 체크포인트·출력 경로는 남겼다. 토픽 설정은 바꾸지 않았다. 근거: [../measurements/2026-11-17-spark-metrics-overhead.json](../measurements/2026-11-17-spark-metrics-overhead.json)

#### 4. 3단계 runbook 보완

- `docs/runbooks/local-development.md`에 Spark 절을 보완한다: 기동·중지·같은 체크포인트 재시작, 측정용 별도 체크포인트 실행, 테스트 명령, 정상 출력·DLQ 쓰기 실패와 offset 소실 시 조치, DLQ 조회·원문 복원 절차 참조, 체크포인트 초기화 시 주의.
- 위 1~3과 기존 검증 결과에서 실제로 실행한 명령만 적는다.
- 결과 (2026-11-17): [로컬 개발 runbook](../runbooks/local-development.md) 7절에 기동·로그·중지, 같은 체크포인트 재시작, 테스트, 측정용 별도 경로 실행, 장애 증상별 조치(정상 출력·DLQ·커밋 실패, offset 소실), 체크포인트 초기화 주의, DLQ 조회·원문 복원 참조를 추가했다. offset 소실 후 새 체크포인트로 시작하는 조치는 실행·검증하지 않았다고 명시했다.

## 완료 기준

해당 단계에 착수할 때 지연 목표값·측정 구간·샘플링 설정을 포함한 정량 기준을 확정한다. Spark 지연 로그의 구현·검증 범위는 위 결과를 따르며, ClickHouse 관련 계측과 Kafka 실환경의 일부 검증은 남아 있다.

불량 3종의 DLQ 격리·메시지 및 오류 사유 조회·원문 bytes 복원·원본 위치 추적, health_check 제외, DLQ 쓰기 실패 시 배치 재시도와 중복 판별, 체크포인트 재개 및 offset 소실 시 실패를 검증한다. 배치 진행 정보와 offset 범위의 연결, 실제 action 기준 시간, 쓰기 시간과 전체 시간의 중복 방지, 이벤트 기준 집계를 검증한다. 소비 지연과 처리 지연을 각각 주입해 지표를 확인한다.
