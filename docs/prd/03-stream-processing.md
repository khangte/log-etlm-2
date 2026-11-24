# Spark 스트림 처리

- 상태: 완료 (5,000 EPS 지속 부하 충족(로컬 Parquet 1차, ClickHouse 경로는 4단계에서 재측정·통과), 출력 장애·재시작·offset 소실·DLQ 조회 검증 완료, 지연 주입 부분 통과 후 판정 정리, 계측 오버헤드 1회 측정. [3단계 마무리 검증 계획](#3단계-마무리-검증-계획-2026-11-17) 종료). 6단계 [Spark 튜닝](#spark-튜닝-6단계-2026-11-20) 진행 중(튜닝 목표 평시·지속 용량·스파이크 모두 충족, [스파이크 판정](#스파이크-판정-생성기-2개-2026-11-23)·[생성기 2개 step 지속 용량 12,000 EPS](#지속-용량-판정-생성기-2개-step-재측정-2026-11-23) 2026-11-23, [`daily.csv` 전체 확인](#daily-전체-확인-2026-11-24) 2026-11-24 미충족, [동시 정지 조사](#daily-동시-정지-원인-조사-2026-11-24)·[producer_stats 묶음 비교](#daily-비교-실행-producer_stats-5초-묶음-2026-11-24): 묶음 INSERT로 IO 압력 정지 제거, 11개 구간 중 4개 미달(2개는 감지기 틱 지연 구간), [지연 원인](#daily4-vm-지연-원인-조사-2026-11-24) 미확정)
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
| `SPARK_DRIVER_MEMORY` | `2g` → `1536m` (6단계) | local 모드에서는 driver heap이 실행 메모리 전체다. 배치 최대 원문 39.7MB(50,000 × 793B)를 파싱·컬럼 전개로 10배 잡아도 약 400MB다. 6단계 측정에서 살아 있는 데이터는 250~560MB였고, 2g에서는 힙 commit이 1.7GB까지 늘어 컨테이너가 3g에 닿았다. 1536m으로 줄여 부하 중 메모리 최대가 약 420MB 줄었다([JVM 튜닝 결과](#jvm-튜닝-결과-2026-11-20)). |
| 컨테이너 `mem_limit` / `cpus` | `3g` / `3` (6단계에서 `cpus` 4) | heap 2g에 JVM 비heap·네이티브 약 1g를 더한다. Kafka 2 + UI 1 + Spark 3 = 6GiB이며 ClickHouse·Grafana·개발 도구에 약 6GB가 남는다. `cpus`는 스파이크 측정에서 상한에 걸린 주기가 16.8%라서 6단계에서 4로 올렸다([자원 조정](06-load-profile.md#자원-조정)). |
| 컨테이너 `MALLOC_ARENA_MAX` (6단계 추가) | `2` (Compose 고정) | glibc malloc arena 상한. 기본값(코어 8 × 8 = 64개)에서는 arena가 해제 메모리를 쥐어 JVM 힙 밖 RSS가 약 0.4GB 늘었다. Python 워커에도 적용된다. 근거는 [메모리 분해](#메모리-분해-2026-11-20)다. |
| `SPARK_TRIGGER_INTERVAL` | `5 seconds` | 배치당 약 25,000건(19.8MB). 반영 지연에 최대 5초를 더하며 60초 갱신 대기보다 작다. |
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | `65000` | 2026-11-23 55,000에서 올렸다([B1](#b1-배치-상한-65000-실험-2026-11-23)). 수용 상한은 13,000 EPS다. 2026-11-19 50,000에서 55,000으로 올렸다. 근거는 [부하 프로파일 자원 조정](06-load-profile.md#자원-조정)이다. 처음 값 50,000은 평상시 예상 25,000건의 2배였다. 5초 내 처리 시에만 최대 10,000 EPS에 해당하며 처리속도를 보장하지 않는다. 첫 지속 부하 실측에서는 약 50,000건 처리에 평균 16.05초가 걸렸고, 2단 검증 적용 후 재측정에서는 배치당 약 25,000건을 평균 2.04초에 처리했다. |
| `SPARK_SHUFFLE_PARTITIONS` | `3` | `local[3]` 태스크 슬롯 수와 같게 두어 셔플 후 태스크가 한 번에 모두 실행된다. 슬롯보다 많으면 태스크가 여러 차례로 나뉘어 실행될 뿐 병렬도는 늘지 않고, 배치당 Parquet 파일 수만 늘어난다. 4단계 `SPARK_JDBC_NUM_PARTITIONS`와 같은 값이다. 체크포인트에 기록되므로 변경하려면 체크포인트를 새로 만든다. |
| `SPARK_JDBC_BATCH_SIZE` (4단계 추가) | `50000` | 가장 큰 쓰기 파티션 행 수 이상이면 쓰기 파티션당 INSERT 1회다. `maxOffsetsPerTrigger`와 같을 필요는 없다. 근거는 [ClickHouse PRD](04-clickhouse-storage.md)다. |
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

원문 파싱·검증·분류와 DataFrame 변환을 `transforms.py` 하나에 모은다. `validation.py`·`classify.py`는 별도로 두지 않는다. `classify()`는 원문 검증과 정상·제외·불량 분류를 수행하고, `classify_batch()`는 이를 UDF로 DataFrame에 적용한다. `select_orders()`는 정상 필터·구조화·평탄화·출력 컬럼 선택, `build_dlq_records()`는 불량 필터·DLQ 메시지 구성을 담당한다. `batch.py`의 `process_batch()`는 변환 → 저장 순서를 제어한다(배치 내 중복 제거는 2026-11-23 D1로 제거). 중첩 필드 전개에는 `select()`를 사용한다. 처리 순서·출력·계측 계약은 유지한다.

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

- 정상 출력 경로에서 배치 내 `event_id` 중복 제거(`dropDuplicates`)를 하지 않는다(2026-11-23 D1, 이전에는 적용). v1 입력 계약(생성기가 이벤트마다 새 UUID 발급, Producer 멱등 전송)상 입력 중복이 생기지 않고, 실제 중복 원인인 마이크로배치·JDBC 재실행 중복은 어떤 Spark 중복 제거로도 막지 못해 아래 ClickHouse 정책이 처리한다. 배치 내 중복 제거는 평시 피크 배치당 셔플 비용 336~375ms였다([job 비용 대조](#job-비용-대조-벤치-2026-11-23)). 같은 ID·다른 `event_timestamp` 입력을 임의로 하나 버리지 않으므로 ClickHouse 충돌 점검이 이를 그대로 드러낸다. 상태를 유지하는 중복 제거(워터마크)도 도입하지 않는다. 비교 근거는 [중복 제거·조회 조합 비교](04-clickhouse-storage.md#중복-제거조회-조합-비교)를 따른다.
- 입력 중복과 JDBC 일부 적재 후 재시도 중복은 모두 ClickHouse에서 처리한다. [ClickHouse 적재 중복 정책](04-clickhouse-storage.md#jdbc-재시도와-적재-중복-처리)에 따라 `ReplacingMergeTree()` / `ORDER BY (event_timestamp, event_id)`로 저장하고 `FINAL` 기반 조회 뷰에서 논리 중복을 제거한다.
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
- 원본 offset 범위는 동일 쿼리·실행·배치 키의 `progress` 로그에서 연결한다. 건수 로그의 정상 건수는 정상 출력 행 수와 같다(배치 내 중복 제거 없음, D1). 성공한 콜백도 이후 체크포인트 실패로 트리거가 실패할 수 있으므로 콜백 성공과 배치 커밋 성공을 구분한다.
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

## Spark 튜닝 (6단계, 2026-11-20)

[부하 프로파일 PRD](06-load-profile.md)의 단일 생성기 튜닝 세 번째 단계다. Producer 단계에서는 `linger.ms=5`를 유지했다([Producer 튜닝 결정](02-kafka-ingestion.md#결정-2026-11-19)). 기준값 측정에서 Spark가 못 맞춘 목표는 다음과 같다([기준값 측정](06-load-profile.md#기준값-측정-완료-기준-4-2026-11-19)).

- 평시 피크(7,500 EPS) 배치 트리거 시간 p99: 4,976ms. 목표는 3,500ms 이하다.
- 지속 용량: 판정값 11,000 EPS는 배치 상한(`SPARK_MAX_OFFSETS_PER_TRIGGER=55000`, 11,000 EPS)과 같다. 실처리량은 약 10,200~10,900 EPS라 10,000 EPS 단계의 여유가 작다.
- 9,000 EPS 이상에서 Spark는 시간의 약 97% 동안 배치를 실행 중이다. Producer ACK 지연의 추정 원인(호스트 CPU 경합)과도 겹친다.

### 비용 분해 (2026-11-20)

운영 배치의 action별 시간(`ops.spark_batch_attempts`, 기준값 `step.csv` 실행, 입력 50,000건 이상 배치 51개 평균, 평균 54,118건):

| action | 평균 |
| --- | ---: |
| 분류·계측 (`classification_ms`: Kafka 읽기, 분류 UDF, persist, 건수·지연 집계) | 2,816ms |
| 정상 출력 (`orders_write_ms`: 구조화·평탄화, 배치 내 중복 제거, JDBC 쓰기) | 1,970ms |
| DLQ 출력 (`dlq_write_ms`) | 214ms |
| 콜백 전체 | 5,003ms |

같은 크기(파티션당 18,000건, 54,000건)의 고정 offset 구간을 batch 모드로 읽어 단계별로 나눠 측정했다. 스트리밍 컨테이너는 멈추고, 같은 이미지·`local[3]`·`--driver-memory 2g`·CPU 4로 1회성 컨테이너를 띄웠다. 출력은 `noop` 형식이라 JDBC·Kafka 쓰기 시간은 빠진다. 각 항목은 워밍업 1회 후 3회 중앙값이다. 측정 스크립트는 [bench_spark_stages.py](../measurements/bench_spark_stages.py)다.

| 측정 | 중앙값 | 해석 |
| --- | ---: | --- |
| Kafka 읽기만 | 729ms | 배치마다 드는 고정 비용에 가깝다 |
| 읽기 + pickle 분류 UDF | 2,066ms | UDF 약 1,340ms. `classify` 순수 CPU(행당 53.7µs × 54,000건 ÷ 3코어 ≈ 970ms)와 Python 워커 직렬화(약 370ms)다 |
| 읽기 + Arrow 분류 UDF | 1,632ms | pickle보다 약 430ms 짧다 |
| 읽기 + `from_json` 평탄화 | 992ms | JVM 파싱 약 260ms |
| 운영 경로(쓰기 제외), pickle | 3,940ms | 분류·계측 약 2,100~2,300ms, 정상 변환 약 1,000~1,300ms, DLQ 변환 약 140ms |
| 운영 경로(쓰기 제외), Arrow | 3,267ms | pickle보다 약 17% 짧다. 순서 효과가 섞였을 수 있어 교차 반복으로 다시 확인한다 |

- 운영 정상 출력 1,970ms와 벤치 정상 변환 약 1,000~1,300ms의 차이(약 0.7~1.0초)가 JDBC 쓰기 시간으로 추정된다.
- `classify` 비용 중 약 28%는 행의 3.3%인 불량 문서의 엄격 검증 경로(건당 약 450µs)다. 정상 문서는 건당 33.5µs이고, `json.loads`(중복 키·비표준 상수 hook 포함) 11.6µs, `strptime` 8.0µs, 1단 빠른 검증 1.4µs, date-time 형식 검사 2.1µs, 의미 검증 1.2µs다.

### 가설

- 분류 UDF(약 1.3초)가 54,000건 배치의 가장 큰 단일 비용이다. Python 워커 직렬화를 Arrow로 바꾸고 `classify` CPU를 줄이면 배치 시간이 줄어든다.
- 배치 시간이 줄면 평시 피크 p99가 3,500ms 아래로 내려가고, 10,000 EPS 단계의 실처리량 여유가 늘어난다.

### 실험

한 번에 하나씩 적용하고 측정한다. 앞 실험에서 채택한 변경은 다음 실험의 기준이 된다.

| 실험 | 변경 | 분류 결과 영향 |
| --- | --- | --- |
| S1 | Arrow 최적화 Python UDF(`useArrow=True`). Spark 이미지에 `pyarrow`·`pandas`를 고정 버전으로 설치한다 | 같은 `classify` 함수라 바뀌지 않아야 한다. pickle·Arrow 결과 대조 테스트로 확인한다 |
| S2 | `classify` 정상 경로 CPU 축소. `strptime` 검사와 중복 키 hook을 같은 판정의 더 싼 구현으로 바꾼다 | 기존 단위 테스트·대조 테스트로 결과 동일을 확인한다 |
| S3 | 불량 문서 엄격 검증 경로 비용 축소 | `error_code`·`error_path`가 바뀌지 않아야 한다. 프로파일로 원인을 확인한 뒤 방법을 정한다 |
| S4 | 이벤트 시각 범위 검사의 `get_json_object` 재파싱 제거. 분류 결과에 원본 `event_timestamp`를 담는다 | DLQ 메시지 스키마는 바뀌지 않는다 |

- 정상 변환·JDBC 쓰기(합계 약 2초)는 S1~S4 뒤 비용 분해를 다시 해서 후보를 정한다.
- 후보에서 뺀 것:
  - Spark SQL 내장 함수로만 검증: 중복 키·`15000.0` 같은 엄격 규칙을 SQL 표현식으로 같게 판정할 수 없다. 규칙을 바꾸려면 이 PRD의 검증 규칙부터 다시 정해야 한다.
  - Scala UDF: 검증기를 JVM 언어로 다시 구현해야 하고, 계약 검증 기준(`jsonschema`)이 둘로 나뉜다.
  - CPU·코어 증설: 호스트 8 논리 코어를 생성기·Kafka·ClickHouse와 나눠 쓰는 자원 예산은 유지한다.
  - 배치 상한(`SPARK_MAX_OFFSETS_PER_TRIGGER`) 변경(2026-11-23 B1에서 다시 정해 65,000으로 올렸다): 지속 용량 판정 범위(11,000 EPS)를 유지한다. 실처리량이 상한을 넘으면 다시 정한다.

### 선택 기준

1. 분류 결과가 같다. `outcome`과 `error_*` 필드가 기존 구현과 같아야 한다(단위 테스트, pickle·Arrow 대조 테스트).
2. 벤치 운영 경로(쓰기 제외) 중앙값이 기준보다 5% 이상 짧다. 기준과 후보는 같은 컨테이너에서 번갈아 실행한다.
3. 채택한 변경을 모두 적용한 뒤 `steady.csv`·`step.csv`를 실행해 [튜닝 목표](06-load-profile.md#튜닝-목표)로 판정한다: 평시 피크 트리거 p99 ≤ 3,500ms, 10,000 EPS 단계 통과, 종단 간 지연 p99 ≤ 10초, 공통 기준 충족.
4. Spark 메모리 최대치가 `mem_limit: 3g` 안이다.

### 실험 결과 (2026-11-20)

벤치는 [비용 분해](#비용-분해-2026-11-20)와 같은 구간·조건에서 JIT 워밍업 3회 후 5회 중앙값이다. 기준과 후보는 1회성 컨테이너를 번갈아 3쌍(S1은 2쌍) 실행했다. 기준 코드는 직전 커밋의 `apps/streaming/src`를 마운트해 실행했다.

| 실험 | 측정 | 기준 | 후보 | 차이 | 판정 |
| --- | --- | ---: | ---: | ---: | --- |
| S1 Arrow UDF | 운영 경로(쓰기 제외) 중앙값 평균 | 3,822ms | 3,569ms | −6.6% | 채택 |
| S1 | 읽기 + 분류 UDF | 2,025ms | 1,828ms | −9.7% | |
| S2 `strptime`·중복 키 hook | `classify` 정상 문서(µs/건) | 36.5 | 23.6 | −35% | 채택 |
| S3 `$ref` 인라인 | 엄격 검증 경로(µs/건, 누락 필드·타입 오류) | 529·494 | 302·355 | −43%·−28% | 채택 |
| S2+S3 | `classify` 혼합 평균(µs/건, 생성기 출력 20,000건) | 약 53 | 약 36 | −32% | |
| S1+S2+S3 | 운영 경로(쓰기 제외) | 3,628ms | 3,102ms | −14.5% | |
| S1+S2+S3 | 읽기 + 분류 UDF | 2,014ms | 1,390ms | −31% | |
| S4 `get_json_object` 제거 | 운영 경로(쓰기 제외), S1~S3 대비 | 3,102ms | 3,052ms | −1.6% | 미채택, 되돌림 |

- S1: 이미지에 `pyarrow==25.0.1`·`pandas==2.3.3`을 추가하고 `classify_udf`를 `useArrow=True`로 등록했다. 생성기 출력 3,000건과 Kafka 경계 입력 9건에서 Arrow UDF·pickle UDF·드라이버 직접 호출의 7개 필드가 모두 같다(`TestArrowUdf`). 첫 1회성 측정(워밍업 1회)의 −17%는 순서 효과가 섞인 값이었고, 교차 반복에서는 −6.6%다.
- S2: `strptime`을 `datetime.fromisoformat(값[:-1])`로 바꿨다. 계약 pattern이 `YYYY-MM-DDTHH:MM:SS.mmmZ` 형식을 보장하므로 남는 판정은 Python datetime 표현 범위(연도 0000, 초 60, 시 24 등)뿐이다. 12개 경계 시각에서 두 방식의 판정이 같다. 중복 키 hook은 C `dict()`로 만든 뒤 키 수가 줄었을 때만 중복 키를 찾는다. 오류 메시지는 순서상 처음 다시 나온 키를 가리켜 기존과 같다.
- S3: 프로파일에서 엄격 검증 경로 시간의 약 25%가 `$ref` 참조 해석(`referencing`)이었다. 계약의 `$ref` 14개는 모두 형제 키워드 없는 `#/$defs/...` 참조라, 엄격 검증기에만 대상 정의를 펼친 사본을 쓴다. 계약 파일과 1단 빠른 검증기는 바꾸지 않는다. 다른 형태의 `$ref`가 생기면 기동 시 실패한다. 정상 주문의 모든 경로에 키 삭제·타입 변경·빈 값·추가 필드 변형을 적용한 문서에서 `best_match` 오류의 키워드·경로·메시지가 원본 스키마 검증기와 같다(`TestInlinedRefs`, 오류 문서 467개).
- S4: 분류 결과에 원본 `event_timestamp`를 담아 `get_json_object`를 없앴으나 차이가 측정 편차(±100ms 안팎) 안이라 선택 기준 2에 못 미쳐 되돌렸다.

### 프로파일 판정: S1+S2+S3 (2026-11-20)

- 원자료: [steady](../measurements/2026-11-20-spark-s13-steady.json), [step](../measurements/2026-11-20-spark-s13-step.json). 기준값은 [steady](../measurements/2026-11-19-baseline-steady.json)·[step](../measurements/2026-11-19-baseline-step.json)이다. 각 1회 실행했다. 생성기 run_id는 steady `7e9f312675304966abd9e780fe51166b`, step `f9a0ab2c52ed42b1af5713b7edb0eaf5`, Spark run_id는 steady `6ddc17f4-7345-444a-8711-fce10701c854`, step `841bf3b4-4ab7-4994-bba1-4cb164c64e5a`다.
- Grafana 캡처: steady(2026-11-20 01:01:30~01:13:30 KST) [파이프라인 운영](../../images/2026-11-20-spark-s13-steady/pipeline.png)·[주문 현황](../../images/2026-11-20-spark-s13-steady/orders.png), step(01:12:51~01:26:51 KST) [파이프라인 운영](../../images/2026-11-20-spark-s13-step/pipeline.png)·[주문 현황](../../images/2026-11-20-spark-s13-step/orders.png).

| 구간 | 트리거 평균 (기준 → 후보) | 트리거 p99 | 실처리량 EPS | 종단 간 p99 | ACK p95 100ms 이내 비율 |
| --- | --- | --- | --- | --- | --- |
| steady base 5,000 | 2,643 → 2,269ms | 3,266 → 2,704ms | 9,451 → 11,014 | 7.38 → 6.80초 | 100% → 100% |
| steady peak 7,500 | 3,542 → 3,226ms | 4,976 → **5,955ms** | 10,558 → 11,559 | 8.06 → 8.56초 | 99.7% → **98.0%** |
| step 8,000 | 3,869 → 3,404ms | 5,209 → 3,828ms | 10,305 → 11,731 | 8.70 → 7.97초 | 99.2% → 99.2% |
| step 9,000 | 4,382 → 3,571ms | 5,845 → 4,700ms | 10,155 → 12,551 | 9.73 → 8.15초 | 93.2% → 97.5% |
| step 10,000 | 4,532 → 3,740ms | 5,601 → 4,627ms | 10,903 → 13,300 | 9.23 → 8.41초 | 92.4% → 95.8% |
| step 11,000 | 4,941 → 4,472ms | 5,642 → 6,237ms | 10,803 → 12,161 | 9.88 → 9.88초 | 72.3% → 79.8% |

- 지속 용량: 10,000 EPS 단계까지 통과했다(목표 충족). 11,000 단계는 backlog 증가 추세(24.3건/초)로 실패했다. 실처리량은 단계마다 15~22% 늘었다.
- 평시 피크: 트리거 평균은 9% 줄었으나 p99(5,955ms)가 목표 3,500ms를 넘었다. 구간 배치 59개 중 5초를 넘은 2개(batch 71: 5,115ms, 89: 5,854ms)가 p99를 정했다. 두 배치는 분류·계측 action이 3.0~3.2초로, 같은 크기의 다른 배치(1.6~1.9초)보다 길었다.
- Spark CPU 상한에 걸린 주기 비율: steady 19.9% → 9.5%, step 23.6% → 11.6%. CPU 사용 시간은 steady 1,184초 → 985초, step 1,930초 → 1,528초다.
- **메모리:** Spark `memory.peak`가 3,221,356,544B로 `mem_limit: 3g`(3,221,225,472B)에 닿았다. OOM kill은 0이다. steady 후반 `anon`은 2.99GB, `file`은 3~12MB로 기준값(`anon` 2.91GB, `file` 58~200MB)보다 page cache가 더 밀려났다. Arrow UDF로 Python 워커마다 pyarrow·pandas가 올라온 영향으로 추정한다. 평시 피크의 느린 배치 2개와의 관계는 확인하지 않았다. 선택 기준 4는 경계 상태다.
- 판정: 선택 기준 3 중 지속 용량·종단 간 지연·공통 기준은 충족했다. 평시 피크 트리거 p99는 미충족이다. 다음 후보는 메모리 여유 확보(드라이버 힙 축소 또는 `mem_limit` 조정)와 느린 배치 원인 확인이다.

### 메모리 분해 (2026-11-20)

S1~S3 적용 이미지로 `steady.csv`를 4회 실행해 Spark 컨테이너 메모리 구성을 나눴다. 수집 스크립트는 [run_spark_mem.sh](../measurements/run_spark_mem.sh), 구간 요약은 [spark_mem_summary.py](../measurements/spark_mem_summary.py), NMT 요약은 [spark_nmt_summary.py](../measurements/spark_nmt_summary.py)다. 수집 항목은 `jstat -gc`(1초), Spark REST executors·cgroup `memory.stat`·프로세스 RSS(5초)다. 지연은 [analyze_profile.py](../measurements/analyze_profile.py)로 집계했다. 실행마다 새 `SPARK_CHECKPOINT_DIR`을 쓰고, Kafka backlog를 다 읽은 뒤 부하를 시작했다.

| 실행 | 조건 | 생성기 run_id | Spark run_id | 원자료 | Grafana 캡처 (KST) |
| --- | --- | --- | --- | --- | --- |
| M1 | 기본 설정 | `8626d3ef2a2d47f691c950e1567d1b08` | `c94f2e3f-00bc-4efa-bc2e-b2117361166a` | [JSON](../measurements/2026-11-20-spark-mem-jstat.json) | 10:32:10~10:44:10 [파이프라인](../../images/2026-11-20-spark-mem-jstat/pipeline.png)·[주문](../../images/2026-11-20-spark-mem-jstat/orders.png) |
| M2 | NMT 켬(`-XX:NativeMemoryTracking=summary`, `jcmd` 30초) | `7e669dd4a4404d60b6b24022c636d108` | `f045386b-85f7-4afa-8414-56229389a829` | [JSON](../measurements/2026-11-20-spark-mem-nmt.json) | 14:55:10~15:07:10 [파이프라인](../../images/2026-11-20-spark-mem-nmt/pipeline.png)·[주문](../../images/2026-11-20-spark-mem-nmt/orders.png) |
| M3 | `MALLOC_ARENA_MAX=2` (JVM·Python 워커 모두) | `04967092b32f4b51a1329f4a8ff7696c` | `e06feed2-631b-45ea-9ec9-fff84d8943f7` | [JSON](../measurements/2026-11-20-spark-mem-arena2.json) | 15:30:57~15:42:57 [파이프라인](../../images/2026-11-20-spark-mem-arena2/pipeline.png)·[주문](../../images/2026-11-20-spark-mem-arena2/orders.png) |
| M4 | M3 반복. 호스트 CPU 상위 프로세스 5초 기록 추가 | `0a557bf8b7d74f1c8431bbec257da7f0` | `c4f45b78-f3dc-482a-9156-9d3d63dea97e` | [JSON](../measurements/2026-11-20-spark-mem-arena2-2.json) | 16:12:49~16:24:49 [파이프라인](../../images/2026-11-20-spark-mem-arena2-2/pipeline.png)·[주문](../../images/2026-11-20-spark-mem-arena2-2/orders.png) |

**메모리 (peak 구간 최대)**

| 항목 | M1 | M2 | M3 | M4 |
| --- | ---: | ---: | ---: | ---: |
| `memory.peak` (컨테이너 시작 이후) | 3,222,913,024B | 3,221,360,640B | **3,117,785,088B** | 3,221,291,008B (부하 전 catch-up 중 도달) |
| 부하 중 `memory.current` 최대 | 3,057MB | 3,072MB | 2,949MB | 3,072MB (warmup), 2,922MB (peak) |
| OOM kill | 0 | 0 | 0 | 0 |
| cgroup `anon` 최대 / `file` 최소 | 2,968 / 28MB | 3,023 / 19MB | 2,851 / 49MB | 2,762 / 106MB |
| JVM RSS | 2,578MB | 2,560MB | 2,362MB | 2,276MB |
| 힙 commit | 1,571MB | 1,445MB | 1,624MB | 1,598MB |
| JVM RSS − 힙 commit | 1,007MB | 1,115MB | **738MB** | **678MB** |
| 힙 사용 최대 / 최소(GC 직후) | 1,364 / 247MB | 1,301 / 231MB | 1,592 / 591MB | 1,398 / 239MB |
| Old 최대 | 472MB | 506MB | 730MB | 563MB |
| GC 시간 비율 / Full GC | 2.5% / 0 | 2.1% / 0 | 1.2% / 0 | 2.7% / 0 |
| storage memory 사용 (최대 1,049MB) | 77MB | 72MB | 97MB | 103MB |
| Python 워커 4개 RSS 합 | 556MB | 563MB | 582MB | 590MB |

- 힙: 살아 있는 데이터는 250~500MB이고 GC 비율은 2% 안팎이다. 힙 commit은 1.4~1.6GB에 머물러 `-Xmx`를 1.5g로 낮춰도 줄어드는 양이 거의 없다.
- M2 NMT committed 최대 합계는 2,013MB다(Java Heap 1,445, Metaspace 178, Code 127, GC 104, Symbol 42, Thread 37(182개), Class 30, Other 29, Arena Chunk 24, Compiler 22, NMT 18). direct buffer가 들어가는 Other가 29MB라 Arrow 버퍼는 원인이 아니다.
- 부하 직후 idle 시점 1회 측정(원본 smaps는 보존하지 않음): JVM `RssAnon` 2,021MB, NMT committed 1,544MB로 NMT가 못 보는 메모리가 약 477MB다. 64MB 정렬 익명 영역이 66개, RSS 합 681MB이고 NMT malloc 추적분은 126MB다. 컨테이너가 보는 `nproc`이 8이라 glibc malloc arena 상한이 64개다. 해제된 메모리를 arena가 쥐고 있는 것으로 판단했다.
- M3: `MALLOC_ARENA_MAX=2`로 JVM 힙 밖 메모리가 약 380MB 줄었고 `memory.peak`가 한도보다 약 100MB 낮아졌다. 대신 힙 commit이 약 180MB 늘었다. 부하 전 catch-up이 75배치(M2는 7배치)로 길어 G1이 힙을 키운 뒤 돌려주지 않은 것으로 추정하나 확인하지 않았다.
- M4: 힙 밖 메모리는 678MB로 M3와 같은 수준이다. 부하 중 `memory.current` 최대는 3,072MB(warmup), peak 구간 2,922MB로 한도 안이었다. 다만 `memory.peak`는 부하 시작 전 catch-up(105배치, 배치당 55,000건) 중에 이미 한도에 닿았다. 힙 commit은 warmup 1,727MB에서 base 1,417MB로 줄어, G1이 힙을 돌려주는 것도 확인했다.

**지연 (같은 `steady.csv`)**

| 실행 | 구간 | 생성 EPS | ACK p95 100ms 이내 비율 | 트리거 평균 / p99 | 5초 초과 배치 | backlog 최대 | 종단 간 p99 | 호스트 사용 코어 평균 |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| M1 | base | 4,733 | 91.1% | 4,375 / 9,770ms | 5 | 0 | 12.32초 | 4.50 |
| M1 | peak | 7,350 | 92.6% | 4,406 / 11,545ms | 12 | 0 | 13.85초 | |
| M2 | base | 4,986 | 100% | 2,764 / 5,354ms | 1 | 0 | 8.12초 | 3.39 |
| M2 | peak | 7,488 | 100% | 3,309 / 4,115ms | 0 | 0 | 7.90초 | |
| M3 | base | 4,979 | 99.4% | 2,674 / 5,546ms | 2 | 0 | 14.51초 | 4.01 |
| M3 | peak | 6,988 | 76.9% | 4,867 / 14,909ms | 19 | 42,539 | 20.17초 | |
| M4 | base | 4,986 | 100% | 2,585 / 3,456ms | 0 | 0 | 7.38초 | 3.32 |
| M4 | peak | 7,469 | 99.0% | 3,393 / 4,387ms | 0 | 0 | 8.11초 | |

- 같은 코드·같은 프로파일인데 평시 피크 트리거 p99가 4,115~14,909ms로 흩어졌다. 지연이 큰 실행(M1·M3)은 Spark와 무관한 생성기 ACK 지연도 함께 커졌고(M3 peak ACK p95 최대 1,828ms), 호스트 사용 코어도 높았다. 호스트 경합이 실행 간 편차의 주 원인으로 보인다. 경합 원인 프로세스는 수집하지 않았다.
- M1은 arena 기본 설정인데도 M3만큼 느렸다. M3의 지연 악화를 `MALLOC_ARENA_MAX=2` 탓으로 볼 근거는 없다. arena 축소가 malloc 경합을 늘렸을 가능성도 배제하지 못했다.
- M4는 M3와 같은 조건인데 평시 피크 트리거 p99가 4,387ms, 5초 초과 배치 0개, 생성기 ACK p95 100ms 이내 99.0%였다. 호스트 사용 코어 평균은 3.32로 M2(3.39)와 비슷했다. M3의 지연 악화는 `MALLOC_ARENA_MAX=2`가 아니라 실행 당시 호스트 경합 때문으로 판단한다. 호스트 top 기록에서 CPU 20% 이상 쓴 WSL 프로세스는 생성기(3회)·Claude Code(1회)뿐이었고, Docker 컨테이너 쪽은 이 기록에 보이지 않는다.
- 판정: `MALLOC_ARENA_MAX=2`는 힙 밖 메모리를 약 380~440MB 줄이고, 지연을 늘린다는 근거는 없다. 채택 후보로 둔다. 남은 위험은 catch-up처럼 배치 상한(55,000건)으로 연속 처리할 때 `memory.peak`가 여전히 3g에 닿는 점이다. 평시 피크 트리거 p99는 M2·M4 모두 4.1~4.4초로 목표 3,500ms를 넘는다.

### JVM 튜닝 실험 (2026-11-20)

[메모리 분해](#메모리-분해-2026-11-20) 결과로 후보를 정했다. 대상은 driver 겸 executor인 JVM 1개다(`local[3]`, `--driver-memory 2g`, 컨테이너 `mem_limit: 3g`).

**메모리 후보**

| 후보 | 현재 | 측정 근거 | 기대 효과 | 위험 |
| --- | --- | --- | --- | --- |
| J1 `MALLOC_ARENA_MAX=2` | 미설정(arena 상한 64) | M3·M4에서 JVM 힙 밖 메모리 1,007~1,115MB → 678~738MB | −380~440MB | 스레드 간 malloc 경합. M4에서 지연 영향 없음 |
| J2 `--driver-memory` 2g → 1.5g | 2g | 살아 있는 데이터 250~560MB, 힙 commit은 catch-up 중 1,727MB까지 늘어남 | catch-up 중 최대 약 200MB | GC 빈도 증가, 큰 배치에서 Full GC |
| J3 jemalloc(`LD_PRELOAD`) | glibc malloc | 미측정 | J1과 같거나 더 큼 | 이미지 변경, 호환 검증 필요 |
| J4 `-XX:ReservedCodeCacheSize` 축소 | 기본 240MB | NMT Code 127MB | 수십 MB | 코드 캐시가 차면 JIT 중단 |
| J5 `-XX:MaxMetaspaceSize`·`-XX:MaxDirectMemorySize` 상한 | 없음 | Metaspace 178MB, Other(direct) 29MB | 절감 없음. 폭주 방지 | 상한이 낮으면 OOM |
| J6 `-Xss`·스레드 수 | 182개 | NMT Thread 37MB | 수 MB | 스택 부족 |

**GC·지연 후보**

| 후보 | 현재 | 측정 근거 | 기대 효과 | 위험 |
| --- | --- | --- | --- | --- |
| J7 GC 로그(`-Xlog:gc`) | 꺼짐 | GC 시간 비율 1.2~2.7%. 느린 배치와 GC 정지의 관계는 미확인 | 느린 배치 원인 진단 | 로그 파일 증가 |
| J8 `-XX:MaxGCPauseMillis` | G1 기본 200ms | GC 비율이 낮아 근거 약함 | J7 결과로 정한다 | 처리량 감소 |
| J9 `-Xms`=`-Xmx` | 미설정 | 힙 commit 1,417~1,727MB 변동 | 힙 크기 조정 비용 제거 | 상시 최대 점유. J2와 상충 |
| J10 `G1PeriodicGCInterval` | 꺼짐 | 스트리밍은 유휴 구간이 짧다 | 거의 없음 | 주기 GC 추가 |

**JVM 밖 같은 예산 항목**: Python 워커 4개 RSS 합 556~593MB(P1), `mem_limit` 3.5g 상향(P2, 호스트 자원 예산 변경 필요), `spark.memory.fraction`(P3, storage 사용 최대 103MB/1,049MB이고 논리 한도라 RSS 절감이 거의 없다).

**진행 순서**

1. J1을 Compose에 반영한다. M3·M4로 측정을 마쳤다. 2026-11-20 반영했다(`compose.yaml` spark `environment`).
2. J1 기준으로 J7 GC 로그를 켜고 `steady.csv`를 실행한다. 평시 피크의 느린 배치(트리거 3,500ms 초과)와 GC 정지 시각을 대조한다. GC 로그는 측정용 override로만 켠다.
3. J1+J7 기준으로 J2(`--driver-memory 1536m`)를 실행한다. 2026-11-20 채택했다. catch-up과 부하 구간의 `memory.peak`, GC 횟수·정지 시간, 트리거 p99를 2단계와 비교한다.
4. 3단계 뒤에도 catch-up 중 `memory.peak`가 3g에 닿으면 J3 또는 P2를 검토한다. J4~J6·J8~J10은 2·3단계 결과가 근거를 줄 때만 실행한다.

**선택 기준**

- 메모리: 부하 구간 `memory.current` 최대가 기준보다 낮고 OOM kill이 0이다. catch-up 중 `memory.peak`도 함께 기록한다.
- 지연: 평시 피크 트리거 p99·종단 간 p99가 기준보다 나빠지지 않는다. 같은 실행의 생성기 ACK p95 100ms 이내 비율이 99% 미만이거나 호스트 사용 코어 평균이 4를 넘으면 호스트 경합 실행으로 보고 다시 측정한다(M3 사례).
- 실행마다 새 `SPARK_CHECKPOINT_DIR`을 쓰고, Kafka 끝 offset까지 catch-up한 뒤 부하를 시작한다.

### JVM 튜닝 결과 (2026-11-20)

| 실행 | 조건 | 생성기 run_id | Spark run_id | 원자료 | Grafana 캡처 (KST) |
| --- | --- | --- | --- | --- | --- |
| J7 | J1 + GC 로그(`SPARK_SUBMIT_OPTS=-Xlog:gc:file=/tmp/gc.log:time,uptime`, 측정용 override) | `2460c8919eba46f9a9b483e8ab9dfaf6` | `b95059b6-2119-44b3-ae8b-3b84e0f25985` | [JSON](../measurements/2026-11-20-spark-jvm-j7-gclog.json) | 17:55:45~18:07:45 [파이프라인](../../images/2026-11-20-spark-jvm-j7-gclog/pipeline.png)·[주문](../../images/2026-11-20-spark-jvm-j7-gclog/orders.png) |
| J2 | J1 + GC 로그 + `--driver-memory 1536m` | `07e29c5e109142fa978f5201ecf68ad3` | `f4dbe991-5f3d-45c3-bfdb-73870dd0c244` | [JSON](../measurements/2026-11-20-spark-jvm-j2-xmx1536m.json) | 18:14:33~18:26:33 [파이프라인](../../images/2026-11-20-spark-jvm-j2-xmx1536m/pipeline.png)·[주문](../../images/2026-11-20-spark-jvm-j2-xmx1536m/orders.png) |

GC 정지와 배치 대조는 [spark_gc_batches.py](../measurements/spark_gc_batches.py)로 했다. Kafka 보존 기간이 지나 토픽 earliest와 latest가 같아 J7은 catch-up 없이 시작했다.

**J7 (GC 로그)**

| 구간 | 생성 EPS | ACK p95 100ms 이내 | 트리거 평균 / p99 | 5초 초과 | backlog 최대 | 종단 간 p99 | 호스트 사용 코어 평균 |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| base | 4,939 | 98.1% | 3,396 / 4,617ms | 0 | 0 | 8.19초 | 4.40 |
| peak | 7,246 | 87.0% | 4,522 / 9,428ms | 14 | 8,434 | 13.72초 | |

- 생성기 ACK 비율 87.0%, 호스트 사용 코어 평균 4.40으로 [선택 기준](#jvm-튜닝-실험-2026-11-20)의 호스트 경합 실행이다. 지연 수치는 판정에 쓰지 않는다. 호스트 top에서 CPU 30% 이상 표본은 생성기 100회, `MainThread` 12회, Claude Code 10회, `docker`·`codex` 각 2회였다. Docker 컨테이너 안 프로세스는 보이지 않는다.
- 메모리: 부하 중 `memory.current` 최대 2,870MB, 컨테이너 시작 이후 `memory.peak` 3,036,024,832B로 한도 안이다. 힙 commit 최대 1,508MB, JVM RSS 최대 2,161MB(힙 밖 약 650MB), Python 워커 합 603MB, OOM kill 0.
- GC: 워밍업 이후 정지 합 11.6초(805회). 종류별로 Remark 124회·합 5,328ms·최대 156ms, Humongous Allocation으로 시작한 Young 181회·합 2,358ms·최대 110ms, Mixed 125회·합 1,334ms, Normal Young 125회·합 1,204ms다. Full GC는 0이다.
- 배치 대조(워밍업 이후 95배치): 트리거 3,500ms 초과 57배치의 배치당 GC 정지 합은 평균 141ms(최대 496ms), 3,500ms 이하 38배치는 평균 90ms(최대 193ms)다. 느린 배치 시간의 약 3%라 GC는 느린 배치의 주원인이 아니다. 가장 느린 배치(81: 9,428ms)의 GC 정지 합은 344ms다.
- Humongous 할당(힙 리전 절반 이상 객체)이 GC 주기를 자주 시작시킨다. 힙 2g의 G1 리전 크기는 1MB다. `-XX:G1HeapRegionSize` 상향은 GC 정지 합(배치당 0.1초 안팎)이 작아 지연 목표에 대한 효과가 작을 것으로 보고 후보에 넣지 않는다.
- 판정: J7 진단 목적은 달성했다. 평시 피크 트리거 p99 초과의 원인은 GC가 아니다. 남은 원인 후보는 호스트 CPU 경합과 배치 처리 자체 시간(분류·JDBC 쓰기)이다. J8(`MaxGCPauseMillis`)은 근거가 없어 실행하지 않는다.

**J2 (`--driver-memory 1536m`)**

비교 기준은 같은 J1 조건의 깨끗한 실행 M4(`--driver-memory 2g`)다.

| 항목 | M4 (2g) | J2 (1536m) |
| --- | ---: | ---: |
| catch-up 배치 수 / catch-up 중 `memory.peak` | 105 / 3,221,291,008B (한도) | 66 / **2,815,111,168B** |
| 부하 중 `memory.current` 최대 | 3,072MB | **2,649MB** |
| peak 구간 `anon` 최대 / `file` 최소 | 2,762 / 106MB | 2,499 / 100MB |
| 힙 commit 최대 (부하 중) | 1,727MB | 1,409MB (GC 로그상 catch-up 포함 1,424MB) |
| JVM RSS 최대 (peak) | 2,276MB | 2,024MB |
| GC 시간 비율 (peak) / Full GC | 2.7% / 0 | 2.5% / 0 |
| base 트리거 평균 / p99 | 2,585 / 3,456ms | 2,840 / 3,710ms |
| peak 트리거 평균 / p99 | 3,393 / 4,387ms | 3,498 / 4,687ms |
| peak 5초 초과 배치 / backlog 최대 | 0 / 0 | 0 / 0 |
| peak 종단 간 p99 | 8.11초 | 8.11초 |
| peak 실처리량 | 10,999 EPS | 10,666 EPS |
| 생성기 ACK p95 100ms 이내 (peak) / 호스트 사용 코어 평균 | 99.0% / 3.32 | 99.3% / 3.62 |

- 메모리: 부하 중 최대가 약 420MB 줄었고 catch-up 중에도 3g에 닿지 않았다. catch-up 배치 수가 M4(105)보다 적어 catch-up 비교는 같은 조건이 아니다. GC 로그상 catch-up 중 Full GC·evacuation 실패는 0이다.
- GC: 워밍업 이후 정지 합 11.3초(923회), 최대 정지 78ms(Remark)다. 힙이 작아져 Remark 최대가 J7 156ms에서 78ms로 줄었다. 트리거 3,500ms 초과 25배치의 배치당 GC 정지 평균 145ms, 이하 72배치 106ms로 J7과 같은 결론이다.
- 지연: peak 트리거 p99가 M4보다 300ms(7%) 길다. 깨끗한 실행 사이 편차(M2 4,115ms, M4 4,387ms)와 같은 크기라 1회 측정으로 악화를 구분할 수 없다. 종단 간 p99는 같고 5초 초과 배치는 둘 다 0이다.
- 판정: 채택. 3g 한도 안 여유를 확보했고, 지연 악화는 측정 편차 범위 안이다. `SPARK_DRIVER_MEMORY`를 `1536m`으로 바꾼다. catch-up 중 `memory.peak`가 3g 아래라 진행 순서 4(J3·P2)는 실행하지 않는다.
- 남은 목표: 평시 피크 트리거 p99 ≤ 3,500ms는 J1·J2 적용 후에도 4.4~4.7초로 미충족이다. GC·메모리가 원인이 아니므로 배치 처리 자체 시간(분류·JDBC 쓰기)을 다시 분해한다.

### 배치 시간 재분해 (2026-11-20)

J1·J2 적용 상태(`MALLOC_ARENA_MAX=2`, `--driver-memory 1536m`)에서 배치 시간을 다시 나눴다. 원자료: [2026-11-20-spark-batch-breakdown.json](../measurements/2026-11-20-spark-batch-breakdown.json). Grafana 캡처(18:35:17~18:47:17 KST): [파이프라인 운영](../../images/2026-11-20-spark-batch-breakdown/pipeline.png), [주문 현황](../../images/2026-11-20-spark-batch-breakdown/orders.png). 생성기 run_id `a71593c27129460f9b92805e1e30680e`, Spark run_id `b9ac80db-93a9-43e2-baed-0556eac75126`.

**입력 건수와 배치 시간(J2 실행, catch-up 포함 185배치, 7,331~54,999건)**

| 항목 | 고정비 | 1,000건당 | 25,000건 | 37,000건 | 55,000건 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 트리거 전체 | 1,519ms | 56.0ms | 2,919ms | 3,591ms | 4,599ms |
| 분류·계측 | 325ms | 33.1ms | 1,154ms | 1,551ms | 2,148ms |
| 정상 출력 | 976ms | 20.8ms | 1,496ms | 1,746ms | 2,120ms |
| DLQ 출력 | 132ms | 2.0ms | 181ms | 205ms | 240ms |

- 평시 피크(7,500 EPS × 5초 ≈ 37,000건) 배치의 **평균**이 이미 약 3.6초다. p99 3,500ms 목표는 평균부터 줄여야 한다.
- 고정비 1.5초 중 약 1초가 정상 출력 action이다.

**평시 피크 배치 단계(위 실행, peak 구간 60배치, 평균 37,106건)**

Spark REST의 job 제출·완료 시각과 콜백 계측 시각으로 나눴다. 배치당 job은 6개다(지표 집계 3, 중복 제거 셔플 map 1, JDBC save 1, DLQ save 1).

| 단계 | 평균 | p50 | 최대 | 내용 |
| --- | ---: | ---: | ---: | --- |
| 트리거 → 콜백 진입 | 58ms | 47 | 148 | offset 조회·계획 |
| 콜백 진입 → 첫 job | **216ms** | 208 | 392 | driver: 분류 DataFrame 구성·persist |
| 지표 집계 job | 1,362ms | 1,326 | 2,058 | Kafka 읽기·분류 UDF·캐시·집계 |
| 집계 → 정상 출력 첫 job | **445ms** | 460 | 682 | driver: 정상 출력 DataFrame 구성, JDBC 연결·테이블 확인 |
| 중복 제거 셔플 map job | 468ms | 449 | 971 | 캐시 읽기·`from_json`·부분 중복 제거 |
| JDBC save job | 819ms | 731 | 2,902 | 셔플 읽기·행 바인딩·INSERT 3개 병렬 |
| 정상 출력 → DLQ job | **119ms** | 118 | 166 | driver: DLQ DataFrame 구성 |
| DLQ save job | 91ms | 80 | 394 | Kafka DLQ 쓰기 |
| 콜백 종료 → 트리거 종료 | 38ms | 35 | 83 | offset 커밋 |
| 트리거 전체 | 3,655ms | 3,571 | 5,886 | |

- job이 돌지 않는 driver 시간이 배치당 약 880ms(24%)다.
- ClickHouse INSERT 서버 처리 시간은 평균 320ms(p95 약 470ms), INSERT당 약 10,500행이다. JDBC 메타데이터 조회(`SELECT 1|* FROM silver.order_events WHERE 1=0`)는 서버에서 각 4~7ms다.

**driver 시간 벤치** ([bench_spark_driver.py](../measurements/bench_spark_driver.py), 37,200건 고정 구간, 워밍업 3회 후 7회 중앙값, 1회성 컨테이너)

| 단계 | 중앙값 |
| --- | ---: |
| 분류 DataFrame 구성(`classify_batch` + `persist`) | 152ms |
| 정상 출력 DataFrame 구성 전체 | 357ms |
| └ `select_orders` | 279ms |
| └ 필터 + `from_json` | 20ms |
| └ 배치 내 중복 제거 / 추적 컬럼 | 5ms / 23ms |
| Catalyst 분석 / 최적화 / 물리 계획 | 0.1ms / 16ms / 6ms |
| JDBC 연결 + 테이블 확인 2회 | 23ms |
| DLQ DataFrame 구성 / 물리 계획 | 61ms / 7ms |

- `select_orders` 1회에 py4j 호출이 약 1,745번 일어난다(cProfile). 컬럼 식 31개 구성이 180ms, `select` 자체는 15ms다. PySpark Column 연산마다 JVM 왕복과 호출 위치 수집(`_capture_call_site`)·활성 세션 조회가 붙는다. 이 식들은 배치마다 같은데 매 배치 다시 만든다.
- JDBC 연결·Catalyst 계획은 원인이 아니다.

**분류 구성 분리 벤치 (2026-11-23)** ([원자료](../measurements/2026-11-23-spark-classify-persist.json), 같은 벤치·같은 조건, 스트리밍 컨테이너 정지 상태. 토픽이 retention으로 비어 생성기로 45,000건을 넣고 끝 구간 37,200건을 읽었다)

| 단계 | 중앙값 | 7회 범위 |
| --- | ---: | ---: |
| 분류 구성 전체(`classify_batch` + `persist`, 기존 항목) | 186ms | 157~408ms |
| └ `classify_batch` 식 구성 | 148ms | 109~271ms |
| └ `persist()` | 77ms | 57~255ms |

- 이번 실행은 전 항목이 11-20보다 느렸다(정상 출력 구성 472ms vs 357ms, `select_orders` 351ms vs 279ms). 절대값보다 비율로 본다.
- 분류 구성 중 약 1/3이 `persist()`(캐시 등록 시 계획 분석)다. Column 식 재사용으로 줄일 수 없는 비용이다.
- 식 구성 중 이벤트 시각 범위 식(`_event_time_window`)은 배치마다 기준 시각 리터럴이 바뀌므로 그대로 재사용할 수 없다. S5에서 분류 쪽 기대 효과는 식 구성 시간 중 고정 부분으로 한정된다.

**call-site 수집 끄기 대조 (2026-11-23)** ([원자료](../measurements/2026-11-23-spark-dataframe-debugging.json), 같은 벤치·같은 구간, `spark-submit --conf spark.python.sql.dataFrameDebugging.enabled=true|false` 연속 실행)

PySpark 4.2는 `functions`·`Column` 메서드 호출마다 `_with_origin`(`pyspark/errors/utils.py`)으로 활성 세션·설정 조회, 호출 스택 수집, JVM origin 설정·해제를 py4j로 한다. 오류 메시지에 Python 호출 위치를 붙이기 위한 것이다. 위 설정이 `false`면 건너뛴다.

| 단계 | true | false |
| --- | ---: | ---: |
| 분류 구성 전체 / 식 구성 / `persist()` | 144 / 109 / 61ms | 107 / 65 / 57ms |
| 정상 출력 구성 전체 | 301ms | 111ms |
| └ `select_orders` | 263ms | 95ms |
| DLQ 구성 | 69ms | 40ms |
| Catalyst 계획·JDBC 확인 | 변화 없음 | 변화 없음 |

- driver 구성 시간이 배치당 약 256ms 줄었다(분류 37, 정상 출력 190, DLQ 29). 설정 한 줄이며 코드는 바뀌지 않는다.
- 대가: DataFrame API 오류 메시지의 Python 호출 위치(파일:줄) 문맥이 빠진다. 분류 결과·쓰기 동작은 바뀌지 않는다.
- 남는 `select_orders` 95ms는 Column 식 재사용(S5)으로 추가로 줄일 수 있다.

**다음 후보**

| 후보 | 변경 | 기대 효과(평시 피크 배치당) | 분류 결과 영향 |
| --- | --- | --- | --- |
| S5a | `spark.python.sql.dataFrameDebugging.enabled=false`로 PySpark call-site 수집을 끈다 | driver 시간 약 0.26초(벤치). 2026-11-23 채택, steady에서 약 0.45초 감소([검증](#s5a-적용-steady-검증-2026-11-23)) | 없음. 오류 메시지의 Python 호출 위치만 빠진다 |
| S5 | 배치마다 같은 Column 식(정상 출력 컬럼 31개, DLQ 메시지 구조체, 분류 컬럼)을 세션 시작 때 한 번 만들고 재사용한다 | driver 시간 약 0.1~0.15초(추정, 미측정). S5a 적용 전 추정은 0.3~0.45초였다. S5a 뒤 남은 구성 시간은 `select_orders` 95ms(그중 `select` 자체 15ms), DLQ 구성 40ms, 분류 식 구성 65ms 중 고정 부분이다(위 call-site 수집 끄기 대조, [원자료](../measurements/2026-11-23-spark-dataframe-debugging.json)). `persist()`와 이벤트 시각 범위 식은 줄지 않는다. 2026-11-23 보류: D1 뒤 평시 피크 p99 3,246ms로 목표를 이미 충족했고, 줄어드는 양이 작다. 변환을 스트리밍 DataFrame에 한 번 정의하는 방식도 같은 이유로 보류한다. 줄어드는 것은 Python 쪽 식 구성 시간뿐이라 효과가 S5와 같다. 대신 범위 검사 기준 시각이 배치 시작 시각(`current_timestamp()`)으로 바뀌어 규칙·테스트를 고쳐야 한다 | 없음. 같은 식을 재사용한다. 기존 테스트로 확인한다 |
| S6 | 배치 내 중복 제거 셔플 비용(map job 468ms) 확인 | 측정 후 결정. 2026-11-23 측정: 336~375ms([job 비용 대조](#job-비용-대조-벤치-2026-11-23)) | 중복 제거 규칙은 바꾸지 않는다 |
| D1 | 정상 출력의 배치 내 `dropDuplicates(["event_id"])`를 제거한다. 중복은 ClickHouse `ReplacingMergeTree`·`FINAL`이 처리한다([입력 중복 정책](#입력-중복과-jdbc-적재-중복의-구분)) | 약 0.35초(벤치). 2026-11-23 채택, 평시 피크 트리거 p99 3,659 → 3,246ms([검증](#d1-적용-steady-검증-2026-11-23)) | 없음. 분류는 그대로이며, 배치 내 중복 입력이 있으면 silver에 물리 행으로 남고 조회 뷰에서 합쳐진다 |

- S5a·D1 적용 후 평시 피크 트리거는 평균 2,621ms, p99 3,246ms로 목표 3,500ms를 충족했다([D1 검증](#d1-적용-steady-검증-2026-11-23)). 이 실행의 driver 시간 합은 393ms다. S5는 그중 약 0.1~0.15초를 줄이는 여유 확보 후보다(S5a 적용 전 추정은 "S5를 적용해도 평균 약 3.2~3.3초, p99 충족 불확실"이었다). 남는 비용은 건수 비례(분류 33ms/1,000건, 정상 출력 21ms/1,000건)다.
- 이 실행의 평시 피크 트리거 p99는 5,886ms(5초 초과 1개)였다. 생성기 ACK p95 100ms 이내 비율이 98.65%라 [선택 기준](#jvm-튜닝-실험-2026-11-20)상 호스트 경합 실행에 해당해 지연 판정에는 쓰지 않는다.

### S5a 적용 steady 검증 (2026-11-23)

`build_spark`에 `spark.python.sql.dataFrameDebugging.enabled=false`를 넣었다(J1·J2 유지). Spark UI 환경 탭에서 값이 `false`임을 확인했다. 스트리밍 테스트 358개가 통과했다. 새 checkpoint `/opt/spark/checkpoints/s5a-steady-20261123`로 시작했고, 토픽에 남아 있던 벤치용 45,000건을 batch 0에서 다 읽은 뒤 부하를 걸었다. 수집은 [run_spark_mem.sh](../measurements/run_spark_mem.sh)로 했다. 단계 분해는 [spark_batch_phases.py](../measurements/spark_batch_phases.py)로 했다. 이 스크립트는 이번에 저장소에 추가했다. 11-20 실행 원자료는 ClickHouse TTL로 지워져서 이 스크립트를 11-20 실행에 다시 돌려 대조하지는 못했다.

| 실행 | 생성기 run_id | Spark run_id | 원자료 | Grafana 캡처 (KST) |
| --- | --- | --- | --- | --- |
| S5a | `df5bd47bf1114e15ae35eaeb3d0a417c` | `fc4221ad-2177-4323-86f1-348158196971` | [JSON](../measurements/2026-11-23-spark-s5a-steady.json) | 10:43:33~10:55:33 [파이프라인](../../images/2026-11-23-spark-s5a-steady/pipeline.png)·[주문](../../images/2026-11-23-spark-s5a-steady/orders.png) |

| 구간 | 생성 EPS | ACK p95 100ms 이내 | 트리거 평균 / p99 | 5초 초과 | backlog 최대 | 종단 간 p99 | 판정 |
| --- | ---: | ---: | --- | ---: | ---: | ---: | --- |
| base | 4,996 | 100% | 2,231 / 2,742ms | 0 | 0 | 6.95초 | 통과 |
| peak | 7,490 | 100% | 2,879 / **3,659ms** | 0 | 0 | 7.50초 | 트리거 p99 미달(목표 3,500ms) |

- 호스트 경합이 없는 실행이다. 호스트 사용 코어 평균은 3.09였다. Spark 시도 122회가 모두 성공했다. silver 행 3,221,332건의 `event_id`는 모두 고유했다. OOM kill은 0, `memory.peak`는 2,636,042,240B였다.

**평시 피크 배치 단계(60배치, 평균 37,304건) — 11-20 실행과 비교**

| 단계 | 11-20 평균 | 11-23 평균 |
| --- | ---: | ---: |
| 트리거 전체 | 3,655ms | 2,882ms |
| 콜백 진입 → 첫 job (driver) | 216ms | 106ms |
| 집계 → 정상 출력 첫 job (driver) | 445ms | 132ms |
| 정상 출력 → DLQ job (driver) | 119ms | 72ms |
| driver 시간 합(트리거 − job 구간) | 약 880ms | 431ms |
| 지표 집계 job | 1,362ms | 1,258ms |
| 중복 제거 셔플 map job | 468ms | 380ms |
| JDBC save job | 819ms | 731ms |

- driver 시간은 배치당 약 450ms 줄었다. 벤치에서 예상한 256ms보다 크다. 11-20 실행은 호스트 경합이 있었기 때문에 py4j 왕복도 더 느렸을 것으로 본다.
- job 구간도 300ms가량 줄었다. 하지만 S5a는 job 안의 실행을 바꾸지 않는다. 이 차이는 호스트 경합 차이로 보고 S5a 효과에 넣지 않는다.
- 판정: S5a를 채택한다. 평시 피크 트리거 p99는 3,659ms로 목표 3,500ms에 159ms 미달이다. 평균은 2,882ms다. 남은 비용은 지표 집계 job 1.26초(분류 UDF), JDBC save 0.73초, 중복 제거 셔플 0.38초다.

### job 비용 대조 벤치 (2026-11-23)

[bench_spark_stages.py](../measurements/bench_spark_stages.py)에 `jobs` 모드를 추가해 job별 비용을 변형끼리 비교했다. 원자료는 [JSON](../measurements/2026-11-23-spark-job-contrast.json)이다.

- 입력: 37,200건(정상 31,541건). 워밍업 3회 뒤 5회 측정의 중앙값이다. 순서 효과를 줄이려고 회차마다 변형 순서를 뒤집었다.
- Spark 설정은 S5a와 같다. 스트리밍 컨테이너를 멈추고 1회성 컨테이너에서 실행했다.
- JDBC는 운영 `write_orders`로 임시 테이블 `silver.order_events_bench`에 실제로 썼다. 이 테이블은 `CREATE TABLE AS silver.order_events`로 만들어 엔진이 같고 MV는 없다. 측정 뒤 테이블을 삭제했고, `spark_writer`에 준 권한도 회수했다.

| 변형 | 중앙값 |
| --- | ---: |
| 지표 집계 action, 지연 분위수 켬 / 끔 | 1,103 / 1,027ms |
| 정상 출력 JDBC, 중복 제거 켬 / 끔 | 984 / 648ms |
| 정상 출력 noop, 중복 제거 켬 / 끔 | 592 / 217ms |

| 비용 | 값 | 판단 |
| --- | ---: | --- |
| 지연 분위수 집계(`percentile_approx` 등) | 약 75ms | 회차 범위가 겹친다. 정밀도를 낮출 근거가 약해 후보에서 뺀다 |
| 배치 내 중복 제거(`dropDuplicates` 셔플) | 336~375ms | 정상 출력 action에서 가장 큰 단일 비용이다 |
| JDBC 쓰기(noop 대비) | 391~431ms | 이번 steady의 ClickHouse 서버 처리 평균 246ms가 여기에 포함된다. 클라이언트 쪽은 약 150~190ms다 |
| 정상 출력 나머지(캐시 읽기·`from_json`·선택) | 약 217ms | |
| 지표 집계 나머지(Kafka 읽기·분류 UDF·캐시 저장) | 약 1,027ms | |

- 배치 내 중복 제거는 [입력 중복 정책](#입력-중복과-jdbc-적재-중복의-구분)상 보조 장치다. Producer 멱등 전송과 주문별 새 ID 발급으로 입력 중복은 사실상 없다. 적재 중복은 `ReplacingMergeTree`와 `FINAL` 조회 뷰가 처리한다. 이것을 빼면 평시 피크 배치당 약 0.35초가 줄 것으로 예상하며, 트리거 p99 목표 미달분 159ms보다 크다. 다만 중복 제거 정책을 바꾸는 일이라 PRD 03·04의 정책을 먼저 고쳐야 한다.
- JDBC 클라이언트 쪽 비용은 약 150~190ms라서, 쓰기 방식을 바꿔도 얻을 수 있는 폭이 작다. 서버 처리 246ms는 쓰기 방식과 관계가 없다.

### D1 적용 steady 검증 (2026-11-23)

정상 출력에서 배치 내 `dropDuplicates(["event_id"])`를 없앴다(S5a·J1·J2 유지). 정책 변경은 [입력 중복과 JDBC 적재 중복의 구분](#입력-중복과-jdbc-적재-중복의-구분)과 [PRD 04 부분 실패와 재시작](04-clickhouse-storage.md#부분-실패와-재시작)에 먼저 반영했다. 스트리밍 테스트 357개가 통과했다(삭제한 중복 제거 단위 테스트 1개 제외, 배치 테스트는 중복 행 유지로 바꿈). 새 checkpoint `/opt/spark/checkpoints/d1-steady-20261123`로 토픽 보관분을 68배치에 걸쳐 다시 읽은 뒤 부하를 걸었다. 그래서 Grafana 캡처의 처음 30초에는 catch-up 끝부분(backlog 약 7만, Kafka → 콜백 최대 25분)이 보인다. 이 구간은 판정 제외 구간(워밍업 120초) 안이다.

| 실행 | 생성기 run_id | Spark run_id | 원자료 | Grafana 캡처 (KST) |
| --- | --- | --- | --- | --- |
| D1 | `daa35e44321448169e000a1c109c06e1` | `7bb9a495-5ad6-4ac0-be8a-ea6f3d023cf5` | [JSON](../measurements/2026-11-23-spark-d1-steady.json) | 11:18:14~11:30:14 [파이프라인](../../images/2026-11-23-spark-d1-steady/pipeline.png)·[주문](../../images/2026-11-23-spark-d1-steady/orders.png) |

| 구간 | 생성 EPS | ACK p95 100ms 이내 | 트리거 평균 / p99 | 5초 초과 | backlog 최대 | 종단 간 p99 | 판정 |
| --- | ---: | ---: | --- | ---: | ---: | ---: | --- |
| base | 4,997 | 100% | 1,887 / 2,358ms | 0 | 0 | 6.51초 | 통과 |
| peak | 7,487 | 100% | 2,633 / **3,246ms** | 0 | 0 | 7.16초 | **통과** |

- 호스트 사용 코어 평균은 2.97이었다. Spark 시도 189회가 모두 성공했다(catch-up 포함).
- 부하 구간(batch_id 68 초과)에서 쓰기 전 정상 행은 3,180,849건이다. silver 물리 행, 고유 `event_id`, `FINAL` 행이 모두 3,180,849건이다. 중복 제거를 없앤 상태에서도 배치 내 중복은 0건이었다.

**평시 피크 배치 단계(60배치, 평균 37,260건) — S5a 실행과 비교**

| 단계 | S5a 평균 | D1 평균 |
| --- | ---: | ---: |
| 트리거 전체 | 2,882ms | 2,621ms |
| 정상 출력 action(`orders_write_ms`) | 1,283ms | 977ms |
| └ 중복 제거 셔플 map job | 380ms | 없음 |
| └ JDBC save job | 731ms | 839ms |
| 지표 집계 job | 1,258ms | 1,302ms |
| driver 시간 합 | 431ms | 393ms |

- 정상 출력 action이 306ms 줄었다. 셔플 map job이 하던 캐시 읽기와 `from_json`이 save job으로 옮겨 가서, save job은 108ms 늘었다.
- 판정: D1을 채택한다. 평시 피크 트리거 p99 3,246ms로 [튜닝 목표](06-load-profile.md#튜닝-목표)의 평시 기준(≤ 3,500ms)을 처음 충족했다. 여유는 254ms다. 이번 판정은 실행 1회 결과이며, 남은 판정 항목은 `step.csv`의 10,000 EPS 단계다.

### D1 적용 step 검증 (2026-11-23)

S5a와 D1을 적용한 상태에서 `step.csv`를 실행했다. 새 checkpoint `/opt/spark/checkpoints/d1-step-20261123`로 토픽 보관분을 136배치에 걸쳐 다시 읽은 뒤 부하를 걸었다. 판정은 [PRD 06 지속 용량 기준](06-load-profile.md#지속-용량-stepcsv)을 따른다. 단일 생성기 단계라서 11,000·12,000 EPS는 참고값이다.

| 실행 | 생성기 run_id | Spark run_id | 원자료 | Grafana 캡처 (KST) |
| --- | --- | --- | --- | --- |
| D1 step | `32b9fdb4963341708078dcc5e2f43f99` | `d2c2ec78-4450-4ec9-a8bf-3b0b977148aa` | [JSON](../measurements/2026-11-23-spark-d1-step.json) | 12:01:49~12:15:49 [파이프라인](../../images/2026-11-23-spark-d1-step/pipeline.png)·[주문](../../images/2026-11-23-spark-d1-step/orders.png) |

| 단계 | 생성 EPS | 트리거 평균 / p99 | 5초 초과 | 실처리량 | backlog 최대 (마지막 60초) | 종단 간 p99 | ACK p95 100ms 이내 | 판정 |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 8k | 7,943 | 2,801 / 3,820ms | 0 | 14,180 | 0 | 7.32초 | 90.8% | 통과 |
| 9k | 8,970 | 2,949 / 3,500ms | 0 | 15,207 | 0 | 7.61초 | 95.8% | 통과 |
| **10k** | 9,934 | 3,419 / 4,738ms | 0 | 14,536 | 0 | 8.16초 | 84.6% | **통과** |
| 11k (참고) | 10,859 | 3,635 / 5,116ms | 1 | 14,899 | 3,630 (증가 추세 3.3건/초) | 8.60초 | 86.6% | 누적 |
| 12k (참고) | 11,817 | 3,714 / 4,860ms | 0 | 14,810 | 97,104 | 15.74초 | 74.0% | 누적 |

- 판정: 10,000 EPS 단계까지 통과했다. 지속 용량은 10,000 EPS다. 이로써 [튜닝 목표](06-load-profile.md#튜닝-목표)의 평시 기준([D1 steady](#d1-적용-steady-검증-2026-11-23))과 지속 용량 기준을 모두 충족했다. 스파이크 기준은 2개 프로세스로 확장한 뒤 판정한다.
- 공통 기준을 충족했다. 생성 = ACK = 6,542,239건이다. 부하 구간(batch_id 136 초과)의 정상 생성 5,560,356건이 silver 적재 건수, 고유 `event_id` 수와 같다. 실패 시도 0, Spark 재시작 0, OOM kill 0이었고, `memory.peak`는 2,906,865,664B였다. 호스트 사용 코어 평균은 3.67이었다.
- 11,000 EPS 이상에서 쌓인 원인은 처리 속도가 아니라 배치 상한이다. 단계별 실처리량은 14,180~15,207 EPS로 입력보다 높다. 하지만 `SPARK_MAX_OFFSETS_PER_TRIGGER=55000`을 트리거 주기 5초로 나눈 11,000 EPS가 설정상 수용 상한이고, 캡처에서도 배치 입력이 55,000건에서 막힌다. 이 상한은 [실험 범위](#실험)에서 유지하기로 한 값이다.
- 생성기 ACK p95 100ms 이내 비율은 84.6~95.8%로 낮았다. 지속 용량 기준에는 들어가지 않아 기록만 한다. 11-19 기준값 step(92.4~99.2%)과 11-20 S13 step(95.8~99.2%)보다도 낮다. 원인은 확인하지 않았다.
- 이전 S13 step(11-20, 호스트 경합)과 비교하면 10k 단계 트리거 평균이 3,740ms에서 3,419ms로, 실처리량이 13,300에서 14,536 EPS로 좋아졌다.

### B1 배치 상한 65,000 실험 (2026-11-23)

[D1 step](#d1-적용-step-검증-2026-11-23)에서 11,000 EPS부터 backlog가 쌓였다. 원인은 처리 속도가 아니라 수용 상한 `SPARK_MAX_OFFSETS_PER_TRIGGER=55000` ÷ 5초 = 11,000 EPS였다. 같은 실행 277개 배치를 회귀하면 배치 시간은 고정 738ms + 1,000건당 49.7ms다. 이 식으로 65,000건 배치는 평균 약 4.0초가 걸리고, 수용 상한은 13,000 EPS가 된다(추정).

- 변경: `SPARK_MAX_OFFSETS_PER_TRIGGER` 55,000 → 65,000. 실행 시 환경변수로만 덮어쓴다. 채택할 때 `.env.example`을 갱신한다.
- 지속 용량 목표(10,000 EPS)는 바꾸지 않는다. 이 실험은 11,000·12,000 EPS 참고 단계의 여유를 확인하고, 상한을 올렸을 때의 위험을 재는 것이다.
- 채택 기준:
  1. 메모리: catch-up(65,000건 배치) 중 `memory.peak`가 3GiB 한도보다 낮고 OOM kill이 0이다. 55,000에서는 2,906,865,664B였다.
  2. 평시: `steady.csv` 평시 피크 트리거 p99 ≤ 3,500ms, 종단 간 p99 ≤ 10초. 평시 배치는 약 37,000건이라 상한과 관계없을 것으로 예상하지만 확인한다.
  3. 지속 용량: `step.csv` 10,000 EPS 단계 통과를 유지한다. 11,000·12,000 EPS는 참고값으로 기록한다.
  4. JDBC: 쓰기 파티션은 Kafka 파티션 3개를 따른다. 파티션당 행 수가 `SPARK_JDBC_BATCH_SIZE` 50,000 이하인지 확인한다. 65,000건이 고르게 나뉘면 파티션당 약 21,700건이다.
- 실행마다 새 checkpoint를 쓰고, catch-up을 마친 뒤 부하를 시작한다.

**결과**

| 실행 | 생성기 run_id | Spark run_id | 원자료 | Grafana 캡처 (KST) |
| --- | --- | --- | --- | --- |
| B1 steady | `c0502d3f59d94b72ba8d90139b0ed650` | `f96af50e-0f51-4149-a371-46041df12cbb` | [JSON](../measurements/2026-11-23-spark-b1-steady.json) | 12:40:37~12:52:37 [파이프라인](../../images/2026-11-23-spark-b1-steady/pipeline.png)·[주문](../../images/2026-11-23-spark-b1-steady/orders.png) |
| B1 step | `69224bca024b4784a600c96dce7f9dd6` | `4558baab-1b69-4330-8523-e8cf4528f30a` | [JSON](../measurements/2026-11-23-spark-b1-step.json) | 13:06:00~13:20:00 [파이프라인](../../images/2026-11-23-spark-b1-step/pipeline.png)·[주문](../../images/2026-11-23-spark-b1-step/orders.png) |

| 기준 | 결과 | 판정 |
| --- | --- | --- |
| 1. 메모리 | steady 실행 앞 catch-up(65,000건 배치 130개) 뒤 `memory.peak` 2,979,885,056B, step 실행 앞 catch-up 뒤 2,967,691,264B, step 실행 종료 시 3,032,039,424B. 3GiB(3,221,225,472B)까지 여유는 최소 189MB다. OOM kill 0 | 충족. 여유가 작다 |
| 2. 평시 | steady 평시 피크 트리거 평균 2,562ms, p99 3,268ms(D1의 55,000에서는 3,246ms), 종단 간 p99 7.08초. base p99 2,330ms. ACK p95 100ms 이내 100%, 호스트 사용 코어 평균 2.83 | 충족 |
| 3. 지속 용량 | 아래 step 표 참고. 10,000 EPS를 포함해 12,000 EPS(참고)까지 모든 단계에서 backlog 0 | 충족 |
| 4. JDBC | catch-up 중 65,000건 배치에서 INSERT당 최대 18,836행(최근 40분 `system.query_log`) | 충족 |

| 단계 | 생성 EPS | 트리거 평균 / p99 | 실처리량 | backlog 최대 | 종단 간 p99 | ACK p95 100ms 이내 |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| 8k | 7,983 | 2,744 / 3,461ms | 14,542 | 0 | 7.25초 | 100% |
| 9k | 8,979 | 2,941 / 3,590ms | 15,264 | 0 | 7.42초 | 100% |
| 10k | 9,933 | 3,308 / 3,843ms | 15,013 | 0 | 7.75초 | 96.6% |
| 11k (참고) | 10,898 | 3,608 / 4,538ms | 15,099 | 0 | 8.12초 | 85.7% |
| 12k (참고) | 11,801 | 3,862 / 4,705ms | 15,279 | 0 | 8.30초 | 83.2% |

- catch-up 중 65,000건 배치의 트리거 시간은 평균 3,802ms, p50 3,618ms, p99 7,148ms였다. 회귀 추정치(약 4.0초)와 비슷하다.
- step 부하 구간에서 가장 큰 배치는 59,792건으로 상한 65,000에 닿지 않았다. 12,000 EPS에서 트리거 평균이 3.9초라 배치당 약 46,000건이 들어왔다.
- 공통 기준: steady는 부하 구간(batch_id 135 초과) 정상 생성 3,179,803건, step은 부하 구간(batch_id 158 초과) 정상 생성 5,567,467건이 각각 silver 적재 건수, 고유 `event_id` 수와 같다. 생성 = ACK, 실패 시도 0, Spark 재시작 0이다.
- 메모리 해석: `memory.peak`에는 회수 가능한 page cache(약 170MB)가 들어 있다. OOM과 직접 관계있는 `anon` 최대는 step 부하 중 55,000(D1 step)에서 2,530MB, 65,000(B1 step)에서 2,694MB로 약 160MB 늘었다. 3GiB(3,072MiB)까지 `anon` 여유는 약 378MB(12%)다. JVM 힙은 `--driver-memory 1536m`으로 상한이 고정되어 있다.
- 판정: B1을 채택한다(2026-11-23). 채택 기준 1~4를 모두 충족했고, 11,000·12,000 EPS 참고 단계도 누적 없이 처리했다. 상한은 backlog가 있을 때만 작동하므로 평시 지연은 바뀌지 않는다(평시 피크 p99 3,246 → 3,268ms). 반면 catch-up·스파이크 회복 속도는 수용 상한이 11,000에서 13,000 EPS로 오르면서 빨라진다. `.env.example`의 `SPARK_MAX_OFFSETS_PER_TRIGGER`를 65000으로 바꿨다.
- 남은 위험: 근거는 실행 1회이고, 메모리는 실행마다 수십~100MB씩 달라질 수 있다. 생성기 2개로 스파이크를 판정할 때 `anon` 최대와 OOM kill을 다시 확인한다. 여유가 부족하면 `mem_limit` 상향(P2)을 검토한다.

### 스파이크 판정: 생성기 2개 (2026-11-23)

[튜닝 목표](06-load-profile.md#튜닝-목표) 중 남은 스파이크 기준을 판정했다. 평시는 [D1 steady](#d1-적용-steady-검증-2026-11-23), 지속 용량은 [D1 step](#d1-적용-step-검증-2026-11-23)과 [B1](#b1-배치-상한-65000-실험-2026-11-23)에서 충족했다.

- 조건: 커밋 `018060b`, `.env` 기본값(`SPARK_MAX_OFFSETS_PER_TRIGGER=65000`, `SPARK_DRIVER_MEMORY=1536m`, Spark `cpus` 4, `mem_limit` 3g). Spark는 `spark` 서비스를 새 체크포인트 `/opt/spark/checkpoints/spike2-20261123`·`latest`로 다시 만들고, 첫 배치 진행 기록을 확인한 뒤 생성기를 시작했다. 생성기 2개 프로세스가 각각 `spike-half.csv`(2,500/6,000/2,500 EPS)로 동시에 실행했다. 판정은 `spike.csv` 구간 경계로 했다([PRD 06 설정](06-load-profile.md#설정)). Grafana는 켜 두었고 Kafka UI는 껐다.
- 실행: 14:52:52~15:05:22 KST(생성기 기준 750초). 생성기 run_id `2a3603ae7c6146f68c7f9847eba4bfa0`·`fd89261af31b456db74e07c5adee6bc2`, Spark run_id `3896a384-961c-4847-9c6b-940201e6e21b`.
- 원자료: [2026-11-23-spark-spike2.json](../measurements/2026-11-23-spark-spike2.json)(`analyze_profile.py` 출력, 자원 기록 포함). Grafana 캡처(14:52:22~15:06:22 KST): [파이프라인 운영](../../images/2026-11-23-spark-spike2/pipeline.png), [주문 현황](../../images/2026-11-23-spark-spike2/orders.png).

**판정: 스파이크 기준 1~3과 공통 기준 통과.** 12,000 EPS 스파이크를 거의 실시간으로 처리했고, backlog는 한 번만 생겨 두 배치 안에 해소됐다. 이로써 튜닝 목표 세 가지(평시·지속 용량·스파이크)를 모두 충족했다.

| 기준 | 결과 | 판정 |
| --- | --- | --- |
| 1. 생성 | spike 구간 생성 11,890 EPS(−0.9%), ACK 11,821 EPS. 두 run_id 합계 | 통과 |
| 2. 누적 | spike 배치 29개, 트리거 평균 4,129ms, p99·최대 8,826ms, 5초 초과 3개. 실처리량 14,164 EPS. backlog 최대 17,555건(경과 345.9초, batch 69) → 다음 배치 4,262건 → 그다음 0. spike 마지막 60초 backlog 0. spike 중 종단 간 지연 p99 12.17초, 최대 13.96초 | 기록 |
| 3. 회복 | spike 종료(경과 450초) 뒤 첫 배치(452.1초)가 backlog 0이고 Kafka → 콜백 p95 4,845ms ≤ warmup 평균 4,791ms × 1.5. 회복 시간 2.1초(기준 60초) | 통과 |
| 공통 1. 건수 | 생성 = ACK 4,751,556. 정상 주문 생성 4,040,208(2,021,601 + 2,018,607) = `silver.order_events` 행 수 = `event_id` 고유 수 | 통과 |
| 공통 2. 오류 | Spark 시도 152개 모두 `succeeded`, run_id 1개(재시작 0회). 생성기 전송 실패·출력 오류 0 | 통과 |
| 공통 3. 자원 | 아래 표 | 기록 |

구간별 요약:

| 구간 | 생성 EPS | 트리거 평균 / p99 | 실처리량 | backlog 최대 | 종단 간 p99 / 최대 | ACK p95 100ms 이내 |
| --- | ---: | --- | ---: | ---: | --- | ---: |
| warmup(120초 이후) | 4,996 | 1,955 / 2,869ms | 12,780 | 0 | 6.73 / 7.30초 | 100% |
| spike | 11,890 | 4,129 / 8,826ms | 14,164 | 17,555 | 12.17 / 13.96초 | 97.0% |
| recovery | 4,989 | 1,994 / 4,738ms | 12,515 | 0 | 6.70 / 8.18초 | 100% |

자원(측정 창 14:52:52~15:05:52 KST):

| 컨테이너 | CPU 상한 걸린 주기 | 걸린 시간 | `memory.peak`(종료 시) | OOM kill |
| --- | ---: | ---: | ---: | ---: |
| spark | 12.8% | 34.9초 | 2,768,064,512B | 0 |
| clickhouse | 4.8% | 34.5초 | 2,573,426,688B | 0 |
| kafka | 4.3% | 11.8초 | 2,147,590,144B | 0 |
| grafana | 0.5% | 4.0초 | 1,009,192,960B | 0 |

- Spark `anon` 최대 2,403MiB(14:58:20 KST), `file` 최대 179MiB. 5초 간격 표본 153개다. 3GiB(3,072MiB)까지 `anon` 여유는 약 669MiB로, [B1 step](#b1-배치-상한-65000-실험-2026-11-23)의 2,694MB보다 낮다. B1의 "남은 위험"(스파이크에서 `anon` 최대·OOM kill 재확인)은 이번 실행으로 확인했고, `mem_limit` 상향(P2)은 필요하지 않다.
- 생성기 코어 평균은 프로세스당 warmup 0.21, spike 0.47~0.48이다. Spark 코어 평균(5초 표본)은 warmup 1.43, spike 2.30, recovery 1.29다.

해석:

- **배치 상한 65,000이 스파이크를 흡수했다.** spike 배치는 대부분 약 60,000건을 3.5~4.4초에 처리해 트리거 주기 안에 끝났다. 2026-11-19 3차 측정(상한 55,000, 튜닝 전)에서는 상한 배치가 평균 5,222ms로 주기를 넘어 backlog가 218,867건까지 쌓였고 회복에 약 40~44초가 걸렸다. 이번에는 회복 2.1초, spike 종단 간 p99가 27.94초에서 12.17초로 줄었다.
- **backlog는 batch 68(트리거 8,826ms) 한 번에서만 생겼다.** 같은 시각에 Producer ACK p99가 최대 2,962ms로 튀었다. 원인 조사 결과는 아래와 같다([근거 JSON](../measurements/2026-11-23-spark-spike2-stall.json)).
  - 지연 구간은 14:58:23~14:58:25와 14:58:32~14:58:40 KST 두 번이고, 두 생성기에서 동시에 일어났다. 750초 실행에서 ACK p99가 300ms를 넘은 1초 행 12개가 모두 이 두 구간에 있다.
  - 두 구간은 이 실행에서 1초를 넘긴 `silver.order_events` 병합 두 건과 겹친다. 14:58:23.7~25.6에 1,859ms(85,665행), 14:58:28.3~37.6에 9,370ms(1,549,484행, 100MB, level 5)다. 1,093ms 병합(06:04 UTC) 때는 지연이 없었다.
  - 같은 구간에 VM 전체의 메모리·IO 압력이 올랐다(ClickHouse `asynchronous_metric_log`의 `/proc/pressure`). `PSI_MEM_full`은 직전 최대 34ms/s에서 54~371ms/s로, `PSI_IO_some`은 최대 376ms/s로 올랐다. `PSI_CPU_full`은 0이고 steal도 0이다. 차단된 프로세스는 1~4개였다.
  - `memory.max`에 닿은 컨테이너는 Kafka뿐이다. `memory.peak`가 한도 2GiB와 같고, `memory.events`의 `max`가 8,391회, direct reclaim이 약 39만 페이지다. Spark·ClickHouse는 `max` 0회다. Kafka heap은 512MB로 고정이라, 나머지 약 1.5GB를 page cache가 채우면 쓰기·읽기 중에 cgroup reclaim이 일어난다.
  - Spark batch 68은 분류 3,647ms(앞 배치 1,672~2,471ms), 정상 쓰기 4,863ms(앞 배치 1,106~1,208ms)였다. 같은 배치 ClickHouse INSERT의 서버 시간은 363~382ms로 평소와 같았다. 14:58:35~37에는 Spark CPU가 0.5~2%였다. 따라서 Spark는 ClickHouse 쓰기가 아니라 Kafka 읽기를 기다린 것으로 본다.
  - 배제한 원인: Kafka GC(구간 안 GC 없음, 앞뒤 GC 26~28ms), Kafka 브로커 이벤트(로그 없음. segment roll은 14:57:53, 보존 삭제는 14:50), VM 정지(`ops.host_stalls` 없음, 1초 표본 끊김 없음), CPU 포화(`PSI_CPU_full` 0).
  - 1차 가설: ClickHouse 대형 병합의 디스크 IO와 Kafka cgroup 메모리 한도(2GiB)의 page cache reclaim이 겹쳐 Kafka 브로커가 1~3초 멈췄고, 이 정지가 Producer ACK와 Spark 읽기를 함께 늦췄다. 다만 Kafka `memory.peak`는 이 실행 전부터 이미 2GiB였고(01:26 UTC 기동 후 누적), `memory.events max` 8,391회도 실행 전후 값을 따로 보지 않았다. 따라서 그 시각에 Kafka가 한도에 닿았다는 증거는 없다.
  - 재현 검증(2026-11-23, 아래): 병합만으로는 재현되지 않았다. 이후 spike4 실행에서 Kafka JVM heap의 swap-in이 원인으로 확인됐다([spike4](#스파이크-재실행-대시보드-닫음-swap-in-기록-2026-11-23)). 이 사고 시각에는 GC가 없었으므로 브로커 스레드의 swap-in으로 추정한다.

#### 병합 재현 검증 (2026-11-23)

- 조건: `steady.csv`(단일 생성기, 5,000 → 7,500 EPS), Spark 새 체크포인트 `merge-repro-20261123`·`latest`, 나머지 설정은 스파이크 판정과 같다. 부하 중 240초·480초에 현재 시간 파티션(`2026-11-23 06:00:00`)을 `OPTIMIZE TABLE silver.order_events PARTITION ... FINAL`로 병합했다. Kafka·ClickHouse·Spark cgroup의 `memory.current`·`memory.stat`·`memory.events`·`memory.pressure`·`io.pressure`와 VM `/proc/pressure`를 1초 간격으로 기록했다.
- 실행: 06:14:46~06:24:46 UTC. 생성기 run_id `40d6e03a933b4130bbaa1ccd46f74dac`, Spark run_id `a70395e5-e5d1-4a98-b60e-301e03a79e68`. 원자료: [2026-11-23-spark-merge-repro.json](../measurements/2026-11-23-spark-merge-repro.json)

| 구간(UTC) | 병합 | Producer ACK p99 최대 | 큐 최대 | Spark 트리거 |
| --- | --- | ---: | ---: | --- |
| 06:15:11~16 (자연 병합) | 3,978ms, 155만 행, 100MB(level 5) | 54.5ms | 257 | 2초대 |
| 06:16:30~06:18:40 (병합 없음, 5,000 EPS) | - | 71.6ms | 387 | 2초대 |
| 06:18:46~53 (`OPTIMIZE` 1) | 6,217ms, 249만 행, 161MB(level 6) | 91.1ms | 434 | 2,808·2,908ms |
| 06:20:00~06:22:40 (병합 없음, 7,500 EPS) | - | 270.5ms | 1,906 | 3초 안팎 |
| 06:22:46~23:00 (`OPTIMIZE` 2, 7,500 EPS) | 13,012ms, 386만 행, 248MB(level 7) | 282.5ms | 1,724 | 4,524·3,900·4,574ms |

- 결과: 사고 때 병합(100MB, 9.4초)보다 크거나 긴 병합을 두 번 일으켰지만, 초 단위 ACK 정지는 나오지 않았다. 병합 구간의 ACK p99는 같은 부하의 병합 없는 구간과 같은 수준이다. Spark 트리거는 병합 중 약 1~1.7초 늘었지만(batch 95~97) 5초 안이었고, 사고 때처럼 Spark CPU가 0으로 떨어지는 대기는 없었다.
- 압력: Kafka cgroup은 `memory.events max`가 늘지 않았다(0회). `memory.current` 최대 1,355MiB, `file` 최대 739MiB로 한도 2GiB에 닿지 않았다. 병합 중 메모리 압력은 ClickHouse cgroup에서 생겼다(`memory.pressure` full 최대 162ms/s). VM 전체 `PSI_MEM_full`은 최대 106ms/s로 사고 때(최대 371ms/s)의 약 1/3이었다.
- 해석: ClickHouse 대형 병합은 단독으로 Kafka ACK를 초 단위로 멈추지 않는다. 사고와 다른 조건은 (1) 입력 12,000 EPS·생성기 2개와 (2) Kafka cgroup이 한도 근처였을 가능성이다. (2)는 이번 재현에서 Kafka 사용량이 1.3GiB에 그쳐 확인하지 못했다. 사고 때 VM 메모리 압력이 3배 컸으므로, 어느 cgroup에서 압력이 생겼는지가 다음 확인 대상이다.
- 남은 작업: 같은 1초 cgroup 샘플러를 켜고 `spike-half.csv` 2개로 다시 실행해, 재발하면 압력이 어느 cgroup에서 생기는지 확인한다. 재발하지 않으면 실행 1회의 일시 현상으로 기록하고 종료한다.

#### 스파이크 재실행: 1초 cgroup 샘플러 (2026-11-23)

- 조건: 스파이크 판정과 같다(생성기 2개 × `spike-half.csv`, Spark 새 체크포인트 `spike3-20261123`·`latest`). 병합 재현 검증의 1초 cgroup 샘플러를 함께 켰다. 실행 중 Grafana 대시보드가 열려 있었다(아래).
- 실행: 06:43:50~06:56:20 UTC. 생성기 run_id `dd63a764f5b74db6a506aec4609485d4`·`814cf7f508e0455ca4f3e216025056fc`, Spark run_id `e9262774-4294-47e4-9d14-73b9e7dd8cce`. 원자료: [2026-11-23-spark-spike3.json](../measurements/2026-11-23-spark-spike3.json)(`analyze_profile.py` 출력 + `cgroup_1s` + `notes`). Grafana 캡처(06:43:20~06:57:20 UTC): [파이프라인 운영](../../images/2026-11-23-spark-spike3/pipeline.png), [주문 현황](../../images/2026-11-23-spark-spike3/orders.png).

**판정: 스파이크 기준 불합격. 대시보드 조회 부하가 섞여 판정에는 쓰지 않는다.**

| 항목 | spike2(판정 실행) | spike3(이번) |
| --- | ---: | ---: |
| spike 생성 EPS | 11,890 | 11,243(−6.3%, 기준 1 미달) |
| spike 트리거 평균 / 최대 | 4,129 / 8,826ms | 6,804 / 11,117ms |
| backlog 최대 | 17,555 | 184,516(spike), 167,474(recovery) |
| 회복 시간 | 2.1초 | 61.2초(기준 60초 초과) |
| spike 종단 간 p99 | 12.17초 | 38.12초 |
| ClickHouse CPU 상한 걸린 주기 | 4.8% | 16.2%(190.4초) |
| `grafana_reader` 쿼리 CPU | 14.2초 | 188.0초 |
| 공통(건수·오류·재시작) | 통과 | 통과(생성 = ACK 4,610,560, 정상 3,919,336 = silver = 고유 수, 시도 140개 모두 성공) |

- 대시보드 조회: `grafana_reader` 쿼리가 실행 전부터 분당 약 58개로 일정했다. spike2 실행 때는 분당 약 14개였으므로 대시보드가 자동 새로고침으로 열려 있었던 것으로 본다. 쿼리 CPU는 실행 데이터가 쌓이면서 분당 1.5초에서 15초로 늘었다. ClickHouse(`cpus` 2)가 CPU 상한에 걸리면서 Spark 정상 쓰기가 느려졌다. [측정 방법](06-load-profile.md#측정-방법)에 대시보드를 닫는다는 조건이 없어 이번에 생긴 문제다.
- ACK 정지 재발: 두 생성기에서 동시에 ACK p99가 1초 안팎으로 튄 구간이 다시 나왔다(06:51:27~36 최대 988ms, 06:51:48~49 최대 1,325ms).
- 1초 cgroup 기록:
  - Kafka cgroup은 실행 내내 `memory.events max`가 늘지 않았다. `memory.current` 최대 1,476MiB, `file` 최대 850MiB였다. **Kafka 메모리 한도 가설은 기각한다.**
  - 정지 순간에 Kafka cgroup의 `io.pressure` some이 튀었다. 06:51:27에 253ms/s, 06:51:46에 394ms/s, 06:51:47에 192ms/s다. 정지 구간 밖에서는 대부분 0~10ms/s다. Kafka `memory.pressure`는 같은 시각 0~23ms/s로 작다.
  - 06:51:47.830에 Kafka Young GC(132)가 1,167ms 걸렸다(User 0.86s, Sys 0.77s, Real 1.17s). 앞뒤 GC는 82·95ms다. 평소와 달리 Sys 시간이 커서 GC 스레드가 커널 안(page fault 등)에서 막힌 것으로 본다. 06:51:48~49 정지는 이 GC와 겹친다.
- 스왑: 실행 뒤 VM 스왑 사용량은 1,537MiB/4,096MiB였다. Kafka cgroup의 `memory.swap.current`는 168MiB, ClickHouse는 99MiB, Spark는 0이다. Kafka heap은 `-Xms512m -Xmx512m`라 전부 확보되어 있으므로, heap 일부가 스왑으로 나가 있었을 수 있다(확인하지 않음).
- 해석: 재발한 정지는 Kafka 프로세스가 커널에서 IO를 기다리며 멈춘 것과 맞다. 후보는 (1) 스왑으로 나간 Kafka heap·버퍼를 다시 읽는 swap-in과 (2) page cache에서 밀려난 로그를 consumer fetch(`sendfile`)가 디스크에서 읽으면서 네트워크 스레드가 막히는 것이다. 이번 샘플러는 `pswpin`·`pgmajfault`를 기록하지 않아 둘을 구분하지 못한다. 14:58 사고의 VM `PSI_MEM_full` 급증은 swap-in 대기도 포함하므로 (1)과도 맞다.
- 다음 확인: 대시보드를 닫고, cgroup별 `pswpin`·`pgmajfault`·`workingset_refault_file`을 1초 간격으로 추가 기록해 같은 조건으로 다시 실행한다. (1)이 맞으면 Kafka `memswap_limit`을 `mem_limit`과 같게 두어 스왑을 막고 재측정한다.

#### 스파이크 재실행: 대시보드 닫음, swap-in 기록 (2026-11-23)

- 조건: spike3와 같다. 실행 전에 Grafana 대시보드를 닫았고(사용자가 Grafana 컨테이너도 중지), 측정 중 대시보드 쿼리는 0건이었다. 1초 샘플러에 cgroup별 `pswpin`·`pswpout`·`pgmajfault`와 VM `/proc/vmstat`을 추가했다.
- 실행: 07:09:09~07:21:39 UTC. 생성기 run_id `e27177fd6f9949c7ac2b57f602e8ed90`·`d6d42920f62f409a968b864cbde37543`, Spark run_id `2b462d25-7468-4738-bb1d-fe4b42ad1783`. 원자료: [2026-11-23-spark-spike4.json](../measurements/2026-11-23-spark-spike4.json)(`analyze_profile.py` 출력 + `cgroup_1s` + `notes`). Grafana 캡처(07:08:40~07:22:40 UTC, 측정 뒤 Grafana를 잠시 켜서 찍음): [파이프라인 운영](../../images/2026-11-23-spark-spike4/pipeline.png), [주문 현황](../../images/2026-11-23-spark-spike4/orders.png).

**판정: 스파이크 기준 1~3과 공통 기준 통과.** spike2에 이어 두 번째 통과다.

| 기준 | spike2 | spike4 |
| --- | ---: | ---: |
| spike 생성 EPS | 11,890(−0.9%) | 11,627(−3.1%) |
| spike 트리거 평균 / 최대 | 4,129 / 8,826ms | 4,907 / 6,276ms |
| backlog 최대 | 17,555 | 5,584 |
| 회복 시간 | 2.1초 | 1.2초 |
| spike 종단 간 p99 / 최대 | 12.17 / 13.96초 | 9.85 / 10.75초 |
| 공통 | 통과 | 통과(생성 = ACK 4,727,946, 정상 4,018,647 = silver = 고유 수, 시도 152개 모두 성공, 재시작 0) |
| CPU 상한 걸린 주기 spark / clickhouse / kafka | 12.8 / 4.8 / 4.3% | 8.2 / 3.8 / 4.3% |

**ACK 정지 원인 확정: Kafka JVM heap의 swap-in.**

- 07:15:43에 두 생성기 ACK p99가 1,849·1,828ms로 튀었다(큐 최대 10,363). 같은 시각 Kafka G1 concurrent mark cycle(GC 154)이 07:15:40.1~43.9에 돌았다. 그중 stop-the-world인 Pause Remark가 07:15:42.099~43.883에 1,785ms 걸렸다. 평소 Young GC는 40~80ms다.
- 07:15:41~44에 Kafka cgroup에서 swap-in이 몰렸다. 초당 `pswpin` 1,304~2,477페이지, `pgmajfault` 759~1,780, `io.pressure` some 100~152ms/s였다. 실행 전체 Kafka swap-in 7,213페이지(약 28MB) 중 약 84%가 이 4초에 있다. 다른 시각에는 0이다. ClickHouse·Spark cgroup은 같은 시각 swap-in이 0~12페이지다.
- 해석: Kafka heap(`-Xms512m -Xmx512m`) 중 오래 쓰지 않은 영역이 스왑에 나가 있었다(실행 전 Kafka `memory.swap.current` 168MiB). G1 marking은 old·humongous 영역까지 heap 전체를 훑으므로, 이때 스왑에 나간 페이지를 디스크에서 다시 읽는다. Remark는 stop-the-world라서 그동안 브로커의 요청 처리가 모두 멈추고, Producer ACK와 Spark fetch가 함께 늦어진다.
- 앞선 관측과 맞춰 보면:
  - spike3 06:51:47의 Young GC 1,167ms(Sys 0.77s)도 같은 구조다. GC가 스왑에 나간 페이지를 만나 커널에서 기다렸다.
  - 14:58 사고(spike2)와 spike3 06:51:27 정지는 GC와 겹치지 않았다. 그때는 GC가 아닌 브로커 스레드가 스왑에 나간 메모리를 만났을 가능성이 크다. VM `PSI_MEM_full`(swap-in 대기 포함)과 Kafka `io.pressure`가 함께 오른 점이 이와 맞는다. 다만 그 두 실행은 `pswpin`을 기록하지 않아 직접 증거는 없다.
  - ClickHouse 병합은 VM 메모리 압력을 키워 스왑 아웃을 늘리는 배경 요인일 수 있다. 하지만 병합만으로는 정지가 재현되지 않았다([병합 재현 검증](#병합-재현-검증-2026-11-23)).
- Kafka가 스왑에 들어간 이유: Kafka cgroup은 01:26 UTC 기동 뒤 `memory.max`(2GiB)에 8,391회 닿았다(이번 세션의 실행들에서는 0회). 한도 reclaim과 VM 전체 reclaim이 page cache와 함께 heap의 차가운 페이지를 스왑으로 내보냈다. VM 스왑 사용량은 약 1.5GiB다.
- 조치 후보(미적용):
  1. Kafka 컨테이너에 `memswap_limit`을 `mem_limit`과 같게(2g) 둬 스왑을 막는다. 한도에 닿으면 page cache만 회수된다. 적용하려면 Kafka 컨테이너를 다시 만들어야 하고, 그 뒤 spike를 다시 측정한다.
  2. 같은 이유로 ClickHouse(스왑 99MiB)도 검토한다. Spark는 스왑 0이다.

#### Kafka 스왑 차단 후 스파이크 재측정 (2026-11-23)

- 변경: Kafka에 `memswap_limit: 2g`(= `mem_limit`)를 적용했다([Kafka PRD](02-kafka-ingestion.md)). 컨테이너를 07:47 UTC에 다시 만들었고, `memory.swap.max` 0을 확인했다. 나머지 조건은 spike4와 같다(대시보드 닫음, 측정 중 `grafana_reader` 쿼리 0건, 1초 샘플러).
- 실행: 07:50:40~08:03:10 UTC. 생성기 run_id `fbbc5f31d3784944b124455a71f7d9fa`·`c9f3052405dc4c8a93c0adb327b34c65`, Spark run_id `f6e3cd71-3e23-482b-bbf2-0b8f68a5067d`, 체크포인트 `spike5-20261123`. 원자료: [2026-11-23-spark-spike5.json](../measurements/2026-11-23-spark-spike5.json). Grafana 캡처(07:50:10~08:04:10 UTC, 측정 뒤 Grafana를 잠시 켜서 찍음): [파이프라인 운영](../../images/2026-11-23-spark-spike5/pipeline.png), [주문 현황](../../images/2026-11-23-spark-spike5/orders.png).

**판정: 스파이크 기준 1~3과 공통 기준 통과.** 회귀는 없다.

| 항목 | spike4(스왑 허용) | spike5(스왑 차단) |
| --- | ---: | ---: |
| spike 생성 EPS | 11,627(−3.1%) | 11,663(−2.8%) |
| spike ACK p95 최대 / 100ms 이내 비율 | 1,763ms / 90.2% | 207ms / 91.7% |
| spike 트리거 평균 / 최대 | 4,907 / 6,276ms | 4,653 / 5,959ms |
| backlog 최대 | 5,584 | 3,117 |
| 회복 시간 | 1.2초 | 4.9초 |
| spike 종단 간 p99 / 최대 | 9.85 / 10.75초 | 9.70 / 10.48초 |
| 공통 | 통과 | 통과(생성 = ACK 4,738,360, 정상 4,028,051 = silver = 고유 수, 시도 151개 모두 성공, 재시작 0) |
| Kafka swap-in / 스왑 사용량 | 7,213페이지 / 168MiB | 0 / 0 |
| Kafka GC 150ms 초과 | Remark 1,785ms | 없음 |
| Kafka `memory.events` max / OOM kill | 0 / 0 | 0 / 0 |
| Kafka anon / `memory.current` 최대 | 626 / 2,015MiB | 721 / 1,674MiB |

- ACK p99가 300ms를 넘은 1초 행은 실행 첫 1초(07:50:40, 692·696ms)뿐이다. 판정에서 빼는 워밍업 구간이다. 새로 띄운 Kafka JVM의 첫 요청이라 그런 것으로 본다(확인하지 않음).
- Kafka anon 최대가 626MiB에서 721MiB로 늘었다. 스왑에 나갈 수 없어 heap 전체가 RAM에 남기 때문이다. 한도 2GiB까지 여유가 크다.
- 한계: Kafka를 방금 다시 만들어서, 차단이 없었더라도 아직 스왑이 쌓이기 전이다. 이 실행은 차단 뒤 OOM·회귀가 없음을 확인할 뿐이다. 스왑으로 인한 정지가 사라졌다는 증거는 아니다. 효과를 확인하려면 Kafka가 한도 근처에서 몇 시간 운영된 뒤 `memory.swap.current`가 0이고 swap-in 정지가 없는지 본다.
- 회복 시간 1.2초 → 4.9초는 성능 차이가 아니다. 회복 시간은 spike 종료(경과 450초) 뒤 조건을 만족한 첫 배치가 끝난 시각까지라서, 배치 주기 5초 안에서 값이 정해진다. 두 실행 모두 spike 종료 전 배치에서 이미 backlog가 0이었다(spike4 446.3초, spike5 449.9초). 그래서 종료 뒤 첫 배치에서 바로 회복으로 집계됐다(spike4 451.2초, spike5 454.9초). spike5는 직전 배치가 종료 0.1초 전에 끝나 집계에서 빠졌을 뿐이다. backlog가 실제로 쌓였다 해소되는 실행(예: [스파이크 측정 3차](06-load-profile.md#스파이크-프로파일-측정-3차-spike-12000-eps-배치-상한-55000건-2026-11-19), 회복 약 40~44초)에서만 이 값으로 처리 능력을 비교할 수 있다. 이번 두 실행의 처리 능력은 backlog 최대(5,584 → 3,117), spike 트리거 평균(4,907 → 4,653ms), spike 종단 간 p99(9.85 → 9.70초)로 비교한다. 모두 spike5가 같거나 조금 낫지만 실행이 각 1회라 차단 효과로 단정하지 않는다.

#### Kafka 스왑 A/B 재현: A(스왑 허용) (2026-11-23)

- 목적: 스왑 정지가 일어난 조건을 다시 만들고, 스왑 허용(A)과 차단(B)에서 정지와 OOM 여부를 비교한다. A를 먼저 실행했다.
- A 조건: `compose.yaml`에서 Kafka `memswap_limit`을 잠시 빼고 Kafka를 다시 만들었다(`memory.swap.max` 2GiB). 실행 뒤 파일은 원래대로 되돌렸다.
- 압력 단계(08:19:40~08:29:46 UTC): 생성기 1개를 5,000 EPS 고정으로 10분 돌렸다. 동시에 별도 컨테이너에서 `kafka-consumer-perf-test`로 DLQ 토픽(5.7GB)과 raw 토픽(1.3GB)을 처음부터 반복해 읽었다. 목적은 Kafka page cache를 한도까지 채워 cgroup 한도 회수를 계속 일으키는 것이다.
- spike 단계: 압력 단계 직후 spike4와 같은 조건으로 실행했다(생성기 2개, 대시보드 닫음, 1초 샘플러). 생성기 run_id `7d5845f6c1ba4065b5029015e3beef72`·`597ae0e4043d4d4da23ede32ebfc8dbc`, Spark run_id `0f8383dc-6789-4797-85b6-647915a75265`, 체크포인트 `abA-spike-20261123`.
- 원자료: [2026-11-23-kafka-swap-ab-a.json](../measurements/2026-11-23-kafka-swap-ab-a.json)(압력 단계 전후 Kafka cgroup, 1초 샘플, spike `analyze_profile.py` 출력)

| 항목 | 압력 단계 | spike 단계 |
| --- | --- | --- |
| Kafka `memory.events` max 증가 | 0 → 13,033 | 13,033 → 16,383 |
| Kafka `memory.current` | 519MiB → 1,955MiB(한도 근처 유지) | - |
| Kafka anon / file | 433 → 611MiB / 78 → 1,313MiB | - |
| Kafka `pswpout` / `memory.swap.current` | 0 / 0 | 0 / 0 |
| Kafka GC 150ms 초과 / OOM kill | 0 / 0 | 0 / 0 |

- spike 판정: 통과(spike 생성 11,687 EPS, 트리거 평균 4,769ms, 최대 5,687ms, backlog 최대 111, 회복 3.2초, 건수 대조 일치, 시도 151개 모두 성공). ACK p99가 300ms를 넘은 1초 행은 실행 첫 1초(08:30:22, 515ms)뿐이다.
- **결과: 재현 실패.** Kafka가 한도에 16,383회 닿았지만 커널은 page cache만 회수했고, anon은 1페이지도 스왑으로 보내지 않았다. 스왑이 없었으므로 정지도 없었다.
- 해석: 회수할 수 있는 page cache가 많으면, cgroup 한도 회수는 anon보다 page cache를 먼저 회수한다. 그러므로 앞서 본 Kafka 스왑 168MiB는 cgroup 한도 회수만으로 생긴 것이 아니다. VM 전체 메모리가 부족했을 때의 전역 회수에서 생겼을 가능성이 크다(추정). 당시 VM 스왑 사용량은 1.5GiB였다. 지금 VM은 가용 메모리 약 5.7GiB이고, `/proc/vmstat` `pswpout`은 07:05 이후 약 1.3시간 동안 18,912페이지(약 74MiB)만 늘었다. 이번 세션에서는 전역 회수가 거의 일어나지 않는 상태다.
- B(스왑 차단)는 같은 절차로는 A와 차이가 날 수 없어 실행하지 않았다. 재현하려면 VM 전체 메모리 압력을 일부러 만들어야 한다.
- 결정(2026-11-23): 효과 근거가 부족해 Kafka `memswap_limit`을 되돌렸다. `compose.yaml`은 스왑 차단 전과 같다. 실행 중인 Kafka도 A 단계에서 다시 만든 컨테이너라 스왑 허용 상태다. VM 전체 메모리 압력 재현은 하지 않았다. [spike5](#kafka-스왑-차단-후-스파이크-재측정-2026-11-23) 기록은 차단 상태의 참고 측정으로 남긴다.
- spike 구간 종단 간 p99 12.17초는 평시 목표(10초)를 넘지만, 스파이크 기준은 spike 중 지연을 기록만 한다.
- 근거는 실행 1회다.

### 지속 용량 판정: 생성기 2개 step (2026-11-23)

[PRD 06 완료 기준 6](06-load-profile.md#완료-기준)의 `step.csv` 전체 판정이다. 단일 생성기 step에서 11,000·12,000 EPS는 참고값이었다.

- 조건: 커밋 `fccadfe`, `.env` 기본값(`SPARK_MAX_OFFSETS_PER_TRIGGER=65000`, Spark `cpus` 4, `mem_limit` 3g), Kafka 스왑 허용(Compose 기본값). Spark는 새 체크포인트 `/opt/spark/checkpoints/step2-20261123`·`latest`로 다시 만들었다. 생성기 2개가 각각 `step-half.csv`로 동시에 실행했다([PRD 06 설정](06-load-profile.md#설정)). Grafana와 Kafka UI는 껐다. 1초 cgroup 샘플러를 켰다.
- 실행: 17:54:15~18:06:15 KST(생성기 기준 720초). 생성기 run_id `a2e407de11a94129bfc7f66bfae35f7a`·`a968333246074b019021cca50b4a6484`, Spark run_id `1cf7a5df-7d18-4913-9651-cf665972f686`.
- 원자료: [2026-11-23-spark-step2.json](../measurements/2026-11-23-spark-step2.json). `docker stats` 첫 표본의 값 없는 줄(`--`) 4개는 지우고 분석했다. Grafana 캡처(17:53:45~18:07:15 KST, 실행 뒤 Grafana를 잠깐 켜서 찍음): [파이프라인 운영](../../images/2026-11-23-spark-step2/pipeline.png), [주문 현황](../../images/2026-11-23-spark-step2/orders.png).

**판정: 보류(측정 환경 이상).** 모든 단계에서 생성과 Spark 처리가 이전 실행보다 크게 느렸다. 원인은 설정이 아니라 측정 환경으로 추정하며, 다시 측정한다.

| 단계 | 생성 EPS (두 run_id 합) | 트리거 평균 / 최대 | 5초 초과 | 실처리량 | backlog 최대 (마지막 60초) | 종단 간 p99 | 판정 |
| --- | ---: | --- | ---: | ---: | ---: | ---: | --- |
| 8k | 7,279 (−9.0%) | 5,481 / 24,926ms | 6 | 7,325 | 6,020 | 32.50초 | 생성 미달 |
| 9k | 8,163 (−9.3%) | 5,575 / 9,728ms | 11 | 9,547 | 39,707 | 23.24초 | 생성 미달 |
| 10k | 9,534 (−4.7%) | 5,262 / 6,539ms | 14 | 9,588 | 0 | 10.52초 | 참고(통과 조건 충족) |
| 11k | 10,383 (−5.6%) | 6,278 / 8,445ms | 17 | 9,854 | 50,232 (증가 536건/초) | 16.60초 | 생성 미달·누적 |
| 12k | 11,408 (−4.9%) | 5,976 / 8,233ms | 17 | 10,877 | 153,109 (증가 588건/초) | 23.30초 | 누적 |

- 공통 기준은 충족했다. 생성 = ACK 6,181,080건, 정상 주문 5,252,983건 = silver 행 수 = 고유 `event_id` 수, 시도 128개 모두 성공, 재시작 0, OOM kill 0이다.
- 보류 근거: 같은 일을 하는 데 CPU가 더 들었고, 처리는 더 느렸다. [B1 step](#b1-배치-상한-65000-실험-2026-11-23)과 비교한 결과는 아래 표와 같다. 10k 단계 실처리량은 15,013 → 9,588 EPS로 줄었다. Spark·Kafka·ClickHouse 모두 비슷한 입력에서 CPU를 1.4~2배 썼다. 단위 작업당 CPU 시간이 늘었다는 뜻이며, 물리 CPU가 느려졌거나 Windows 호스트와 경합한 것으로 추정한다. WSL2는 steal 시간을 보여 주지 않아 확인하지 못했다.

  | 10k 단계 | B1 step(생성기 1개) | 이번(생성기 2개) |
  | --- | ---: | ---: |
  | VM 사용 코어 평균(실행 전체) | 3.63 | 5.91 |
  | Spark / Kafka / ClickHouse 코어 | 1.94 / 0.31 / 0.46 | 2.63 / 0.61 / 0.69 |
  | 실처리량 | 15,013 EPS | 9,588 EPS |

- 생성기 2개는 합계 약 1.1코어를 썼다. 이것만으로는 늘어난 VM 사용량(+2.3코어)을 설명하지 못한다. 같은 2개 구성의 [spike2](#스파이크-판정-생성기-2개-2026-11-23)(약 3시간 전)에서는 spike 실처리량이 14,164 EPS였다.
- 최대 지연은 batch 43(08:58:09 UTC, 트리거 24.9초, 분류 14.2초)이다. 직전 batch 42에서 분류 5.8초, 정상 쓰기 3.2초로 느려지기 시작했다. 이때 Spark CPU는 1.3~1.6코어로 떨어졌다. VM 정지(샘플러 공백)는 08:55:56에 4.9초, 08:58:48에 2.2초 있었다. Kafka와 VM의 swap-in은 실행 내내 거의 없었다(VM `pswpin` +808페이지). 이번 지연은 앞서 본 Kafka swap-in 정지와 다른 현상이다.
- 같은 구간에 ClickHouse `system.asynchronous_metric_log` 병합이 약 7초마다 1.8~4.9초씩 돌았다. 영향 크기는 확인하지 않았다.
- 재측정 전 사용자가 Windows 쪽을 확인했다. 전원 모드는 고성능이었고, CPU는 WSL이 가장 많이 썼으며 다른 프로세스는 적었다. VM 안에는 VS Code 확장의 Claude 프로세스가 떠 있었지만(평균 CPU 3.3%), 재측정 도중에 VS Code를 내렸다. 그러므로 이번 저하의 원인은 확정하지 못했다.

### 지속 용량 판정: 생성기 2개 step 재측정 (2026-11-23)

[보류된 step](#지속-용량-판정-생성기-2개-step-2026-11-23)을 같은 조건으로 다시 실행했다.

- 조건: 위와 같다. 새 체크포인트 `/opt/spark/checkpoints/step3-20261123`를 썼다. 생성기 시작 직후(09:13 UTC 무렵) 사용자가 VS Code(VM 안 `vscode-server`)를 내렸다.
- 실행: 18:11:51~18:23:51 KST(생성기 기준 720초). 생성기 run_id `c39b3a926c154958ba6084c24d73c488`·`27c06e40c62947398c5e00d33dcb6e7d`, Spark run_id `2a195684-cdfc-4123-b6f2-90050709b854`.
- 원자료: [2026-11-23-spark-step3.json](../measurements/2026-11-23-spark-step3.json). Grafana 캡처(18:11:20~18:25:00 KST, 실행 뒤 Grafana를 잠깐 켜서 찍음): [파이프라인 운영](../../images/2026-11-23-spark-step3/pipeline.png), [주문 현황](../../images/2026-11-23-spark-step3/orders.png).

**판정: 지속 용량 12,000 EPS(`step.csv` 전 단계 통과).** [PRD 06 완료 기준 6](06-load-profile.md#완료-기준)의 step 판정을 마쳤다.

| 단계 | 생성 EPS (두 run_id 합) | 트리거 평균 / 최대 | 5초 초과 | 실처리량 | backlog 최대 (마지막 60초) | 종단 간 p99 | ACK p95 100ms 이내 | 판정 |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 8k | 7,781 (−2.7%) | 3,509 / 5,411ms | 1 | 10,958 | 0 | 10.10초 | 94.1% | 통과 |
| 9k | 8,866 | 3,609 / 4,165ms | 0 | 12,284 | 0 | 8.07초 | 97.4% | 통과 |
| 10k | 9,841 | 3,861 / 4,504ms | 0 | 12,704 | 0 | 8.39초 | 98.2% | 통과 |
| 11k | 10,732 | 4,299 / 5,251ms | 1 | 12,474 | 0 | 8.86초 | 92.3% | 통과 |
| **12k** | 11,683 (−2.6%) | 4,996 / 7,876ms | 8 | 12,199 | 24,354 (8,793, 감소 추세 −87.3건/초) | 11.39초 | 86.5% | **통과** |

- 12k 단계 마지막 60초 backlog 최대 8,793건은 기준(12,000 × 5초 = 60,000건) 이하이고, 추세는 감소였다. 다만 트리거 평균이 5초에 닿았고 실처리량(12,199 EPS)이 입력과 거의 같아 여유가 없다. backlog는 09:22 UTC 무렵 한 번 쌓였다가(batch 124, 트리거 7.7초) 해소됐다.
- 공통 기준을 충족했다. 생성 = ACK 6,436,143건, 정상 주문 5,470,901건 = silver 행 수 = 고유 `event_id` 수, 시도 144개 모두 성공, 재시작 0, OOM kill 0, Spark `memory.peak` 2,781,769,728B다.
- 자원: VM 사용 코어 평균 5.08(p95 6.93)이었다. 10k 단계 기준 Spark / Kafka / ClickHouse는 2.13 / 0.51 / 0.46코어, 생성기 2개 합계는 0.86코어였다. VM 정지(샘플러 공백)는 09:14:00 UTC 무렵 2.5초와 2.2초 두 번 있었다. 같은 시각에 Producer ACK p99가 약 2.6초까지 튀었다. 이 시각은 VS Code를 내린 시점 근처지만 연관은 확인하지 않았다.
- 보류 실행과 비교하면 10k 단계 실처리량은 9,588 → 12,704 EPS로 회복했다. 하지만 [B1 step](#b1-배치-상한-65000-실험-2026-11-23)(생성기 1개, 15,013 EPS)보다는 약 15% 낮다. Kafka CPU는 0.31 → 0.51코어로 늘었다. Producer 연결이 2개로 나뉜 영향으로 추정하며, 확인하지 않았다.
- 보류 실행의 저하 원인은 확정하지 못했다. 재측정에서 달라진 조건은 VS Code 종료 하나뿐이다. 근거는 실행 1회다.

### daily 전체 확인 (2026-11-24)

[PRD 06 완료 기준 7](06-load-profile.md#완료-기준)의 `daily.csv` 전체 실행이다(최대 7,500 EPS, 60분).

- 조건: 커밋 `172892d`(S5a·D1 적용, S5 보류), `.env` 기본값(`SPARK_MAX_OFFSETS_PER_TRIGGER=65000`, Spark `cpus` 4, `mem_limit` 3g). 단일 생성기(`PRODUCER_WORKERS=1`). Spark는 새 체크포인트 `/opt/spark/checkpoints/daily-20261123`·`latest`로 다시 만들고 첫 배치 성공을 확인한 뒤 생성기를 시작했다. 샘플러는 [run_spark_mem.sh](../measurements/run_spark_mem.sh)다. Kafka UI는 껐다. Grafana는 생성기 시작 2분 뒤(14:10:03 UTC, 워밍업 구간 안)부터 켜 두었다.
- 실행: 2026-11-23 23:08:07~2026-11-24 00:08:07 KST(생성기 기준 3,600초). 생성기 run_id `73ac328ce5914b5cb801654bc0fd1a48`, Spark run_id `1e18e2c0-8b54-4951-b34f-05985bc297b1`.
- 원자료: [2026-11-24-spark-daily.json](../measurements/2026-11-24-spark-daily.json)(`analyze_profile.py` 출력, 자원 기록 포함). `docker stats` 첫 표본의 값 없는 줄(`--`) 2개는 지우고 분석했다. Grafana 캡처(23:07:30~00:09:30 KST): [파이프라인 운영](../../images/2026-11-24-spark-daily/pipeline.png), [주문 현황](../../images/2026-11-24-spark-daily/orders.png).

**판정: 미충족.** 11개 구간 중 7개가 평시 기준 하나 이상을 넘었다. backlog는 전 구간 0이고 종단 간 p99는 전 구간 10초 이하다. 미충족은 짧은 정지 몇 번 때문이다. 구간 배치가 29~149개라 p99가 사실상 최댓값이 되므로, 배치 하나만 튀어도 구간 판정이 실패한다.

| 구간 | 목표 EPS | 생성 EPS | ACK p95 100ms 이내 | 트리거 평균 / p99 | 5초 초과 | backlog 최대 | 종단 간 p99 / 최대 | 판정 |
| --- | ---: | ---: | ---: | --- | ---: | ---: | --- | --- |
| 00-02 | 1,500 | 1,499 | **98.9%** | 1,068 / 1,529ms | 0 | 0 | 5.81 / 6.13초 | 전송 미달 |
| 02-06 | 800 | 800 | 99.5% | 768 / 1,544ms | 0 | 0 | 5.57 / 6.21초 | 통과 |
| 06-08 | 2,000 | 2,000 | 99.3% | 1,093 / **5,176ms** | 1 | 0 | 6.69 / 9.66초 | 트리거 미달 |
| 08-10 | 3,500 | 3,500 | 100% | 1,471 / 2,006ms | 0 | 0 | 6.16 / 6.72초 | 통과 |
| 10-12 | 5,000 | 4,959 | **98.3%** | 2,119 / **6,529ms** | 1 | 0 | 8.33 / 10.12초 | 전송·트리거 미달 |
| 12-13 | 6,500 | 6,445 | 99.3% | 2,472 / **3,803ms** | 0 | 0 | 7.44 / 8.36초 | 트리거 미달 |
| 13-18 | 5,000 | 4,994 | 99.7% | 1,880 / 2,630ms | 0 | 0 | 6.61 / 7.28초 | 통과 |
| 18-20 | 6,000 | 5,981 | 99.0% | 2,121 / 3,075ms | 0 | 0 | 6.89 / 7.69초 | 통과 |
| 20-22 | 7,500 | 7,463 | **96.7%** | 2,650 / **4,016ms** | 0 | 0 | 7.33 / 8.24초 | 전송·트리거 미달 |
| 22-23 | 4,000 | 3,979 | 99.3% | 1,765 / 2,289ms | 0 | 0 | 6.46 / 6.91초 | 통과 |
| 23-24 | 2,500 | 2,482 | **98.0%** | 1,236 / 1,556ms | 0 | 0 | 6.00 / 6.26초 | 전송 미달 |

- 3,500ms를 넘은 배치(워밍업 제외)는 8개다. batch 224(경과 1,117초, 5,176ms, 입력 10,010건), batch 330~332(1,647~1,659초, 4,265·6,529·3,540ms), batch 373(1,862초, 3,803ms), batch 608·637·643(3,037~3,212초, 3,670·3,565·4,016ms)이다.
- 저부하 구간(06-08, 2,000 EPS)에서도 입력 10,010건 배치가 5.2초 걸렸다. 그래서 건수에 비례하는 비용이 아니라 일시 정지로 본다.
- 느린 배치와 Producer ACK p95 100ms 초과(`ops.producer_stats` 1초 행)를 시각으로 대조했다.
  - batch 224(트리거 14:26:45 UTC): 3초 뒤 ACK p95가 3,504ms였다.
  - batch 608(14:58:45 UTC): 같은 무렵(14:58:47) ACK p95가 1,317ms였다.
  - batch 643(15:01:40~44 UTC): 실행 중 시작한 1초 구간(15:01:42.364·43.448)의 ACK p95가 188·568ms였다.
  - batch 331(14:35:40 UTC): 같은 무렵(14:35:44) ACK p95는 169ms로 작았다.
  - 느린 배치 8개 중 3개(224·608·643)가 Producer ACK 정지와 겹친다. 이 3개는 Spark 단독 문제가 아니라 호스트 쪽 공통 정지로 보인다(추정). 자세한 대조는 [동시 정지 원인 조사](#daily-동시-정지-원인-조사-2026-11-24)에 있다.
- 20-22(7,500 EPS) 구간을 [D1 steady](#d1-적용-steady-검증-2026-11-23) 평시 피크와 비교했다. 트리거 평균은 2,650ms 대 2,633ms로 같고, p99는 4,016ms 대 3,246ms로 나빴다. 이 구간의 3,500ms 초과 배치는 59개 중 3개다.
- 1차 확인(실행 직후):
  - Kafka GC는 아니다. 실행 중 GC 51회 중 100ms를 넘은 것이 없다.
  - VM 정지 기록(`ops.host_stalls`)은 비어 있지만 근거가 되지 않는다. 이 실행에서는 `host-monitor` 서비스가 꺼져 있었다(2026-11-24 확인).
  - 정지 시각의 호스트 상위 프로세스는 생성기(최대 1.2코어)와 Claude Code 정도였다.
  - ClickHouse가 CPU 상한에 걸린 주기 비율은 6.8%(353.7초)로 세 컨테이너 중 가장 높았다. Spark는 4.9%(46.5초), Kafka는 3.7%(42.0초)다.
  - Grafana 조회 서버 시간은 23:35 KST 무렵부터 패턴이 바뀌었다(캡처). 원인은 확인하지 않았다.
- 공통 기준은 통과했다. 생성 = ACK 13,763,986건, 정상 주문 생성 11,700,825건 = silver 행 수 = 고유 `event_id` 수다. Spark 시도 722개가 모두 성공했고 재시작은 0회, OOM kill은 0이다.
- 자원: VM 사용 코어는 평균 2.35(p95 5.30)였다. `memory.peak`는 Spark 2,797,056,000B, ClickHouse 2,194,247,680B, Kafka 1,925,046,272B다. 20-22 구간 컨테이너 평균은 Spark / ClickHouse / Kafka 1.53 / 0.47 / 0.30코어다.
- 근거는 실행 1회다.

#### daily 동시 정지 원인 조사 (2026-11-24)

같은 실행의 기록을 시각으로 대조했다. 대조한 기록은 Spark 배치 단계, `ops.producer_stats`, ClickHouse `system.asynchronous_metric_log`(OS 지표: PSI, 블록 장치)·`system.part_log`, cgroup `io.stat`이다. 원자료: [2026-11-24-spark-daily-stall.json](../measurements/2026-11-24-spark-daily-stall.json). 1번 파인(codex)의 재검토를 받아 수치와 서술을 고쳤다(아래 '재검토 반영').

**결론: 느린 배치 8개 중 4개(224·373·608·643)가 IO 압력과 겹치고, 331은 CPU 압력과 겹친다. 저장장치 경로와 CPU 스케줄링이 영향을 준 것으로 의심되지만, 정지가 생긴 계층과 유발 주체는 확정하지 못했다.**

- 판단 근거는 배치 실행 구간마다 같은 구간 OS 표본의 최댓값을 뽑아 비교한 것이다. 정상 배치에서는 높은 압력이 드물다.

| 배치 실행 구간 표본 최댓값 | 느린 배치(> 3,500ms) | 정상 배치 |
| --- | ---: | ---: |
| `PSI_IO_full` > 20% | 4/8 | 8/689 |
| `PSI_IO_full` > 10% | 4/8 | 26/689 |
| `PSI_CPU_some` > 50% | 4/8 | 2/689 |
| `sde` 미완료 요청 > 10 | 4/8 | 51/689 |

- PSI는 수집 간격으로 나눈 값이다. 블록 지표는 `sde`다. `sde`는 Docker 볼륨 디스크이며, Spark 체크포인트(`/opt/spark/checkpoints`)가 여기에 있다. 정상 배치에도 IO 압력이 20%를 넘은 경우가 8번 있으므로, IO 압력만으로 느린 배치가 생기지는 않는다.

| 느린 배치 / ACK 정지 | 실행 구간(UTC) | 같은 구간 표본 | 분류 |
| --- | --- | --- | --- |
| batch 224(5,176ms) | 14:26:45.0~50.2 | 직전 14:26:42~44 `PSI_IO_full` 11·37·34%, `sde` 미완료 요청 12~13개. :43·44 `sde` 쓰기 완료 0B(:42는 491,520B 완료). :45·46은 수집 공백. :47 표본(간격 2.756초)은 IO full 0%·CPU some 86.9%·`sde` 41.9MB | IO + CPU 복합 |
| ACK p95 3,504ms | 14:26:48.757 시작 1초 구간 | 위와 같은 사건 | IO + CPU 복합 |
| batch 373(3,803ms) | 14:39:10.0~13.8 | IO full 20%, `sde` 미완료 요청 51개, CPU some 66%(직전 14:38:52~14:39:02 `silver` 234MB 병합) | IO + CPU |
| ACK p95 679.7ms | 14:57:30.352 시작 1초 구간 | 14:57:22~28 IO full 최대 51% | IO |
| batch 608(3,670ms), ACK p95 1,317ms | 14:58:45.0~48.7, ACK 14:58:47.369 | IO full 27%, `sde` 미완료 요청 181개 | IO |
| batch 643(4,016ms) | 15:01:40.0~44.0 | IO full 24%, `sde` 미완료 요청 13개. 15:01:47~50 상승은 배치 종료 뒤라서 제외 | IO |
| batch 331(6,529ms) | 14:35:40.0~46.5 | IO full ≤ 4%. CPU some 56~61%(14:35:44~46). Spark 2.7~3.6코어(상한 4), ClickHouse 1.1~1.3코어 | CPU 경합 유력 |
| batch 330(4,265ms)·332(3,540ms) | 14:35:35~39, 14:35:46.5~50.1 | 330: CPU some 최대 23%. 332: 331과 같은 CPU 피크 끝부분 | 미분류(가설: CPU) |
| batch 637(3,565ms) | 15:01:10.0~13.6 | 기준을 넘는 지표 없음 | 미분류 |

- batch 224의 Spark 체크포인트 WAL 단계(`wal_commit_ms`)는 2,888ms였다. 평소에는 20~50ms다. 이 값은 해당 단계의 경과 시간이며, 블록 쓰기 서비스 시간을 직접 잰 것이 아니다. 같은 시각 CPU 압력도 컸으므로 디스크 지연과 스케줄링 지연이 겹친 사건일 수 있다.
- 정지를 설명하는 후보는 서로 배타적이지 않다. 이번 데이터로는 어느 것이 맞는지 판정할 수 없다.
  - dirty writeback 스로틀(`balance_dirty_pages`)
  - ext4 journal·flush·barrier 대기
  - 게스트·호스트 CPU 스케줄링
  - Windows VHDX 쪽 지연
- 디스크 쓰기량(컨테이너 기동 뒤 누적 cgroup `io.stat`, 2026-11-24 00:3x KST 조회): ClickHouse 8.8GB(쓰기 요청 28.9만 회), Kafka 3.5GB(5.4만 회), Spark 0.0GB(0.4만 회). 누적 구간이 실행 1시간과 다르므로 기여율로 쓰지 않는다. 가장 많이 쓴 컨테이너라는 사실만으로 순간 정지를 일으킨 주체가 정해지지도 않는다.
- ClickHouse part 크기 합(실행 1시간, `part_log` 결과 part 크기이며 물리 쓰기량이 아님):

| 테이블 | 종류 | 횟수 | 결과 part 크기 합 |
| --- | --- | ---: | ---: |
| `ops.producer_stats` | 병합 | 3,593 | 3.31GB |
| `silver.order_events` | 병합 | 418 | 2.54GB |
| `silver.order_events` | 새 part | 2,160 | 0.85GB |
| `system.asynchronous_metric_log` | 병합 | 282 | 0.23GB |
| `ops.producer_stats` | 새 part | 3,596 | 0.02GB |

- `ops.producer_stats` 반복 재작성이 관측됐다. 생성기는 1초마다 1행을 `async_insert`(flush 대기)로 넣었고, 1초마다 새 part 1개가 생겼다. 병합은 기존 병합 part와 새 1행 part, 두 입력을 이어서 합쳤다(`merged_from`). 결과는 0.84~1.01MB였다. 병합 결과 크기 합 대 새 part 크기 합은 약 162배다. 이것은 이번 실행에서의 관측이며, 무파티션 `MergeTree`가 늘 이렇게 동작한다는 뜻은 아니다.
- Kafka GC 정지는 없었다(100ms 초과 0회). 정지 시각의 `PSI_MEM_full`은 0~11%였다. 이것만으로 메모리 회수 영향을 배제하지는 않는다.

**재검토 반영(1번 파인, 2026-11-24)** — 첫 기록의 오류:
- PSI를 초당 값으로 보고 `/1e4`만 해서 %로 바꿨다. 실제로는 직전 측정 이후 delta다. 수집 간격이 1초가 아닌 표본은 값이 부풀었다. 예: 14:26:47 CPU some 239.6%는 간격 2.756초로 나누면 86.9%다.
- 같은 초에 표본이 2개 이상인 경우를 초 단위로 합산해 값이 부풀었다(실행 중 표본 3,480개 중 704개).
- 미완료 요청 수를 모든 장치 합계로 적었다. 실제 값은 `sde` 12~13개다. 14:26:42~44 "3초간 쓰기 0바이트"도 틀렸다(:42에 `sde` 491,520B 완료, :45·46은 공백이라 판정 불가).
- 14:57대 ACK 1,317ms는 실제로는 14:58:47의 값이다. 14:57대 값은 679.7ms다.
- batch 643과 겹친다고 적은 15:01:47~50 IO 상승은 배치 종료(15:01:44.0) 뒤다.
- batch 330~332를 한꺼번에 CPU 경합으로 분류했다. CPU 압력이 큰 것은 331뿐이다.
- '가상 디스크 아래쪽 정지'로 단정했다. 근거가 부족해 '계층 미확정'으로 낮췄다.

**다음 단계(미적용, 재검토 권고 순서)**
1. 계측을 보완한다. 1초 샘플러에 다음 항목을 추가한다.
   - cgroup별 `io.stat` 장치별 delta와 `io.pressure`·`cpu.pressure`·`cpu.stat` throttling
   - `/proc/vmstat`(`nr_dirty`·`nr_writeback`·`nr_dirtied`·`nr_written`·swap·major fault)
   - `sde` 장치 통계(`/sys/block/sde/stat`)
   - 샘플러 실제 수집 간격
   - 정지 시각 Grafana `query_log`의 CPU·동시성·읽기량
   - 가능하면 정지 순간 task `wchan`, writeback·jbd2 trace도 남긴다. 이것으로 dirty 스로틀과 journal 대기를 구분한다.
   - 구현(2026-11-24): [sample_host_1s.sh](../measurements/sample_host_1s.sh)를 privileged 1회성 컨테이너(`--cgroupns=host --pid=host`, cgroup2 직접 마운트)에서 1초마다 실행한다. 기록 항목은 다음과 같다.
     - Kafka·Spark·ClickHouse cgroup의 `io.stat`·`io.pressure`·`cpu.pressure`·`cpu.stat`·`memory.stat`(major fault, swap-in, dirty·writeback 페이지)
     - VM의 `/proc/vmstat`·`/proc/pressure/*`, `/sys/block/{sdd,sde}/stat`
     - D 상태(uninterruptible) task의 이름·`wchan`
     - 모든 값은 누적값이며, `/proc/uptime` 기준 실제 수집 시각을 함께 남긴다. delta·정규화는 [host_1s_summary.py](../measurements/host_1s_summary.py)가 한다. trace(ftrace)는 넣지 않는다.
   - 측정 전에 `host-monitor`를 켜서 VM 정지 기록이 남는지 확인한다.
2. 변경 없이 같은 조건으로 기준 실행을 한 번 더 한다.
3. `ops.producer_stats` 쓰기 증폭만 줄이고 같은 조건으로 비교한다. 방식은 생성기가 1초 행 여러 개를 모아 한 번에 INSERT하는 것을 우선한다. 지금 writer는 `wait_for_async_insert=1` 요청 완료를 기다린 뒤 다음 요청을 보낸다. 그래서 서버 timeout만 늘리면 서버가 합치는 효과가 아니라, 클라이언트 큐에서 기다리는 효과가 된다. Grafana 표시 지연, HTTP timeout, 종료 시 남은 행 처리도 함께 정한다. 1초 해상도를 유지하는 것과 표시 지연을 유지하는 것은 다르다.

#### daily 기준 재실행: 1초 커널 계측 (2026-11-24)

[다음 단계](#daily-동시-정지-원인-조사-2026-11-24) 2번이다. 설정을 바꾸지 않고 daily와 같은 조건으로 다시 실행했다. 1초 샘플러([sample_host_1s.sh](../measurements/sample_host_1s.sh))와 `host-monitor`를 켰다.

- 조건: 커밋 `cc415d2`. 실행 전에 Kafka·ClickHouse·Spark·Grafana를 정지했다가 다시 시작했다. Spark 체크포인트는 `/opt/spark/checkpoints/daily2-20261124`·`latest`다. 단일 생성기, `.env` 기본값이다. daily와 다른 점은 `host-monitor`가 켜져 있고 Grafana가 생성기 시작 전부터 켜져 있었다는 것이다.
- 실행: 2026-11-24 00:45:27~01:45:27 KST. 생성기 run_id `6b5c70c2492a40ed885fe9719d08a9ca`, Spark run_id `8ef78151-942f-4275-8029-f4e963804d46`.
- 원자료: [2026-11-24-spark-daily2.json](../measurements/2026-11-24-spark-daily2.json)(`analyze_profile.py`), [2026-11-24-spark-daily2-host1s.json](../measurements/2026-11-24-spark-daily2-host1s.json)(`host_1s_summary.py`). Grafana 캡처(00:45:00~01:46:30 KST): [파이프라인 운영](../../images/2026-11-24-spark-daily2/pipeline.png), [주문 현황](../../images/2026-11-24-spark-daily2/orders.png).

**판정: 미충족.** 11개 구간 중 8개가 기준을 넘었다. daily보다 ACK 미달 구간이 많다. backlog는 전 구간 0이다.

| 구간 | 생성 EPS | ACK p95 100ms 이내 | 트리거 평균 / p99 | 5초 초과 | 종단 간 p99 / 최대 | 판정 |
| --- | ---: | ---: | --- | ---: | --- | --- |
| 00-02 | 1,498 | 99.4% | 1,153 / 2,098ms | 0 | 5.89 / 6.86초 | 통과 |
| 02-06 | 800 | 99.3% | 793 / 992ms | 0 | 5.59 / 5.81초 | 통과 |
| 06-08 | 1,999 | 99.7% | 1,132 / 1,753ms | 0 | 5.88 / 6.27초 | 통과 |
| 08-10 | 3,472 | **96.7%** | 1,543 / 2,148ms | 0 | 6.24 / 6.79초 | 전송 미달 |
| 10-12 | 4,987 | **98.7%** | 1,929 / 2,849ms | 0 | 6.63 / 7.52초 | 전송 미달 |
| 12-13 | 6,399 | **96.6%** | 2,411 / **6,852ms** | 1 | **10.01** / 11.48초 | 전송·트리거·종단 간 미달 |
| 13-18 | 4,959 | **97.6%** | 1,949 / **4,087ms** | 1 | 6.99 / 11.21초 | 전송·트리거 미달 |
| 18-20 | 5,976 | **97.3%** | 2,207 / **4,302ms** | 0 | 6.99 / 8.67초 | 전송·트리거 미달 |
| 20-22 | 7,453 | **96.0%** | 2,603 / **3,760ms** | 0 | 7.12 / 7.89초 | 전송·트리거 미달 |
| 22-23 | 3,988 | **96.0%** | 1,567 / 2,086ms | 0 | 6.22 / 6.49초 | 전송 미달 |
| 23-24 | 2,496 | **98.7%** | 1,151 / 1,524ms | 0 | 5.91 / 6.24초 | 전송 미달 |

- 공통 기준은 통과했다. 생성 = ACK 13,738,469건, 정상 주문 생성 11,675,146건 = silver 행 수 = 고유 `event_id` 수다. Spark 시도 722개가 모두 성공했고 재시작은 0회다. Producer ACK 지연은 p50 5.9ms, p95 8.2ms, p99 74.9ms다.
- `host-monitor`가 VM 멈춤 1건을 기록했다(01:08:15 KST, 2,182ms).

**1초 계측 결과**

| 배치 실행 구간 표본 최댓값 | 느린 배치(> 3,500ms, 워밍업 batch 1 포함) | 정상 배치 |
| --- | ---: | ---: |
| VM `cpu.pressure` some > 50% | 5/8 | 1/713 |
| VM `io.pressure` full > 20% | 2/8 | 12/713 |
| Kafka cgroup `io.pressure` full > 20% | 3/8 | 30/713 |
| Spark cgroup `io.pressure` full > 20% | 2/8 | 8/713 |
| ClickHouse cgroup `io.pressure` full > 20% | 0/8 | 6/713 |

| Producer ACK p95 기준 1초 구간(전후 표본) | ACK p95 > 100ms | ACK p95 ≤ 100ms |
| --- | ---: | ---: |
| Kafka cgroup `io.pressure` full > 20% | 28/72 | 106/3,404 |
| VM `io.pressure` full > 20% | 10/72 | 55/3,404 |
| VM `cpu.pressure` some > 50% | 10/72 | 8/3,404 |
| 샘플러 수집 간격 > 1.5초 | 18/72 | 50/3,404 |
| Kafka major fault > 100/s | 0/72 | 0/3,404 |

- 느린 배치는 이번에는 IO보다 **VM CPU 압력**과 더 강하게 겹쳤다. 그중 batch 382·648은 WAL 단계가 707·397ms로 길었다. 나머지는 평소와 같았다(19~41ms).
- ACK 정지는 **Kafka cgroup IO 압력**과 가장 강하게 겹쳤다(ACK 정지 초의 39%, 정상 초의 3%). 정지 순간에는 샘플러 자신도 늦게 깨어났다(수집 간격 최대 4.08초). VM 전체가 잠시 멈추거나 스케줄되지 못한 경우도 섞여 있다.
- 기각한 가설: Kafka cgroup 메모리 한도 회수가 writeback을 기다려 IO가 막혔다는 가설. 실행 뒤 Kafka `memory.events` max는 0으로, 한도에 닿지 않았다. `memory.current`는 1,826MB / 한도 2,048MB였다. Kafka major fault도 0이라서 swap-in 정지도 아니다.
- 한계:
  - D 상태 task는 실행 전체에서 2건만 잡혔다. 1초 스냅샷으로는 순간 정지를 잡지 못해, `wchan`으로 dirty 스로틀과 journal 대기를 구분하지 못했다.
  - 샘플러 컨테이너는 `--cpus 0.5`다. 수집 지연 일부는 샘플러 자신의 CPU 제한 때문일 수 있다.
  - cgroup `cpu.stat`의 throttled 시간은 여러 CPU의 합이라서 %로 바꾸면 100%를 넘을 수 있다.
- 해석(추정): 정지는 한 가지가 아니다. Kafka 쓰기 경로의 IO 대기와 VM 전체 CPU 압력·스케줄링 정지가 섞여 있다. 같은 실행 안에서도 daily와 비율이 달라 재현성이 낮다. 근거는 실행 2회다.

#### daily 비교 실행: producer_stats 5초 묶음 (2026-11-24, 중단)

[다음 단계](#daily-동시-정지-원인-조사-2026-11-24) 3번이다. 커밋 `b1a85f1`(`ops.producer_stats` 1초 행을 5초 묶어 INSERT)로 [기준 재실행](#daily-기준-재실행-1초-커널-계측-2026-11-24)과 같은 조건에서 시작했다. Spark run_id는 `34784717-9484-4ffb-b4f2-0ecd8e90128f`, 체크포인트는 `daily3-20261124`다.

- 사용자 요청으로 생성기 경과 390초(02-06 구간, 521,241건)에 SIGINT로 중단했다. 생성기 종료 코드는 130이다. 판정하지 않았고 원자료도 남기지 않았다. 나중에 같은 조건으로 처음부터 다시 실행한다.

#### daily 비교 실행: producer_stats 5초 묶음 (2026-11-24)

[다음 단계](#daily-동시-정지-원인-조사-2026-11-24) 3번이다. 중단한 실행을 처음부터 다시 했다.

- 조건: 커밋 `ff558c6`(`b1a85f1` 묶음 INSERT 포함). 나머지는 [기준 재실행](#daily-기준-재실행-1초-커널-계측-2026-11-24)과 같다. 스택을 정지했다가 다시 시작했고, `host-monitor`·Grafana·1초 샘플러를 켰다. Spark 체크포인트는 `/opt/spark/checkpoints/daily4-20261124`·`latest`다.
- 실행: 2026-11-24 10:07:55~11:07:55 KST. 생성기 run_id `727cc305f5d64edfbad88d7ac11b2fa5`, Spark run_id `be529b40-903f-45cf-b0b6-59e3a41efb35`.
- 원자료: [2026-11-24-spark-daily4.json](../measurements/2026-11-24-spark-daily4.json), [2026-11-24-spark-daily4-host1s.json](../measurements/2026-11-24-spark-daily4-host1s.json). Grafana 캡처(10:07:30~11:09:00 KST): [파이프라인 운영](../../images/2026-11-24-spark-daily4/pipeline.png), [주문 현황](../../images/2026-11-24-spark-daily4/orders.png).
- 적용 확인: 실행 시작 후 30초 동안 `producer_stats` 새 part가 4개(23행)였다. 묶기 전에는 30초에 30개였다.

**판정: 미충족(11개 구간 중 4개 미달).** 기준 재실행(8개 미달)과 첫 daily(7개 미달)보다 적다. IO 압력 정지는 이 실행에서 사라졌다. 남은 미달 4개 중 2개는 `host-monitor` 틱 간격이 2초 이상으로 늘어난 구간이다([VM 지연 원인 조사](#daily4-vm-지연-원인-조사-2026-11-24)).

| 구간 | 생성 EPS | ACK p95 100ms 이내 | 트리거 평균 / p99 | 5초 초과 | 종단 간 p99 / 최대 | 판정 |
| --- | ---: | ---: | --- | ---: | --- | --- |
| 00-02 | 1,476 | **98.8%** | 1,706 / **5,936ms** | 1 | 7.46 / 9.63초 | 전송·트리거 미달(감지기 틱 지연) |
| 02-06 | 797 | 99.8% | 930 / **6,652ms** | 2 | **10.06** / 15.42초 | 트리거·종단 간 미달(감지기 틱 지연) |
| 06-08 | 2,000 | 99.7% | 1,114 / 1,780ms | 0 | 5.78 / 6.26초 | 통과 |
| 08-10 | 3,499 | 100% | 1,576 / 2,545ms | 0 | 6.18 / 6.85초 | 통과 |
| 10-12 | 4,987 | 99.7% | 2,008 / 2,900ms | 0 | 6.66 / 7.79초 | 통과 |
| 12-13 | 6,455 | **98.7%** | 2,382 / 3,003ms | 0 | 6.91 / 7.34초 | 전송 미달(150행 중 2행) |
| 13-18 | 4,991 | 100% | 2,014 / 2,674ms | 0 | 6.47 / 7.08초 | 통과 |
| 18-20 | 5,989 | 100% | 2,400 / 3,116ms | 0 | 6.76 / 7.42초 | 통과 |
| 20-22 | 7,471 | 99.3% | 2,745 / **3,641ms** | 0 | 7.22 / 7.82초 | 트리거 미달 |
| 22-23 | 3,994 | 100% | 1,581 / 2,204ms | 0 | 6.21 / 6.74초 | 통과 |
| 23-24 | 2,493 | 100% | 1,192 / 1,566ms | 0 | 5.91 / 6.30초 | 통과 |

- 공통 기준은 통과했다. 생성 = ACK 13,785,927건, silver 행 수 = 고유 `event_id` 수 = 11,718,917건이다. Spark 시도 720개가 모두 성공했고 재시작은 0회다.
- `host-monitor` 틱 간격(1초 sleep 포함, 감지 시각 기준): 10:12:02 2,022ms, 10:12:15 2,082ms, 10:19:40 6,610ms, 10:19:54 4,901ms. 마지막 간격 동안 `producer_stats` INSERT 1건(5행)이 5초 timeout으로 실패했다. 그래서 02-06 구간 1초 행이 600개 중 593개다. 묶음 INSERT라서 실패 1건의 손실이 최대 5행으로 커진다.
- 10:19 두 번째 간격 직후 표본에서 VM `cpu.pressure` some이 94~101%, ClickHouse cgroup CPU 압력이 98~100%였다. 이 표본은 수집 지연 중 순차로 읽은 값이라 오차가 있다. D 상태 task로 `runc@kernel_clone`(컨테이너 프로세스 생성)이 연속으로 잡혔다. 측정 스크립트는 5초마다 `docker exec`를 한다. 관련 여부는 확인하지 않았다.

**기준 재실행과 비교**

| 항목 | 기준 재실행 | 묶음 INSERT |
| --- | ---: | ---: |
| `producer_stats` 새 part / 병합 | 3,600 / 3,591회 | 626 / 579회 |
| `producer_stats` 병합 결과 part 크기 합 | 3.92GB | 0.72GB(−82%) |
| `silver.order_events` 병합 결과 part 크기 합 | 2.97GB | 2.66GB |
| 실행 중 cgroup 쓰기 ClickHouse / Kafka / Spark(1초 계측 합) | 10.01 / 3.46 / 0.02GB | 5.56 / 3.47 / 0.02GB |
| 실행 중 `sde` 쓰기 합 / 평균 | 15.54GB / 4.22MB/s | 10.92GB / 3.01MB/s |
| VM `io.pressure` full > 20%인 표본(전체 약 3,260개) | 23 | 0 |
| Kafka cgroup `io.pressure` full > 20%인 표본 | 49 | 5 |
| VM `cpu.pressure` some > 50%인 표본 | 6 | 36(대부분 감지기 틱 지연 직후) |
| ACK 기준 미달 구간 | 8 | 2 |

- 해석: `producer_stats` 쓰기 증폭을 줄이자 디스크 쓰기와 IO 압력 정지가 크게 줄었다. Kafka ACK 정지도 함께 줄었다. 이는 공유 디스크(`sde`) 쓰기 경합이 Kafka 쓰기 경로를 막았다는 해석과 맞는다(추정, 실행 1회씩 비교).
- 남은 문제는 감지기 틱 지연(최대 6.6초)과 그 직후의 CPU 압력이다. 원인 조사는 아래 절에 있다.
- 결정: 묶음 INSERT(`b1a85f1`)를 유지한다.

#### daily4 VM 지연 원인 조사 (2026-11-24)

[daily 비교 실행](#daily-비교-실행-producer_stats-5초-묶음-2026-11-24)에서 `host-monitor` 틱 간격이 늘어난 원인을 조사했다. 1번 파인(codex)이 읽기 전용으로 재검증했다.

**결론: 10:19의 두 지연 구간은 Windows에서 AhnLab V3 Lite 계열 필터를 unload/reload한 작업 구간과 시간적으로 겹친다. 인과 경로는 확인하지 못했다.** 원인은 확정하지 않고, 호스트·가상화 계층의 영향 가능성을 조사 대상으로 둔다.

- `host-monitor`의 `gap_ms`는 `sleep 1`을 포함한 두 벽시계 틱 사이 전체 간격이다. 실제 정지 시간이 아니다. 감지기 틱 간격(KST)은 10:12:00.602~02.624, 10:12:13.261~15.343, 10:19:33.791~40.401, 10:19:49.295~54.196이다. `host-monitor`(`cpus` 0.25)와 1초 샘플러(`cpus` 0.5) 자신의 throttle·스케줄링 지연도 배제하지 못했다.
- 첫 10:19 간격 안에서도 1초 샘플러는 10:19:34.614·38.701에 실행됐다. 마지막 표본에서는 세 컨테이너가 약 2.52코어를 썼다. 그래서 6.6초 내내 VM 전체가 멈췄다는 해석은 맞지 않다.
- 간격 직전 표본에서 Spark·ClickHouse·Kafka cgroup CPU 합은 0.13~0.44코어였다(VM 8코어). 세 서비스의 지속적인 CPU 포화로는 설명하기 어렵다. 다만 이것만으로 호스트의 vCPU 미할당이 입증되지는 않는다. 세 cgroup 밖의 VM 프로세스·커널 작업, CFS quota throttle은 보지 않았다. CPU PSI some은 runnable task의 대기 비율이지 VM 전체 미할당 비율이 아니다. `cpu.stat` throttled 시간은 스레드·CPU별 누적이라서 wall-time %로 읽으면 안 된다.
- 1초 샘플러 한계: tick 시작 시각 하나를 붙인 뒤 파일을 순서대로 읽는다. 그래서 지연이 중간에 끼면 동시 스냅샷이 아니다. 예: 10:12:02.431 표본은 wall 차 2.608초, uptime 차 1.26초다. 이 근처의 VM PSI 146.5%, 장치 util 140.6%, 7.62코어는 인과 증거로 쓰지 않는다. 개선하려면 각 카운터 읽기 전후의 monotonic 시각을 남긴다.
- Windows 이벤트(System, Microsoft-Windows-FilterManager, RecordId 147712~147719, Windows 시간대 KST):
  - unload 성공: 10:19:31.638 `MeDVpDrv_V3LITE40`, 32.246 `MeDCoreD_V3LITE40`, 41.656 `TSFltDrv_V3LITE40`, 42.929 `ArtDrv`
  - load 성공: 10:19:47.317 `ArtDrv`, 48.192 `MeDCoreD`, 52.297 `MeDVpDrv`, 56.490 `TSFltDrv`
  - 첫 간격(33.8~40.4)은 앞의 두 unload 뒤, 뒤의 두 unload 앞이다. 둘째 간격(49.3~54.2)은 두 load 뒤에 시작하고, `MeDVpDrv` load가 그 안에 있다. 이벤트만으로는 detach·attach 각각의 실제 시작·끝을 정할 수 없다.
  - 전후 드라이버 버전·빌드 timestamp가 같으므로 확인된 것은 unload/reload다. 바이너리 교체나 제품 업데이트인지는 V3 로그·파일 hash로 따로 확인해야 한다. 10:19:52.988 SecurityCenter "V3 Lite 상태 ON"은 상태 보고일 뿐이다.
  - CodeIntegrity/Operational에 10:19:32.636과 52.974~53.059에 svchost의 AhnLab `v3amsi64.dll` signing-level 불충족(3033)이 있다. V3 관련 활동의 추가 단서이며 원인 근거는 아니다.
- 메커니즘은 미입증이다. minifilter unload는 미완료 IO 정리를 기다릴 수 있다. 하지만 정상 unload가 볼륨 전체 IO를 멈춘다는 보장은 없고, VHDX IO 대기와 VM vCPU 정지는 다른 현상이다. 10:19 게스트 IO full은 대체로 0~3%였고, 관측된 표본에서 `sde` 미완료 요청 0, 쓰기 완료도 계속 있었다. 해당 필터 instance가 WSL·Docker VHDX 볼륨에 붙어 있는지(`fltmc instances`)도 확인하지 않았다.
- 10:12 두 간격: 대응 FilterManager 기록이 없다. 10:11:58.713 표본에서 VM swap-in 110페이지/s, major fault 122/s가 있었다. 10:12:02 표본에서는 `nr_dirty` 576.6 → 34.4MB, writeback 120.6MB, `sdd` 미완료 요청 23이 있었다. CPU만으로 묶지 않는다. Time-Service·Storage-ClassPnP 로그는 10:10~10:21에 0건이었다.
- 이전 실행 대조: System 로그는 11/10~11/24, Application 로그는 11/17~11/24 기간이 남아 있다. 11/23 23:00~11/24 11:10의 FilterManager 기록은 위 10:19 8건뿐이다. 그래서 첫 daily의 느린 배치와 기준 재실행의 01:08:15.769 지연은 같은 재로드로 설명되지 않는다. 11/24 00:01:10.975에도 SecurityCenter ON이 있으나 FilterManager 이벤트는 없었다.
- 읽지 못한 로그: Hyper-V·Defender 일부는 권한 부족이다. 읽지 못한 로그를 이벤트가 없었다는 근거로 쓰지 않는다.
- 다음에 확인할 것(미적용):
  - Hyper-V VP 실행·대기 시간과 Windows CPU·DPC/ISR trace(성능 모니터·WPR)
  - `fltmc instances`로 VHDX 볼륨의 필터 확인
  - V3 보안 기록(이벤트 로그 > 기타, 시스템 보안)
  - 샘플러의 카운터별 monotonic 시각
- 근거는 시각 대응 1회다.

**추가 검증(2026-11-24, 서브에이전트·1번 파인 서브에이전트 3명, 읽기 전용)**

- 기저율:
  - Windows System 로그는 2026-07-30부터 남아 있다. 그 안의 V3 필터 재로드는 172회다(11-14~11-19는 매일 14:25 KST).
  - `host-monitor` 감시 시간(26.5~33.4시간) 안에 재로드는 4회 있었다. 4회 모두 ±15초 안에 틱 지연이 있었다(11-19·11-20·11-23·11-24). 우연 기대치는 0.04~0.05회다. 지연이 서로 독립이라는 가정의 근사치다.
  - 거꾸로 보면 감시 중 틱 지연(60초 이하) 44건 중 재로드와 겹친 것은 6건이다. 지연 대부분은 V3로 설명되지 않는다.
  - 틱 지연 출처는 `docker logs` 55행이다. `ops.host_stalls`·ClickHouse system 로그는 11-23 01:18 UTC부터만 남아 있다.
- 교란: 재로드 4회 중 3회는 호스트가 절전에서 깨어난 지 1.2~13분 뒤였다. 교란이 없는 사례는 11-19 정기 재로드 1건뿐이고, 그때 간격은 2,128ms로 임계값을 겨우 넘었다.
- 필터 부착: VHDX(Ubuntu `ext4.vhdx`, Docker `docker_data.vhdx`, swap)는 모두 C:에 있다. 레지스트리상 V3 필터 4개의 기본 인스턴스는 자동 부착이다. `fltmc`는 관리자 권한이 필요해 실제 부착은 확인하지 못했다.
- V3 로그는 암호화·바이너리라 읽지 못했다. 설치 폴더의 업데이트 관련 파일(`cfg.db`, `mupdate2.dat`, `mass.dat` 등) 수정 시각은 10:19:10~10:19:59 KST다.
- VM 전체 정지는 반증됐다. 첫 6.610초 간격(10:19:33.791~40.401 KST) 안에서도 다음이 실행됐다.
  - `producer_stats` 행(10:19:35.817, 38.698, 39.698)
  - `generator_writer` INSERT 성공(10:19:35.917, 5~7ms)
  - 1초 샘플러
- 새 경쟁 가설(CFS bandwidth throttle):
  - 10:19:54.198~59.893(약 5.7초) 동안 ClickHouse cgroup `usage_usec`은 833µs만 늘었다. 같은 동안 `nr_throttled`는 208 → 264, `throttled_usec`는 +24.830초였다.
  - 같은 구간에 Kafka·Spark·샘플러는 계속 실행됐다.
  - 커널(6.18 `fair.c`)은 throttle 중에도 period overrun만큼 `nr_throttled`를 더하고, unthrottle 때 과거 throttle 시간을 한꺼번에 합산한다. 그래서 +56을 새 quota 소진 56회로, +24.8초를 이 구간 정지 시간으로 읽으면 안 된다.
  - CPU 제한이 걸린 cgroup이 수 초간 throttle 상태에서 풀리지 않는 현상일 수 있다. `host-monitor`(`cpus` 0.25)도 제한이 걸려 있다. 기동 뒤 `nr_throttled` 208, `throttled_usec` 17.2초다. 그러므로 **감지기 자신의 throttle이 틱 지연을 만들었을 가능성**이 있다.

**감지기 비교 실험 계획(2026-11-24)**

- 목적: 틱 지연이 CPU 제한이 걸린 감지기 자신의 문제인지, VM 수준 지연인지 같은 실행 안에서 가린다.
- 방법: daily 부하 중에 세 감지기를 동시에 돌린다. 기존 설정은 바꾸지 않는다.
  1. 기존 `host-monitor`(`cpus` 0.25, 임계 2,000ms)
  2. [tick_gap_probe.sh](../measurements/tick_gap_probe.sh) 컨테이너(같은 이미지, CPU 제한 없음, 임계 1,500ms)
  3. 같은 스크립트를 WSL Ubuntu 셸에서 실행(Docker cgroup 밖, 같은 VM 커널, 임계 1,500ms)
- 판정:
  - 1만 지연을 기록하면 감지기 자신의 throttle이 원인이다.
  - 셋이 같은 시각에 기록하면 VM 수준 지연이다.
  - 2·3의 벽시계 간격과 `/proc/uptime` 간격을 함께 남겨 시계 보정 영향도 본다.

### D1 결정 기록: 배치 내 중복 제거 제거 (2026-11-23)

#### 배경

S5a를 적용한 뒤에도 평시 피크 트리거 p99가 3,659ms로 목표 3,500ms를 넘었다. [job 비용 대조](#job-비용-대조-벤치-2026-11-23)에서 정상 출력 action의 가장 큰 단일 비용은 배치 내 `dropDuplicates(["event_id"])`의 셔플(336~375ms)이었다.

#### 제거 이유와 근거

| 근거 | 내용 | 출처 |
| --- | --- | --- |
| 비용이 크다 | 벤치에서 336~375ms가 들었다. 운영 피크에서는 셔플 map job 평균이 380ms였다. 목표 미달분 159ms보다 크다 | [job 비용 대조](#job-비용-대조-벤치-2026-11-23), [S5a 검증](#s5a-적용-steady-검증-2026-11-23) |
| 입력 계약상 배치 내 중복이 생기지 않는다 | v1 입력원은 로그 생성기 하나다. 생성기는 이벤트마다 `uuid.uuid4()`로 새 `event_id`를 만들고(`generator.py`), 중복을 일부러 만드는 이상 데이터 종류가 없다. Producer는 `enable.idempotence=true`라서 재시도 중복을 브로커가 걸러 낸다(`kafka_producer.py`). 두 steady 실행에서 중복이 0건이었던 것은 이 설계가 지켜졌다는 확인일 뿐이며, 실제 입력원의 중복률에 대한 근거가 아니다 | [Kafka 연동](02-kafka-ingestion.md), [S5a 검증](#s5a-적용-steady-검증-2026-11-23), [D1 검증](#d1-적용-steady-검증-2026-11-23) |
| 실제 중복 원인을 막지 못한다 | 중복은 주로 마이크로배치·JDBC 재실행에서 생긴다. 재실행되면 같은 배치가 같은 행을 다시 쓰므로, 배치 안 중복 제거로는 막을 수 없다. 배치 간 중복도 막지 못한다 | [입력 중복 정책](#입력-중복과-jdbc-적재-중복의-구분) |
| 최종 방어선이 따로 있다 | `ReplacingMergeTree()` / `ORDER BY (event_timestamp, event_id)`와 `FINAL` 조회 뷰 `gold.orders_deduplicated`가 원래부터 정확성을 보장한다. 주문 현황 대시보드는 이 뷰를 읽는다 | [PRD 04 조회 시 중복 제거](04-clickhouse-storage.md#조회-시-중복-제거) |
| 충돌을 숨긴다 | 같은 ID에 `event_timestamp`가 다른 행이 한 배치에 들어오면 `dropDuplicates`가 하나를 임의로 남긴다. 제거하면 두 행이 모두 저장되어 "ID 충돌" 알림이 이를 드러낸다 | `infra/grafana/provisioning/alerting/rules.yaml` |

결과: 평시 피크 트리거 p99가 3,659ms에서 3,246ms로 내려가 목표를 충족했다([D1 검증](#d1-적용-steady-검증-2026-11-23)).

#### 검토한 대체안과 채택하지 않은 이유

| 대체안 | 내용 | 채택하지 않은 이유 |
| --- | --- | --- |
| A. 현행 유지 | 배치 내 `dropDuplicates` | 비용은 크고, 같은 5초 배치 안의 빠른 재전송만 걸러 낸다. v1 입력 계약에는 그런 재전송이 없다 |
| B. 워터마크 기반 상태 중복 제거 | `foreachBatch` 앞 스트리밍 DataFrame에 `withWatermark` + `dropDuplicatesWithinWatermark` | (1) 셔플이 다시 생기고, 배치당 키 약 37,000개의 상태 조회·기록 비용이 더해진다. p99 여유 254ms를 넘을 가능성이 높다(미측정). (2) 워터마크 10분이면 상태 키가 약 255만 개라 RocksDB state store와 off-heap 메모리 예산이 필요하다. (3) 배치 재실행 중복은 상태도 함께 복원되어 막지 못하므로, ClickHouse 정책은 계속 필요하다. (4) 상태가 생기면 셔플 파티션 수가 checkpoint에 고정된다. 파싱할 수 없는 행의 null 키 처리와 DLQ 레코드 중복 제거 정책도 새로 정해야 한다. 초기 설계에서도 같은 이유로 제외했다([PRD 04 조합 C](04-clickhouse-storage.md#중복-제거조회-조합-비교)) |
| C. 셔플 없는 파티션 내 중복 제거 | 같은 `event_id`가 같은 Kafka 파티션에 들어온다는 전제로, 파티션 안에서만 중복을 제거한다(S6b) | 생성기의 메시지 key 규칙을 확인하지 않았다. Spark DataFrame에는 셔플 없이 파티션 안에서만 중복을 제거하는 API가 없어, Python `mapPartitions` 같은 별도 구현이 필요하다. 그래도 막는 범위는 A와 같아 얻는 것이 없다 |
| D. ClickHouse INSERT 블록 중복 제거 | 같은 INSERT 블록의 재전송을 서버가 무시하게 한다(블록 중복 제거 설정·토큰) | 같은 블록의 재시도만 막는다. 재실행 때는 파티션 분할이 달라져 블록이 달라질 수 있다. 행 단위 정확성은 이미 `ReplacingMergeTree`·`FINAL`이 보장한다. 해당 설정의 비복제 테이블 동작은 공식 문서로 확인하지 않았다 |
| E. 지연 뷰에도 `FINAL` 적용 | `FINAL` 없이 silver를 읽는 `gold.order_event_latency`를 `FINAL`로 바꾼다 | 지연 패널에 섞이는 중복은 병합 전 일부 행뿐이고, 입력 중복이 0이라 실제 영향이 없다. `FINAL`은 같은 행을 읽을 때 약 3.8배 느리다([PRD 04 지속 부하 측정 결과](04-clickhouse-storage.md#지속-부하-측정-결과-2026-11-18)). JDBC 재시도 중복에 대해서는 D1 이전부터 같은 성질이었다 |

#### 받아들인 대가

- 배치 안 중복 입력이 있으면 silver에 물리 행으로 남는다. 백그라운드 병합 전까지 그렇고, 병합 시점은 보장되지 않는다. 업무 집계는 `FINAL` 뷰를 쓰므로 바뀌지 않는다.
- `FINAL` 없이 silver를 읽는 지연 지표에는 병합 전 중복이 섞일 수 있다.
- 쓰기 파티션은 Kafka 파티션 분배를 그대로 따른다. 이전에는 셔플이 파티션을 다시 나눴다.

#### 다시 검토할 조건

- 생성기가 아닌 실제 클라이언트 입력원이나 멱등이 아닌 producer가 추가될 때. 먼저 입력 중복률(`count()`와 `uniqExact(event_id)`의 차이)을 측정하고, B를 다시 검토한다.
- 이 결정은 중복이 들어오는 상황에서 운영 경로로 검증하지 않았다. 생성기에 중복 주입 기능이 없기 때문이다. 배치 테스트(`test_건수_로그와_출력`)에서는 같은 `event_id` 행이 그대로 쓰이는 것만 확인했다. 중복을 넣었을 때 `FINAL` 뷰의 집계가 정확한지를 실제 부하로 확인하려면 생성기에 중복 주입 기능이 필요하다.
- `FINAL` 없이 silver를 집계하는 하류가 생길 때.
- 입력 중복률을 상시 감시하는 패널이나 알림은 아직 없다. 지금은 측정 실행 때만 확인한다.

## 완료 기준

해당 단계에 착수할 때 지연 목표값·측정 구간·샘플링 설정을 포함한 정량 기준을 확정한다. Spark 지연 로그의 구현·검증 범위는 위 결과를 따르며, ClickHouse 관련 계측과 Kafka 실환경의 일부 검증은 남아 있다.

불량 3종의 DLQ 격리·메시지 및 오류 사유 조회·원문 bytes 복원·원본 위치 추적, health_check 제외, DLQ 쓰기 실패 시 배치 재시도와 중복 판별, 체크포인트 재개 및 offset 소실 시 실패를 검증한다. 배치 진행 정보와 offset 범위의 연결, 실제 action 기준 시간, 쓰기 시간과 전체 시간의 중복 방지, 이벤트 기준 집계를 검증한다. 소비 지연과 처리 지연을 각각 주입해 지표를 확인한다.
