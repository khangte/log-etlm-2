# Spark 스트림 처리

- 상태: 구현 중 (기본 처리·지연 계측 검증, 5,000 EPS 지속 부하 기준 미달·장애 검증 일부 남음)
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
5. **의미 검증:** 정상 주문 후보에는 데이터 계약의 금액 계산, 할인 상한, 상품 풀 일관성과 지역·도시 조합 규칙을 적용한다. 풀 기준과 배포 방식은 [3단계 구현 결정](#3단계-구현-결정)을 따른다.
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
| 금액·상품·지역 의미 규칙 위반 | 불량 / 의미 검증 | 위반 규칙과 사유를 보존 |
| timestamp·정수 변환 실패 또는 범위 초과 | 불량 / 타입 변환 | 원래 값과 실패 경로를 원문에서 추적 가능하게 보존 |

실패 단계·필드 경로·사유를 식별할 수 있게 한다. DLQ 오류 코드와 메시지 스키마는 [3단계 구현 결정](#3단계-구현-결정)을 따른다. JDBC 연결 실패 등 인프라 장애는 이벤트 불량으로 분류하지 않는다.

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
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | `50000` | 배치당 입력 상한은 평상시 예상 25,000건의 2배다. 5초 내 처리 시에만 최대 10,000 EPS에 해당하며 처리속도를 보장하지 않는다. 아래 지속 부하 실측에서는 약 50,000건 처리에 평균 16.05초가 걸렸다. |
| `SPARK_SHUFFLE_PARTITIONS` | `3` | `local[3]` 태스크 슬롯 수와 같게 두어 셔플 후 태스크가 한 번에 모두 실행된다. 슬롯보다 많으면 태스크가 여러 차례로 나뉘어 실행될 뿐 병렬도는 늘지 않고, 배치당 Parquet 파일 수만 늘어난다. 4단계 `SPARK_JDBC_NUM_PARTITIONS`와 같은 값이다. 체크포인트에 기록되므로 변경하려면 체크포인트를 새로 만든다. |
| `SPARK_JDBC_BATCH_SIZE` (4단계 추가) | `50000` | `maxOffsetsPerTrigger`와 같게 두어 파티션 편중과 무관하게 쓰기 파티션당 INSERT 1회로 만든다. |
| `SPARK_JDBC_NUM_PARTITIONS` (4단계 추가) | `3` | ClickHouse 동시 연결 3개. 배치당 part 3개, 약 0.6 part/s다. |
| `SPARK_CHECKPOINT_DIR` | `/opt/spark/checkpoints/orders` | 볼륨에 마운트한다. Git에서 제외한다. |
| `SPARK_OUTPUT_DIR` | `/opt/spark/output/orders` | 3단계 정상 주문 검증용 Parquet 경로. 체크포인트와 다른 볼륨이다. 4단계에서 JDBC writer로 교체하며 제거한다. |
| `SPARK_STARTING_OFFSETS` | `earliest` | [Kafka PRD](02-kafka-ingestion.md)의 최초 실행 기준. 재시작은 체크포인트 위치가 우선한다. |
| `SPARK_KAFKA_BOOTSTRAP_SERVERS` | `kafka:19092` | Compose 내부 주소. |

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
- 계약 검증은 `contracts/schemas`의 파일을 `Draft202012Validator`와 형식 검사기로 실행한다. `integer`는 원래 JSON 정수만 허용하도록 재정의해 `27000.0`처럼 소수점 표기 숫자도 거부한다(JSON Schema 기본은 허용). `pattern`도 재정의한다. Python `re.search`의 `$`는 끝 개행 앞에서도 일치하므로 검증기가 `$` 앵커를 `\Z`로 바꿔 ECMA-262와 같이 전체 일치시키고 끝 개행을 `SCHEMA_PATTERN`으로 거부한다. 계약 스키마 파일은 바꾸지 않는다. 오류가 여러 개면 `best_match` 1건을 기록한다.
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
| conversion | `CONVERSION_TIMESTAMP` | 형식 검사를 통과했으나 시각으로 변환 불가. 방어용이며 잘못된 날짜·윤초 `60`·연도 `0000`은 `SCHEMA_FORMAT`이 먼저 거부한다. |
| conversion | `CONVERSION_LONG_RANGE` | 금액·가격·수량이 `LongType` 범위 초과 |

표준 불량 3종의 기대값은 `parse`/`INVALID_JSON`, `schema`/`SCHEMA_REQUIRED`/`properties.order_id`, `schema`/`SCHEMA_TYPE`/`properties.total_amount`다.

Kafka timestamp 종류가 LogAppendTime(`timestampType=1`)이 아니면 이벤트 불량이 아닌 설정 오류로 보고 `raise_error`로 쿼리를 실패시킨다.

### 3단계 정상 출력 검증 경로

- ClickHouse는 4단계이므로 3단계는 정상 주문(31개 이벤트 컬럼 + Kafka 추적 4개, 배치 내 `dropDuplicates(["event_id"])` 적용 후)을 `SPARK_OUTPUT_DIR`의 Parquet에 append한다. 경로는 체크포인트(`spark-checkpoints`)와 다른 named volume `spark-output`이며 Git 밖에 있다.
- 쓰기 보장은 최소 한 번이다. 배치 재시도 시 같은 `event_id` 행이 중복될 수 있으므로 검증은 `event_id` 고유 건수로 한다.
- 4단계 경계: `streaming.sinks.write_orders` 한 함수만 Spark JDBC writer로 교체하고 `SPARK_OUTPUT_DIR`·`spark-output` 볼륨을 제거한다. 분류·평탄화·DLQ·계측 코드는 바꾸지 않는다.

### 진행 정보와 건수 계측

- `StreamingQueryListener`가 쿼리 시작·종료(id·runId·예외)와 매 진행 정보(`StreamingQueryProgress.json`: 배치 ID, 입력 건수, 파티션별 offset 범위, 처리속도, `durationMs`)를 한 줄 JSON으로 로그에 남긴다.
- `foreachBatch`가 배치마다 `batch_id`·입력·정상 주문·제외·DLQ 건수를 로그에 남긴다. 입력 건수 = 정상 + 제외 + DLQ이다. 지연 계측의 세부 항목과 구현 경계는 [처리지연 측정](#처리지연-측정)을 따른다.

### 파일 구조와 실행·테스트

원문 파싱·검증·분류와 DataFrame 변환을 `transforms.py` 하나에 모은다. `validation.py`·`classify.py`는 별도로 두지 않는다. `classify()`는 원문 검증과 정상·제외·불량 분류를 수행하고, `classify_batch()`는 이를 UDF로 DataFrame에 적용한다. `select_orders()`는 정상 필터·구조화·평탄화·배치 내 중복 제거, `build_dlq_records()`는 불량 필터·DLQ 메시지 구성을 담당한다. 중첩 필드 전개에는 `select()`를 사용한다. 처리 순서·출력·계측 계약은 유지한다.

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

## 완료 기준

해당 단계에 착수할 때 지연 목표값·측정 구간·샘플링 설정을 포함한 정량 기준을 확정한다. Spark 지연 로그의 구현·검증 범위는 위 결과를 따르며, ClickHouse 관련 계측과 Kafka 실환경의 일부 검증은 남아 있다.

불량 3종의 DLQ 격리·메시지 및 오류 사유 조회·원문 bytes 복원·원본 위치 추적, health_check 제외, DLQ 쓰기 실패 시 배치 재시도와 중복 판별, 체크포인트 재개 및 offset 소실 시 실패를 검증한다. 배치 진행 정보와 offset 범위의 연결, 실제 action 기준 시간, 쓰기 시간과 전체 시간의 중복 방지, 이벤트 기준 집계를 검증한다. 소비 지연과 처리 지연을 각각 주입해 지표를 확인한다.
