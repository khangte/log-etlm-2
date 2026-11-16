# Spark 스트림 처리

- 상태: 초안
- 적용 범위: 제품 v1

## 목표

Kafka에서 로그를 읽어 주문 로그를 검증하고 변환한다.

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

아래는 확정한 기능 요구사항이며 아직 구현·실행 검증하지 않았다. 원본 이벤트 스키마와 버전은 변경하지 않는다.

1. **원본 확보:** Kafka `value`를 `raw_value`(BinaryType)로 보존하고 topic·partition·offset·timestamp를 확보한다. 파싱 실패 시에도 원문 bytes와 원본 위치를 추적할 수 있어야 한다. Kafka null value는 정상 이벤트로 간주하지 않으며 null과 빈 bytes를 구분해 격리한다.
2. **JSON 파싱:** UTF-8 JSON 객체 1개를 입력으로 받는다. 잘못된 UTF-8, 빈 값, JSON 문법 오류는 파싱 실패로 분류하고, 객체가 아닌 유효 JSON은 스키마 위반으로 분류한다. 파서의 관대한 확장 문법으로 잘못된 JSON을 정상 수용하지 않는다.
3. **계약 선택:** 지원 조합은 `order_completed` / `1.0.0`, `health_check` / `1.0.0`이다. 분류 필드 누락·타입 오류와 미지원 이벤트·버전은 불량으로 격리한다.
4. **계약 검증:** 해당 JSON Schema의 필수값·원래 JSON 타입·null 금지·허용 필드·형식·enum·범위·배열 길이를 검증한다. 중첩 객체에도 `additionalProperties=false`를 적용한다. `event_name=health_check`라는 이유만으로 검증 없이 제외하지 않는다.
5. **의미 검증:** 정상 주문 후보에는 데이터 계약의 금액 계산, 할인 상한, 상품 풀 일관성과 지역·도시 조합 규칙을 적용한다. 검증에 사용하는 풀 기준과 배포 방식은 구현 전에 확정한다.
6. **변환·평탄화:** 검증을 통과한 주문만 아래 명시적 Spark 타입으로 변환한다. 변환 불가·표현 범위 초과는 변환 실패로 격리하고, 기본값 대입·절삭·반올림·대소문자 보정으로 오류를 숨기지 않는다.
7. **출력 분류:** 정상 주문, 정상 health_check 제외, 불량 격리 중 하나로만 분류한다. 분류 자체는 입력 이벤트마다 한 번이며 출력 재시도의 중복 문제와 구분한다.

Spark 파싱 스키마는 명시적 `StructType`으로 정의하며 실행 데이터의 스키마 추론에 의존하지 않는다. `from_json` 등 타입 파싱 결과만으로 JSON Schema 검증이 완료됐다고 가정하지 않는다. 문자열·숫자·불리언의 자동 변환과 누락 필드의 null 처리로 원래 타입 오류를 놓치지 않도록 원본 기준 검증을 함께 수행한다. 엄격한 검증을 구현할 구체 API는 Spark 버전·성능 검증과 함께 확정한다.

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

실패 단계·필드 경로·사유를 식별할 수 있게 하며 구체적인 DLQ 오류 코드와 메시지 스키마는 별도 설계한다. JDBC 연결 실패 등 인프라 장애는 이벤트 불량으로 분류하지 않는다.

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

Spark는 단일 컨테이너의 local 모드로 실행한다. 아래 값은 PRD 부하 기준의 계산값이며 실측 결과가 아니다. 3단계 구현 시 `.env.example`과 `compose.yaml`에 같은 이름으로 추가한다.

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
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | `50000` | 평상시 배치의 2배. 밀린 backlog를 10,000 EPS로 읽어 유입 5,000 EPS를 빼고 순 5,000 EPS로 해소한다. |
| `SPARK_SHUFFLE_PARTITIONS` | `3` | `local[3]` 태스크 슬롯 수와 같게 두어 셔플 후 태스크가 한 번에 모두 실행된다. 슬롯보다 많으면 태스크가 여러 차례로 나뉘어 실행될 뿐 병렬도는 늘지 않고, 배치당 Parquet 파일 수만 늘어난다. 4단계 `SPARK_JDBC_NUM_PARTITIONS`와 같은 값이다. 체크포인트에 기록되므로 변경하려면 체크포인트를 새로 만든다. |
| `SPARK_JDBC_BATCH_SIZE` | `50000` | `maxOffsetsPerTrigger`와 같게 두어 파티션 편중과 무관하게 쓰기 파티션당 INSERT 1회로 만든다. |
| `SPARK_JDBC_NUM_PARTITIONS` | `3` | ClickHouse 동시 연결 3개. 배치당 part 3개, 약 0.6 part/s다. |
| `SPARK_CHECKPOINT_DIR` | `/opt/spark/checkpoints/orders` | 볼륨에 마운트한다. Git에서 제외한다. |
| `SPARK_STARTING_OFFSETS` | `earliest` | [Kafka PRD](02-kafka-ingestion.md)의 최초 실행 기준. 재시작은 체크포인트 위치가 우선한다. |
| `SPARK_KAFKA_BOOTSTRAP_SERVERS` | `kafka:19092` | Compose 내부 주소. |

- 토픽 이름은 기존 `KAFKA_TOPIC`·`KAFKA_DLQ_TOPIC`을 재사용한다. ClickHouse JDBC URL·계정은 4단계에서 추가한다.
- Spark·Java·PySpark·Kafka 커넥터·JDBC Driver 버전은 호환성을 확인한 뒤 구현 시 확정한다.

### 값 선택 이유

- **`maxOffsetsPerTrigger`는 필수다.** 없으면 `earliest` 첫 배치가 보관분 전체를 읽는다. 1시간 보관분만 약 1,800만 건·14.3GB로 heap 2g에서 처리할 수 없다.
- **JDBC 배치 크기 기본값 1,000을 쓰지 않는다.** 쓰기 파티션당 약 7,083행이 INSERT 8회로 나뉘어 초당 약 4.8개 part가 생긴다. 50,000이면 약 0.6 part/s이고 INSERT당 7,000행 이상이 되어 ClickHouse 병합 부담이 줄어든다.
- **트리거를 5초보다 줄이지 않는다.** 화면 반영 지연은 60초 갱신 대기가 지배하므로 트리거 단축 이득이 작고 배치·part 수만 늘어난다.

### 확인 필요

- 메모리 10배 전개율은 가정이다. Python UDF를 쓰면 Python worker 메모리가 heap 밖에서 늘어나므로 `mem_limit`을 다시 정한다.
- CPU는 Kafka 2 + UI 1 + Spark 3 + 생성기 1에 ClickHouse가 더해지면 8개를 넘어 경합한다. 5,000 EPS 유지 여부를 실측한다.
- `foreachBatch` 안에서 AQE가 셔플 파티션을 합쳐 쓰기 파티션이 3보다 줄어드는지 확인한다.
- 배치 처리시간이 트리거 주기 5초를 넘지 않는지, backlog가 해소되는지 실측한다.

## 입력·DLQ 및 장애 처리

### 입력 중복과 JDBC 적재 중복의 구분

- `foreachBatch` 안의 `batch_df.dropDuplicates(["event_id"])`는 해당 배치 안의 중복만 제거한다. 콜백 이전의 스트리밍 DataFrame에서 상태를 유지하는 중복 제거를 적용하면 배치 간 입력 중복도 처리할 수 있으나 v1에서는 도입하지 않는다. Producer 멱등 전송과 주문별 새 ID 발급으로 입력 중복이 사실상 없고, 마이크로배치 재실행 중복은 상태 중복 제거로 막지 못하기 때문이다. 비교 근거는 [중복 제거·조회 조합 비교](04-clickhouse-storage.md#중복-제거조회-조합-비교)를 따른다.
- JDBC 일부 적재 후 재시도는 입력 중복 제거만으로 방지하지 못한다. [ClickHouse 적재 중복 정책](04-clickhouse-storage.md#jdbc-재시도와-적재-중복-처리)에 따라 `ReplacingMergeTree()` / `ORDER BY (event_timestamp, event_id)`로 저장하고 `FINAL` 기반 조회 뷰에서 논리 중복을 제거한다.
- 동일 ID 재전송은 동일 업무 내용이라는 전제를 따른다. 원본 ID와 `event_timestamp`를 유지하고 Kafka 위치·Spark 배치 식별자를 중복 키로 추가하지 않는다. 같은 ID에 다른 `event_timestamp`가 들어오면 저장 단계에서 합쳐지지 않으며 ClickHouse 충돌 점검으로 드러낸다. 그 밖의 다른 내용 충돌 감지는 별도 설계한다.
- 이 정책은 정상 주문 조회의 중복 합산을 막는 기준이다. 물리 INSERT 한 번 보장이나 정상 출력·DLQ의 원자성을 의미하지 않는다. DLQ 상세 스키마·오류 코드는 이번 단계 구현 전에 확정하며, DLQ 데이터 재처리 정책과 기능은 후속 작업으로 보류한다.

[Kafka 연동](02-kafka-ingestion.md)의 원본·DLQ 계약을 따른다. 아래는 3단계 구현 요구사항이며 아직 구현하지 않았다.

- 원본 `raw.ecommerce.orders.v1`의 3개 파티션을 읽고, 파싱·주문 검증 실패는 별도 토픽 `dlq.ecommerce.orders.v1`의 1개 파티션으로 보낸다. 원본과 DLQ 파티션을 일대일 대응시키지 않는다.
- 정상 health_check는 제외 건수로 집계하고 DLQ에 보내지 않는다. 정상 주문은 ClickHouse 적재 경로로 전달한다.
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
| Kafka → 배치 시작 지연 | `batch_started_at - kafka_append_timestamp` | 해당 배치가 읽은 이벤트 기준 소비 대기 근사치이며 실제 수신 지연과 구분 |

- 시작 시각보다 늦게 Kafka에 추가된 이벤트가 같은 배치에 포함되는 경우 해당 차이를 대기시간으로 사용하지 않는다. 범위 불일치 건수로 구분하며 0으로 보정하지 않는다. 정확한 수신 지연은 실제 읽기 경계에 별도 계측이 있을 때만 제공한다.
- 진행 정보는 `StreamingQueryListener` 또는 `lastProgress` 등으로 수집한다. 배치 ID, 입력 건수, topic·partition별 offset 범위, 처리속도와 실패·재시도를 함께 기록한다. 재시작 시 배치 ID가 충돌하지 않도록 쿼리·실행 식별자도 남긴다.
- 변환 코드 선언 전후의 시간 차이를 실제 처리시간으로 사용하지 않는다. Spark는 지연 실행하므로 실제 action과 진행 정보에서 측정한다.
- JDBC write action 호출부터 반환까지는 선행 변환·스케줄링 등이 포함될 수 있는 출력 action 소요시간으로 기록한다. 개별 INSERT 요청 시간이나 순수 정제 시간으로 표시하지 않는다. 요청별 JDBC 계측 방식은 구현 전에 확정한다.
- `current_timestamp()`를 변환 전후에 추가해 처리시간을 계산하지 않는다. 같은 쿼리에서 같은 값을 반환하므로 연산별 완료 시각이 아니다.
- `event_timestamp`, Kafka 추가 시각·topic·partition·offset을 후속 저장 컬럼으로 보존한다. items 분리 이후에도 지연 분포는 원본 이벤트 기준으로 집계해 상품 개수에 따른 중복 가중을 피한다.
- health_check 제외·정상 주문·불량 격리 건수를 구분한다. 파싱 실패는 Kafka 메타데이터로 추적하고 생성 기준 지연 계산 불가 건수를 기록한다.

참고: [Spark 진행 정보](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#monitoring-streaming-queries), [current_timestamp 의미](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_timestamp.html).

## 완료 기준

해당 단계에 착수할 때 지연 목표값·측정 구간·샘플링 설정을 포함한 정량 기준을 확정한다. 아래 계측은 요구사항이며 아직 구현·검증하지 않았다.

불량 3종의 DLQ 격리·메시지 및 오류 사유 조회·원문 bytes 복원·원본 위치 추적, health_check 제외, DLQ 쓰기 실패 시 배치 재시도와 중복 판별, 체크포인트 재개 및 offset 소실 시 실패를 검증한다. 배치 진행 정보와 offset 범위의 연결, 실제 action 기준 시간, 쓰기 시간과 전체 시간의 중복 방지, 이벤트 기준 집계를 검증한다. 소비 지연과 처리 지연을 각각 주입해 지표를 확인한다.
