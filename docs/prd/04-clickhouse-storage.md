# ClickHouse 적재

- 상태: 초안
- 적용 범위: 제품 v1

## 목표

변환된 주문 데이터를 ClickHouse에 적재하고 조회한다.

## 범위 및 후속 정의 사항

정상 주문의 입력 매핑은 [Spark 평탄화 매핑](03-stream-processing.md#정상-주문-평탄화-매핑)을 따른다. 현재 정의는 주문 이벤트 1건당 1행, 이벤트 컬럼 31개와 Kafka 추적 컬럼 4개다. ClickHouse 타입·추가 컬럼은 [컬럼 타입](#order_events-컬럼-타입), 저장 계층은 [저장 계층](#저장-계층), 운영 지표 저장 구조는 [운영 지표 저장](#운영-지표-저장)을 따른다.

자원 예산: [개발환경](00-development-environment.md)의 WSL 메모리 12GB·CPU 8개를 공유한다. 파이프라인 운영·측정 시 Kafka UI는 실행하지 않는다. 이때 Compose 배정은 Kafka CPU 2·2GiB, Spark CPU 3·3GiB이며, 남은 CPU 3개·메모리 약 7GiB를 ClickHouse·Grafana·호스트의 로그 생성기와 WSL 자체가 나눠 쓴다. ClickHouse의 CPU·메모리 제한은 [자원 제한](#자원-제한)을 따르며 지속 적재 중 병합과 `FINAL` 조회의 CPU 경합을 실측한다. Kafka UI는 Compose 프로필 `tools`로 분리해 기본 실행에서 제외한다([Kafka PRD](02-kafka-ingestion.md#kafka-ui)).

적재는 [Spark PRD의 역할 구분](03-stream-processing.md#스택별-역할과-적재-방식)에 따라 `foreachBatch` → Spark JDBC writer → ClickHouse 공식 JDBC Driver → ClickHouse 순서로 수행한다. 정상 주문 DataFrame은 append 방식으로 기록한다.

Spark 변환 구현 전에 조회 요구사항을 바탕으로 한 행의 의미, 컬럼·타입·필수값·시간대·금액 단위와 원본 추적 컬럼을 정의한다. 부분 적재·재시도에 따른 중복 처리 정책과 필요한 테이블 엔진·정렬 키 초안도 함께 정한다. 확정한 계약은 이 문서와 Spark PRD에 함께 반영한다.

4단계에서는 실제 테이블과 JDBC 연동을 구성하고 적재 실패·중복 처리, 집계 정확성과 스택 단계별 지연 지표 기록을 검증한다. 샘플 이벤트의 조회 가능 지연(생성 → 최초 조회 확인) 측정은 v1 범위에서 제외한다([조회 가능 지연 제외](#조회-가능-지연-제외)). 엔진·중복 키·조회 경로·컬럼 타입·보관 정책은 아래 정책을, 버전·DDL 관리·JDBC 쓰기 설정·계정·자원 제한은 [실행 구성](#실행-구성)을 따른다.

마이크로배치 전체나 정상 적재·DLQ 기록 사이의 원자성을 가정하지 않는다. `batch_id` 기록만으로 중복 제거가 보장되지는 않으며, 중복 처리 정책은 재시작과 일부 JDBC 쓰기 성공 후 실패하는 경우를 포함해야 한다.

참고: [ClickHouse Spark JDBC 연동](https://clickhouse.com/docs/integrations/apache-spark/spark-jdbc), [Spark foreachBatch 쓰기 보장](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#using-foreach-and-foreachbatch).

## 저장 계층

메달리안 구조(Bronze·Silver·Gold)를 ClickHouse 데이터베이스 이름으로 드러낸다. 아래는 설계 결정이며 아직 구현하지 않았다.

| 층 | 위치 | 내용 |
| --- | --- | --- |
| Bronze (원본) | Kafka 원본 토픽 `raw.ecommerce.orders.v1` | Producer가 보낸 원본 bytes. 보관 1시간 |
| Silver (정제) | `silver.order_events` | Spark 검증·평탄화를 통과한 정상 주문. 이벤트당 1행(재시도 중복 가능) |
| Gold (조회) | `gold.orders_deduplicated`, `gold.order_event_latency`와 대시보드 집계 뷰 | `FINAL`로 중복을 제거한 주문 조회 경로와 이벤트 단위 지연 조회 경로. 테이블이 아닌 `SQL SECURITY DEFINER` 뷰 |
| 운영 | `ops.*` | 파이프라인 운영 지표. 메달리안 층에 속하지 않음 |

- 이 문서와 Grafana PRD의 `order_events`는 `silver.order_events`, `orders_deduplicated`는 `gold.orders_deduplicated`를 가리킨다.
- 선택 근거: 층 구분을 구조적으로 보여주는 것이 목적이다. 데이터베이스 분리는 추가 저장·CPU 비용 없이 이름만으로 층을 드러낸다. 계정 권한도 데이터베이스 단위로 나눌 수 있다(Grafana 읽기 계정은 `gold`·`ops`만). 이를 위해 Gold 뷰는 정의자 권한으로 실행한다. 상세는 [계정과 권한](#계정과-권한)을 따른다.
- 비교한 대안:
  - 문서에만 표기: 변경이 없지만 실제 스키마에서 층이 보이지 않는다.
  - ClickHouse Bronze 테이블 추가: 원본을 24시간 보관해 재처리 범위가 늘어난다. 하지만 원본 행당 약 900바이트·하루 4억 행 이상으로 쓰기량이 약 2배가 된다. 남은 CPU 3개 예산에서 병합 부하 위험이 크므로 도입하지 않는다.
  - Gold 집계 테이블(1분 롤업 refreshable MV): [중복 제거·조회 조합 비교](#중복-제거조회-조합-비교)의 조합 D와 같다. 화면 반영 지연이 최대 60초 늘어나므로 직접 조회가 시간 예산을 넘을 때 도입한다.
  - ClickHouse DLQ 테이블: 대시보드에서 DLQ 사유별 건수·샘플 원문을 볼 수 있다. 하지만 Spark 쓰기 대상과 실패 경로가 하나 늘어난다. v1은 `ops.spark_batch_attempts`의 배치별 DLQ 건수만 표시하고, 사유별 분석이 필요해지면 도입한다. 원문은 Kafka DLQ 토픽(보관 6시간)에서 확인한다.
- 이벤트를 저장하는 물리 테이블은 `silver.order_events` 하나다. 상품 분리 테이블은 주문당 상품이 1종이므로, 차원 테이블은 차원 값이 이벤트에 들어 있으므로 두지 않는다.
- 제약: Bronze 보관이 1시간이므로 그보다 오래된 원본은 재처리할 수 없다. Silver 보관은 24시간이다.

## order_events 컬럼 타입

아래는 설계 결정이며 아직 구현하지 않았다. Spark 평탄화 매핑의 31개 이벤트 컬럼과 Kafka 추적 컬럼 4개에 Spark 식별·처리 시각 컬럼과 서버 적재 시각을 더한다.

| 컬럼 | ClickHouse 타입 |
| --- | --- |
| `event_timestamp`, `kafka_append_timestamp`, `spark_processed_at` | `DateTime64(3, 'UTC')` |
| `ingested_at` | `DateTime64(3, 'UTC') DEFAULT now64(3)` |
| `total_amount`, `discount_amount`, `price`, `quantity` | `Int64` |
| `event_id`, `order_id`, `user_id`, `session_id`, `current_url`, `referrer_url` | `String` |
| `event_name`, `schema_version`, `user_tier`, `os`, `os_version`, `app_version`, `device_model`, `device_type`, `carrier`, `country`, `region`, `city`, `utm_source`, `utm_medium`, `payment_method`, `currency`, `product_id`, `product_name`, `category`, `kafka_topic`, `spark_query_id`, `spark_run_id` | `LowCardinality(String)` |
| `is_logged_in` | `Bool` |
| `kafka_partition` | `Int32` |
| `kafka_offset`, `spark_batch_id` | `Int64` |

- 시각은 UTC로 저장하고 한국 시간 변환은 Grafana 표시에서만 한다. 밀리초 정밀도를 유지한다.
- `spark_processed_at`은 `foreachBatch` 콜백 진입 시각이다. 같은 배치의 운영 지표 행과 같은 값이며 Kafka → Spark 지연 계산에 쓴다. `ingested_at`은 서버 기본값이며 INSERT 완료·조회 가능 시각으로 간주하지 않는다.
- 모든 컬럼은 Nullable이 아니다. 입력 계약이 null을 허용하지 않고 Spark가 필수값을 검증한다.
- 선택 근거:
  - 금액·수량 `Int64`: Spark `LongType`과 같아 JDBC 변환이 없다. 음수는 계약 검증에서 막는다. `UInt64`는 JDBC의 부호 있는 long과 맞지 않는다.
  - 코드·차원 `LowCardinality(String)`: 값 종류가 적어 저장량이 줄고 차원별 GROUP BY가 빨라진다. 실행 중 고정인 Spark 쿼리·실행 ID도 같다.
  - ID·URL `String`: 원본 형식을 보존한다. `UUID` 타입은 표기 형식을 바꿀 수 있다.
  - Nullable 회피: Nullable은 별도 null 마스크로 저장·조회 비용이 늘어난다.
- 비교한 대안:
  - 고정 코드(`user_tier`, `payment_method`, `category`, `currency`)의 `Enum8`: 더 엄격하지만, 계약에 새 값이 추가되면 DDL 변경 전까지 INSERT가 실패해 주문 적재가 멈춘다. 값 검증은 Spark 계약 검증이 이미 담당한다.
  - 모든 문자열 `String`: 가장 단순하지만 저장량이 늘고 차원별 집계가 느려진다.

### 압축 코덱

아래는 설계 결정이며 아직 구현·실측하지 않았다. 모든 컬럼에 코덱을 명시한다.

| 컬럼 | 코덱 |
| --- | --- |
| 시각 컬럼 4개(`event_timestamp`, `kafka_append_timestamp`, `spark_processed_at`, `ingested_at`) | `CODEC(Delta, ZSTD(1))` |
| `Int64`·`Int32` 컬럼(금액·수량·`kafka_partition`·`kafka_offset`·`spark_batch_id`) | `CODEC(T64, ZSTD(1))` |
| `current_url`, `referrer_url` | `CODEC(ZSTD(3))` |
| 그 외 `String`·`LowCardinality(String)`·`Bool` | `CODEC(ZSTD(1))` |

- 선택 근거: 로컬 WSL 환경의 디스크 용량이 적기 때문이다. 기본 코덱 LZ4(자체 설치 기준) 상태의 추정 용량은 하루 11~22GB이며 이를 줄이는 것이 목적이다. 코덱 변경은 파티션·보관 기간과 달리 보관 정책과 조회 결과에 영향이 없다.
  - `Delta`: 이웃 값의 차이만 저장한다. 시각은 part 안에서 `event_timestamp` 순으로 정렬돼 차이가 작다. 단독으로는 압축하지 않는 전처리 코덱이므로 ZSTD를 뒤에 붙인다.
  - `T64`: 정수에서 쓰지 않는 상위 비트를 잘라낸다. 금액·offset은 `Int64` 범위보다 훨씬 작다.
  - ZSTD 레벨: 기본 레벨 1로 시작한다. URL은 길고 반복이 많아 레벨 3을 쓴다. 레벨을 올리면 INSERT·병합 CPU가 늘어나므로 남은 CPU 3개 예산 안에서만 올린다.
  - 참고: [Column compression codecs](https://clickhouse.com/docs/reference/statements/create/table/codec), [CREATE TABLE의 기본 압축](https://clickhouse.com/docs/sql-reference/statements/create/table)
- 비교한 대안:
  - 기본 LZ4 유지: CPU가 가장 적지만 디스크 사용량이 가장 크다.
  - `DoubleDelta`: 간격이 일정한 시계열에 적합하다. 주문 이벤트 시각은 간격이 일정하지 않으므로 `Delta`를 쓴다.
  - 파티션을 잘게 나눠 초과 보관 줄이기: 1시간 파티션의 초과 보관은 최대 1시간분(약 4%)이다. 파티션을 더 나누면 part가 늘고 압축률이 떨어져 이득이 거의 없다.
- 검증: 4단계에서 같은 데이터로 `system.columns`의 `data_compressed_bytes`·`data_uncompressed_bytes`를 LZ4와 비교한다. 압축률이 낮거나 병합 CPU가 예산을 넘는 컬럼은 코덱을 조정하고 결과를 이 절에 기록한다.

## 실행 구성

아래는 2026-11-17 조사 기준의 설계 결정이며 아직 구현·실행 검증하지 않았다.

### 버전

| 구성 요소 | 버전 |
| --- | --- |
| ClickHouse 서버 이미지 | `clickhouse/clickhouse-server:26.8.38.9` |
| ClickHouse JDBC Driver | `com.clickhouse:clickhouse-jdbc:0.11.2`, classifier `all` |
| JDBC 드라이버 클래스·URL | `com.clickhouse.jdbc.ClickHouseDriver`, `jdbc:clickhouse:http://clickhouse:8123` |

- 선택 근거:
  - ClickHouse 26.8: 저장소 태그가 `v26.8.38.9-lts`인 LTS 계열이다. [보안 지원 표](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md)의 지원 대상(26.10·26.9·26.8·26.3) 중 가장 최근 LTS다. 월간 릴리스보다 지원 기간이 길어 v1 동안 버전 교체 부담이 작다. 다른 이미지처럼 `latest`나 부 버전 태그가 아닌 정확한 패치 태그로 고정한다.
  - JDBC 0.11.2: 2026-11-13 공개된 최신 릴리스다. [공식 JDBC 문서](https://clickhouse.com/docs/integrations/language-clients/java/jdbc)의 요구사항은 Java 8 이상이라 Spark 이미지의 Java 17에서 쓸 수 있다. `all` classifier는 의존성을 포함한 단일 jar라 Kafka 커넥터 jar처럼 Spark 이미지 빌드 시 `/opt/spark/jars/`에 받아 두면 실행 시 의존성 해석이 없다.
- 비교한 대안:
  - ClickHouse 26.10(최신 월간): 기능은 최신이지만 지원 기간이 짧아 v1 도중 지원이 끝난다. 26.3(이전 LTS)은 지원 종료가 더 가깝다.
  - JDBC 0.9.x: 0.10에서 `async_insert=0` 강제가 사라지기 전의 동작이다. 최신 수정이 빠지고 이 PRD는 동기 INSERT를 URL에 명시하므로 이전 동작에 기댈 이유가 없다.
- 제약: 공식 문서에 JDBC 드라이버·ClickHouse 서버·Spark 버전 호환 표가 없다. 4단계에서 실제 적재로 확인한다. 0.11.0부터 `system.query_log`의 `http_user_agent` 형식이 바뀌었으므로 INSERT 식별은 사용자 이름과 쿼리 종류로 한다.

### DDL 관리

- DDL은 `infra/clickhouse/`에 둔다. 스키마는 `schema.sql`, 계정·권한은 비밀번호를 환경변수로 받는 `init.sh`가 적용한다.
- Compose에 일회성 `clickhouse-init` 서비스를 둔다. 같은 ClickHouse 이미지의 `clickhouse-client`로 `clickhouse` 서비스 healthy 후 실행한다. Spark·Grafana는 `service_completed_successfully` 조건으로 이 서비스 완료 후 시작한다.
- 모든 생성 문은 `IF NOT EXISTS`로 작성해 `up`마다 다시 실행해도 결과가 같게 한다. 계정은 `CREATE USER OR REPLACE` 후 권한을 다시 부여해 `.env`의 비밀번호 변경을 반영한다.
- 선택 근거: 기존 `kafka-init`과 같은 구조라 실행·확인 방법이 하나로 통일된다. 매 `up`마다 적용되므로 DDL 파일과 실제 스키마가 어긋난 채 남지 않는다.
- 비교한 대안: 이미지의 `/docker-entrypoint-initdb.d`는 별도 서비스가 없다. 하지만 [entrypoint.sh](https://github.com/ClickHouse/ClickHouse/blob/master/docker/server/entrypoint.sh)는 데이터 디렉터리에 `data`가 있으면 "Skipping initialization"만 출력하고 스크립트를 실행하지 않는다. 볼륨이 남아 있으면 DDL 변경이 조용히 빠진다.
- 제약: `IF NOT EXISTS`는 기존 테이블의 컬럼·설정을 바꾸지 않는다. 스키마 변경은 `schema.sql`에 `ALTER` 문을 추가하거나, 보관이 24시간이므로 볼륨을 지우고 다시 만든다. 어느 방식을 썼는지 변경 기록에 남긴다.

### JDBC 쓰기 설정

| 옵션 | 값 | 기본값 |
| --- | --- | --- |
| `batchsize` | 50000 | 1000 |
| `numPartitions` | 3 | 없음(DataFrame 파티션 수) |
| URL 서버 설정 | `clickhouse_setting_async_insert=0` | 서버 기본값 |
| `isolationLevel` | 기본값 유지 | `READ_UNCOMMITTED` |

- 전제: 정상 주문 4,250건/s, 트리거 5초, shuffle 파티션 3개, `SPARK_MAX_OFFSETS_PER_TRIGGER=50000`이다. 평소 배치당 약 21,250행, 쓰기 파티션당 약 7,083행(정수 분할 시 7,083·7,083·7,084)이다.
- 실측 대조(2026-11-17, [3단계 2단 검증 지속 부하](../measurements/2026-11-17-spark-sustained-load-2stage.json)): 본 측정 배치 120개의 정상 주문 행은 최소 21,092·평균 21,240·최대 21,381(표준편차 66)로 전제와 일치한다. warmup backlog 배치의 최대는 입력 49,999행 중 정상 주문 42,654행(85.3%)이다.
  - 파티션별 행 수는 측정하지 않았다. 행이 hash로 3개 파티션에 고르게 나뉜다고 보고 이항분포 표준편차로 추정하면 평소 가장 큰 파티션은 약 7,330행(평균 7,127 + 3σ), 실측 backlog는 약 14,500행(14,218 + 3σ)이다.
  - 이론상 최악은 50,000 offset이 모두 정상 주문이고 한 파티션에 몰린 경우로 파티션당 50,000행이다.
- INSERT 수 계산: Spark `JdbcUtils.savePartition`은 파티션(태스크)마다 자체 연결을 열고, `batchsize` 행마다 `executeBatch`를 호출한 뒤 남은 행이 있으면 한 번 더 호출한다. ClickHouse JDBC v2는 `executeBatch` 1회를 `INSERT ... VALUES (..),(..)` 쿼리 1개로 보낸다. 따라서 INSERT 수는 파티션별 `ceil(파티션 행 수 / batchsize)`의 합이다. `batchsize`는 파티션마다 적용되는 상한이며 회당 행 수를 늘리지 않는다. ClickHouse의 "초당 약 1회" 권장은 서버가 받는 전체 INSERT 수 기준이다.
- 기준: [ClickHouse INSERT 전략 권장](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy)의 "초당 약 1회"를 상한으로 보고 초당 1회 이하를 목표로 한다. 정확히 1회를 맞추지 않는다.
- `batchsize = 50000`: 아래 순서로 정했다.
  1. 초당 1회 이하 = 배치(5초)당 5회 이하다.
  2. 파티션 크기가 거의 같으면 파티션 INSERT 수도 같다. 파티션당 2회면 6회로 5회를 넘으므로 파티션당 1회여야 한다. 조건은 `batchsize ≥ 가장 큰 파티션 행 수`이며 평소 부하에서는 7,084 이상이다.
  3. 7,084 이상이면 평소 결과는 값과 무관하게 같다. 배치당 3회, 초당 0.6회, 회당 약 7,083행이다.
  4. 그중 `SPARK_MAX_OFFSETS_PER_TRIGGER`와 같은 50,000을 쓴다. 배치는 최대 50,000 offset(레코드)이므로 한 배치의 정상 주문 행도 50,000을 넘지 않는다. 파티션 불균등·정상 주문 비율 변동·backlog 따라잡기에서도 파티션당 1회, 배치당 최대 3회가 유지된다. 트리거 지연 시 다음 배치가 바로 시작되지만 배치 시작 간격은 5초보다 짧아지지 않으므로 초당 0.6회 이하가 항상 보장된다.
  - 회당 최대 행 수도 50,000으로 권장 상한 100,000 안이다.
- `numPartitions = 3`: shuffle 파티션 수와 같아 `coalesce`가 일어나지 않는다. 동시 JDBC 연결은 3개로 제한된다.
- `async_insert=0`: Spark가 이미 클라이언트에서 행을 묶으므로 동기 INSERT를 쓴다. 공식 문서는 클라이언트에서 묶을 수 있으면 동기 INSERT를 권장한다. 동기 INSERT는 응답이 곧 저장 완료라 [부분 실패와 재시작](#부분-실패와-재시작)의 실패 판단이 단순하다. 0.10부터 드라이버가 이 값을 강제하지 않으므로 서버 기본값에 기대지 않고 명시한다.
- `isolationLevel`: Spark는 드라이버가 트랜잭션을 지원하지 않으면 이 값을 쓰지 않는다. ClickHouse는 JDBC 트랜잭션을 쓰지 않으므로 기본값을 둔다.
- 실측 기반 재계산 결과도 50,000이다. 평소 하한은 약 7,400(추정 최대 파티션 7,330 이상)이며 그 이상이면 평소 결과는 같다. 실측 backlog까지 파티션당 1회로 유지하려면 약 15,000 이상이 필요하고, 이론상 최악까지 보장하는 값은 `SPARK_MAX_OFFSETS_PER_TRIGGER`와 같은 50,000뿐이다.

  | 후보 | 평소 배치 | 실측 backlog 배치 | 이론상 최악 |
  | --- | --- | --- | --- |
  | 7,400 | 3회(초당 0.6회) | 6회(초당 1.2회) | 초당 1회 초과 |
  | 15,000 | 3회 | 3회 | 초당 1회 초과 가능 |
  | **50,000 (채택)** | 3회 | 3회 | 3회 보장 |

- 비교한 대안(평소 부하 기준, backlog는 파티션당 최대 약 14,000~16,700행):

  | 설정 | 배치당 INSERT | 초당 | 회당 행 | 판단 |
  | --- | --- | --- | --- | --- |
  | 기본값 `batchsize=1000` | 24회(파티션당 8회) | 4.8회 | 83~1,000 | 권장 빈도의 약 5배이고 파티션 끝 INSERT가 1,000행 미만이다 |
  | `batchsize=4722`(2회 구간 산술 중앙) | 6회 | 약 1.2회 | 2,361~4,722 | 초당 1회에 가장 가깝게 맞춘 값이지만 1회를 넘는다. 초당 1회 이하를 목표로 하면 필요 없다 |
  | `batchsize=7084`(1회 조건 최솟값) | 3회 | 0.6회 | 약 7,083 | 평소 결과는 같지만 파티션이 1행만 커져도 2회가 된다 |
  | `batchsize=10000`(공식 이상값 하한) | 3회 | 0.6회 | 약 7,083 | 평소 결과는 같다. 이상값 10,000~100,000은 회당 실제 행 수 기준이라 `batchsize`로 맞춰지지 않는다. backlog 배치는 파티션당 2회, 초당 최대 1.2회다. VALUES 문자열 최대 크기는 50,000의 1/5이다 |
  | **`batchsize=50000` (채택)** | 3회 | 0.6회 | 약 7,083 | backlog를 포함해 항상 파티션당 1회, 초당 0.6회 이하다 |
  | `batchsize=4250`, `numPartitions=1` | 5회 | 1.0회 | 4,250 | `coalesce(1)`로 쓰기가 태스크 1개에서 직렬로 실행돼 JDBC write action 시간이 늘어난다 |
  | `batchsize=50000`, `numPartitions=2` | 2회 | 0.4회 | 약 10,625 | 회당 행 수가 이상값에 들어가지만 쓰기 병렬성이 3에서 2로 준다 |
  | `async_insert=1` | - | - | - | 서버가 다시 묶어 part 수는 줄지만 클라이언트 배치와 중복이고 flush 실패 시점이 응답과 분리된다 |

- 제약:
  - 회당 행 수는 `batchsize`가 아닌 입력량이 정한다. 평소 회당 약 7,083행으로 권장 최소 1,000행 이상이지만 공식 이상값(10,000~100,000)보다 작다. EPS가 낮아져 파티션이 1,000행 미만이 되면 `batchsize`로 막을 수 없다.
  - backlog 배치는 최대 50,000행을 VALUES 문자열 하나로 만든다. 문자열 크기는 수십 MB 수준으로 추정하며 Spark 컨테이너 3GiB 안에서 버티는지 실측한다.
  - `batchsize`는 `SPARK_MAX_OFFSETS_PER_TRIGGER`에 맞춘 값이다. 이 값이나 shuffle 파티션 수를 바꾸면 다시 정하며, 값은 환경변수로 받는다.
  - 위 동작은 [Spark v4.2.1 `JdbcUtils.savePartition`](https://github.com/apache/spark/blob/v4.2.1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L784)과 [clickhouse-java v0.11.2 `PreparedStatementImpl`](https://github.com/ClickHouse/clickhouse-java/blob/v0.11.2/jdbc-v2/src/main/java/com/clickhouse/jdbc/PreparedStatementImpl.java#L348) 소스로 확인했다. INSERT 1회는 `INSERT ... VALUES (?, ...)` 형태 문에만 해당하며 Spark 일반 append가 이 형태다. 빈 파티션은 INSERT를 보내지 않는다.
  - `numPartitions`는 파티션 수 상한으로만 `coalesce`하며 행 수를 다시 고르게 나누지 않는다. shuffle 3개와 `dropDuplicates`도 파티션 균등을 보장하지 않는다.
  - 태스크·`foreachBatch` 재실행은 INSERT를 추가한다. INSERT 수와 part 수는 다르다. 시간 파티션 경계를 걸친 INSERT는 part를 2개 만들므로 정각 직후 part 수가 잠시 늘어난다.
  - 4단계 실측 항목: 파티션별 행 수, `system.query_log`의 INSERT 수·`written_rows`·시각(`executeBatch` 1회 = INSERT 1회 실측 포함), 생성 part 수, 배치 처리시간·재시도, Spark 메모리.
  - Spark JDBC는 시각을 `setTimestamp`로 보낸다. 이 경로의 시간대 처리는 [JDBC 날짜·시간 가이드](https://clickhouse.com/docs/integrations/language-clients/java/jdbc_date_time_guide)에 명시돼 있지 않다. Spark 세션 시간대는 이미 `UTC`이며, 4단계에서 알려진 UTC 값을 적재·조회해 `DateTime64(3, 'UTC')` 값이 같은지 확인한다.
- 참고: [Spark JDBC 옵션](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html), [ClickHouse Spark JDBC](https://clickhouse.com/docs/integrations/apache-spark/spark-jdbc), [비동기 INSERT](https://clickhouse.com/docs/optimize/asynchronous-inserts)

### 계정과 권한

| 계정 | 권한 | 사용처 |
| --- | --- | --- |
| 관리자(`CLICKHOUSE_USER`) | 접근 관리 포함 전체 | `clickhouse-init`과 수동 점검 |
| `spark_writer` | `silver.order_events`·`ops.spark_batch_attempts`·`ops.spark_query_progress`에 INSERT·SELECT | Spark JDBC 적재와 드라이버 지표 |
| `generator_writer` | `ops.producer_stats`에 INSERT | 호스트의 로그 생성기 |
| `grafana_reader` | `gold.*`·`ops.*`·`system.query_log`·`system.part_log`에 SELECT. `readonly = 1`, `max_execution_time`만 `CHANGEABLE_IN_READONLY` | Grafana 데이터 소스 |
| `gold_definer` | `silver.order_events`에 SELECT. `HOST NONE`으로 로그인 불가 | Gold 뷰의 `DEFINER` |

- 관리자는 이미지 환경변수 `CLICKHOUSE_USER`·`CLICKHOUSE_PASSWORD`와 `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`로 만든다. 이 설정을 쓰면 이미지가 인증 없는 `default` 사용자를 제거한다. 나머지 계정은 `init.sh`가 SQL로 만들고 비밀번호는 `.env`에서 받는다.
- 선택 근거:
  - 쓰는 프로세스마다 계정을 나눠, 각 계정이 쓸 테이블에만 INSERT할 수 있게 한다. 계정 이름이 `system.query_log`의 `user`에 남아 INSERT 원천을 구분하는 데도 쓴다.
  - `spark_writer`의 SELECT: Spark JDBC는 append 전에 `SELECT * FROM <table> WHERE 1=0`으로 테이블 존재와 스키마를 확인하므로 INSERT만으로는 쓰기가 실패한다.
  - `grafana_reader`의 `readonly = 1`: [Grafana ClickHouse 플러그인 설정](https://grafana.com/docs/plugins/grafana-clickhouse-datasource/latest/configure/)은 플러그인이 쓰기 쿼리를 막지 않으므로 읽기 전용 사용자를 권장한다. 플러그인이 `max_execution_time`을 바꿔야 하므로 이 설정만 `CHANGEABLE_IN_READONLY`로 허용하고 `readonly = 2`는 쓰지 않는다.
  - Gold 뷰의 `SQL SECURITY DEFINER`: 기본 설정의 일반 뷰는 조회하는 계정의 권한으로 실행되므로 Grafana 계정에 `silver` SELECT가 필요해진다([CREATE VIEW SQL SECURITY](https://clickhouse.com/docs/sql-reference/statements/create/view)). 정의자 권한으로 실행하면 Grafana 계정은 `gold`·`ops`만 읽으면서 [저장 계층](#저장-계층)의 구분이 권한에도 그대로 드러난다. 정의자는 관리자 대신 `silver.order_events` SELECT만 가진 `gold_definer`로 둬, 뷰를 거쳐 얻는 권한을 이 테이블 읽기로 제한한다.
- 비교한 대안:
  - 모든 서비스가 관리자 계정 하나를 사용: 설정은 가장 단순하지만 Grafana에서 `DROP` 같은 쓰기 쿼리가 실행될 수 있고 INSERT 원천을 계정으로 구분할 수 없다.
  - `grafana_reader`에 `silver` SELECT 부여: 뷰 설정이 필요 없지만 대시보드가 중복 제거 경로를 우회해 원본을 직접 집계할 수 있고 계층 구분이 권한에 드러나지 않는다.
  - 정의자를 관리자로 지정: 계정이 하나 줄지만 뷰가 전체 권한으로 실행된다.
- 제약: `DEFINER` 뷰의 범위 조건 가지치기와 권한 동작, `CHANGEABLE_IN_READONLY` 구문은 4단계에서 26.8로 확인한다.

### 자원 제한

| 서비스 | `cpus` | `mem_limit` | 기타 |
| --- | --- | --- | --- |
| `clickhouse` | 2 | 4g | `ulimits.nofile` 262144 |
| `grafana` | 0.5 | 512m | - |

- 선택 근거:
  - 예산: Kafka UI를 제외하면 남은 자원은 CPU 3개·메모리 약 7GiB다. ClickHouse에 CPU 2개를 주면 ClickHouse가 병합·`FINAL` 조회로 포화돼도 호스트의 로그 생성기·Grafana가 CPU 1개를 쓸 수 있다. `cpus`는 예약이 아닌 상한이므로 ClickHouse가 한가할 때는 다른 프로세스가 그 CPU를 쓴다.
  - 메모리 4g: 적재 블록·병합·`FINAL` 조회가 쓰는 메모리다. 남은 약 3GiB는 Grafana 512MiB와 생성기·WSL 자체가 쓴다.
  - `nofile` 262144: [Docker Hub 이미지 안내](https://hub.docker.com/r/clickhouse/clickhouse-server)의 실행 예시 값이다. 1시간 파티션 25개와 컬럼별 파일로 열린 파일 수가 많다.
  - Grafana 0.5·512MiB: 1분·5초 갱신 패널의 쿼리 전송과 렌더링만 하며, 계산은 ClickHouse가 한다. 5단계에서 실측해 조정한다.
- 저메모리 설정: [ClickHouse 운영 팁](https://clickhouse.com/docs/operations/tips)은 메모리 16GB 미만에서 기본값이 메모리 예외를 일으킬 수 있다고 안내한다. `infra/clickhouse/config.d/`에 아래를 적용한다.
  - `metric_log`, `trace_log`, `text_log`, `asynchronous_metric_log`를 끈다. 대시보드가 쓰는 `query_log`·`part_log`는 유지한다.
  - mark cache를 512MiB로 줄인다. 문서는 약 500MB까지 줄일 수 있다고 한다.
  - 문서의 다른 항목(쿼리 처리 스레드 1개, `max_block_size` 축소, 병렬 파싱 끄기)은 적용하지 않는다. CPU 2개에서 `FINAL` 조회와 병합을 같이 해야 하므로 스레드를 1개로 묶으면 조회 시간이 늘어난다. 메모리 예외가 실측되면 다시 검토한다.
- 비교한 대안:
  - ClickHouse CPU 3개: 남은 CPU를 모두 주면 병합 여유가 커지지만 포화 시 생성기 EPS가 떨어져 측정 부하가 흔들린다.
  - 메모리 제한 없음: 예외는 줄지만 WSL 12GB 안에서 Kafka·Spark와 경합해 OOM 대상이 예측되지 않는다.
- 제약: ClickHouse가 컨테이너 메모리 제한을 서버 메모리 상한 계산에 반영하는지는 4단계에서 `system.server_settings`의 `max_server_memory_usage`로 확인한다. 지속 적재 중 CPU·메모리 사용률과 part 수(`parts_to_delay_insert` 기본 1000 대비)를 기록해 값을 조정한다.

## JDBC 재시도와 적재 중복 처리

아래는 제품 v1의 설계 결정이며 아직 구현·실행 검증하지 않았다. 목표는 물리적으로 한 번만 INSERT하는 것이 아니라, 재시도 중복이 있더라도 정상 조회에서 이벤트당 한 건으로 계산하는 것이다.

### 이벤트 식별과 테이블 기준

- 논리 중복 키는 `event_id`다. v1 주문 완료 이벤트는 불변으로 취급하며 같은 `event_id`의 재전송·재처리는 이벤트 시각·주문 금액 등 원본 업무 필드가 동일해야 한다. 다른 ID의 주문은 값이 같아도 별개로 유지한다.
- 정상 주문 테이블의 이름은 `order_events`, 엔진은 버전 인자 없는 `ReplacingMergeTree()`, 정렬 키는 `ORDER BY (event_timestamp, event_id)`로 정한다. 선택 근거는 [중복 제거·조회 조합 비교](#중복-제거조회-조합-비교)를 따른다. 컬럼 타입은 [order_events 컬럼 타입](#order_events-컬럼-타입)을 따른다.
- 보관 기간은 24시간이다. 파티션과 TTL은 아래로 정한다.

  ```sql
  PARTITION BY toStartOfHour(event_timestamp)
  TTL toDateTime(event_timestamp) + INTERVAL 24 HOUR
  SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600
  ```

  - 선택 근거: [ClickHouse 파티션 키 권장](https://clickhouse.com/docs/best-practices/choosing-a-partitioning-key)은 파티션을 쿼리 최적화가 아닌 데이터 수명 관리 용도로 쓰고 값 종류를 100~1,000개 미만으로 두도록 한다. 24시간 보관에 1시간 파티션이면 활성 파티션이 약 25개다. `ttl_only_drop_parts`로 만료된 시간 단위 part를 다시 쓰지 않고 통째로 삭제해 남은 CPU 예산의 TTL 병합 부담을 줄인다. 실제 보관은 24~25시간이다.
  - `ttl_only_drop_parts`: 26.8 소스(`MergeTreeSettings.cpp`) 기준 기본값은 `false`다. 켜면 part의 모든 행이 만료됐을 때 part 전체를 삭제하고, 끄면 만료된 행만 지운다.
  - `merge_with_ttl_timeout = 3600`: 삭제 TTL 병합을 다시 실행하기 전 최소 대기 시간(초)이며 26.8 기본값은 14400(4시간)이다. 기본값이면 만료된 part가 최대 약 4시간 더 남아 보관이 최대 약 29시간이 되고, 그만큼 디스크를 더 쓴다. 1시간 파티션과 같은 주기로 맞춰 보관을 최대 약 26시간으로 줄인다. part 전체 삭제는 다시 쓰기가 없어 주기를 줄여도 CPU 부담이 작다. 대기 시간은 파티션별로 계산하므로 실제 지연은 이보다 짧을 수 있다. 실제 삭제 시점은 4단계에서 `system.parts`로 확인한다.
    - 비교한 대안: 기본값 4시간 유지는 설정이 없지만 디스크 여유가 적은 환경에서 약 4시간분(약 6천만 행)을 더 보관한다. 더 짧게(예: 600초) 두면 삭제는 빨라지지만 1시간 파티션보다 짧은 주기는 보관 기간을 더 줄이지 못한다.
  - 중복 제거와의 관계: ReplacingMergeTree는 같은 파티션 안에서만 행을 합친다. 같은 `event_id`는 같은 `event_timestamp`를 가진다는 불변 전제로 같은 이벤트는 항상 같은 파티션에 들어간다.
  - 비교한 대안: 일 단위 파티션은 하루치가 모두 만료돼야 part가 삭제돼 최대 48시간 보관된다. 파티션 없는 행 단위 TTL은 TTL 병합마다 part를 다시 써 CPU를 쓴다.
  - 제약: 24시간보다 오래된 이벤트를 재처리하면 INSERT 직후 삭제 대상이 된다. 시간 경계를 걸친 배치는 part를 2개 만든다. 용량은 하루 약 3.67억 행, 압축 후 행당 30~60바이트 가정 시 11~22GB이며 실측한다.
- 정렬 키에 `event_timestamp`를 둔 것은 조회 시간 범위로 읽을 구간을 줄이기 위해서다. 같은 `event_id`는 같은 `event_timestamp`를 가진다는 불변 전제에서만 중복 행이 같은 정렬 키로 모인다. 이 전제가 깨지면 `ReplacingMergeTree`와 `FINAL`은 두 행을 합치지 않는다.
- Kafka 위치·append 시각, Spark 쿼리·실행·배치 ID, 적재 시각은 중복 키에 넣지 않는다. 재발행·재실행에 따라 값이 달라도 같은 이벤트로 취급한다. 재시도 시 `event_id`를 새로 생성하지 않는다.
- 같은 키의 여러 행 중 보존되는 행에 따라 추적 메타데이터가 달라질 수 있다. 정상 테이블의 메타데이터를 모든 시도 또는 최초 수신 이력으로 간주하지 않는다. 시도별 운영 지표는 별도로 기록한다.
- 같은 ID에 서로 다른 업무 내용이 들어오는 경우는 정상 중복이 아닌 계약 위반이다. 현재 생성기는 매 주문마다 새 ID를 발급하며, 이 정책은 동일 업무 내용의 재전송·재처리를 대상으로 한다. 다른 내용의 충돌 감지·격리는 별도 설계 항목이며 이 엔진만으로 구현됐다고 간주하지 않는다.
- 같은 ID에 다른 `event_timestamp`가 들어온 충돌은 합치지 않고 드러낸다. 어느 행이 맞는지 정할 규칙이 없으므로 한 행을 임의로 고르지 않는다. 충돌 건수는 아래 점검 쿼리로 주기적으로 집계해 운영 대시보드에 표시하며, 대시보드 갱신마다 실행하지 않는다. 충돌이 0이 아닌 구간의 주문 집계는 부풀 수 있음을 함께 표시한다. 두 시각이 점검 구간보다 멀리 떨어진 충돌은 이 쿼리로 찾지 못하므로 전체 구간 점검을 낮은 빈도로 별도 실행한다. 점검 구간과 주기는 4단계에서 확정한다.

  ```sql
  -- 같은 event_id에 서로 다른 event_timestamp가 있는 충돌 건수를 센다.
  SELECT count() FROM (
    SELECT event_id FROM silver.order_events
    WHERE event_timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY event_id HAVING uniqExact(event_timestamp) > 1
  );
  ```

- 이벤트 수정이 필요해지면 버전·충돌 정책을 먼저 정의한다. 이 경우 `event_timestamp`가 바뀔 수 있으므로 정렬 키에서 빼고, 원본 이벤트의 버전 값을 쓰는 `ReplacingMergeTree(version)`과 ID마다 변하지 않는 시간 값(시간 정보를 가진 ID 형식 또는 불변 필드)을 계약에 추가한다. Kafka offset과 적재 시각은 재실행 때 달라지므로 버전으로 쓰지 않는다. 불변 시간 값을 둘 수 없으면 `ORDER BY event_id`와 1분 롤업 조합으로 전환한다.

### 부분 실패와 재시작

1. `foreachBatch`에서 정상 주문을 JDBC append로 기록한다. Spark 태스크 또는 JDBC 요청 일부가 성공한 뒤 나머지가 실패해도 마이크로배치 전체가 롤백된다고 가정하지 않는다.
2. 실패·타임아웃은 예외로 전달한다. 응답 유실은 서버에 저장됐을 수도 있는 불확실한 결과로 취급한다. 태스크 재시도를 거쳐 쿼리가 실패하면 복구 후 같은 체크포인트로 재시작해 미완료 배치를 다시 처리한다.
3. 재처리에서는 원본 이벤트 ID·업무 필드를 유지한다. 이미 저장된 행도 다시 INSERT될 수 있으며, 정상 적재가 끝난 뒤 DLQ 기록만 실패한 경우도 같은 정책을 적용한다.
4. `batch_id`는 추적용이다. 배치 완료 표식을 별도로 적는 것만으로 다중 JDBC 쓰기와 원자적으로 커밋되지 않으므로, 이를 근거로 미완료 쓰기를 건너뛰지 않는다. INSERT 전 존재 여부를 SELECT하는 방식에도 의존하지 않는다.
5. Spark `dropDuplicates`는 입력 중복 감소에 사용할 수 있지만 JDBC 재시도 중복의 최종 방어선은 아니다. v1은 배치 내 `dropDuplicates`만 사용하고 워터마크 기반 상태 중복 제거는 도입하지 않는다. 근거는 [중복 제거·조회 조합 비교](#중복-제거조회-조합-비교)를 따른다.

### 조회 시 중복 제거

ReplacingMergeTree는 백그라운드 병합 시 같은 정렬 키의 행을 줄이지만 병합 시점을 보장하지 않는다. 따라서 Grafana와 정확성 검증은 `order_events FINAL`을 사용하는 조회 뷰 `orders_deduplicated`를 통해 수행한다. 뷰 정의는 DDL 파일 작성 시 구현한다.

```sql
-- 조회 뷰에서 중복을 제거한 후 주문 수와 매출을 집계한다.
CREATE VIEW gold.orders_deduplicated
DEFINER = gold_definer SQL SECURITY DEFINER AS
SELECT * FROM silver.order_events FINAL;

-- 실제 대시보드 쿼리에는 이벤트 시각 범위와 필요한 집계 차원을 추가한다.
SELECT count() AS order_count, sum(total_amount) AS revenue
FROM gold.orders_deduplicated
WHERE event_timestamp >= now() - INTERVAL 15 MINUTE;
```

- 대시보드 쿼리는 항상 `event_timestamp` 범위 조건을 포함한다. 범위 조건이 없으면 전체 누적 행을 읽는다. 뷰를 거친 조회에서 범위 조건이 정렬 키 가지치기에 실제로 쓰이는지는 4단계에서 읽은 행 수로 확인한다.
- 뷰는 `SQL SECURITY DEFINER`로 만든다. 정의자 `gold_definer`의 권한으로 `silver`를 읽으므로 Grafana 읽기 계정에는 `silver` 권한을 주지 않는다. 근거는 [계정과 권한](#계정과-권한)을 따른다.

- 주문 수뿐 아니라 매출·할인·판매 수량·평균 주문 금액·최신성도 동일한 중복 제거 경로를 사용한다. 주문 수만 `uniqExact(event_id)`로 계산하고 매출은 원본 테이블에서 합산하는 방식은 사용하지 않는다.
- `OPTIMIZE FINAL` 실행이나 백그라운드 병합 완료를 조회 정확성의 전제조건으로 삼지 않는다. 일반 INSERT 기반 사전 합산 Materialized View를 추가하면 중복이 누적될 수 있으므로, 현재는 중복 제거 후 조회 집계를 기준으로 한다.
- 원본 테이블의 물리 행 수가 즉시 유일해지는 것, 미완료 배치의 모든 행이 한꺼번에 보이는 것, DLQ와 정상 출력의 원자성은 보장하지 않는다. 최종 배치 성공 후 누락 없이 논리 중복이 제거된 결과를 검증한다.
- `FINAL`과 `(event_timestamp, event_id)` 정렬 키의 조회 비용은 실측한다. 시간 파티션 추가는 같은 이벤트가 같은 중복 키·파티션으로 모이는 조건과 집계 정확성을 검증한 뒤 적용한다. 보관 데이터 삭제 후의 과거 재투입을 영구적으로 차단하는 정책은 아니다.
- DLQ 키 지정은 중복 제거가 아니다. DLQ에는 재시도 중복이 남을 수 있으며, DLQ 소비·재처리 시 원본 topic·partition·offset 기준 중복 판별을 별도로 설계한다.

참고: [ClickHouse ReplacingMergeTree 및 FINAL](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree).

### 중복 제거·조회 조합 비교

Spark 중복 제거, ClickHouse 테이블 엔진·정렬 키, 대시보드 조회 경로의 조합을 조회 비용·집계 정확성·데이터 반영 지연으로 비교했다. 아래 수치는 PRD 부하 기준의 계산값이며 실측 결과가 아니다.

비교 전제는 다음과 같다.

- 정상 주문은 4,250건/s다. 최근 15분 구간은 약 382.5만 행, 1일 누적은 약 3.67억 행이다.
- [주문 현황 대시보드](05-grafana-dashboard.md#주문-현황-대시보드)는 최근 15분 조회·1분 집계·1분 자동 갱신이다. 패널당 조회는 분당 1회다.
- T는 Spark 트리거 주기다. 반영 지연은 이벤트 생성부터 화면 반영까지의 최악값이다.

| 조합 | Spark 중복 제거 | ClickHouse 엔진·정렬 키 | 대시보드 조회 | 조회 비용 | 집계 정확성 | 반영 지연(최악) |
| --- | --- | --- | --- | --- | --- | --- |
| A | 배치 내 `dropDuplicates` | `ReplacingMergeTree()`, `ORDER BY event_id` | `FINAL` 뷰 | 조회마다 전체 누적 행을 읽음. 1일 뒤 약 3.67억 행 × 패널 수 | 정확 | T + 배치 + 60초 + 조회시간. 조회시간이 누적량에 따라 증가 |
| **B (채택)** | 배치 내 `dropDuplicates` | `ReplacingMergeTree()`, `ORDER BY (event_timestamp, event_id)` | `FINAL` 뷰 + 시간 범위 | 15분 구간 약 382.5만 행. 누적량과 무관 | 불변 전제에서 정확 | T + 배치 + 60초 + 조회시간 |
| C | 워터마크 기반 상태 중복 제거 | B와 동일 | B와 동일 | B + Spark 상태 저장. 워터마크 10분이면 약 255만 키 | B와 동일. 추가 이득 없음 | B와 동일 |
| D | 배치 내 `dropDuplicates` | 원본은 B와 동일. 1분 롤업 테이블은 `MergeTree`이며 refreshable materialized view가 `FINAL` 결과로 교체 | 롤업 테이블 | 대시보드 조회는 수십 행. 분당 1회 재계산이 B 조회 1회분 | 재계산 시점 기준 정확 | B + 롤업 주기 최대 60초 |
| E | 워터마크 기반 상태 중복 제거 | `MergeTree`, 중복 제거 없음 | 원본 테이블 직접 | 가장 낮음 | 부정확. JDBC 재실행 중복이 합산됨 | B와 동일 |

B를 채택한 이유는 다음과 같다.

- **A:** 정렬 키가 `event_id`뿐이면 시간 범위 조건으로 읽을 구간을 줄일 수 없다. 갱신을 1분으로 늦춰도 조회 1회 비용이 누적 행 수에 비례해 계속 늘어난다.
- **B:** `ReplacingMergeTree`와 `FINAL`을 유지하므로 A와 같은 정확성을 가지며, 정렬 키 앞의 `event_timestamp`로 조회 구간만 읽는다. v1 이벤트는 불변이므로 같은 `event_id`의 중복 행은 같은 정렬 키로 모인다.
- **C:** Producer는 `enable.idempotence=true`이고 생성기는 주문마다 새 ID를 발급하므로 Kafka 입력 중복은 사실상 없다. 실제 중복 원인은 마이크로배치 재실행이며, 재실행 시 Spark 상태는 해당 배치 이전으로 복원돼 같은 행을 다시 내보낸다. 상태 저장 비용과 늦은 데이터 정책만 추가된다.
- **D:** 1분 갱신에서는 B의 직접 조회가 조회 시간 예산에 들어올 것으로 본다. 롤업은 화면 반영 최악 지연을 약 2분으로 늘리고 구성 요소를 추가하므로, B의 실측 조회시간이 예산을 넘을 때 도입한다.
- **E:** 일반 `MergeTree`는 같은 이벤트의 재적재를 합치지 않는다. Spark 상태 중복 제거는 JDBC 재실행 중복을 막지 못하므로 주문 수·매출이 부풀 수 있다.
- **반영 지연:** 모든 조합에서 대시보드 갱신 대기 60초가 가장 큰 항이다. Spark 트리거 주기를 수 초 아래로 줄여도 화면 반영 이득은 작다.

B의 제약은 불변 전제 의존이다. 같은 ID에 다른 `event_timestamp`가 들어오면 A는 한 행을 임의로 남기지만 B는 두 행을 모두 합산한다. 처리 방식은 [이벤트 식별과 테이블 기준](#이벤트-식별과-테이블-기준)의 충돌 감지 정책을 따른다.

### 중복·재시도 검증 기준

- 최근 15분 조회가 읽는 행 수가 누적 행 수가 아닌 조회 구간에 비례하는지 확인한다. 같은 ID에 다른 `event_timestamp`를 주입해 충돌 점검 쿼리가 건수를 보고하는지 확인한다.
- 동일 업무 이벤트를 같은 ID로 여러 번 INSERT해 병합 대기 없이 조회 뷰에서 주문 수·매출·할인·판매 수량이 한 번만 반영되는지 확인한다.
- 동일 이벤트의 Kafka 위치와 Spark 배치·실행 ID가 달라도 한 건이며, 서로 다른 이벤트 ID는 업무 값이 같아도 각각 유지되는지 확인한다.
- 일부 JDBC 쓰기 성공 후 실패, 서버 저장 후 응답 유실, 정상 적재 성공 후 DLQ 실패를 주입한다. 같은 체크포인트로 복구해 최종 결과를 기대 이벤트 집합과 대조하고 누락·중복 합산이 없는지 확인한다.
- 추적 메타데이터의 대표 행 선택이 최초 수신·최초 적재 측정값을 대신하지 않는지 확인하고, 재시도·실패 지표가 남는지 검증한다.
- 목표 부하와 조회 범위에서 중복 제거 뷰의 조회시간·데이터 지연을 측정한다. 성능 목표와 전체 DDL이 확정되기 전에는 이 정책의 성능 검증을 완료로 처리하지 않는다.

## 처리지연 측정

[운영 대시보드](05-grafana-dashboard.md#파이프라인-운영-대시보드)를 위한 단계별 지연 지표의 저장 위치는 아래 [운영 지표 저장](#운영-지표-저장)을 따른다.

### 운영 지표 저장

아래는 제품 v1의 설계 결정이며 아직 구현·검증하지 않았다.

- 지연 지표 저장소는 ClickHouse 하나로 한다. Grafana 운영 대시보드는 주문 대시보드와 같은 ClickHouse 데이터 소스를 조회한다. Prometheus 등 별도 지표 저장소는 v1에 도입하지 않는다.
- 선택 근거: 대시보드 요구 지연은 대부분 이벤트·배치 단위이며 정확한 p50·p95·p99와 측정 건수·측정 불가를 표시해야 한다. 원본 값을 행으로 저장하면 SQL `quantile` 계열 함수로 계산할 수 있고, 주문 데이터와 같은 SQL로 대조할 수 있다. 히스토그램 기반 수집은 근사 분위수이고 이벤트별 지연을 남기지 않는다. 별도 저장소는 backlog 같은 인프라 게이지와 알림이 필요해질 때 다시 검토한다.
- **이벤트 단위 지연:** 생성 → Kafka(`kafka_append_timestamp - event_timestamp`)와 Kafka → Spark 지연은 `order_events` 행의 시각 컬럼으로 계산한다. 이를 위해 `spark_processed_at`(콜백 진입 시각) 컬럼을 추가한다. 이 경로는 정상 주문만 대상이며 DLQ 경로의 지연은 포함하지 않는다.
  - Grafana는 `silver`를 직접 읽지 않고 아래 `gold.order_event_latency` 뷰로 조회한다. `FINAL`은 쓰지 않는다.

    ```sql
    CREATE VIEW gold.order_event_latency
    DEFINER = gold_definer SQL SECURITY DEFINER AS
    SELECT event_id, event_timestamp, kafka_append_timestamp, spark_processed_at, spark_run_id, spark_batch_id
    FROM silver.order_events;
    ```

  - 선택 근거: Grafana 계정을 `gold`·`ops`로 제한하기 위한 뷰다([계정과 권한](#계정과-권한)). 지연 계산에 필요한 컬럼만 노출한다. 재처리 중복 행은 실제로 다시 처리된 시도의 지연이므로 합치지 않는다. `FINAL`로 합치면 어느 시도의 행이 남는지 정해지지 않고 병합 비용만 든다.
  - 비교한 대안: `gold.orders_deduplicated`에서 지연 계산은 경로가 하나로 줄지만, 남는 행이 임의라 지연 분위수가 특정 시도를 대표하지 않고 `FINAL` 비용이 든다.
- **배치 단위 지표:** 배치당 1행을 별도 운영 지표 테이블에 저장한다. 원천은 두 곳이다.
  - `foreachBatch` 콜백 지표: 쿼리·실행·배치 ID, 상태·실패 단계, 분류·정상 쓰기·DLQ 쓰기 action 시간, 콜백 전체 시간, Kafka → 콜백 지연 분위수, 분류별 건수. 실패한 시도도 1행으로 남겨 재시도를 관측한다.
  - `StreamingQueryListener.onQueryProgress` 진행 정보: 트리거 전체 시간(`durationMs`), 입력·처리 EPS, 파티션별 시작·끝 offset과 소스의 최신 offset. backlog 근사값은 최신 offset과 처리 완료 offset의 차이로 계산한다. 진행 정보는 배치 성공 후 비동기로 전달되므로 콜백 지표와 별도 행으로 저장하고 배치 ID로 연결한다.
- **지표 쓰기 실패 정책:** 지표 INSERT 실패는 경고 로그만 남기고 주문 배치를 실패시키지 않는다. 지표 손실을 허용하며, 빈 구간은 대시보드에 0이 아닌 측정 불가로 표시한다. 기존 지표 로그 출력은 유지해 손실 구간의 대조 근거로 쓴다. 지표 쓰기는 주문 적재 경로와 같은 재시도·중복 정책의 대상이 아니다.
- 배치 재실행으로 같은 배치 ID의 지표 행이 여러 개 생길 수 있다. 이 행은 시도별 기록이므로 합치지 않으며, 시도를 구분할 컬럼(실행 ID·콜백 시작 시각 등)을 둔다.
- **ClickHouse 내부 지표:** INSERT 요청별 처리시간·실패는 `system.query_log`, part 생성·병합은 `system.part_log`에서 SQL로 조회한다. 별도 계측 코드나 Prometheus 엔드포인트를 쓰지 않는다. 대시보드별 원천은 [Grafana 패널별 데이터 원천](05-grafana-dashboard.md#패널별-데이터-원천)을 따른다.
- **지표 INSERT 방식:** Spark 드라이버에서 Python 표준 라이브러리(`urllib`)로 ClickHouse HTTP 인터페이스에 `INSERT ... FORMAT JSONEachRow`를 POST한다. 설정은 `async_insert=1`, `wait_for_async_insert=1`이다. 실패는 위 지표 쓰기 실패 정책에 따라 경고 로그로만 남긴다.
  - 근거 1: 콜백·progress 지표는 이미 JSON으로 직렬화되는 dict다. 변환 없이 그대로 보낼 수 있고 추가 의존성이 없다.
  - 근거 2: Spark JDBC로 쓰면 배치당 1~2행을 위해 DataFrame 생성과 Spark job이 추가된다. `onQueryProgress` 리스너 스레드에서 Spark job을 실행하는 문제도 생긴다. `clickhouse-connect`는 같은 HTTP 방식에 Spark 이미지 의존성만 추가한다.
  - 근거 3: [ClickHouse INSERT 전략 권장](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy)은 동기 INSERT를 1,000행 이상, 초당 1회 수준으로 묶도록 한다. 클라이언트에서 묶을 수 없으면 `async_insert=1`과 `wait_for_async_insert=1`을 강하게 권장한다. 지표는 5초마다 1~2행인 작은 INSERT이므로 서버 측 묶음을 사용한다. `wait_for_async_insert=0`은 오류를 클라이언트가 알 수 없어 사용하지 않는다.
  - 비교에서 제외한 대안: 지표 Kafka 토픽 → ClickHouse Kafka 엔진은 ClickHouse 장애 중에도 지표를 보존하는 실무 구조다. 하지만 토픽·Kafka 엔진 테이블·MV가 추가되고 ClickHouse의 소비 부하가 남은 CPU 예산을 쓴다. v1은 지표 손실을 허용하므로 도입하지 않는다.
  - 배치당 비용은 실측하지 않은 추정이다. 4단계에서 지표 INSERT 시간을 콜백 시간과 함께 기록한다.
- **운영 지표 테이블 구조:** `ops` 데이터베이스에 원천별 테이블 3개를 둔다. 모두 `MergeTree`, 파티션 없음, 행 단위 TTL 24시간이다.

  | 테이블 | 행 단위 | 쓰는 곳 |
  | --- | --- | --- |
  | `ops.producer_stats` | 생성기 1초 구간당 1행 | 호스트의 로그 생성기 |
  | `ops.spark_batch_attempts` | `foreachBatch` 콜백 시도당 1행. 실패 포함 | Spark 드라이버 |
  | `ops.spark_query_progress` | `onQueryProgress` 배치당 1행 | Spark 드라이버 리스너 |

  - 목적: 병목이 생겼을 때 스택의 어느 구간이 원인인지 대시보드로 가린다. 구간마다 소요시간과 그 구간 앞에 쌓이는 대기열을 함께 기록한다. 처리량이 입력보다 낮은 구간이 있으면 그 앞의 대기열이 계속 늘어난다.

    | 구간 | 소요시간 신호 | 앞단 대기열 | 원천 |
    | --- | --- | --- | --- |
    | 생성·직렬화 | 생성·직렬화 시간, 목표 대비 실제 EPS | - | `ops.producer_stats` |
    | Producer → Kafka | ACK 지연, 생성 → Kafka 지연 | Producer 큐 길이 | `ops.producer_stats` |
    | Kafka → Spark | Kafka → 콜백 진입 지연 | offset backlog | `ops.spark_query_progress`, `gold.order_event_latency` |
    | Spark 분류 | 분류 action 시간, 트리거 시간 대비 트리거 주기 | offset backlog | `ops.spark_batch_attempts`, `ops.spark_query_progress` |
    | Spark → JDBC | 정상·DLQ 쓰기 action 시간 | offset backlog | `ops.spark_batch_attempts` |
    | ClickHouse INSERT·병합 | 서버 측 INSERT 처리시간, part 생성·병합 | - | `system.query_log`, `system.part_log` |

    - 판정 순서: ACK EPS가 목표보다 낮고 Producer 큐가 늘면 생성기·Producer 구간이다. offset backlog가 계속 늘면 Spark 이후 구간이며, 배치 시간 내역으로 분류와 쓰기를 가른다. JDBC 쓰기 action은 길고 `query_log`의 INSERT 시간은 짧으면 Spark 태스크·네트워크, 둘 다 길면 ClickHouse 쪽이다.
  - 컬럼은 아래로 정한다. 시각은 `DateTime64(3, 'UTC')`, 분위수는 `Float64`, 건수는 `UInt64`다. 측정 대상이 없는 구간·배치의 분위수는 Nullable 대신 건수 0으로 구분하고 분위수 값은 0으로 둔다. 대시보드는 건수 0이면 측정 불가로 표시한다.
    - `ops.producer_stats`: `interval_start`, `run_id`, `target_eps`, `generated`, `acked`, `failed`, `undelivered`, `queue_len_max`, 그리고 `ack_latency_ms`·`gen_to_kafka_ms`·`gen_serialize_us` 각각의 `_p50`·`_p95`·`_p99`·`_count`. `ORDER BY interval_start`.
    - `ops.spark_batch_attempts`: `query_id`, `run_id`, `batch_id`, `callback_started_at`, `status`, `failed_stage`, `input_rows`, `order_rows`, `excluded_rows`, `dlq_rows`, `classification_ms`, `orders_write_ms`, `dlq_write_ms`, `callback_total_ms`, `kafka_to_callback_ms`의 `_p50`·`_p95`·`_p99`·`_max`·`_count`, `kafka_ts_missing_count`, `kafka_ts_future_count`. `ORDER BY callback_started_at`.
    - `ops.spark_query_progress`: `query_id`, `run_id`, `batch_id`, `trigger_at`, `trigger_execution_ms`, `add_batch_ms`, `latest_offset_ms`, `query_planning_ms`, `wal_commit_ms`, `commit_offsets_ms`, `num_input_rows`, `input_rows_per_sec`, `processed_rows_per_sec`, `end_offsets Map(Int32, Int64)`, `latest_offsets Map(Int32, Int64)`. backlog는 조회 시 두 Map의 차이로 계산한다. `ORDER BY trigger_at`.
    - 실패 단계·실패한 action의 시간은 실행하지 않았거나 측정하지 못한 값이다. `status`·`failed_stage`로 구분하고 시간 값은 0으로 둔다.
  - 선택 근거: 원천마다 도착 시점·행 단위·컬럼이 다르다. 나누면 각 테이블 컬럼이 명확하고 Nullable이 필요 없다. 같은 배치는 쿼리·실행·배치 ID로 조인한다. 행 수가 시간당 수천 행 이하라 파티션 없이 행 단위 TTL을 써도 병합 부담이 작다. 콜백 지표는 3단계 계측 로그의 dict를 그대로 행으로 옮긴다.
  - 비교한 대안: 테이블 하나에 `source` 컬럼으로 구분하면 컬럼 절반이 비어 Nullable이 많아진다. `(time, metric_name, value, labels)` 긴 형식은 지표를 추가해도 DDL을 바꾸지 않지만 쿼리마다 피벗이 필요하고 타입 보장이 약하다.
- **생성기·Producer 지표:** 로그 생성기가 1초 구간 통계를 Spark 지표와 같은 HTTP + `async_insert` 방식으로 `ops.producer_stats`에 INSERT한다. 항목은 ACK EPS, 전송 실패·미전달, 큐 길이, ACK 지연과 생성 → Kafka 지연의 구간별 분위수·건수, 생성·직렬화 시간이다. 실패 시 경고만 남기고 생성을 계속한다.
  - 선택 근거: 수집 방식이 Spark 지표와 하나로 통일된다. 운영 대시보드의 앞 구간(생성기·Producer)을 측정 불가로 남기지 않는다.
  - 비교한 대안: 4단계 범위에서 제외하면 범위는 작지만 운영 대시보드 앞 구간이 비어 있다. 지표 Kafka 토픽 경유는 위 지표 INSERT 방식에서 제외한 구조를 생성기에만 도입하는 셈이다.
  - 제약: 구간별 분위수는 해당 구간 값으로만 표시하고 여러 구간을 평균·합산하지 않는다. 생성기는 호스트에서 실행하므로 ClickHouse HTTP 포트를 `127.0.0.1`에만 게시한다.

JDBC write action 전체 시간과 실제 INSERT 요청별 시간은 구분한다. Spark 측 action 계측만으로 요청별 INSERT 시간을 측정했다고 간주하지 않는다. 요청별 시간은 ClickHouse `system.query_log`의 서버 측 처리시간으로 측정하며, 네트워크·클라이언트 대기는 포함하지 않는다.

[공통 측정 원칙](01-log-generator.md#공통-측정-원칙)을 따른다. INSERT 응답과 실제 조회 가능 시점을 구분하며, INSERT 성공을 조회 가능으로 간주하지 않는다.

| 기준점·지표 | 기록 위치·계산 | 의미 |
| --- | --- | --- |
| INSERT 서버 처리시간 | `system.query_log`의 INSERT 쿼리별 `query_duration_ms`·`written_rows`·예외 | 서버가 요청을 받은 뒤 처리를 마칠 때까지의 시간. 전송·응답 대기는 제외 |

- 적재 방식과 응답의 성공 의미를 명시한다. 비동기 INSERT는 큐 수락만 확인하는지 실제 flush 완료까지 기다리는지 설정을 함께 기록한다. 분산 테이블을 사용하면 실제 대시보드 조회 경로에서 확인한다.
- 배치별 쓰기 지표에 Spark 실행·배치 ID, 시도 번호, 건수, 실패와 재시도를 기록한다. 성공 응답 시각은 응답 후에 알 수 있으므로 이미 보낸 행에 저장하지 않고 별도 운영 지표로 관리한다.
- `event_timestamp`, Kafka 추가 시각·topic·partition·offset, Spark 배치 식별자를 [컬럼 타입](#order_events-컬럼-타입)에 따라 보존한다. 서버 기본값으로 생성한 적재 시각은 평가 시점을 명시하며 INSERT 완료·조회 가능 시각으로 간주하지 않는다.
- 불량 격리 경로의 지연은 정상 주문 적재와 별도로 관리한다.

### 조회 가능 지연 제외

샘플 이벤트를 ClickHouse에서 처음 발견한 시각(`first_observed_at`)으로 생성 → 조회 가능 지연을 재는 샘플러와 `ops.visibility_samples`는 v1 범위에서 제외한다.

- 선택 근거: v1 운영 지표의 우선 목적은 병목 구간 판별이다. 샘플러는 종단 간 지연 하나를 관측할 뿐 어느 구간이 원인인지 알려주지 않는다. 구간 판별은 위 3개 테이블과 `system.query_log`·`system.part_log`로 충분하다. 샘플러는 별도 서비스로 Kafka 소비와 ClickHouse 조회가 남은 CPU 예산을 쓰고 구현 범위가 가장 크다.
- 비교한 대안: 4단계에 포함하면 완료 기준의 조회 가능 지연을 실측할 수 있지만 위 비용이 든다. `ingested_at - event_timestamp`는 적재 시각을 조회 가능 시각으로 간주하지 않는다는 원칙에 따라 조회 가능 지연으로 표시하지 않는다.
- 제약: 대시보드는 생성 → 조회 가능 지연을 표시하지 않는다. 데이터 최신성(현재 시각 − 최신 `event_timestamp`)은 [Grafana PRD](05-grafana-dashboard.md#처리지연-측정과-표시)대로 표시하며 이벤트별 지연과 구분한다. 필요해지면 이 절을 갱신하고 샘플러를 추가한다.

## 완료 기준

측정 부하는 3단계와 같은 5,000 EPS 지속 부하로 고정한다. 단계별 지연·대시보드 조회 시간 목표는 첫 측정 결과를 기준으로 확정해 이 절에 기록한다. 아래 계측은 요구사항이며 아직 구현·검증하지 않았다.

- 선택 근거: 3단계와 같은 부하를 써서 Spark 결과와 비교할 수 있다. 실측 전에 지연 목표를 정하면 근거가 없는 숫자가 된다. 3단계도 측정 후 기준을 확정하는 방식으로 진행했다.
- 비교한 대안: 목표를 먼저 고정(예: Kafka → Spark p95 ≤ 10초, 15분 조회 p95 ≤ 1초)하면 합격 여부가 명확하지만 수치 근거가 약하다.

INSERT 성공 의미와 조회 경로, 중복 제거를 확인한다. 운영 지표 3개 테이블에 구간별 지표가 배치·구간마다 기록되는지 확인한다. 쓰기 지연과 적재 실패를 각각 주입해 해당 구간 지표(`orders_write_ms`, `query_log` INSERT 시간, offset backlog)가 바뀌는지로 병목 구간을 판별할 수 있는지 검증한다.
