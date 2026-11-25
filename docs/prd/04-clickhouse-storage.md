# ClickHouse 적재

- 상태: 완료 (ClickHouse 서비스·스키마·계정, Spark JDBC 적재, Spark·생성기 운영 지표 기록 구현·검증, 5,000 EPS 지속 부하(12분·25분)·장애 주입 측정 완료, DLQ 쓰기 실패 주입 부분 통과 후 멈춤 감시·자동 재시작 구현·검증, WSL 시계 대응(timesyncd 비활성화) 적용·검증. 호스트 멈춤 기록·`asynchronous_metric_log` 추가 구현·검증(2026-11-19). [측정 후 남은 작업](#측정-후-남은-작업-2026-11-18) 모두 종료)
- 적용 범위: 제품 v1

## 목표

변환된 주문 데이터를 ClickHouse에 적재하고 조회한다.

## 범위 및 후속 정의 사항

정상 주문의 입력 매핑은 [Spark 평탄화 매핑](03-stream-processing.md#정상-주문-평탄화-매핑)을 따른다. 현재 정의는 주문 이벤트 1건당 1행, 이벤트 컬럼 31개와 Kafka 추적 컬럼 4개다. ClickHouse 타입·추가 컬럼은 [컬럼 타입](#order_events-컬럼-타입), 저장 계층은 [저장 계층](#저장-계층), 운영 지표 저장 구조는 [운영 지표 저장](#운영-지표-저장)을 따른다.

자원 예산은 [개발환경](00-development-environment.md)의 WSL 메모리 12GB·CPU 8개를 기준으로 한다. 운영·측정에서는 Kafka UI를 제외한다. 현재 Compose 상한은 Kafka CPU 3·2GiB, Spark CPU 4·3GiB이며, 나머지 서비스의 상한은 [자원 제한](#자원-제한)을 따른다. CPU 상한의 합이 VM의 8개를 넘는 배분 근거는 [PRD 03 자원 계산](03-stream-processing.md#ack-미달-원인-대조와-자원-계산-2026-11-24)에 있다. Kafka UI는 Compose의 `tools` 프로필로 실행한다([Kafka PRD](02-kafka-ingestion.md#kafka-ui)).

적재는 [Spark PRD의 역할 구분](03-stream-processing.md#스택별-역할과-적재-방식)에 따라 `foreachBatch` → Spark JDBC writer → ClickHouse 공식 JDBC Driver → ClickHouse 순서로 수행한다. 정상 주문 DataFrame은 append 방식으로 기록한다.

Spark 변환 구현 전에 조회 요구사항을 바탕으로 한 행의 의미, 컬럼·타입·필수값·시간대·금액 단위와 원본 추적 컬럼을 정의한다. 부분 적재·재시도에 따른 중복 처리 정책과 필요한 테이블 엔진·정렬 키 초안도 함께 정한다. 확정한 계약은 이 문서와 Spark PRD에 함께 반영한다.

4단계에서는 실제 테이블과 JDBC 연동을 구성하고 적재 실패·중복 처리, 집계 정확성과 스택 단계별 지연 지표 기록을 검증한다. 샘플 이벤트의 조회 가능 지연(생성 → 최초 조회 확인) 측정은 v1 범위에서 제외한다([조회 가능 지연 제외](#조회-가능-지연-제외)). 엔진·중복 키·조회 경로·컬럼 타입·보관 정책은 아래 정책을, 버전·DDL 관리·JDBC 쓰기 설정·계정·자원 제한은 [실행 구성](#실행-구성)을 따른다.

마이크로배치 전체나 정상 적재·DLQ 기록 사이의 원자성을 가정하지 않는다. `batch_id` 기록만으로 중복 제거가 보장되지는 않으며, 중복 처리 정책은 재시작과 일부 JDBC 쓰기 성공 후 실패하는 경우를 포함해야 한다.

참고: [ClickHouse Spark JDBC 연동](https://clickhouse.com/docs/integrations/apache-spark/spark-jdbc), [Spark foreachBatch 쓰기 보장](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#using-foreach-and-foreachbatch).

## 저장 계층

메달리안 구조(Bronze·Silver·Gold)는 아래와 같이 적용했다. ClickHouse 스키마·조회 권한 검증은 [1차 구현 검증](#1차-구현-검증-2026-11-17)에 기록했다.

| 층 | 위치 | 내용 |
| --- | --- | --- |
| Bronze (원본) | Kafka 원본 토픽 `raw.ecommerce.orders.v1` | Producer가 보낸 원본 bytes. 보관 기준 1시간·파티션당 8GiB |
| Silver (정제) | `silver.order_events` | Spark 검증·평탄화를 통과한 정상 주문. 이벤트당 1행(재시도 중복 가능) |
| Gold (조회) | `gold.orders_deduplicated`, `gold.order_event_latency`와 대시보드 집계 뷰 | `FINAL`로 중복을 제거한 주문 조회 경로와 이벤트 단위 지연 조회 경로. 테이블이 아닌 `SQL SECURITY DEFINER` 뷰 |
| 운영 | `ops.*` | 파이프라인 운영 지표. 메달리안 층에 속하지 않음 |

- 이 문서와 Grafana PRD의 `order_events`는 `silver.order_events`, `orders_deduplicated`는 `gold.orders_deduplicated`를 가리킨다.
- 선택 근거: 층 구분을 구조적으로 보여주는 것이 목적이다. 데이터베이스 분리는 추가 저장·CPU 비용 없이 이름만으로 층을 드러낸다. 계정 권한도 데이터베이스 단위로 나눌 수 있다(Grafana 읽기 계정은 `gold`·`ops`만). 이를 위해 Gold 뷰는 정의자 권한으로 실행한다. 상세는 [계정과 권한](#계정과-권한)을 따른다.
- 비교한 대안:
  - 문서에만 표기: 변경이 없지만 실제 스키마에서 층이 보이지 않는다.
  - ClickHouse Bronze 테이블 추가: 원본을 24시간 보관해 재처리 범위가 늘어난다. 하지만 원본 행당 약 900바이트·하루 4억 행 이상으로 쓰기량이 약 2배가 된다. 공유 CPU 환경에서 병합 부하 위험이 크므로 도입하지 않는다.
  - Gold 집계 테이블(1분 롤업 refreshable MV): [중복 제거·조회 조합 비교](#중복-제거조회-조합-비교)의 조합 D와 같다. 화면 반영 지연이 최대 60초 늘어나므로 직접 조회가 시간 예산을 넘을 때 도입한다.
  - ClickHouse DLQ 테이블: 대시보드에서 DLQ 사유별 건수·샘플 원문을 볼 수 있다. 하지만 Spark 쓰기 대상과 실패 경로가 하나 늘어난다. v1은 `ops.spark_batch_attempts`의 배치별 DLQ 건수만 표시하고, 사유별 분석이 필요해지면 도입한다. 원문은 Kafka DLQ 토픽(보관 기준 6시간·파티션당 12GiB)에서 확인한다.
- 이벤트를 저장하는 물리 테이블은 `silver.order_events` 하나다. 상품 분리 테이블은 주문당 상품이 1종이므로, 차원 테이블은 차원 값이 이벤트에 들어 있으므로 두지 않는다.
- 제약: Kafka는 시간·용량 기준에 따라 세그먼트 단위로 비동기 삭제한다. 원본 재처리 범위는 실행 시점에 브로커에 남은 오프셋으로 확인한다. Silver에는 이벤트 시각 기준 24시간 TTL을 적용한다. [Kafka 보관 설정](02-kafka-ingestion.md).

## order_events 컬럼 타입

구현된 스키마는 Spark 평탄화 매핑의 31개 이벤트 컬럼과 Kafka 추적 컬럼 4개에 Spark 식별·처리 시각 컬럼과 서버 적재 시각을 더한다.

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

모든 컬럼에 코덱을 적용했다. 기본 LZ4와 비교한 실측은 아래에 기록했다.

| 컬럼 | 코덱 |
| --- | --- |
| 시각 컬럼 4개(`event_timestamp`, `kafka_append_timestamp`, `spark_processed_at`, `ingested_at`) | `CODEC(Delta, ZSTD(1))` |
| `Int64`·`Int32` 컬럼(금액·수량·`kafka_partition`·`kafka_offset`·`spark_batch_id`) | `CODEC(T64, ZSTD(1))` |
| `current_url`, `referrer_url` | `CODEC(ZSTD(3))` |
| 그 외 `String`·`LowCardinality(String)`·`Bool` | `CODEC(ZSTD(1))` |

- 선택 근거: 로컬 WSL 환경의 디스크 용량이 적기 때문이다. 기본 코덱 LZ4 대비 디스크 사용량을 줄이는 것이 목적이다. 비교 실측과 일일 용량 환산은 아래 결과에 기록했다. 코덱 변경은 파티션·보관 기간과 달리 보관 정책과 조회 결과에 영향이 없다.
  - `Delta`: 이웃 값의 차이만 저장한다. 시각은 part 안에서 `event_timestamp` 순으로 정렬돼 차이가 작다. 단독으로는 압축하지 않는 전처리 코덱이므로 ZSTD를 뒤에 붙인다.
  - `T64`: 정수에서 쓰지 않는 상위 비트를 잘라낸다. 금액·offset은 `Int64` 범위보다 훨씬 작다.
  - ZSTD 레벨: 기본 레벨 1로 시작한다. URL은 길고 반복이 많아 레벨 3을 쓴다. 레벨을 올리면 INSERT·병합 CPU가 늘어나므로 공유 CPU 환경에서 적재·병합 부하를 실측한 뒤 조정한다.
  - 참고: [Column compression codecs](https://clickhouse.com/docs/reference/statements/create/table/codec), [CREATE TABLE의 기본 압축](https://clickhouse.com/docs/sql-reference/statements/create/table)
- 비교한 대안:
  - 기본 LZ4 유지: CPU가 가장 적지만 디스크 사용량이 가장 크다.
  - `DoubleDelta`: 간격이 일정한 시계열에 적합하다. 주문 이벤트 시각은 간격이 일정하지 않으므로 `Delta`를 쓴다.
  - 파티션을 잘게 나눠 초과 보관 줄이기: 1시간 파티션에서는 같은 part 안에서 가장 이른 행과 가장 늦은 행의 TTL 만료 시각 차이가 1시간 미만이다. 파티션을 더 나누면 part가 늘고 압축률이 떨어져 이득이 거의 없다.
- 검증: 4단계에서 같은 데이터로 `system.columns`의 `data_compressed_bytes`·`data_uncompressed_bytes`를 LZ4와 비교한다. 압축률이 낮거나 병합 CPU가 예산을 넘는 컬럼은 코덱을 조정하고 결과를 이 절에 기록한다.
- 결과 (2026-11-18): 같은 3,060,098행에서 코덱 적용 198.3MB, 기본 LZ4 396.0MB로 절반이다(압축 전 905.9MB, 압축률 4.57:1 대 2.29:1). 행당 디스크 64.8바이트로 가정(30~60바이트)보다 약간 크며, 하루 3.67억 행이면 약 23.8GB다. 무작위 ID 문자열 3개(`event_id`·`order_id`·`session_id`)가 압축 후 크기의 83%다. 상세는 [지속 부하 측정 결과](#지속-부하-측정-결과-2026-11-18)를 따른다.

## 실행 구성

현재 실행 구성이다. 초기 버전·적재·스키마·권한 검증은 [1차 구현 검증](#1차-구현-검증-2026-11-17)에 기록했다.

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
- 제약: 공식 문서에 JDBC 드라이버·ClickHouse 서버·Spark 버전 호환 표가 없다. 실제 적재 결과는 [1차 구현 검증](#1차-구현-검증-2026-11-17)에 기록했다. 0.11.0부터 `system.query_log`의 `http_user_agent` 형식이 바뀌었으므로 INSERT 식별은 사용자 이름과 쿼리 종류로 한다.

### DDL 관리

- DDL은 `infra/clickhouse/`에 둔다. 스키마는 `schema.sql`, 계정·권한은 비밀번호를 환경변수로 받는 `init.sh`가 적용한다.
- Compose에 일회성 `clickhouse-init` 서비스를 둔다. 같은 ClickHouse 이미지의 `clickhouse-client`로 `clickhouse` 서비스 healthy 후 실행한다. Spark·Grafana는 `service_completed_successfully` 조건으로 이 서비스 완료 후 시작한다.
- 테이블과 `gold.orders_deduplicated`는 `IF NOT EXISTS`로 생성한다. `gold.order_event_latency`는 `CREATE OR REPLACE VIEW`로 정의를 갱신해 기존 볼륨에서도 `ingested_at`을 노출한다. 계정은 `CREATE USER OR REPLACE` 후 권한을 다시 부여해 `.env`의 비밀번호 변경을 반영한다.
- 선택 근거: 기존 `kafka-init`과 같은 방식으로 스키마와 권한을 초기화한다. 기존 테이블 변경은 별도 마이그레이션을 적용한다.
- 비교한 대안: 이미지의 `/docker-entrypoint-initdb.d`는 별도 서비스가 없다. 하지만 [entrypoint.sh](https://github.com/ClickHouse/ClickHouse/blob/master/docker/server/entrypoint.sh)는 데이터 디렉터리에 `data`가 있으면 "Skipping initialization"만 출력하고 스크립트를 실행하지 않는다. 볼륨이 남아 있으면 DDL 변경이 조용히 빠진다.
- 제약: `IF NOT EXISTS`는 기존 테이블의 컬럼·설정을 바꾸지 않는다. 스키마 변경은 `schema.sql`에 `ALTER` 문을 추가하거나, 보관이 24시간이므로 볼륨을 지우고 다시 만든다. 어느 방식을 썼는지 변경 기록에 남긴다.

### JDBC 쓰기 설정

| 옵션 | 값 | 기본값 |
| --- | --- | --- |
| `batchsize` | 50000 | 1000 |
| `numPartitions` | 3 | 없음(DataFrame 파티션 수) |
| URL 서버 설정 | `clickhouse_setting_async_insert=0` | 서버 기본값 |
| `isolationLevel` | 기본값 유지 | `READ_UNCOMMITTED` |

- 현재 설정은 트리거 5초, Kafka 입력 상한 `SPARK_MAX_OFFSETS_PER_TRIGGER=65000`, JDBC `batchsize=50000`, `numPartitions=3`이다. 입력 상한은 50,000 → 55,000 → 65,000으로 변경했다. 65,000 채택 근거는 [PRD 03 B1 실험](03-stream-processing.md#b1-배치-상한-65000-실험-2026-11-23)에 있다.
- INSERT 수는 각 쓰기 파티션의 `ceil(행 수 / batchsize)`를 더해 계산한다. 정상 주문 4,250건/s가 5초간 들어와 세 파티션에 고르게 분배되면 배치당 약 21,250행, INSERT당 약 7,083행, 배치당 3회다. 5초 주기 기준 평균 초당 0.6회다.
- `batchsize=50000`을 유지한다. 65,000건 중 정상 주문이 약 85%이고 고르게 분배되면 쓰기 파티션당 약 18,400행이다. B1 catch-up의 실제 INSERT당 최댓값은 18,836행이었다.
- 이론상 정상 주문이 한 파티션에 50,000행을 초과하면 해당 파티션은 INSERT 2회가 필요하다. 입력 상한 65,000건에서는 두 파티션이 동시에 이 조건을 충족할 수 없으므로 재시도를 제외하면 배치당 최대 4회다. 5초 주기 기준 평균 초당 0.8회로, [ClickHouse INSERT 전략 권장](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy)의 초당 약 1회를 기준으로 삼은 목표 안이다.
- `numPartitions=3`은 동시 JDBC 연결을 3개로 제한한다. D1 이후에는 Kafka 파티션 3개를 그대로 써 `coalesce`가 일어나지 않는다.
- `async_insert=0`: Spark가 이미 클라이언트에서 행을 묶으므로 동기 INSERT를 쓴다. 공식 문서는 클라이언트에서 묶을 수 있으면 동기 INSERT를 권장한다. 동기 INSERT는 응답이 곧 저장 완료라 [부분 실패와 재시작](#부분-실패와-재시작)의 실패 판단이 단순하다. 0.10부터 드라이버가 이 값을 강제하지 않으므로 서버 기본값에 기대지 않고 명시한다.
- `isolationLevel`: Spark는 드라이버가 트랜잭션을 지원하지 않으면 이 값을 쓰지 않는다. ClickHouse는 JDBC 트랜잭션을 쓰지 않으므로 기본값을 둔다.
- 초기 50,000 상한에서 비교한 결과, JDBC 기본값 1,000은 평소 배치당 24회, 10,000은 평소 3회였지만 당시 backlog 추정에서는 최대 6회였다. 초기 정상 주문 행 수 실측은 [3단계 2단 검증 지속 부하](../measurements/2026-11-17-spark-sustained-load-2stage.json)에 있다.

- 제약:
  - 회당 행 수는 `batchsize`가 아닌 입력량이 정한다. 평소 회당 약 7,083행으로 권장 최소 1,000행 이상이지만 공식 이상값(10,000~100,000)보다 작다. EPS가 낮아져 파티션이 1,000행 미만이 되면 `batchsize`로 막을 수 없다.
  - backlog 배치는 파티션마다 INSERT 1회에 최대 50,000행을 VALUES 문자열로 만든다. 실제 JDBC 쓰기 크기와 Spark 메모리는 [B1 실험](03-stream-processing.md#b1-배치-상한-65000-실험-2026-11-23)에 기록했다.
  - `batchsize`는 가장 큰 쓰기 파티션 행 수 기준이다. `SPARK_MAX_OFFSETS_PER_TRIGGER`나 shuffle 파티션 수를 바꾸면 조건을 다시 확인하며, 값은 환경변수로 받는다.
  - 위 동작은 [Spark v4.2.1 `JdbcUtils.savePartition`](https://github.com/apache/spark/blob/v4.2.1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L784)과 [clickhouse-java v0.11.2 `PreparedStatementImpl`](https://github.com/ClickHouse/clickhouse-java/blob/v0.11.2/jdbc-v2/src/main/java/com/clickhouse/jdbc/PreparedStatementImpl.java#L348) 소스로 확인했다. INSERT 1회는 `INSERT ... VALUES (?, ...)` 형태 문에만 해당하며 Spark 일반 append가 이 형태다. 빈 파티션은 INSERT를 보내지 않는다.
  - `numPartitions`는 파티션 수 상한으로만 `coalesce`하며 행 수를 다시 고르게 나누지 않는다. 쓰기 파티션은 Kafka 파티션 3개를 그대로 따르며(2026-11-23 D1로 셔플 제거) 파티션 균등은 Kafka 분배에 달려 있다.
  - 태스크·`foreachBatch` 재실행은 INSERT를 추가한다. INSERT 수와 part 수는 다르다. INSERT에 여러 시간 파티션의 행이 포함되면 여러 part가 생길 수 있다.
  - 4단계 실측 항목: 파티션별 행 수, `system.query_log`의 INSERT 수·`written_rows`·시각(`executeBatch` 1회 = INSERT 1회 실측 포함), 생성 part 수, 배치 처리시간·재시도, Spark 메모리.
  - Spark JDBC는 시각을 `setTimestamp`로 보낸다. 이 경로의 시간대 처리는 [JDBC 날짜·시간 가이드](https://clickhouse.com/docs/integrations/language-clients/java/jdbc_date_time_guide)에 명시돼 있지 않다. Spark 세션 시간대는 이미 `UTC`이며, UTC 값이 그대로 저장된 결과는 [1차 구현 검증](#1차-구현-검증-2026-11-17)에 기록했다.
- 참고: [Spark JDBC 옵션](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html), [ClickHouse Spark JDBC](https://clickhouse.com/docs/integrations/apache-spark/spark-jdbc), [비동기 INSERT](https://clickhouse.com/docs/optimize/asynchronous-inserts)

### 계정과 권한

| 계정 | 권한 | 사용처 |
| --- | --- | --- |
| 관리자(`CLICKHOUSE_USER`) | 접근 관리 포함 전체 | `clickhouse-init`과 수동 점검 |
| `spark_writer` | `silver.order_events`·`ops.spark_batch_attempts`·`ops.spark_query_progress`에 INSERT·SELECT | Spark JDBC 적재와 드라이버 지표 |
| `generator_writer` | `ops.producer_stats`에 INSERT | 호스트의 로그 생성기 |
| `monitor_writer` | `ops.host_stalls`에 INSERT | `host-monitor` 컨테이너([호스트 멈춤 기록](#호스트-멈춤-기록)) |
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
- ClickHouse 26.8에서 뷰 권한과 읽기 전용 계정 동작은 [1차 구현 검증](#1차-구현-검증-2026-11-17), 뷰의 범위 조건 가지치기는 [지속 부하 측정 결과](#지속-부하-측정-결과-2026-11-18)에 기록했다.

### 자원 제한

| 서비스 | `cpus` | `mem_limit` | 기타 |
| --- | --- | --- | --- |
| `clickhouse` | 2 | 4g | `ulimits.nofile` 262144 |
| `grafana` | 0.5 | 1g | 6단계에서 512m → 1g ([자원 조정](06-load-profile.md#자원-조정)) |
| `host-monitor` | 1.0 | 256m | 2026-11-24에 0.25에서 상향 |

- 선택 근거:
  - 예산: Kafka 3 + Spark 4 + ClickHouse 2 + Grafana 0.5 + host-monitor 1.0으로 운영 서비스의 CPU 상한 합은 10.5다. VM의 8개 CPU를 공유하며, 상한을 겹쳐 배정한 근거와 실측은 [PRD 03 자원 계산](03-stream-processing.md#ack-미달-원인-대조와-자원-계산-2026-11-24)에 있다.
  - ClickHouse 4GiB는 적재 블록·병합·`FINAL` 조회에 사용한다. Kafka·Spark·ClickHouse·Grafana·host-monitor의 메모리 상한 합은 10.25GiB다.
  - `nofile` 262144: [Docker Hub 이미지 안내](https://hub.docker.com/r/clickhouse/clickhouse-server)의 실행 예시 값이다. 1시간 파티션 25개와 컬럼별 파일로 열린 파일 수가 많다.
  - Grafana 0.5 CPU·1GiB: 쿼리 전송과 렌더링을 담당한다. 메모리 상한은 6단계에서 512MiB에서 1GiB로 올렸다.
  - 호스트 감지기 1.0 CPU·256MiB: 기존 0.25 CPU에서 발생한 감지기 자체의 throttle과 상향 근거는 [호스트 멈춤 기록](#호스트-멈춤-기록)에 있다.
- 저메모리 설정: [ClickHouse 운영 팁](https://clickhouse.com/docs/operations/tips)은 메모리 16GB 미만에서 기본값이 메모리 예외를 일으킬 수 있다고 안내한다. `infra/clickhouse/config.d/`에 아래를 적용한다.
  - `metric_log`, `trace_log`, `text_log`를 끈다. 대시보드가 쓰는 `query_log`·`part_log`는 유지한다.
  - `asynchronous_metric_log`는 켜고 TTL `event_date + INTERVAL 3 DAY DELETE`를 둔다(2026-11-19 변경). 처음에는 저메모리 설정으로 껐지만, 2026-11-19 01:50(KST) 지연 급증을 조사할 때 그 시각의 CPU·메모리·IO 수치가 없어 원인을 좁히지 못했다([호스트 멈춤 기록](#호스트-멈춤-기록)). 이 테이블은 서버의 비동기 지표(`OSMemoryAvailable`, `LoadAverage1`, `OSIOWaitTimeNormalized` 등)를 초 단위로 남긴다. 컨테이너 안에서 읽는 `/proc` 값이라 WSL VM 전체 수치다. 보존 기간은 `query_log`와 같은 이유로 3일이다. 하루 행 수와 디스크 사용량은 켠 뒤 실측해 [구현 기록](#구현-기록)에 남긴다.
  - `query_log`·`part_log`에 TTL `event_date + INTERVAL 3 DAY DELETE`를 둔다. 기본값에는 TTL이 없어 무기한 쌓인다. 대시보드(15분)·경보(5분)는 24시간이면 충분하지만, 지난 측정을 다음 날 다시 조사할 수 있게 3일로 둔다. 2026-11-18 부하 측정 하루분이 두 테이블 합계 약 23MiB였으므로 3일분도 작다. `silver.order_events`와 일반 운영 지표에는 24시간 TTL을 적용한다. `ops.host_stalls`와 `system.asynchronous_metric_log`에는 3일 TTL을 적용한다.
  - TTL을 바꾸면 ClickHouse가 기동 시 기존 테이블을 `query_log_N`·`part_log_N`으로 이름을 바꾸고 새 테이블을 만든다. 옛 테이블은 자동 삭제되지 않으므로 확인 후 직접 지운다.
  - mark cache를 512MiB로 줄인다. 문서는 약 500MB까지 줄일 수 있다고 한다.
  - 문서의 다른 항목(쿼리 처리 스레드 1개, `max_block_size` 축소, 병렬 파싱 끄기)은 적용하지 않는다. CPU 2개에서 `FINAL` 조회와 병합을 같이 해야 하므로 스레드를 1개로 묶으면 조회 시간이 늘어난다. 메모리 예외가 실측되면 다시 검토한다.
- 비교한 대안:
  - ClickHouse CPU 3개: 남은 CPU를 모두 주면 병합 여유가 커지지만 포화 시 생성기 EPS가 떨어져 측정 부하가 흔들린다.
  - 메모리 제한 없음: 예외는 줄지만 WSL 12GB 안에서 Kafka·Spark와 경합해 OOM 대상이 예측되지 않는다.
- 서버 메모리 상한이 컨테이너 4GiB 제한을 반영한 결과는 [1차 구현 검증](#1차-구현-검증-2026-11-17), CPU·메모리 사용률과 활성 part 수는 [지속 부하 측정 결과](#지속-부하-측정-결과-2026-11-18)에 기록했다.

## JDBC 재시도와 적재 중복 처리

제품 v1은 재시도로 물리 행이 중복돼도 정상 조회에서 이벤트당 한 건으로 계산한다. 복구 후 건수 대조는 [장애 주입 측정 결과](#장애-주입-측정-결과-2026-11-18)에 기록했다.

### 이벤트 식별과 테이블 기준

- 논리 중복 키는 `event_id`다. v1 주문 완료 이벤트는 불변으로 취급하며 같은 `event_id`의 재전송·재처리는 이벤트 시각·주문 금액 등 원본 업무 필드가 동일해야 한다. 다른 ID의 주문은 값이 같아도 별개로 유지한다.
- 정상 주문 테이블의 이름은 `order_events`, 엔진은 버전 인자 없는 `ReplacingMergeTree()`, 정렬 키는 `ORDER BY (event_timestamp, event_id)`로 정한다. 선택 근거는 [중복 제거·조회 조합 비교](#중복-제거조회-조합-비교)를 따른다. 컬럼 타입은 [order_events 컬럼 타입](#order_events-컬럼-타입)을 따른다.
- 보관 기간은 24시간이다. 파티션과 TTL은 아래로 정한다.

  ```sql
  PARTITION BY toStartOfHour(event_timestamp)
  TTL toDateTime(event_timestamp) + INTERVAL 24 HOUR
  SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600
  ```

  - 선택 근거: [ClickHouse 파티션 키 권장](https://clickhouse.com/docs/best-practices/choosing-a-partitioning-key)은 파티션을 쿼리 최적화가 아닌 데이터 수명 관리 용도로 쓰고 값 종류를 100~1,000개 미만으로 두도록 한다. 24시간 보관에 1시간 파티션이면 활성 파티션이 약 25개다. `ttl_only_drop_parts`로 만료된 시간 단위 part를 다시 쓰지 않고 통째로 삭제해 남은 CPU 예산의 TTL 병합 부담을 줄인다. 24시간은 행의 TTL 만료 기준이다. 실제 삭제 시점은 part의 모든 행이 만료되는 시각과 백그라운드 작업 일정에 따라 달라진다.
  - `ttl_only_drop_parts`: 26.8 소스(`MergeTreeSettings.cpp`) 기준 기본값은 `false`다. 켜면 part의 모든 행이 만료됐을 때 part 전체를 삭제하고, 끄면 만료된 행만 지운다.
  - `merge_with_ttl_timeout = 3600`은 삭제 TTL 병합을 다시 실행하기 전 최소 대기 시간이다. 26.8 기본값 14,400초보다 짧게 설정해 만료된 part의 삭제를 더 자주 시도할 수 있게 한다. 실제 삭제 시점은 별도 실측 기록이 없다.
  - 중복 제거와의 관계: ReplacingMergeTree는 같은 파티션 안에서만 행을 합친다. 같은 `event_id`는 같은 `event_timestamp`를 가진다는 불변 전제로 같은 이벤트는 항상 같은 파티션에 들어간다.
  - 비교한 대안: 일 단위 파티션은 같은 part에 포함될 수 있는 시각 범위가 넓어 가장 이른 행이 만료된 뒤 part 전체가 만료되기까지 더 오래 걸릴 수 있다. 파티션 없는 행 단위 TTL은 TTL 병합마다 part를 다시 써 CPU를 쓴다.
  - 제약: 24시간보다 오래된 이벤트는 INSERT 직후 삭제 대상이 되고, 한 INSERT 블록에 시간 파티션이 100개를 초과하면 `max_partitions_per_insert_block=100` 제한에 걸린다. 그래서 Spark가 `[콜백 진입 − 24시간, 콜백 진입 + 5분]` 밖 주문을 적재하지 않고 DLQ(`SEMANTIC_TIMESTAMP_OUT_OF_RANGE`)로 보낸다([이벤트 시각 범위 규칙](03-stream-processing.md#이벤트-시각-범위-규칙)). TTL을 바꾸면 이 하한도 함께 바꾼다. 하루 약 3.67억 행과 [압축 코덱 실측](#압축-코덱)의 행당 64.8바이트를 기준으로 환산하면 약 23.8GB다.
- 정렬 키에 `event_timestamp`를 둔 것은 조회 시간 범위로 읽을 구간을 줄이기 위해서다. 같은 `event_id`는 같은 `event_timestamp`를 가진다는 불변 전제에서만 중복 행이 같은 정렬 키로 모인다. 이 전제가 깨지면 `ReplacingMergeTree`와 `FINAL`은 두 행을 합치지 않는다.
- Kafka 위치·append 시각, Spark 쿼리·실행·배치 ID, 적재 시각은 중복 키에 넣지 않는다. 재발행·재실행에 따라 값이 달라도 같은 이벤트로 취급한다. 재시도 시 `event_id`를 새로 생성하지 않는다.
- 같은 키의 여러 행 중 보존되는 행에 따라 추적 메타데이터가 달라질 수 있다. 정상 테이블의 메타데이터를 모든 시도 또는 최초 수신 이력으로 간주하지 않는다. 시도별 운영 지표는 별도로 기록한다.
- 같은 ID에 서로 다른 업무 내용이 들어오는 경우는 정상 중복이 아닌 계약 위반이다. 현재 생성기는 매 주문마다 새 ID를 발급하며, 이 정책은 동일 업무 내용의 재전송·재처리를 대상으로 한다. 다른 내용의 충돌 감지·격리는 별도 설계 항목이며 이 엔진만으로 구현됐다고 간주하지 않는다.
- 같은 ID에 다른 `event_timestamp`가 들어온 충돌은 합치지 않고 드러낸다. 어느 행이 맞는지 정할 규칙이 없으므로 한 행을 임의로 고르지 않는다. Grafana는 `gold.order_event_latency`에서 해시로 그룹화하고 이벤트 시각의 최솟값·최댓값을 비교해 최근 1시간의 충돌을 1분마다 평가한다. 경보 상태를 주문 대시보드에 표시한다. 충돌이 0이 아닌 구간의 주문 집계는 부풀 수 있음을 함께 표시한다. 두 시각이 점검 구간보다 멀리 떨어진 충돌은 이 쿼리로 찾지 못한다. 전체 구간 점검은 24시간 분량에서 메모리 4GB를 넘어 v1에서 실행하지 않는다. 점검 구간·주기와 Grafana 계정으로 실행하는 쿼리는 [Grafana 경보](05-grafana-dashboard.md#경보)를 따른다.

  ```sql
  -- 4단계 관리자 검증용. 같은 event_id에 서로 다른 event_timestamp가 있는 충돌 건수를 센다.
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
5. Spark 중복 제거는 JDBC 재시도 중복의 방어선이 아니다. v1은 처음에 배치 내 `dropDuplicates`를 썼으나, 입력 중복이 사실상 없고 평시 피크 배치당 셔플 비용이 336~375ms라 2026-11-23 제거했다([Spark D1](03-stream-processing.md#입력-중복과-jdbc-적재-중복의-구분)). 워터마크 기반 상태 중복 제거도 도입하지 않는다. 근거는 [중복 제거·조회 조합 비교](#중복-제거조회-조합-비교)를 따른다.

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

- 대시보드 쿼리는 항상 `event_timestamp` 범위 조건을 포함한다. 범위 조건이 없으면 전체 누적 행을 읽는다. 뷰의 범위 조건이 정렬 키 가지치기에 쓰이는 동작은 [지속 부하 측정](#지속-부하-측정-결과-2026-11-18)에서 읽은 행 수로 확인했다.
- 뷰는 `SQL SECURITY DEFINER`로 만든다. 정의자 `gold_definer`의 권한으로 `silver`를 읽으므로 Grafana 읽기 계정에는 `silver` 권한을 주지 않는다. 근거는 [계정과 권한](#계정과-권한)을 따른다.

- 주문 수·매출·할인·판매 수량·평균 주문 금액은 동일한 중복 제거 경로를 사용한다. 데이터 최신성은 `gold.order_event_latency`에서 조회 범위 내 최신 `event_timestamp`를 읽어 계산한다. 주문 수만 `uniqExact(event_id)`로 계산하고 매출은 원본 테이블에서 합산하는 방식은 사용하지 않는다.
- `OPTIMIZE FINAL` 실행이나 백그라운드 병합 완료를 조회 정확성의 전제조건으로 삼지 않는다. 일반 INSERT 기반 사전 합산 Materialized View를 추가하면 중복이 누적될 수 있으므로, 현재는 중복 제거 후 조회 집계를 기준으로 한다.
- 원본 테이블의 물리 행 수가 즉시 유일해지는 것, 미완료 배치의 모든 행이 한꺼번에 보이는 것, DLQ와 정상 출력의 원자성은 보장하지 않는다. 최종 배치 성공 후 누락 없이 논리 중복이 제거된 결과를 검증한다.
- `FINAL`과 `(event_timestamp, event_id)` 정렬 키의 조회 비용은 [15분 이상 지속 부하 측정](#15분-이상-지속-부하-측정-결과-2026-11-18)에 기록했다. 시간 파티션 경계에서도 정렬 키와 집계가 동작하는 결과는 [충돌 점검](#충돌-점검-쿼리-검증-결과-2026-11-18)에 있다. 보관 데이터 삭제 후의 과거 재투입을 영구적으로 차단하는 정책은 아니다.
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
| **B (채택)** | 없음(2026-11-23 D1 전에는 배치 내 `dropDuplicates`) | `ReplacingMergeTree()`, `ORDER BY (event_timestamp, event_id)` | `FINAL` 뷰 + 시간 범위 | 15분 구간 약 382.5만 행. 누적량과 무관 | 불변 전제에서 정확 | T + 배치 + 60초 + 조회시간 |
| C | 워터마크 기반 상태 중복 제거 | B와 동일 | B와 동일 | B + Spark 상태 저장. 워터마크 10분이면 약 255만 키 | B와 동일. 추가 이득 없음 | B와 동일 |
| D | 배치 내 `dropDuplicates` | 원본은 B와 동일. 1분 롤업 테이블은 `MergeTree`이며 refreshable materialized view가 `FINAL` 결과로 교체 | 롤업 테이블 | 대시보드 조회는 수십 행. 분당 1회 재계산이 B 조회 1회분 | 재계산 시점 기준 정확 | B + 롤업 주기 최대 60초 |
| E | 워터마크 기반 상태 중복 제거 | `MergeTree`, 중복 제거 없음 | 원본 테이블 직접 | 가장 낮음 | 부정확. JDBC 재실행 중복이 합산됨 | B와 동일 |

B를 채택한 이유는 다음과 같다.

- **A:** 정렬 키가 `event_id`뿐이면 시간 범위 조건으로 읽을 구간을 줄일 수 없다. 갱신을 1분으로 늦춰도 조회 1회 비용이 누적 행 수에 비례해 계속 늘어난다.
- **B:** `ReplacingMergeTree`와 `FINAL`을 유지하므로 A와 같은 정확성을 가지며, 정렬 키 앞의 `event_timestamp`로 조회 구간만 읽는다. v1 이벤트는 불변이므로 같은 `event_id`의 중복 행은 같은 정렬 키로 모인다.
- **B의 Spark 중복 제거(2026-11-23 갱신):** 배치 내 `dropDuplicates`는 아래 C와 같은 이유로 정확성 이득이 없어 제거했다. 조회 정확성은 `ReplacingMergeTree`·`FINAL`로 같다.
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

제품 v1의 운영 지표 저장 구조다. Spark·생성기 지표와 호스트 감지기의 검증은 [구현 기록](#구현-기록)에 있다.

- 지연 지표 저장소는 ClickHouse 하나로 한다. Grafana 운영 대시보드는 주문 대시보드와 같은 ClickHouse 데이터 소스를 조회한다. Prometheus 등 별도 지표 저장소는 v1에 도입하지 않는다.
- 선택 근거: 대시보드 요구 지연은 대부분 이벤트·배치 단위이며 정확한 p50·p95·p99와 측정 건수·측정 불가를 표시해야 한다. 원본 값을 행으로 저장하면 SQL `quantile` 계열 함수로 계산할 수 있고, 주문 데이터와 같은 SQL로 대조할 수 있다. 히스토그램 기반 수집은 근사 분위수이고 이벤트별 지연을 남기지 않는다. 별도 저장소는 backlog 같은 인프라 게이지와 알림이 필요해질 때 다시 검토한다.
- **이벤트 단위 지연:** 정상 주문 행의 시각 컬럼을 사용해 다음 네 값을 계산한다.

  | 지표 | 계산 |
  | --- | --- |
  | 생성 → Kafka | `kafka_append_timestamp - event_timestamp` |
  | Kafka → Spark 콜백 진입 | `spark_processed_at - kafka_append_timestamp` |
  | Spark 콜백 진입 → ClickHouse 서버 적재 시각 | `ingested_at - spark_processed_at` |
  | 생성 → ClickHouse 서버 적재 시각 | `ingested_at - event_timestamp` |

  - `ingested_at`은 서버 기본값으로 기록한 시각이다. 최초 조회 가능 시점은 [조회 가능 지연 제외](#조회-가능-지연-제외)를 따른다.
  - Grafana는 `silver`를 직접 읽지 않고 아래 `gold.order_event_latency` 뷰로 조회한다. `FINAL`은 쓰지 않는다.

    ```sql
    CREATE OR REPLACE VIEW gold.order_event_latency
    DEFINER = gold_definer SQL SECURITY DEFINER AS
    SELECT event_id, event_timestamp, kafka_append_timestamp, spark_processed_at, spark_run_id, spark_batch_id, ingested_at
    FROM silver.order_events;
    ```

  - 선택 근거: Grafana 계정을 `gold`·`ops`로 제한하기 위한 뷰다([계정과 권한](#계정과-권한)). 지연 계산에 필요한 컬럼만 노출한다. 재처리 중복 행은 실제로 다시 처리된 시도의 지연이므로 합치지 않는다. `FINAL`로 합치면 어느 시도의 행이 남는지 정해지지 않고 병합 비용만 든다.
  - 비교한 대안: `gold.orders_deduplicated`에서 지연 계산은 경로가 하나로 줄지만, 남는 행이 임의라 지연 분위수가 특정 시도를 대표하지 않고 `FINAL` 비용이 든다.
- **배치 단위 지표:** 배치당 1행을 별도 운영 지표 테이블에 저장한다. 원천은 두 곳이다.
  - `foreachBatch` 콜백 지표: 쿼리·실행·배치 ID, 상태·실패 단계, 분류·정상 쓰기·DLQ 쓰기 action 시간, 콜백 전체 시간, Kafka → 콜백 지연 분위수, 분류별 건수. 실패한 시도도 1행으로 남겨 재시도를 관측한다.
  - `StreamingQueryListener.onQueryProgress` 진행 정보: 트리거 전체 시간(`durationMs`), 입력·처리 EPS, 파티션별 끝 offset과 소스의 최신 offset. backlog 근사값은 최신 offset과 처리 완료 offset의 차이로 계산한다. 진행 정보는 배치 성공 후 비동기로 전달되므로 콜백 지표와 별도 행으로 저장하고 배치 ID로 연결한다.
- **지표 쓰기 실패 정책:** 지표 INSERT 실패는 경고 로그만 남기고 주문 배치를 실패시키지 않는다. 지표 손실을 허용하며, 빈 구간은 대시보드에 0이 아닌 측정 불가로 표시한다. 기존 지표 로그 출력은 유지해 손실 구간의 대조 근거로 쓴다. 지표 쓰기는 주문 적재 경로와 같은 재시도·중복 정책의 대상이 아니다.
- 배치 재실행으로 같은 배치 ID의 지표 행이 여러 개 생길 수 있다. 이 행은 시도별 기록이므로 합치지 않으며, 시도를 구분할 컬럼(실행 ID·콜백 시작 시각 등)을 둔다.
- **ClickHouse 내부 지표:** INSERT 요청별 처리시간·실패는 `system.query_log`, part 생성·병합은 `system.part_log`에서 SQL로 조회한다. 별도 계측 코드나 Prometheus 엔드포인트를 쓰지 않는다. 대시보드별 원천은 [Grafana 패널별 데이터 원천](05-grafana-dashboard.md#패널별-데이터-원천)을 따른다.
- **지표 INSERT 방식:** Spark 드라이버에서 Python 표준 라이브러리(`urllib`)로 ClickHouse HTTP 인터페이스에 `INSERT ... FORMAT JSONEachRow`를 POST한다. 설정은 `async_insert=1`, `wait_for_async_insert=1`이다. 실패는 위 지표 쓰기 실패 정책에 따라 경고 로그로만 남긴다.
  - 근거 1: 콜백·progress 지표는 이미 JSON으로 직렬화되는 dict다. 변환 없이 그대로 보낼 수 있고 추가 의존성이 없다.
  - 근거 2: Spark JDBC로 쓰면 배치당 1~2행을 위해 DataFrame 생성과 Spark job이 추가된다. `onQueryProgress` 리스너 스레드에서 Spark job을 실행하는 문제도 생긴다. `clickhouse-connect`는 같은 HTTP 방식에 Spark 이미지 의존성만 추가한다.
  - 근거 3: [ClickHouse INSERT 전략 권장](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy)은 동기 INSERT를 1,000행 이상, 초당 1회 수준으로 묶도록 한다. 클라이언트에서 묶을 수 없으면 `async_insert=1`과 `wait_for_async_insert=1`을 강하게 권장한다. 지표는 5초마다 1~2행인 작은 INSERT이므로 서버 측 묶음을 사용한다. `wait_for_async_insert=0`은 오류를 클라이언트가 알 수 없어 사용하지 않는다.
  - 비교에서 제외한 대안: 지표 Kafka 토픽 → ClickHouse Kafka 엔진은 ClickHouse 장애 중에도 지표를 보존하는 실무 구조다. 하지만 토픽·Kafka 엔진 테이블·MV가 추가되고 ClickHouse의 소비 부하가 남은 CPU 예산을 쓴다. v1은 지표 손실을 허용하므로 도입하지 않는다.
  - `callback_total_ms`는 지표 INSERT 호출 전에 확정한다. 지표 INSERT 자체의 대기 시간은 이 값에 포함되지 않으며 별도 컬럼에도 기록하지 않는다.
- **운영 지표 테이블 구조:** `ops` 데이터베이스에 원천별 테이블 4개를 둔다. 모두 `MergeTree`, 파티션 없음이다. 행 단위 TTL은 24시간이고, `ops.host_stalls`만 3일이다.

  | 테이블 | 행 단위 | 쓰는 곳 |
  | --- | --- | --- |
  | `ops.producer_stats` | 생성기 1초 구간당 1행 | 호스트의 로그 생성기 |
  | `ops.spark_batch_attempts` | `foreachBatch` 콜백 시도당 1행. 실패 포함 | Spark 드라이버 |
  | `ops.spark_query_progress` | `onQueryProgress` 배치당 1행 | Spark 드라이버 리스너 |
  | `ops.host_stalls` | 감지한 호스트 멈춤 1건당 1행. TTL 3일 | `host-monitor` 컨테이너([호스트 멈춤 기록](#호스트-멈춤-기록)) |

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
    - 실측으로 확인한 해석 제약(2026-11-18, [장애 주입 측정 결과](#장애-주입-측정-결과-2026-11-18)):
      - offset backlog(`latest_offsets − end_offsets`)는 배치 계획 시점 값이라 `maxOffsetsPerTrigger` 상한에 걸리지 않으면 0이다. 처리가 약간 느려진 상태는 backlog가 아닌 배치 입력 건수 증가, 트리거 시간 5초 초과, Kafka → 콜백 p95 증가로 드러난다. 대시보드는 backlog만으로 정상 판정하지 않는다.
      - 재시작 후 첫 진행 정보는 `latest_offsets`가 빈 Map이다. 이 행의 backlog는 음수가 되므로 빈 Map이면 측정 불가로 표시한다.
      - 권한·인증 오류 INSERT는 `query_log`에 `ExceptionBeforeStart`, `tables`가 빈 배열로 남는다. INSERT 실패 패널은 `tables`가 아닌 `user = 'spark_writer'`와 `exception_code != 0`으로 거른다.
      - 실패한 INSERT의 `written_rows`는 실제로 남은 행 수가 아니다. 실패한 쓰기가 남긴 중복은 `silver.order_events`의 물리 행 수와 고유 `event_id` 수 차이로 센다.
  - 컬럼은 아래로 정한다. 시각은 `DateTime64(3, 'UTC')`, 분위수는 `Float64`, 건수는 `UInt64`다. 측정 대상이 없는 구간·배치의 분위수는 Nullable 대신 건수 0으로 구분하고 분위수 값은 0으로 둔다. 대시보드는 건수 0이면 측정 불가로 표시한다.
    - `ops.producer_stats`: `interval_start`, `run_id`, `target_eps`, `generated`, `acked`, `failed`, `undelivered`, `queue_len_max`, 그리고 `ack_latency_ms`·`gen_to_kafka_ms`·`gen_serialize_us` 각각의 `_p50`·`_p95`·`_p99`·`_count`. `ORDER BY interval_start`.
    - `ops.spark_batch_attempts`: `query_id`, `run_id`, `batch_id`, `callback_started_at`, `status`, `failed_stage`, `input_rows`, `order_rows`, `excluded_rows`, `dlq_rows`, `classification_ms`, `orders_write_ms`, `dlq_write_ms`, `callback_total_ms`, `kafka_to_callback_ms`의 `_p50`·`_p95`·`_p99`·`_max`·`_count`, `kafka_ts_missing_count`, `kafka_ts_future_count`. `ORDER BY callback_started_at`.
    - `ops.spark_query_progress`: `query_id`, `run_id`, `batch_id`, `trigger_at`, `trigger_execution_ms`, `add_batch_ms`, `latest_offset_ms`, `query_planning_ms`, `wal_commit_ms`, `commit_offsets_ms`, `num_input_rows`, `input_rows_per_sec`, `processed_rows_per_sec`, `end_offsets Map(Int32, Int64)`, `latest_offsets Map(Int32, Int64)`. backlog는 조회 시 두 Map의 차이로 계산한다. `ORDER BY trigger_at`.
    - 실패 단계·실패한 action의 시간은 실행하지 않았거나 측정하지 못한 값이다. `status`·`failed_stage`로 구분하고 시간 값은 0으로 둔다.
  - 선택 근거: 원천마다 도착 시점·행 단위·컬럼이 다르다. 나누면 각 테이블 컬럼이 명확하고 Nullable이 필요 없다. 같은 배치는 쿼리·실행·배치 ID로 조인한다. 행 수가 시간당 수천 행 이하라 파티션 없이 행 단위 TTL을 써도 병합 부담이 작다. 콜백 지표는 3단계 계측 로그의 dict를 그대로 행으로 옮긴다.
  - 비교한 대안: 테이블 하나에 `source` 컬럼으로 구분하면 컬럼 절반이 비어 Nullable이 많아진다. `(time, metric_name, value, labels)` 긴 형식은 지표를 추가해도 DDL을 바꾸지 않지만 쿼리마다 피벗이 필요하고 타입 보장이 약하다.
- **생성기·Producer 지표:** 로그 생성기가 1초 구간 통계를 Spark 지표와 같은 HTTP + `async_insert` 방식으로 `ops.producer_stats`에 INSERT한다. 항목은 ACK EPS, 전송 실패·미전달, 큐 길이, ACK 지연과 생성 → Kafka 지연의 구간별 분위수·건수, 생성·직렬화 시간이다. 실패 시 경고만 남기고 생성을 계속한다. 1초 구간 행은 생성기가 최대 5초 모아 INSERT 1회로 보낸다(2026-11-24). 1초 해상도는 그대로이고, Grafana 표시는 최대 5초 늦어진다. 매초 1행 INSERT는 1초마다 part를 만들었고, 병합이 테이블 전체(약 1MB)를 거의 매초 다시 써서 1시간에 결과 part 3.31GB를 만들었다([daily 동시 정지 조사](03-stream-processing.md#daily-동시-정지-원인-조사-2026-11-24)). 종료 때는 기다리지 않고 남은 행을 바로 보낸다.
  - 선택 근거: 수집 방식이 Spark 지표와 하나로 통일된다. 운영 대시보드의 앞 구간(생성기·Producer)을 측정 불가로 남기지 않는다.
  - 비교한 대안: 4단계 범위에서 제외하면 범위는 작지만 운영 대시보드 앞 구간이 비어 있다. 지표 Kafka 토픽 경유는 위 지표 INSERT 방식에서 제외한 구조를 생성기에만 도입하는 셈이다.
  - 제약: 구간별 분위수는 해당 구간 값으로만 표시하고 여러 구간을 평균·합산하지 않는다. 생성기는 호스트에서 실행하므로 ClickHouse HTTP 포트를 `127.0.0.1`에만 게시한다.

#### 호스트 멈춤 기록

- 배경: 2026-11-19 01:50(KST)에 생성기 ACK p99 13.6초, Spark 트리거 18초 급증이 있었다. 조사 결과는 다음과 같았다.
  - Kafka 로그 시각(`16:50:41.492` UTC)과 docker 수신 시각(`16:50:46.64`)이 약 5초 벌어졌다. 같은 시각 ClickHouse 서버 로그에도 공백이 있었다. 두 컨테이너가 동시에 멈춘 것이므로 WSL VM 전체가 멈춘 것이다.
  - 멈춤 사이 Kafka controller heartbeat가 timeout 났고, `__consumer_offsets` 코디네이터가 epoch 15에서 16으로 다시 올라오면서 지연이 길어졌다.
  - 원인(CPU 경합·메모리 회수·IO)은 확정하지 못했다. 그 시각의 자원 수치가 없었고, Windows 이벤트 로그(System·Application·Hyper-V·Kernel-Power)에도 단서가 없었다. 커널 OOM 기록과 컨테이너 재시작은 없었다.
- 요구사항: VM 멈춤을 그 순간에 직접 기록하고, 멈춤 직전의 CPU·메모리·IO 압박을 함께 남긴다.
- 구현: Compose 서비스 `host-monitor`가 1초마다 벽시계(`date +%s%N`)를 읽는다. 직전 틱과의 간격이 `HOST_STALL_THRESHOLD_MS`(기본 2000ms) 이상이면 1행을 `ops.host_stalls`에 INSERT한다.
  - INSERT는 백그라운드로 실행해 루프가 ClickHouse 응답을 기다리느라 다음 틱을 놓치지 않게 한다. 같은 내용을 stdout에도 쓴다. ClickHouse가 같이 멈춰 INSERT가 실패해도 `docker logs host-monitor`로 대조할 수 있다.
  - 이미지는 이미 받아 둔 `clickhouse/clickhouse-server`를 재사용한다. bash, GNU `date`, `clickhouse-client`가 들어 있어 새 이미지가 필요 없다.
  - 계정은 [계정과 권한](#계정과-권한)의 `monitor_writer`를 쓴다.
  - CPU 상한은 `cpus: 1.0`이다(2026-11-24, 0.25에서 올림). 0.25에서는 감지기 자신이 CFS throttle에 걸려, 제한 없는 감지기에는 없는 지연(3.9·2.2초)과 실제보다 긴 간격(10.6초 대 6.8초)을 기록했다([감지기 비교 실행](03-stream-processing.md#감지기-비교-실행-2026-11-24)). 루프는 스레드 하나라서 1.0이면 사실상 제한에 걸리지 않는다. 평소 사용량은 한 코어의 약 0.4%다. 상한을 아예 없애지 않는 이유가 있다. ClickHouse가 오래 응답하지 않는 동안 멈춤이 연달아 생기면 백그라운드 `clickhouse-client`가 쌓일 수 있는데, 이때도 한 코어로 묶기 위해서다.
- `ops.host_stalls` 컬럼:
  - `detected_at DateTime64(3, 'UTC')`: 멈춤 뒤 처음 깨어난 시각
  - `gap_ms UInt32`: 직전 틱과의 간격
  - `cpu_some_avg10`·`memory_some_avg10`·`memory_full_avg10`·`io_some_avg10`·`io_full_avg10 Float64`: 깨어난 직후 읽은 `/proc/pressure/*`의 `avg10`(직전 10초 중 대기한 비율 %). 멈춤 직전 구간을 덮는다.
  - `mem_available_mb UInt32`: `/proc/meminfo`의 `MemAvailable`
  - `load1 Float64`: `/proc/loadavg`의 1분 평균
  - `ORDER BY detected_at`, TTL 3일. 행이 드물어 파티션을 두지 않는다.
- 판정: CPU some이 높고 메모리가 낮으면 CPU 경합, memory full이 높으면 메모리 회수 정체, io full이 높으면 디스크다. 세 값이 모두 낮은데 멈췄다면 VM 밖(Hyper-V 스케줄링·Windows)이다. 이때는 `asynchronous_metric_log`의 앞뒤 추이와 Windows 이벤트 로그를 함께 본다.
- 선택 근거: PSI와 meminfo는 컨테이너 안에서도 VM 전체 값이다. 따라서 별도 호스트 에이전트 없이 Compose 서비스 하나로 기록할 수 있다. 멈춤 1건당 1행이라 저장 부담이 없다.
- 비교한 대안:
  - `asynchronous_metric_log`만 사용: 자원 추이는 남지만, 멈춤 자체는 행 간격 공백으로 거꾸로 추정해야 한다. ClickHouse가 같이 멈추면 그 구간 값도 없다.
  - 호스트 systemd 서비스: Compose 밖이라 `docker compose up`으로 재현되지 않는다.
- 제약: 감지 하한은 약 1초다. 그보다 짧은 멈춤과 VM 밖의 원인은 이 기록만으로 구분하지 않는다. Grafana 패널·경보는 이번 범위에 넣지 않는다.

JDBC write action 전체 시간과 실제 INSERT 요청별 시간은 구분한다. Spark 측 action 계측만으로 요청별 INSERT 시간을 측정했다고 간주하지 않는다. 요청별 시간은 ClickHouse `system.query_log`의 서버 측 처리시간으로 측정하며, 네트워크·클라이언트 대기는 포함하지 않는다.

[공통 측정 원칙](01-log-generator.md#공통-측정-원칙)을 따른다. INSERT 응답과 실제 조회 가능 시점을 구분하며, INSERT 성공을 조회 가능으로 간주하지 않는다.

| 기준점·지표 | 기록 위치·계산 | 의미 |
| --- | --- | --- |
| INSERT 서버 처리시간 | `system.query_log`의 INSERT 쿼리별 `query_duration_ms`·`written_rows`·예외 | 서버가 요청을 받은 뒤 처리를 마칠 때까지의 시간. 전송·응답 대기는 제외 |

- 적재 방식과 응답의 성공 의미를 명시한다. 비동기 INSERT는 큐 수락만 확인하는지 실제 flush 완료까지 기다리는지 설정을 함께 기록한다. 분산 테이블을 사용하면 실제 대시보드 조회 경로에서 확인한다.
- 배치별 쓰기 지표에 Spark 실행·배치 ID, 콜백 시작 시각, 건수와 실패 단계를 기록한다. 재시도는 배치 ID가 같은 시도 행을 실행 ID와 콜백 시작 시각으로 구분한다. JDBC write action의 소요시간은 `orders_write_ms`에 기록한다. 성공 응답의 절대 시각은 수집하지 않는다.
- `event_timestamp`, Kafka 추가 시각·topic·partition·offset, Spark 배치 식별자를 [컬럼 타입](#order_events-컬럼-타입)에 따라 보존한다. 서버 기본값으로 생성한 적재 시각은 평가 시점을 명시하며 INSERT 완료·조회 가능 시각으로 간주하지 않는다.
- 불량 격리 경로의 지연은 정상 주문 적재와 별도로 관리한다.

### 조회 가능 지연 제외

샘플 이벤트를 ClickHouse에서 처음 발견한 시각(`first_observed_at`)으로 생성 → 조회 가능 지연을 재는 샘플러와 `ops.visibility_samples`는 v1 범위에서 제외한다.

- 선택 근거: v1 운영 지표의 우선 목적은 병목 구간 판별이다. 샘플러는 종단 간 지연 하나를 관측할 뿐 어느 구간이 원인인지 알려주지 않는다. 구간 판별은 위 3개 테이블과 `system.query_log`·`system.part_log`로 충분하다. 샘플러는 별도 서비스로 Kafka 소비와 ClickHouse 조회가 남은 CPU 예산을 쓰고 구현 범위가 가장 크다.
- 비교한 대안: 4단계에 포함하면 완료 기준의 조회 가능 지연을 실측할 수 있지만 위 비용이 든다. `ingested_at - event_timestamp`는 적재 시각을 조회 가능 시각으로 간주하지 않는다는 원칙에 따라 조회 가능 지연으로 표시하지 않는다.
- 제약: 대시보드는 생성 → 조회 가능 지연을 표시하지 않는다. 데이터 최신성(현재 시각 − 최신 `event_timestamp`)은 [Grafana PRD](05-grafana-dashboard.md#처리지연-측정과-표시)대로 표시하며 이벤트별 지연과 구분한다. 필요해지면 이 절을 갱신하고 샘플러를 추가한다.

## 구현 기록

### 1차 구현 검증 (2026-11-17)

- 범위: `infra/clickhouse/`(스키마·계정·저메모리 설정), Compose `clickhouse`·`clickhouse-init`, Spark JDBC 적재(`streaming.sinks.write_orders`), Spark 운영 지표(`streaming.ops_metrics`). 생성기 `ops.producer_stats` 기록은 아직 구현하지 않았다.
- 스키마·계정: `clickhouse-init`을 두 번 실행해 같은 결과로 끝나는 것을 확인했다. `grafana_reader`는 `gold` 뷰 조회만 되고 `silver` 조회와 `DROP`은 `ACCESS_DENIED`, `spark_writer`는 `gold` 조회가 `ACCESS_DENIED`, `gold_definer`는 로그인 불가다. DEFINER 뷰는 26.8에서 그대로 동작했다.
- 자원: `max_server_memory_usage`는 3,865,470,566바이트로 컨테이너 4g의 90%다. 컨테이너 제한을 반영한다. `part_log`는 첫 INSERT 후 생성됐고 `metric_log` 등 4개 로그 테이블은 만들어지지 않았다.
- 적재: 별도 체크포인트·`latest`로 Spark를 띄우고 생성기 150,000건(5,000 EPS, 30초)을 보냈다. 정상 주문 127,355건이 생성기 `normal_order` 건수와 같았고 `event_id` 고유 수도 127,355다. 시각 컬럼은 UTC 값 그대로 저장됐다(`event_timestamp` 12:28:13.694, `kafka_append_timestamp` 12:28:14.358, KST 21:28).
- INSERT 수: 정상 주문이 있는 배치 4개에서 `spark_writer` INSERT 10회, `written_rows` 합 127,355, 회당 5,514~14,269행이다. 4개 배치 × 쓰기 파티션 3개의 상한 12회 이하이고 회당 행 수가 `batchsize` 50,000보다 작아, 한 파티션이 INSERT 2회로 나뉜 경우는 없다. 배치별 INSERT 수는 따로 집계하지 않았다.
- 운영 지표: 배치 6개(실패 1개 포함) 모두 `ops.spark_batch_attempts`에 기록됐다. 기존 체크포인트의 offset이 Kafka 보관 기간을 지나 실패한 시도도 `status=failed`, `failed_stage=classification_and_metrics_action`으로 남았다. `ops.spark_query_progress`의 offset Map과 backlog 계산(`latest − end`)이 동작했다.
- 관찰: 기동 직후 배치는 트리거 시간 4.8~11.3초로 5초를 넘었고 정상 쓰기 action은 2.1~5.4초였다. warmup·backlog 배치라 지속 부하 판단에 쓰지 않는다. 지속 부하 측정에서 다시 본다.
- 테스트: Spark 컨테이너 pytest 319개 통과(JDBC·지표 INSERT는 대체).

### 생성기 지표 구현 검증 (2026-11-18)

- 범위: `log_generator.ops_metrics`(행 변환·전송 스레드), `KafkaOutput.interval_metrics`(구간 건수·큐 최댓값·지연 히스토그램), `main.run`의 1초 구간 집계. 설정 `PRODUCER_CLICKHOUSE_HTTP_URL`(빈 값이면 기록 끔), 비밀번호는 `CLICKHOUSE_GENERATOR_PASSWORD`를 쓴다.
- 방식: INSERT는 백그라운드 스레드가 쌓인 행을 묶어 보낸다. `wait_for_async_insert=1`의 응답 대기가 생성 루프의 속도 조절을 막지 않게 하기 위해서다. 종료 시 flush 후 마지막 부분 구간을 보내고 최대 10초 기다린다. 생성 루프가 1초 넘게 멈췄던 구간은 한 행으로 합친다.
- 생성·직렬화 시간은 `EventGenerator.next_event()` 호출 시간(us)이다. 생성 → Kafka 지연은 `event_timestamp`를 읽을 수 있는 이벤트만 집계하므로 `gen_to_kafka_ms_count`가 `acked`보다 작다.
- 실측: Kafka·ClickHouse만 띄우고 25,000건(5,000 EPS)을 보냈다. 6행이 기록됐고 `generated`·`acked` 합이 각각 25,000으로 최종 통계와 같았다. 첫 구간은 `acked` 0, `queue_len_max` 5,001이고 둘째 구간 ACK p99가 1,092.7ms였다. 브로커 연결 준비 동안 큐에 쌓였다가 다음 구간에 ACK된 것으로, 기동 직후 구간은 지속 부하 판단에서 제외한다. 이후 구간 ACK p99는 8~43ms, 직렬화 p99는 129~185us다.
- 테스트: 생성기 pytest 41개 통과(스키마 컬럼 일치, 구간 합계, HTTP 요청 형식, 실패 시 경고만 남김).

### 지속 부하·장애 주입 측정 계획 (2026-11-18)

두 측정은 서로의 지표를 오염시키므로 별도 실행으로 나눈다. 지속 부하(기준선)를 먼저 하고 장애 주입을 뒤에 한다. 공통 조건은 다음과 같다.

- 생성기 5,000 EPS, Kafka UI 끔, 자원·설정은 `.env`·Compose 기본값 그대로다. 코드·설정은 측정 중 바꾸지 않는다.
- 실행마다 별도 체크포인트와 `startingOffsets=latest`를 쓴다. 초기 offset 저장(`sources/0/0`)을 확인한 뒤 입력을 시작한다. 토픽·기존 체크포인트·기존 행은 지우지 않는다.
- 실행의 행은 `spark_query_id`(체크포인트마다 고정, 재시작해도 같음)로 구분한다. 생성기 지표는 `run_id`로 구분한다.
- 원시 로그와 스크립트는 세션 임시 디렉터리에 두고, 요약 JSON은 `docs/measurements/`에 남긴다.

#### 지속 부하 (기준선)

- 입력 720초(워밍업 120초 + 본 측정 600초), 입력 중단 후 최대 180초 동안 backlog가 0이 되는지 본다. 3단계 [지속 부하 측정](03-stream-processing.md#지속-부하-측정-계획-2026-11-17)과 같은 길이다.
- 측정 항목:
  - 처리량·backlog: 본 측정 구간 ACK EPS(`ops.producer_stats`), 배치 시간 내역(`ops.spark_batch_attempts`), backlog 추이(`ops.spark_query_progress`).
  - INSERT: `system.query_log`의 `spark_writer` INSERT 횟수·`query_duration_ms`·`written_rows`.
  - part·병합: 10초마다 활성 part 수(`system.parts`), `system.part_log`의 `NewPart`·`MergeParts` 건수·병합 시간. `parts_to_delay_insert`(기본 1000) 대비 여유를 본다.
  - `FINAL` 조회 비용: 본 측정 중 30초마다 `grafana_reader`로 `gold.orders_deduplicated` 15분·5분 범위 집계를, 관리자로 `silver.order_events` 15분 범위 집계(`FINAL` 없음)를 실행한다. `query_log`의 `query_duration_ms`·`read_rows`·`memory_usage`를 비교한다. 5분·15분 `read_rows` 비율로 범위 조건이 정렬 키 가지치기에 쓰이는지 확인한다.
  - 압축률: 이번 실행 행을 코덱 그대로인 임시 테이블과 코덱을 뺀(기본 LZ4) 임시 테이블에 각각 복사하고 `OPTIMIZE FINAL` 후 `system.columns`의 압축 전·후 바이트를 비교한다. 같은 데이터·같은 part 상태에서 비교하기 위해서다. 임시 테이블은 비교 후 지운다.
  - 자원: 5초마다 `docker stats`로 ClickHouse·Spark·Kafka CPU·메모리.
- 판정: ACK EPS 4,750 이상, 쿼리 실패 없음, 본 측정 후반 backlog가 계속 늘지 않음, 입력 중단 후 180초 안에 backlog 0. 고유 `event_id` 수 = 생성기 `normal_order` 건수. `FINAL` 조회 시간 목표는 이 측정 결과로 정해 [완료 기준](#완료-기준)에 기록한다.

#### 장애 주입

한 실행 안에서 순서대로 주입하고, 주입 사이에 기준 수준으로 회복하는지 본다. 주입은 모두 관리자 계정의 DDL·권한 변경이며 코드는 바꾸지 않는다. 생성기는 계속 5,000 EPS로 보낸다.

| 순서 | 장애 | 주입 방법 | 되돌림 | 기대 지표 |
| --- | --- | --- | --- | --- |
| 1 | 쓰기 지연 | `silver.order_events`에 `sleep(3)`을 실행하는 materialized view(대상 `Null` 테이블)를 60초 붙인다. INSERT마다 서버 처리시간이 3초 늘고 행은 저장된다 | view 삭제 | `orders_write_ms`와 `query_log` INSERT 시간이 함께 늘어 ClickHouse 구간으로 판별된다. 트리거가 5초를 넘으면 backlog가 늘고, 생성기 ACK 지연·큐는 변하지 않는다 |
| 2 | 적재 실패(저장 후 오류) | `throwIf(1)`을 실행하는 materialized view를 붙인다. 원본 행은 저장된 뒤 클라이언트는 오류를 받는다. 서버 저장 후 응답 실패와 같은 상태다 | 쿼리 실패 확인 후 view 삭제, 같은 체크포인트로 재시작 | 실패 시도가 `status=failed`, `failed_stage=orders_write_action`으로 남는다. `query_log`는 `written_rows`가 있는 예외다. 재시작 후 같은 `batch_id`가 다시 실행돼 물리 중복이 생기고 `FINAL` 뷰는 한 번만 센다. 중단 동안 backlog가 늘고 회복한다 |
| 3 | 적재 실패(저장 전 거부) | `spark_writer`의 `silver.order_events` INSERT 권한을 회수한다 | 실패 확인 후 권한 재부여, 같은 체크포인트로 재시작 | 2와 같은 실패 시도 기록, `query_log`는 `ACCESS_DENIED`·`written_rows` 0이다. 물리 중복이 생기지 않는다 |

- 구간(생성기 경과 기준): 워밍업 0~120초, 기준 120~240초, 지연 주입 240~300초, 회복 300~420초, 저장 후 오류 420초부터 재시작·회복까지, 회복 후 저장 전 거부, 재시작·회복 후 입력 중단. 입력은 900초다.
- 최종 대조: 고유 `event_id` 수 = 생성기 `normal_order` 건수(누락 없음). 물리 행 수 − 고유 수 = 2번에서 다시 실행된 배치의 정상 건수. `FINAL` 뷰의 실행 구간 집계 = 고유 수.
- 판정: 각 장애에서 위 기대 지표만으로 원인 구간(생성기·Producer / Spark / ClickHouse INSERT / 권한·연결)을 가를 수 있는지, 누락·이중 합산이 없는지 확인한다.
- 제외: 같은 ID에 다른 `event_timestamp`를 넣는 충돌 점검과 Spark 측 처리 지연(CPU 제한)은 이번 측정에 포함하지 않는다. Spark 처리 지연의 지표 구분은 [3단계 지연 주입](03-stream-processing.md#2-소비-지연처리-지연-주입)에서 확인했다.

### 지속 부하 측정 결과 (2026-11-18)

**판정: 통과.** 5,000 EPS 목표 입력을 ClickHouse 적재까지 backlog 없이 처리했고, 누락·중복이 없었다.

- 실행: 09:56:31~10:08:01 KST(실제 경과 690.6초), 체크포인트 `/opt/spark/checkpoints/ch-load-20261118`, 컨테이너 `log-etlm-2-spark-chload-20261118-a`. 근거: [측정 JSON](../measurements/2026-11-18-clickhouse-sustained-load.json).
- 시계 주의: WSL 단조 시계가 Windows 시계보다 약 4.3% 빠르다(60초 비교: Windows 57.94초, 단조 60.46초). realtime은 약 29초마다 1.3초씩 뒤로 보정된다(`Time jumped backwards`). 생성기는 단조 시계로 속도를 맞추므로 실제 입력은 **약 5,213 EPS**(목표보다 약 4% 많음)이고, 측정한 ms 값도 같은 비율로 크다. 구간은 실제 경과 기준으로 나눴다(워밍업 0~114.9초, 본 측정 114.9~690.6초). 3단계 측정도 같은 영향을 받았을 수 있다.

| 항목 | 결과 (본 측정) |
| --- | --- |
| 생성기 | 3,600,000건 ACK, 실패·미전달 0, exit 0. 실제 ACK EPS 5,216. 구간별 ACK p99는 p50 8.5ms, 최대 89.7ms. 큐 최대 438 |
| Spark 배치 | 115개 모두 성공. 콜백 p50 2.49초, p95 3.12초, 최대 4.63초. 트리거 5초 초과 0. 분류 p50 1.23초, 정상 쓰기 p50 1.09초(p95 1.47초), DLQ 쓰기 p50 0.16초 |
| backlog·대기 | backlog 최대 0. Kafka → 콜백 배치별 p95는 p50 4.79초, 최대 5.76초. 전반·후반 차이 없음. 입력 중단 후 약 5초에 마지막 배치 처리 |
| INSERT (`query_log`) | 345회, 회당 약 7,400행(최대 9,114). 서버 처리 p50 161ms, p95 255ms, 최대 561ms. 오류 0 |
| part·병합 | 활성 part 최대 24개, 파티션당 최대 11개(`parts_to_delay_insert` 1,000 대비 여유 큼). 실행 중 `NewPart` 398, `MergeParts` 72회(평균 429ms, p95 1.04초, 최대 3.95초) |
| 건수 대조 | 물리 행 = 고유 `event_id` = `FINAL` 뷰 = 생성기 `normal_order` = **3,060,098**. 같은 시간 범위의 다른 실행 행 0 |
| 자원 (`docker stats` 5초 표본) | ClickHouse CPU 평균 24%, p95 77%, 최대 173%(제한 200%), 메모리 최대 611MiB/4GiB. Spark CPU 평균 144%, 최대 311%, 메모리 최대 2.07GiB/3GiB. Kafka CPU 평균 27%, 메모리 최대 871MiB |

`FINAL` 조회 비용 (30초마다 20회, 실행 길이가 15분보다 짧아 15분 범위는 그때까지의 전체 행을 읽었다):

| 조회 | 서버 시간 p50 / p95·최대 | 읽은 행 p50 (최대) | 메모리 p50 |
| --- | --- | --- | --- |
| `gold.orders_deduplicated` 15분 (`grafana_reader`) | 64ms / 336ms | 1,765,594 (3,051,621) | 21.3MB |
| `gold.orders_deduplicated` 5분 | 57ms / 99ms | 1,316,088 (1,336,564) | 17.6MB |
| `silver.order_events` 15분, `FINAL` 없음 | 17ms / 66ms | 1,765,594 (3,051,621) | 5.5MB |

- 범위 가지치기: 5분 조회가 읽은 행은 5분 분량(약 133만 행)에서 멈추고 15분 조회는 누적에 따라 늘었다. 뷰를 거친 조회에서도 `event_timestamp` 범위 조건이 정렬 키 가지치기에 쓰인다.
- `FINAL` 비용: 같은 행을 읽을 때 `FINAL`이 없는 조회보다 약 3.8배 느리고 메모리는 약 4배다. 그래도 15분 범위 p95가 1초보다 충분히 작으므로 [조합 D(롤업)](#중복-제거조회-조합-비교)는 도입하지 않는다.
- 압축: [압축 코덱](#압축-코덱)의 결과를 따른다. 이번 실행 행을 복사한 임시 테이블 2개를 `OPTIMIZE FINAL` 후 비교했고 비교 후 지웠다.

### 장애 주입 측정 결과 (2026-11-18)

**판정: 통과.** 세 장애 모두 지표만으로 원인 구간을 가를 수 있었고, 누락·이중 합산이 없었다. 기대와 다른 점 3개는 [판정 순서](#운영-지표-저장)의 해석 제약에 반영했다.

- 실행: 10:10:59~10:25:27 KST(실제 경과 867.7초, 실제 입력 약 5,186 EPS), 체크포인트 `/opt/spark/checkpoints/ch-fault-20261118`, 컨테이너 a(시작)·b(저장 후 오류 뒤)·c(저장 전 거부 뒤). 주입은 모두 되돌렸고 `default` 데이터베이스에 남은 객체가 없으며 `spark_writer` 권한이 원래대로인 것을 확인했다. 근거: [측정 JSON](../measurements/2026-11-18-clickhouse-fault-injection.json).

| 구간 (실제 경과) | 정상 쓰기 p50 | INSERT 서버 p50 | 분류 p50 | 트리거 > 5초 | Kafka → 콜백 p95 최대 | 생성기 ACK p99 최대 |
| --- | --- | --- | --- | --- | --- | --- |
| 기준 120~240초 | 1,128ms | 151ms | 1,201ms | 0/24 | 5.64초 | 51ms |
| 쓰기 지연 240~300초 | **4,214ms** | **3,192ms** | 1,458ms | **9/11** | 5.93초 | 49ms |
| 회복 300~420초 | 1,104ms | 166ms | 1,229ms | 0/24 | 4.80초 | 26ms |

- 쓰기 지연: 정상 쓰기와 서버 INSERT 시간이 함께 약 3초 늘고 분류·생성기 지표는 그대로여서 ClickHouse INSERT 구간으로 판별된다. 트리거가 5초를 넘으면서 배치 입력이 약 25,000 → 29,400건으로 늘었다. **backlog는 0으로 남았다**(상한 50,000 미만이라 계획 시점 `latest = end`). 되돌린 뒤 첫 구간부터 기준으로 돌아왔다.
- 저장 후 오류(420초 주입): 약 3초 뒤 batch 84의 INSERT 3개가 `ExceptionWhileProcessing`(code 395)로 실패하고, 시도 행에 `status=failed`, `failed_stage=orders_write_action`이 남았다. 6.2초 뒤 쿼리가 exit 1로 끝났다. 재시작(b) 후 batch 84가 같은 입력 30,176건으로 다시 실행돼 성공했다. 재시작 첫 배치는 Kafka → 콜백 p95 19.5초, 다음 배치들은 상한 50,000건으로 backlog 최대 87,402를 처리했고 주입 약 61초 뒤 기준으로 돌아왔다.
  - 실패한 INSERT 3개의 `written_rows`는 8,573·8,562·8,512였지만 실제로 남은 것은 1개(8,573행)뿐이었다. 원본 저장과 view 실행 순서에 따라 저장 여부가 달라진다. 재실행이 25,647행을 다시 넣어 **물리 중복 8,573행**이 생겼다.
- 저장 전 거부(600초 주입): batch 115의 INSERT 3개가 `ExceptionBeforeStart`(code 497 `Not enough privileges`, `written_rows` 0, `tables` 빈 배열)로 실패했다. 시도 행은 저장 후 오류와 같이 `orders_write_action`이고, 4.4초 뒤 쿼리가 종료됐다. 재시작(c) 후 batch 115가 다시 실행됐고 중복은 없었다. 회복은 주입 약 53초 뒤다.
- 두 실패는 Spark 시도 행만으로는 같아 보인다. `query_log`의 예외 유형(`ExceptionWhileProcessing` 대 `ExceptionBeforeStart`)과 코드로 ClickHouse 처리 중 오류인지 권한·연결 거부인지를 가른다. 세 장애 동안 생성기 ACK 지연·큐는 기준과 같아 생성기·Producer 구간은 원인에서 제외된다.
- 재시작 후 첫 진행 정보(batch 84, 115)는 `latest_offsets`가 빈 Map이라 backlog 계산이 음수였다.
- 재실행 배치의 정상 쓰기는 4.9초·4.7초로 기준의 4배다. 재시작 직후 JVM·JDBC 연결 준비 비용으로 보며, 3단계 재시작 직후 분류 action 증가와 같은 현상이다.
- 최종 대조: 생성기 4,500,000건 ACK(실패·미전달 0). 고유 `event_id` = `FINAL` 뷰 = `normal_order` = **3,825,832**(누락 없음). 물리 행 3,834,405 − 고유 수 = 8,573 = 저장 후 오류에서 남은 행. 시도 행은 성공 166, `orders_write_action` 실패 2.

### 충돌 점검 쿼리 검증 결과 (2026-11-18)

**판정: 통과(쿼리 수정 없음).** 관리자 계정으로 `conflict-test-<uuid>-*` ID 8행을 `silver.order_events`에 INSERT해 [점검 쿼리](#이벤트-식별과-테이블-기준)를 그대로 실행했다.

| 케이스 | 입력 | 1시간 점검 쿼리 | `gold.orders_deduplicated` |
|---|---|---|---|
| A 같은 파티션 충돌 | 같은 ID, 5분 전·4분 전 | 검출 | 2행 |
| C 파티션 경계 충돌 | 같은 ID, 정시 1분 전·1초 후 | 검출 | 2행 |
| D 구간 밖 충돌 | 같은 ID, 2시간 전·10분 전 | 미검출 | 2행(조회 범위가 둘 다 포함할 때) |
| K 대조군 | 같은 ID·같은 timestamp 2행 | 미검출 | 1행 |

- 시험 ID만 보면 기대 2건(A, C)에 실제 2건이다. 전체 구간(`WHERE` 없음) 점검은 3건(A, C, D)이다. 쿼리 전체 결과도 2건이어서 기존 데이터의 1시간 구간에는 충돌이 없었다.
- 충돌 행은 `FINAL`로도 합쳐지지 않고 2행이 남는다. 파티션이 달라도(C) 마찬가지다. 같은 ID·같은 timestamp 재전송은 1행으로 합쳐진다.
- 점검 구간 밖으로 떨어진 충돌(D)은 1시간 쿼리가 놓치고, `WHERE` 없는 전체 구간 쿼리만 찾는다. 설계 문구와 일치한다.
- 시험 후 `ALTER TABLE ... DELETE WHERE event_id LIKE 'conflict-test-%'`로 삭제했고 0건을 확인했다.
- 제안: 당시에는 전체 보관 구간 점검을 약 24시간 분량으로 가정했다. 1시간 점검은 대시보드 갱신보다 낮은 주기(예: 1분)로, 전체 구간 점검은 더 낮은 주기(예: 1시간)로 둔다. 비용은 지속 부하 상태에서 측정하지 않았으므로 구간·주기는 5단계에서 확정한다.

### DLQ 쓰기 실패 주입 결과 (2026-11-18)

**판정: 부분 통과.** 정상 주문 중복·DLQ 누락은 없었다. 장애를 유지하면 2분 뒤 쿼리가 실패했지만, 재시도 중에 장애를 되돌린 실행에서는 쿼리가 실패하지도 진행하지도 않고 멈췄다. 이는 [부분 실패와 재시작](#부분-실패와-재시작)의 "실패하면 쿼리를 실패시킨다" 전제와 다르며, 코드는 바꾸지 않았다.

- 주입: DLQ는 ClickHouse 테이블이 아니라 Kafka 토픽이라 ClickHouse 권한으로는 주입할 수 없다. 3단계 [시나리오 B](03-stream-processing.md#실패-지점과-재실행-시-기대-중복)와 같이 DLQ 토픽 `max.message.bytes`를 100으로 바꿨고, `--delete-config`로 되돌렸다. 생성기 2,000 EPS, 실행마다 새 체크포인트·`startingOffsets=latest`로 실행했다. 근거: [측정 JSON](../measurements/2026-11-18-clickhouse-dlq-failure.json).
- 1차(되돌림이 재시도 중에 일어남, 체크포인트 `ch-dlq-20261118`): 12:59:19 입력 시작(300초), 13:00:54 주입, 13:02:56 되돌림.
- 2차(장애 유지, 체크포인트 `ch-dlq2-20261118`): 13:09:54 입력 시작(240초), 13:10:56 주입, 쿼리 종료 확인 후 13:13:01 되돌림.

| 항목 | 1차: 재시도 중 되돌림 | 2차: 장애 유지 |
| --- | --- | --- |
| Spark 동작 | DLQ 쓰기에서 producer가 `MESSAGE_TOO_LARGE`로 배치를 나눠 재시도(경고 42,800줄). 13:02:56 producer 네트워크 스레드가 `StackOverflowError`로 죽고 태스크 3개가 `KafkaProducer.flush`에서 무기한 대기. 쿼리 종료 없음 | 같은 재시도(경고 39,286줄) 후 120초에 `TimeoutException: Expiring 9 record(s) ... 120004 ms`. 쿼리 `terminated with error`, exit 1 |
| 시도 행(`ops.spark_batch_attempts`) | batch 17 실패 행 없음. 콜백이 끝나지 않아 13:00:50 이후 시도·진행 행이 모두 없음 | batch 13 `failed`/`dlq_write_action`, `dlq_write_ms` 120,956 |
| 복구 | 되돌린 뒤 3.8분 동안 멈춤을 확인하고 컨테이너 중지(exit 143), 같은 체크포인트로 재시작. batch 17이 같은 입력 9,933건으로 재실행·성공 | 같은 체크포인트로 재시작. batch 13이 같은 입력 9,996건으로 재실행·성공 |
| 정상 주문 재INSERT | 8,456행(batch 17 정상 건수 전체) | 8,479행(batch 13 정상 건수 전체) |
| `query_log` 주문 INSERT | 46회, `written_rows` 518,628 = 고유 510,172 + 8,456. 예외 0 | 36회, 415,820 = 407,341 + 8,479. 예외 0 |
| 최종 행 대조 | 고유 `event_id` = `FINAL` 뷰 = `normal_order` = **510,172** | **407,341**, 같음 |
| DLQ (생성기 `invalid_order` 대비) | 메시지 29,997, 고유 키 29,988 = 29,988(누락 0). 중복 키 9 | 메시지 24,253, 고유 키 24,253 = 24,253(누락 0). 중복 0 |
| backlog | 멈춘 동안 진행 행이 없어 계산 불가. 재시작 후 최대 358,462, 재시작 약 64초 뒤 0 | 실패 전 120초 동안 진행 행 없음. 재시작 후 최대 241,574, 입력 종료 15초 뒤 0 |

- 정상 주문 중복: 두 실행 모두 실패 배치의 정상 주문이 실패 전에 전부 저장됐고 재실행이 같은 행을 다시 넣었다(3단계 기대 중복표와 같음). 분석 시점(재시작 약 1~2분 뒤)에는 `ReplacingMergeTree` 백그라운드 병합이 같은 `(event_timestamp, event_id)` 행을 이미 합쳐 물리 행 = 고유 수였다. 실행 중 물리 중복은 `query_log`의 `written_rows` 합계로 확인했다. 따라서 [저장 후 오류](#장애-주입-측정-결과-2026-11-18)처럼 "물리 행 − 고유 수"로 재INSERT 양을 사후에 재는 방법은 병합 뒤에는 0이 나오므로 쓸 수 없다. `FINAL` 뷰 결과는 병합 여부와 무관하게 고유 수와 같았다.
- DLQ 중복: 1차의 9건(DLQ offset 1332716~1332724)은 13:02:56.019에 기록됐다. 되돌림이 적용된 직후 재시도 중이던 9건이 들어간 뒤 producer 스레드가 죽었고, 재실행이 같은 키를 다시 썼다. 3단계 설계대로 키(`topic:partition:offset`)로 판별된다. 2차는 실패 배치의 DLQ가 하나도 기록되지 않아 중복이 없다.
- 지표 식별: ClickHouse는 실패를 보지 못한다(`spark_writer` 예외 0, 정상 INSERT는 성공). 원인은 Spark 시도 행의 `failed_stage=dlq_write_action`으로만 식별되며, 그 행도 `delivery.timeout.ms`(기본 120초) 뒤에야 남는다. 그동안 그리고 1차의 멈춤 동안에는 시도·진행 행이 새로 생기지 않아 backlog도 계산되지 않는다. 즉 DLQ 쓰기 장애는 "새 진행 행이 트리거 주기(5초)보다 오래 없음"과 Spark 로그 `splitting and retrying ... MESSAGE_TOO_LARGE`로 먼저 드러난다. 생성기 지표(ACK·큐)는 그대로였다.
- 설계와 다른 점(이후 대응은 [멈춤 감시·자동 재시작 검증](#멈춤-감시자동-재시작-검증-2026-11-18)): 1차의 멈춤은 장애를 고친 뒤에도 풀리지 않고 쿼리도 종료되지 않으므로, "실패하면 쿼리를 실패시키고 같은 체크포인트로 재시작한다"는 복구 절차가 자동으로 시작되지 않는다. 2차와 3단계 B처럼 장애가 120초 넘게 이어지면 정상 실패한다. 멈춤 발생 조건(재시도 중 응답 성공과 producer 내부 재귀)은 1회 관찰이며 재현 조건은 확정하지 않았다.

### 멈춤 감시·자동 재시작 검증 (2026-11-18)

**판정: 통과.** [Spark PRD 쓰기 보장과 장애 경계](03-stream-processing.md#쓰기-보장과-장애-경계)의 멈춤 감시(`SPARK_STALL_TIMEOUT_SECONDS` 90초)·Compose `restart: on-failure:5`(이후 상한 제거, PRD 03 참조)·DLQ 전송 타임아웃(30초)을 구현한 뒤, 한 체크포인트에서 장애 3개를 차례로 주입했다. 재시작 4회 모두 사람 개입 없이 복구됐고 정상 주문·DLQ 누락은 없다. 위 1차 실행의 `StackOverflowError` 멈춤은 2회 재현됐다. 근거: [측정 JSON](../measurements/2026-11-18-spark-stall-recovery.json).

- 실행: 15:46:33~15:57:40 KST, 생성기 2,000 EPS·660초, Compose `spark` 서비스(체크포인트 `ch-stall2-20261118`, `startingOffsets=latest`).

| 시나리오 | 주입 | 동작 | 복구 |
| --- | --- | --- | --- |
| S1 태스크 멈춤 | 15:47:36 Python worker(`pyspark.daemon`) SIGSTOP | 진행·유휴 이벤트 95초 공백 → 15:49:11 `query stalled` 로그·exit 1 | 15:49:20 같은 체크포인트로 쿼리 재시작 |
| S2 DLQ 장애 유지 | 15:51:17 DLQ `max.message.bytes=100`, 15:52:37 되돌림 | 30.6초에 `TimeoutException: Expiring ... 30000 ms`, 시도 행 `failed`/`dlq_write_action`(`dlq_write_ms` 30,583) → exit 1 | 15:52:02 재시작. 장애가 남은 재실행 중 되돌림에서 `StackOverflowError` 멈춤 → 15:53:32 감시 종료 → 15:53:39 재시작 |
| S3 재시도 중 되돌림 | 15:54:27 주입, 19초 뒤 15:54:46 되돌림 | `StackOverflowError` 멈춤 재현 | 15:55:53 감시 종료 → 15:56:02 재시작 |

- 행 대조: 물리 행 = 고유 `event_id` = `FINAL` = 성공 배치 `order_rows` 합 = **1,121,986**. 실행 중 재INSERT는 병합으로 이미 합쳐졌다.
- DLQ: 메시지 66,122, 고유 키 66,108 = 성공 배치 `dlq_rows` 합 66,108(누락 0, 재실행 중복 14).
- 배출: 생성기 종료 6초 뒤 backlog 0. 최종 `RestartCount` 4로 상한 5에 닿지 않았다.
- 복구 시간: 멈춤은 마지막 이벤트 뒤 90~95초에 감지되고, 재시작 후 쿼리 시작까지 7~9초다. 예외로 끝나는 DLQ 실패는 약 30초 뒤 종료된다(이전 120초).
- 멈춤 재현 조건: `MESSAGE_TOO_LARGE` 분할 재시도 중 토픽 설정을 되돌리면 producer 네트워크 스레드가 `StackOverflowError`로 죽는다(3회 중 3회). 원인은 Kafka 클라이언트 내부로 보고, 근본 수정 대신 감시·재시작으로 복구한다.
- 재시작 상한: 앞선 시도(`ch-stall-20261118`)는 Kafka·ClickHouse가 꺼진 상태라 bootstrap 주소 해석 실패로 5회 연속 재시작 뒤 멈췄다. 상한 동작은 확인했고, 결과 대조에서는 제외했다(이후 상한 제거, PRD 03 참조).
- 코드 보정: 첫 실행에서 감시 기준 시각이 `query_context`에 들어가 시작·배치 지표 로그에 섞였다. 리스너의 별도 값으로 분리한 뒤 S1만 다시 실행해 16:03:02 감시 종료 → 16:03:09 재시작, 물리 = 고유 373,993을 확인했다.
- 남은 한계: 감시는 Driver Python 프로세스 안 스레드다. 이 프로세스 자체가 멈추거나 이벤트는 오는데 처리량만 0인 경우는 감지하지 못하며, 진행 행 공백 경보(5단계 Grafana)가 이를 보완한다.

### 15분 이상 지속 부하 측정 결과 (2026-11-18)

**판정: 통과.** 15분 범위가 이번 실행 행으로 꽉 찬 상태에서도 `FINAL` 조회는 목표 안이었고, 정시 파티션 경계에서 적재 지표 변화가 없었다. 경계 뒤 이전 파티션의 part 수는 병합으로 줄지 않았다(아래).

- 실행: 13:48:38~14:13:31 KST(실제 경과 1,492.5초), 입력 중단 후 14:17:40까지 part 수만 더 관찰했다. 체크포인트 `/opt/spark/checkpoints/ch-load25-20261118`, 컨테이너 `log-etlm-2-spark-chload25-20261118-a`, 생성기 1,500초 분량. 정시 경계 14:00:00 KST(파티션 `2026-11-18 05:00:00` UTC 시작)는 경과 681초다. 근거: [측정 JSON](../measurements/2026-11-18-clickhouse-sustained-load-25m.json).
- 구간: 워밍업 0~119.3초, 본 측정 119.3~1,492.5초. 15분 범위 꽉 참은 첫 행 + 930초(여유 30초) 이후 562초다.
- 시계: 이번 실행은 단조 시계가 실제보다 약 0.56% 느렸다(보정 비율 k 0.9944, 이전 실행 약 1.043). 실제 입력은 **약 5,025 EPS**다. 보정 방식은 이전과 같다.

| 항목 | 결과 (본 측정) |
| --- | --- |
| 생성기 | 7,500,000건 ACK, 실패·미전달 0, exit 0. 실제 ACK EPS 5,026. 구간별 ACK p99는 p50 8.0ms, 최대 103.5ms. 큐 최대 549 |
| Spark 배치 | 301개 모두 성공(본 측정 275개). 콜백 p50 2.21초, p95 2.68초, 최대 3.12초. 트리거 최대 3.19초, 5초 초과 0(워밍업 2개 제외). 정상 쓰기 p50 0.92초(p95 1.21초) |
| backlog·대기 | backlog 최대 0. Kafka → 콜백 배치별 p95는 p50 4.78초, 최대 4.91초. 입력 중단 약 10초 뒤 backlog 0 |
| INSERT (`query_log`) | 822회, 회당 약 7,100행(최대 8,215). 서버 처리 p50 146ms, p95 205ms, 최대 305ms. 오류 0 |
| 정시 경계 ±60초 | 트리거 최대 3.19초, INSERT p95 233ms, Kafka → 콜백 p95 최대 4.80초. 본 측정 전체와 차이 없음 |
| part·병합 | 활성 part 최대 47개(이전 실행의 다른 파티션 포함), 파티션당 최대 13개. 실행 중 `NewPart` 894, `MergeParts` 166회(평균 364ms, p95 847ms, 최대 3.18초) |
| 건수 대조 | 물리 행 = 고유 `event_id` = `FINAL` 뷰 = 생성기 `normal_order` = **6,374,939**. 같은 시간 범위의 다른 실행 행 0 |
| 자원 (`docker stats` 5초 표본) | ClickHouse CPU 평균 19%, p95 70%, 최대 131%, 메모리 최대 777MiB. Spark CPU 평균 104%, 최대 302%, 메모리 최대 2.45GiB. Kafka 메모리 최대 960MiB |

`FINAL` 조회 비용 (15분 범위가 꽉 찬 562초 동안 30초마다 18회):

| 조회 | 서버 시간 p50 / p95·최대 | 읽은 행 p50 (최대) | 메모리 p50 |
| --- | --- | --- | --- |
| `gold.orders_deduplicated` 15분 (`grafana_reader`) | 63ms / 239ms | 3,838,826 (3,841,291) | 15.0MB |
| `gold.orders_deduplicated` 5분 | 33ms / 114ms | 1,282,633 (1,293,045) | 12.4MB |
| `silver.order_events` 15분, `FINAL` 없음 | 29ms / 70ms | 3,838,826 (3,841,291) | 5.5MB |

- 읽은 행 평탄화: 15분 조회의 읽은 행은 꽉 찬 뒤 약 384만 행(15분 × 정상 주문 약 4,270건/초)에서 ±0.1% 안으로 멈췄다. 경계 뒤 두 파티션에 걸친 조회도 같다. 본 측정 전체(45회)의 15분 조회 p95는 138ms다.
- `FINAL` 비용: 같은 행을 읽을 때 `FINAL`이 없는 조회보다 p50 약 2.2배, 메모리 약 2.7배다. 15분 p95가 목표 1초의 약 1/4이라 [조합 D(롤업)](#중복-제거조회-조합-비교) 미도입 판단을 유지한다. 표본이 18회라 p95는 최댓값과 같다.

정시 경계 전후 파티션별 활성 part 수 (10초 표본):

| 시점 (KST) | 이전 파티션 13시 KST (`04:00` UTC) | 새 파티션 14시 KST (`05:00` UTC) |
| --- | --- | --- |
| 13:48~14:00 (적재 중) | 5~12개, INSERT·병합에 따라 증감 | 없음 |
| 14:00:06 표본 | 8개, 3,757,792행(최종 행 수) | 없음 |
| 14:00:16 표본 | 8개 | 첫 part 1개 |
| 14:00~14:13 (새 파티션 적재 중) | 8개 그대로 | 2~13개, 증감 |
| 14:13:31~14:17:40 (입력 중단 후) | 8개 그대로 | 9개 그대로 |

- 이전 파티션의 마지막 병합은 14:00:02 KST이고, 이후 14:18까지 병합이 없었다. 남은 8개는 level 5 2개(106만·154만 행), level 4 3개, level 2 2개, level 1 1개(7,369행)다. 새 INSERT가 끝난 파티션을 백그라운드 병합이 하나로 합치지 않는다. 크기 차이가 큰 part만 남아 병합 선택 조건을 넘지 못한 것으로 보이며 원인은 확인하지 않았다. 이전 실행의 다른 파티션도 5~8개로 남아 있다.
- 따라서 "경계 뒤 이전 파티션 part 수가 병합으로 줄어든다"는 관찰되지 않았다. 다만 파티션당 part 수는 적재 중 최대 13개, 적재 후 8~9개로 일정해 목표 50개와 `parts_to_delay_insert` 1,000 대비 여유가 크다. 닫힌 파티션의 part 수가 `FINAL` 비용에 주는 영향은 1시간 이상 범위 조회에서만 생기므로 5단계 대시보드 범위를 정할 때 본다.

### 측정 후 남은 작업 (2026-11-18)

- 대시보드 조회 반영: backlog 해석 제약, 빈 `latest_offsets` 처리, INSERT 실패 필터, 진행 행 공백·Spark 재시작 횟수 경보(5단계 Grafana). → 5단계에서 반영·검증([Grafana 구현 기록](05-grafana-dashboard.md#구현-기록)).
- WSL 시계 대응(종료, 2026-11-19): Windows 시간 동기 복구(2026-11-18 15:33 `w32tm /resync`)와 `systemd-timesyncd` 비활성화를 적용했다. 생성기 `CLOCK_MONOTONIC_RAW` 전환은 적용하지 않는다. 근거는 [timesyncd 비활성화 검증](#timesyncd-비활성화-검증-2026-11-19)이다.

### WSL 시계 차이 조사 (2026-11-18)

**판정: 원인 대부분 확인.** Windows 시각을 따라가는 WSL 쪽 동기와 NTP 서버를 따라가는 `systemd-timesyncd`가 서로 반대 방향으로 시계를 당긴다. 두 기준이 약 0.3초 어긋나 있어 매 poll마다 보정이 반복되고, 그 보정이 `CLOCK_MONOTONIC` 속도를 바꾼다. 줄다리기 세기가 시점마다 달라 실행마다 오차 크기·부호가 바뀐다.

- 환경: 커널 6.18.58.1-microsoft-standard-WSL2, Ubuntu 24.04, clocksource `tsc`(CPU 플래그 `constant_tsc`·`nonstop_tsc`·`tsc_known_freq` 있음), 커널 인자 `hv_utils.timesync_implicit=1`, `/dev/ptp0`(hyperv) 있음, chrony 없음, timesyncd 활성(서버 `ntp.ubuntu.com`).
- 관찰(14:25~14:32 KST, 2초 간격 211개, Windows QPC 기준): `CLOCK_MONOTONIC_RAW` 0ppm(QPC와 일치), `CLOCK_MONOTONIC` +5,300ppm(+0.53%), `CLOCK_REALTIME` 평균 −4ppm(Windows 시각 대비 ±160ms 안 톱니). 4코어 부하 2분 동안 단조 +0.55%로 부하 영향은 없다.
- 톱니 패턴: 30.0초마다 realtime이 약 160ms 뒤로 step하고, 같은 순간 커널 PLL에 offset −0.3초(`STA_PLL|STA_NANO`)가 걸려 단조 시계가 −1.5% 속도로 느려졌다가 회복한다. 그 사이에는 adjtimex `tick`이 10,138~10,168(기본 10,000, 약 +1.5%)로 4~8초마다 바뀌며 단조 시계를 빠르게 한다. journald `Time jumped backwards`와 resolved `Clock change detected`가 같은 주기로 09:38부터 계속 기록된다.
- 반대 방향 두 주체:
  - timesyncd: offset −298ms, 지연 260ms, poll 최소 32초 고정, 주파수 −25ppm. 0.4초 미만 offset은 `ADJ_OFFSET`으로 slew한다(systemd v255 소스). 매 poll 약 −0.3초를 slew한다.
  - Windows 쪽 동기: tick을 바꾸는 주체는 확인하지 못했다. timesyncd 소스에는 `ADJ_TICK`이 없고, 메인라인 `hv_utils`는 step(`do_settimeofday64`)만 한다. 4~8초 주기가 Hyper-V TimeSync 샘플 주기(약 5초)와 맞아 WSL 커널 또는 WSL init의 동기 처리로 추정한다.
- 기준이 어긋난 이유: Windows가 시간 동기를 잃었다. `w32tm /query /status`는 Leap 3(미동기), 마지막 성공 동기 09:39:24, 루트 분산 8.5초다. `w32tm /stripchart` 결과 Windows는 time.google.com보다 약 0.29초 앞선다. Linux realtime도 Windows를 따라 NTP 서버들보다 0.17~0.34초 앞선다(SNTP 질의). 계단 시작(09:38)과 Windows 마지막 동기 시각이 겹친다. 08:58·10:15에 `hv_utils: TimeSync IC` 재등록(호스트 일시 정지·재개 흔적)이 있다.
- 측정 결과와의 관계: 12분 측정 때는 step이 약 1.3초로 지금(160ms)보다 커서 단조 시계가 4.3% 빨랐다. 25분 측정 때는 반대로 0.56% 느렸다. 같은 줄다리기가 시점마다 다른 균형을 보인 것으로 해석한다. 다만 당시 tick 값은 기록이 없어 직접 확인하지 못했다.
- 남은 가설: tick을 조정하는 주체(WSL 커널 패치 또는 WSL init)와 160ms step의 주체. timesyncd를 멈추면 톱니가 사라지는지는 설정 변경이라 확인하지 않았다. → 2026-11-19 [timesyncd 비활성화 검증](#timesyncd-비활성화-검증-2026-11-19)에서 확인했다. 줄다리기는 사라졌고, 30초 step은 WSL/Hyper-V 쪽 동기가 만드는 것으로 남았다(약 2.5ms).
- 권고(적용은 사용자 결정):
  - 측정 시간 기준: 생성기 속도 제어·경과 측정은 `CLOCK_MONOTONIC_RAW`(`time.clock_gettime(time.CLOCK_MONOTONIC_RAW)`)를 쓴다. 관찰에서 Windows QPC와 1ppm 안으로 맞았고 NTP·tick 보정을 받지 않는다. 현재 생성기는 `time.monotonic()`(`CLOCK_MONOTONIC`)을 쓴다.
  - 분석: 시계를 고치기 전 측정은 지금처럼 실제 경과로 비율 k를 구해 보정한다. realtime은 평균 속도는 맞지만 step(지금 ±160ms, 이전 1.3초)이 있어 1초 단위 구간 경계와 Spark·ClickHouse 쪽 시각 차이 지표가 그만큼 흔들린다.
  - 근본 해결: Windows 시간 동기를 복구한다(`w32tm /resync`). Ubuntu WSL 문서는 24.04 이하에서 Hyper-V 동기와 충돌을 피하려고 timesyncd 비활성화를 권장한다. 둘 다 적용하면 줄다리기가 없어진다.
- 원시 샘플은 저장소에 두지 않았다(세션 임시 디렉터리).

### timesyncd 비활성화 검증 (2026-11-19)

**판정: 통과.** `sudo systemctl disable --now systemd-timesyncd` 적용 후 커널 PLL 보정이 사라지고 tick이 고정되어 단조 시계 오차가 +65ppm(0.0065%)으로 줄었다. 5,000 EPS 측정에서 무시할 수 있는 크기이므로 생성기 시계는 `time.monotonic()`을 유지한다.

- 방법: WSL Ubuntu에서 2초 간격으로 `CLOCK_REALTIME`·`CLOCK_MONOTONIC`·`CLOCK_MONOTONIC_RAW`와 `adjtimex`(읽기 전용) 값을 기록하고, 30초마다 time.google.com에 SNTP 질의를 했다. 기준은 `CLOCK_MONOTONIC_RAW`다(2026-11-18 관찰에서 Windows QPC와 0ppm).
- 결과:

| 항목 | 2026-11-18 조사 | 비활성화 전(3분) | 비활성화 직후(5분) | 안정 후(5분) |
| --- | ---: | ---: | ---: | ---: |
| `CLOCK_MONOTONIC` 대비 RAW | +5,300ppm | +575ppm | +324ppm | +65ppm |
| adjtimex tick | 10,138~10,168 | 10,007~10,018 | 9,818~10,024 | 10,001 고정 |
| 커널 PLL offset·status | −0.3초, `STA_PLL` | −22ms, `0x2001` | 0, `0x0` | 0, `0x0` |
| realtime 30초 주기 step | 약 160ms | 약 −22ms | −52ms 1회 | 약 −2.5ms |
| NTP 대비 realtime(SNTP, 서버−로컬) | −0.17~−0.34초 | −112~+31ms | −47~+9ms | −30~+10ms |

- 30초 주기 realtime step은 timesyncd를 멈춘 뒤에도 남았다(`systemd-resolved`의 `Clock change detected`가 30초마다 계속 기록됨). step의 주체는 timesyncd가 아니라 WSL/Hyper-V 쪽 동기다. 크기가 약 2.5ms로 줄어 1초 구간 지표에 주는 영향은 작다.
- 비활성화 직후 5분은 timesyncd가 남긴 보정이 풀리는 과도 구간이다. 판정은 안정 후 값으로 한다.
- SNTP 값은 왕복 지연 약 270ms 때문에 ±수십 ms 흔들린다. 오차 범위 판단용이지 정밀 기준이 아니다.
- 한계: 측정 시간은 총 13분이다. Windows 시간 동기가 다시 끊기는 경우와 WSL 재시작 후 상태는 확인하지 않았다. Windows 동기가 끊기면 realtime이 Windows를 따라 NTP와 벌어지므로 장시간 측정 전에는 `w32tm /query /status`로 동기 상태를 확인한다.
- 되돌리기: `sudo systemctl enable --now systemd-timesyncd`
- 운영 환경은 이 문제와 무관하다. 운영 서버는 시간 동기 주체를 하나(예: chrony)만 둔다.

### 호스트 멈춤 기록 구현 검증 (2026-11-19)

[호스트 멈춤 기록](#호스트-멈춤-기록)과 `asynchronous_metric_log` 켜기를 적용·확인했다. 파이프라인(Spark·생성기)이 멈춘 상태에서 확인했다.

- 적용: ClickHouse 설정 파일을 바꾼 뒤 `docker compose restart clickhouse`는 실패했다(`error mounting ... low-memory.xml ... no such file or directory`). Docker Desktop이 파일 단위 bind mount의 이전 파일을 잡고 있었기 때문이다. `docker compose up -d --force-recreate clickhouse`로 재생성하면 적용된다. 데이터는 볼륨에 있어 유지됐다(`silver.order_events` 39,226,433행).
- `asynchronous_metric_log`:
  - 테이블 TTL이 `event_date + toIntervalDay(3)`로 생성됐다.
  - 지표 216개가 초마다 기록된다. 하루 약 1,870만 행이다.
  - 첫 25,515행이 20.83KiB(행당 0.84B)였다. 이 값으로 하루 약 15MiB, 3일 약 45MiB로 추정한다. 몇 분 분량으로 낸 초기 추정이라 하루 운영 후 다시 잰다.
  - `PSI_CPU_some`·`PSI_MEM_full`·`PSI_IO_full` 등 PSI 누적값도 포함돼, 멈춤 앞뒤 압박 추이를 이 테이블로도 볼 수 있다.
- `host-monitor`:
  - `docker pause`로 컨테이너를 4초 얼린 뒤 풀었더니 `gap_ms` 4669인 1행이 `ops.host_stalls`에 들어갔다. PSI·`mem_available_mb`·`load1` 값도 채워졌다.
  - `detected_at`은 UTC로 저장된다.
  - 평상시 사용량은 메모리 2.0MiB, CPU 0.27%다.
- 후속 확인(2026-11-24): 60분 `daily.csv` 재실행에서 감지기 3개 모두 지연 0건, `ops.host_stalls` 0건이었다. 이 실행에서는 오탐이 관측되지 않았다. 실제 VM 전체 정지 시 감지는 미확인이다. [PRD 03 판정](03-stream-processing.md#daily-전체-확인-재실행-kafka-cpus-3-2026-11-24) · [감지기 원자료](../measurements/2026-11-24-daily8-ticks/).

## 완료 기준

측정 부하는 3단계와 같은 5,000 EPS 지속 부하로 고정한다. 단계별 지연·대시보드 조회 시간 목표는 첫 측정 결과([지속 부하 측정 결과](#지속-부하-측정-결과-2026-11-18))를 기준으로 아래와 같이 정한다.

| 지표 | 목표 | 실측 12분 (2026-11-18) | 실측 25분, 정시 경계 포함 (2026-11-18) | 판정 |
| --- | --- | --- | --- | --- |
| 생성기 실제 ACK EPS | 4,750 이상 | 5,216 | 5,026 | 통과 |
| 트리거 시간 | 5초 초과 없음(워밍업·재시작 직후 제외) | 최대 4.76초 | 최대 3.19초 | 통과 |
| Kafka → 콜백 배치별 p95 | 6초 이하 | 최대 5.76초 | 최대 4.91초 | 통과 |
| 서버 INSERT 시간 p95 | 500ms 이하 | 255ms | 205ms | 통과 |
| `gold.orders_deduplicated` 15분 조회 p95 | 1초 이하 | 336ms(15분 미만 범위) | 239ms(15분 꽉 찬 범위) | 통과 |
| 파티션당 활성 part 수 | 50 이하 | 최대 11 | 최대 13 | 통과 |

- 선택 근거: 실측값보다 여유를 둔 값이다(INSERT 약 2배, 15분 조회 약 3배, part 수 약 4.5배). 트리거·Kafka → 콜백 목표는 트리거 주기 5초에 묶여 있어 여유를 작게 둔다. 15분 조회는 조합 D(롤업) 도입 판단 기준으로 쓴다.
- 비교한 대안: 실측 최댓값 그대로 목표로 쓰면 환경 변동(시계 차이 약 4%, 다른 프로세스)만으로 실패한다.

INSERT 성공 의미와 조회 경로, 중복 제거를 확인한다. 배치·구간별 운영 지표 3개 테이블과 감지 시 기록하는 `ops.host_stalls`의 적재를 확인한다. 쓰기 지연과 적재 실패를 각각 주입해 해당 구간 지표(`orders_write_ms`, `query_log` INSERT 시간, offset backlog)가 바뀌는지로 병목 구간을 판별할 수 있는지 검증한다.
