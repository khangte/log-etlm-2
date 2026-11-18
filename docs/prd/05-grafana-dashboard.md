# Grafana 시각화

- 상태: 초안
- 적용 범위: 제품 v1

## 목표

주문 현황을 확인하는 업무 대시보드와 각 단계의 지연·병목을 확인하는 파이프라인 운영 대시보드를 Grafana에서 제공한다. 3·4단계에서 남긴 Spark 멈춤·재시작·ID 충돌 감시를 Grafana 경보로 보완한다.

## 범위 및 후속 정의 사항

4단계에서 대시보드가 읽을 저장 계약(`gold` 뷰 2개, `ops` 테이블 3개, `system.query_log`·`system.part_log`)과 읽기 계정 `grafana_reader`를 구현·검증했다([ClickHouse 구현 기록](04-clickhouse-storage.md#구현-기록)). 5단계는 이 계약 위에 Grafana 서비스, 데이터 소스, 대시보드 2개, 경보 규칙을 추가한다. 저장 계약을 바꿔야 하면 ClickHouse PRD를 먼저 갱신한다. 아래 요구사항은 아직 구현·검증하지 않았다.

- 범위: Grafana 컨테이너, ClickHouse 데이터 소스, 주문 현황·파이프라인 운영 대시보드, [경보](#경보) 규칙. 모두 파일로 프로비저닝한다.
- 제외: 외부 알림 발송(Slack·메일·Webhook), 사용자·팀 권한 관리, 롤업 테이블([조합 D](04-clickhouse-storage.md#중복-제거조회-조합-비교) 미도입 판단 유지), 생성 → 조회 가능 지연([조회 가능 지연 제외](04-clickhouse-storage.md#조회-가능-지연-제외)).

## 실행 구성

### 버전

| 구성 요소 | 버전 | 근거 |
| --- | --- | --- |
| Grafana | `grafana/grafana:13.2.6` | 2026-11-18 기준 최신 안정 버전. Docker Hub `13.2.6` 태그(2026-11-10) |
| ClickHouse 데이터 소스 플러그인 | `grafana-clickhouse-datasource` 4.23.5 | [Grafana 플러그인 카탈로그](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) 최신 버전(2026-11-10) |

- 다른 서비스와 같이 정확한 버전으로 고정한다. 부 버전 태그(`13.2`)는 같은 설정이 다른 이미지로 바뀔 수 있어 쓰지 않는다.
- 플러그인은 `GF_PLUGINS_PREINSTALL_SYNC=grafana-clickhouse-datasource@4.23.5`로 기동 전에 설치한다. Grafana 설정의 `preinstall_sync`는 프로비저닝 전에 설치를 끝내므로 데이터 소스 프로비저닝이 플러그인 없음으로 실패하지 않는다. 버전을 고정하므로 `preinstall_auto_update`의 자동 갱신 대상이 아니다.
  - 비교한 대안: 플러그인을 넣은 이미지를 따로 빌드하면 기동 시 네트워크가 필요 없지만 Dockerfile과 빌드 단계가 추가된다. 로컬 개발 환경은 이미 이미지 내려받기에 네트워크를 쓰므로 도입하지 않는다.
  - 제약: 첫 기동과 플러그인 볼륨이 없는 기동은 grafana.com 접속이 필요하다.

### Compose 서비스

루트 `compose.yaml`에 `grafana` 서비스를 추가한다.

- 포트: `127.0.0.1:${GRAFANA_HOST_PORT}:3000`. ClickHouse HTTP와 같이 루프백에만 게시한다.
- 자원: `cpus` 0.5, `mem_limit` 512m([자원 제한](04-clickhouse-storage.md#자원-제한)). 5단계 측정에서 조정한다.
- 기동 순서: `clickhouse-init` 성공 후 기동한다. `grafana_reader` 계정이 있어야 데이터 소스 상태 확인이 통과한다.
- 관리자: `GF_SECURITY_ADMIN_USER`·`GF_SECURITY_ADMIN_PASSWORD`를 `.env`에서 필수 값(`:?`)으로 받는다. 익명 접근과 회원 가입은 끈다.
- 저장: Grafana 내부 DB(SQLite)는 이름 있는 볼륨 `grafana-data`에 둔다. 대시보드·데이터 소스·경보 규칙의 원본은 저장소의 프로비저닝 파일이며, 볼륨을 지워도 같은 상태로 복원된다.
- `.env.example`에 `GRAFANA_HOST_PORT`·`GRAFANA_ADMIN_USER`·`GRAFANA_ADMIN_PASSWORD`를 추가한다. `CLICKHOUSE_GRAFANA_PASSWORD`는 4단계 키를 그대로 쓴다.

### 프로비저닝

[예정 폴더 구조](overview.md#예정-폴더-구조)를 따른다.

```
infra/grafana/
├── provisioning/
│   ├── datasources/clickhouse.yaml   # 데이터 소스
│   ├── dashboards/dashboards.yaml    # 대시보드 파일 공급자
│   └── alerting/rules.yaml           # 경보 규칙
└── dashboards/
    ├── orders.json                   # 주문 현황
    └── pipeline.json                 # 파이프라인 운영
```

- 선택 근거: 화면에서 만든 대시보드는 Grafana 볼륨에만 남아 Git으로 변경을 추적할 수 없다. 파일 프로비저닝은 [Grafana 공식 프로비저닝](https://grafana.com/docs/grafana/latest/administration/provisioning/) 방식이며 추가 도구가 없다.
- 대시보드 공급자는 `allowUiUpdates: false`로 둔다. 화면에서 바꾼 내용은 저장되지 않으므로 JSON 모델을 내보내 파일에 반영한다.
- 비교한 대안: Terraform·Grafana API 스크립트는 여러 환경을 관리할 때 유리하지만 v1은 로컬 단일 환경이다.

### 데이터 소스

| 항목 | 값 | 근거 |
| --- | --- | --- |
| 이름·UID | `ClickHouse` / `clickhouse` | 대시보드 JSON이 UID로 참조한다. 고정하지 않으면 재생성 시 패널이 끊긴다 |
| 프로토콜·주소 | HTTP, `clickhouse:8123` | 생성기·Spark 지표가 이미 쓰는 인터페이스다. native 포트를 열 필요가 없다 |
| 계정 | `grafana_reader`, 비밀번호는 `secureJsonData`에 환경변수 `$CLICKHOUSE_GRAFANA_PASSWORD`로 주입 | [계정과 권한](04-clickhouse-storage.md#계정과-권한). 파일에 비밀번호를 두지 않는다 |
| 편집 | `editable: false` | 프로비저닝 파일이 원본이다 |

- 조회·경보 쿼리는 모두 이 데이터 소스 하나를 쓴다. 데이터베이스 이름을 쿼리에 명시한다(`gold.`·`ops.`·`system.`).

### 시간대와 시간 조건

- ClickHouse 시각 컬럼은 `DateTime64(3, 'UTC')`다. 대시보드 `timezone`은 `Asia/Seoul`로 두어 표시만 한국 시간으로 바꾼다. 쿼리 안에서 시간대를 변환하지 않는다.
- 시간 조건은 플러그인 매크로 `$__timeFilter(<컬럼>)`로 대시보드 시간 범위를 넘긴다. 시간 버킷은 `$__timeInterval(<컬럼>)` 또는 `toStartOfInterval`로 만든다.
- 모든 패널·경보 쿼리는 시간 범위 조건을 포함한다. `system.query_log`·`system.part_log`는 정렬 키가 `(event_date, event_time)`이므로 `event_date`와 `event_time`에 함께 조건을 건다. 범위 조건 없이 누적 시스템 로그를 읽지 않는다.

## 주문 현황 대시보드

주문 데이터 조회는 [ClickHouse 중복 처리 정책](04-clickhouse-storage.md#jdbc-재시도와-적재-중복-처리)의 `orders_deduplicated` 뷰를 사용한다. 해당 뷰는 `order_events FINAL`로 이벤트 중복을 제거한다. 모든 패널 쿼리는 `event_timestamp` 범위 조건을 포함한다. 이 경로와 1분 갱신을 선택한 근거는 [중복 제거·조회 조합 비교](04-clickhouse-storage.md#중복-제거조회-조합-비교)를 따른다. 백그라운드 병합 완료를 기다리지 않고 아래 지표와 차원별 집계를 모두 이 경로에서 계산한다. 원본 테이블에서 직접 합산하지 않는다.

| 지표 | 계산 기준 |
| --- | --- |
| 주문 수 | `event_id` 기준 중복 제거한 정상 주문 이벤트 수 |
| 주문 매출 | 중복 제거한 정상 주문의 `total_amount` 합계 |
| 평균 주문 금액 | 주문 매출 / 주문 수. 주문이 없으면 값 없음으로 표시 |
| 상품 판매 수량 | 중복 제거한 정상 주문의 `items[].quantity` 합계 |
| 할인 금액 | 중복 제거한 정상 주문의 `discount_amount` 합계 |

- 매출은 할인 적용 후 주문 완료 금액(KRW)이며 취소·환불을 반영한 순매출은 아니다.
- 분당 주문 수·매출 추이, 카테고리별 판매 수량·매출, 지역(`region`)별 주문 수·매출, 결제수단별 주문 수·매출, 회원 등급별 주문 수·평균 주문 금액을 제공한다.
- 현재는 주문당 상품 1종이므로 주문 매출을 해당 카테고리에 귀속한다. 다종 상품으로 확장하면 할인 배분과 주문 금액 중복 합산 방지 기준을 먼저 정의한다.
- 집계 기준은 `event_timestamp`이며 저장·처리는 UTC, 화면 표시는 한국 시간으로 한다. 최근 15분 조회·1분 집계·1분 자동 갱신을 기본값으로 한다.
- 진행 중인 분: 분당 추이 패널은 진행 중인 분을 막대·선으로 그리되 마지막 버킷을 별도 색(필드 오버라이드)으로 표시하고 패널 설명에 "마지막 1분은 미완료"를 적는다. 합계 패널(주문 수·매출 등)은 진행 중인 분을 포함한 조회 범위 전체 값이다.
  - 비교한 대안: 진행 중인 분을 쿼리에서 빼면 오해는 없지만 화면 반영이 최대 1분 더 늦어진다.
- 지원 시간 범위: 기본 15분, 최대 1시간까지를 v1 지원 범위로 한다. 15분 조회 p95는 4단계에서 239ms로 측정했다([완료 기준](04-clickhouse-storage.md#완료-기준)). 1시간 조회는 닫힌 파티션의 part가 병합되지 않은 채 남는 상태([15분 이상 지속 부하](04-clickhouse-storage.md#15분-이상-지속-부하-측정-결과-2026-11-18))에서 측정하지 않았으므로 5단계에서 측정한다. 1시간을 넘는 범위는 Grafana가 막지 않지만 성능 목표 대상이 아니다.
- 충돌 표시: [ID 충돌 경보](#경보)가 발생 중이면 주문 대시보드 상단에 경보 상태 패널로 표시한다. 충돌 행은 `FINAL`로도 합쳐지지 않아 해당 구간 주문 수·매출이 부풀 수 있다([충돌 점검 쿼리 검증](04-clickhouse-storage.md#충돌-점검-쿼리-검증-결과-2026-11-18)).

## 파이프라인 운영 대시보드

우선 목적은 병목이 생겼을 때 어느 구간이 원인인지 판별하는 것이다. 구간별 소요시간과 앞단 대기열(Producer 큐, Kafka offset backlog)을 함께 표시하며, 판정 순서는 [ClickHouse 운영 지표 저장](04-clickhouse-storage.md#운영-지표-저장)을 따른다. 단계 내부 소요시간과 단계 사이 대기·전달 지연을 구분한다. 생성 → ClickHouse 조회 가능 지연은 v1에서 측정하지 않는다([조회 가능 지연 제외](04-clickhouse-storage.md#조회-가능-지연-제외)). 각 지연 패널은 측정 구간·단위(ms)·평균·p50·p95·p99·측정 건수·실패 건수와 샘플링 여부를 제공한다. 이벤트 단위와 배치·요청 단위 지표는 서로 다른 모집단임을 표시한다.

| 구간 | 지표와 측정 기준 | 해석·제약 |
| --- | --- | --- |
| 로그 생성기 | 생성 호출 직전 → 최종 UTF-8 bytes 반환 직후 | 생성·직렬화 소요시간. EPS 조절 대기는 제외 |
| Producer → Kafka | 전송 API 호출 → delivery report 생성의 ACK 지연 | 클라이언트 대기·전송·브로커 확인 포함. [Kafka PRD](02-kafka-ingestion.md)의 `msg.latency()` 기준 |
| 생성 → Kafka | Kafka LogAppendTime − `event_timestamp` | 생성 이후 브로커 추가까지의 지연. ACK 지연과 겹치므로 합산하지 않음 |
| Kafka → Spark | `foreachBatch` 콜백 진입 시각 − Kafka LogAppendTime | 소비 대기 근사치. 트리거 시작부터 콜백 진입까지의 준비 시간 포함. 미래 timestamp·누락은 별도 집계하며 트리거 시작 기준 지연으로 표시하지 않음 |
| Spark 배치 | `StreamingQueryProgress.durationMs.triggerExecution` | 트리거 전체 소요시간. 출력·체크포인트 등 포함 범위를 명시하며 순수 정제 시간으로 표시하지 않음 |
| Spark → ClickHouse | 정상 DataFrame의 JDBC write action 호출 직전 → 반환 또는 실패 | 선행 변환·스케줄링·태스크 재시도 등이 포함될 수 있는 출력 action 소요시간 |
| ClickHouse INSERT | `system.query_log`의 INSERT 쿼리별 `query_duration_ms` | 서버 측 요청별 처리시간. 네트워크·클라이언트 대기는 포함하지 않으며 Spark 측 JDBC write action 시간과 별도 패널로 표시 |
| Grafana 조회·화면 | 조회 요청 → 응답 수신, 이벤트 생성 → 화면 반영 | 조회 응답시간과 화면 반영 지연을 별도 표시. 화면 반영은 브라우저 계측 필요 |

- 지연과 함께 Producer ACK EPS·전송 실패·큐 길이, Spark 입력·처리 EPS·정상/제외/DLQ 건수, Kafka 파티션별 backlog, ClickHouse 쓰기 실패·재시도, 데이터 최신성을 표시한다.
- Kafka backlog는 보관 중인 끝 offset과 Spark 체크포인트의 처리 완료 offset 경계를 기준으로 정의한다. consumer group lag와 동일하다고 가정하지 않는다. offset 차이는 레코드 건수의 근사 지표로 표시하고 실제 입력 건수와 구분한다. 수집 경로는 아래 패널별 데이터 원천의 progress 지표를 따른다.
- 재시도는 시도별 소요시간과 최종 성공까지의 시간을 구분한다. JDBC write action·INSERT 요청·Spark 배치 시간은 포함 관계가 있으므로 합산하지 않는다.
- 생성기·Producer·Spark·JDBC·조회 관측 지표는 별도 운영 데이터로 수집한다. 주문 행만으로 모든 지표를 계산한다고 가정하지 않는다. 지표 저장소와 Grafana 데이터 소스는 ClickHouse 하나로 하며, 저장 구조와 지표 손실 정책은 [ClickHouse 운영 지표 저장](04-clickhouse-storage.md#운영-지표-저장)을 따른다. `ops` 테이블과 `silver.order_events`의 보관은 24시간이므로 운영 대시보드 조회 범위도 24시간 이내다.

### 패널별 데이터 원천

두 대시보드 모두 ClickHouse 데이터 소스 하나로 조회한다. 데이터베이스 구분은 [저장 계층](04-clickhouse-storage.md#저장-계층)을 따른다. 주문 현황 대시보드는 `gold.orders_deduplicated`만 사용한다. Grafana 계정은 `gold`·`ops`와 `system.query_log`·`system.part_log`만 읽으며 `silver`를 직접 조회하지 않는다([계정과 권한](04-clickhouse-storage.md#계정과-권한)). 파이프라인 운영 대시보드의 원천은 아래와 같다. 분위수는 원천 행에서 SQL로 계산하며 배치별 분위수를 다시 평균·합산하지 않는다.

| 패널 | 원천 |
| --- | --- |
| 생성 → Kafka, Kafka → Spark 지연 분위수 | `gold.order_event_latency`의 `event_timestamp`·`kafka_append_timestamp`·`spark_processed_at`. 정상 주문만 대상이며 재처리 시도 행을 합치지 않음 |
| Spark 트리거 시간·입력/처리 EPS·backlog | `ops.spark_query_progress` |
| 분류·정상 쓰기·DLQ 쓰기 action 시간, 분류별 건수, 실패·재시도 | `ops.spark_batch_attempts`. 실패한 시도 포함 |
| Spark 재시작 | `ops.spark_query_progress`의 `run_id` 변화. 같은 `query_id`에서 `run_id`가 바뀌면 재시작 1회 |
| ClickHouse INSERT 요청별 처리시간·실패 | `system.query_log`. `user = 'spark_writer'`로 Spark 적재 요청만 거름 |
| part 생성·병합 | `system.part_log` |
| 로그 생성기·Producer 지표 | `ops.producer_stats`. 1초 구간 통계이며 구간별 분위수를 다시 합치지 않음 |
| Grafana 조회 서버 시간 | `system.query_log`의 `user = 'grafana_reader'` 행 |

- `query_log`·`part_log`의 flush 주기는 서버 기본 설정 7,500ms다(2026-11-18 실행 중인 서버의 `config.xml`에서 확인). 해당 패널은 최대 약 7.5초 늦게 반영되며 패널 설명에 적는다. 이 값을 바꾸지 않는다.
- 운영 대시보드는 최근 15분 조회·5초 갱신을 기본값으로 한다. 5초는 Grafana 기본 `min_refresh_interval`과 같고 Spark 트리거 주기와 같다. 시간 버킷은 5초(트리거 1회분)로 하며, 백분위 패널의 집계 구간을 패널 설명에 적는다. 미수집·미계측 값은 0이 아닌 측정 불가로 표시한다.

### 4단계 실측에서 정한 표시 규칙

[ClickHouse 측정 후 남은 작업](04-clickhouse-storage.md#측정-후-남은-작업-2026-11-18)을 아래와 같이 반영한다.

| 대상 | 규칙 | 근거 |
| --- | --- | --- |
| offset backlog | `latest_offsets − end_offsets`를 파티션별·합계로 표시한다. backlog 패널 옆에 트리거 시간, 배치 입력 건수, Kafka → 콜백 p95를 함께 둔다. backlog 0만으로 정상 판정하지 않는다 | backlog는 배치 계획 시점 값이라 `maxOffsetsPerTrigger` 상한 아래에서는 0이다. 느려진 상태는 입력 건수·트리거 시간·p95로 드러난다 |
| 재시작 직후 진행 행 | `latest_offsets`가 빈 Map(`empty(latest_offsets)`)이면 backlog를 NULL로 두어 측정 불가로 표시한다 | 빈 Map이면 차이가 음수가 된다 |
| INSERT 실패 | `user = 'spark_writer' AND exception_code != 0`으로 센다. `tables`·`query_kind`로 거르지 않는다 | 권한·인증 오류는 `ExceptionBeforeStart`, `tables` 빈 배열로 남는다 |
| 실패 INSERT의 행 수 | 실패 요청의 `written_rows`를 적재 행 수로 표시하지 않는다. 실패 건수만 표시한다 | 실패 요청의 `written_rows`는 실제로 남은 행 수가 아니다 |
| 실패 시도의 시간 | `status != 'succeeded'`인 시도의 0 값 시간은 분위수 계산에서 뺀다. 실패 시도는 `failed_stage`별 건수로 따로 표시한다 | 실패 단계·실패한 action의 시간은 측정하지 못해 0으로 저장된다 |
| 측정 건수 0 | `_count = 0`인 구간·배치의 분위수는 NULL로 바꿔 측정 불가로 표시한다 | 측정 대상이 없으면 분위수를 0으로 저장한다 |

### 데이터 최신성

- 계산: ClickHouse `now64(3)` − `gold.orders_deduplicated`의 조회 범위 내 `max(event_timestamp)`. 범위 안에 행이 없으면 데이터 없음으로 표시하고 0이나 큰 값으로 대체하지 않는다.
- 시계 제약: `event_timestamp`는 호스트 생성기의 realtime 시계, `now64()`는 ClickHouse 컨테이너 시계다. WSL에서 realtime이 주기적으로 뒤로 보정되는 현상(관찰 160ms~1.3초)이 있어 최신성 값은 그만큼 흔들린다([WSL 시계 차이 조사](04-clickhouse-storage.md#wsl-시계-차이-조사-2026-11-18)). 1초 미만 차이를 지연 변화로 해석하지 않으며 패널 설명에 적는다.

## 경보

Grafana 통합 경보(unified alerting)로 아래 규칙을 파일 프로비저닝한다. 평가 쿼리는 같은 데이터 소스와 `grafana_reader`로 실행한다. v1은 외부 알림을 보내지 않고 경보 상태를 Grafana 경보 화면과 대시보드의 경보 상태 패널에서 확인한다.

| 경보 | 조건 | 평가 주기·대기 | 근거 |
| --- | --- | --- | --- |
| Spark 진행 공백 | 최근 30초 `ops.producer_stats`의 `acked` 합계 > 0인데 최근 30초 `ops.spark_query_progress`에 `num_input_rows > 0`인 행이 없음 | 10초 / 대기 없음 | Driver 감시 스레드는 프로세스 자체가 멈추거나 이벤트는 오는데 처리량만 0인 경우를 감지하지 못한다([멈춤 감시·자동 재시작 검증](04-clickhouse-storage.md#멈춤-감시자동-재시작-검증-2026-11-18)). 입력이 있을 때만 판정해 생성기 정지를 장애로 보지 않는다 |
| Spark 재시작 | 최근 15분 같은 `query_id`의 서로 다른 `run_id` 수 ≥ 2 | 1분 / 대기 없음 | Compose 재시작은 횟수 상한이 없어 지속 실패가 무한 반복된다(PRD 03 참조). 반복 재시작을 사람이 알아채는 유일한 경로다 |
| 배치 실패 | 최근 5분 `ops.spark_batch_attempts`의 `status != 'succeeded'` 행 ≥ 1 | 1분 / 대기 없음 | 실패 시도는 재시작으로 자동 복구되더라도 원인 확인이 필요하다 |
| ClickHouse INSERT 실패 | 최근 5분 `system.query_log`의 `user = 'spark_writer' AND exception_code != 0` ≥ 1 | 1분 / 대기 없음 | 위 표시 규칙과 같은 필터 |
| ID 충돌(1시간) | 최근 1시간 같은 `event_id`에 서로 다른 `event_timestamp`가 있는 건수 > 0 | 1분 / 대기 없음 | [충돌 감지 정책](04-clickhouse-storage.md#이벤트-식별과-테이블-기준)의 1시간 점검 |
| ID 충돌(전체 보관 구간) | 보관 구간(24시간) 전체에서 같은 조건 건수 > 0 | 1시간 / 대기 없음 | 두 시각이 1시간 이상 떨어진 충돌은 1시간 점검이 놓친다 |

- 진행 공백 30초는 트리거 주기 5초의 6배, Driver 감시 시간 90초의 1/3이다. 감시가 재시작하기 전에 경보가 먼저 뜬다. `ops` 지표 쓰기 실패로도 공백이 생길 수 있으므로 경보 설명에 Spark 로그 확인을 적는다.
- 충돌 점검은 `silver`를 읽지 않고 `FINAL` 없는 `gold.order_event_latency`(`event_id`·`event_timestamp` 포함)로 계산한다. 같은 `event_timestamp`의 재처리 중복은 `uniqExact(event_timestamp) > 1` 조건에 걸리지 않는다.

  ```sql
  -- 같은 event_id에 서로 다른 event_timestamp가 있는 충돌 건수(1시간 점검).
  SELECT count() FROM (
    SELECT event_id FROM gold.order_event_latency
    WHERE event_timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY event_id HAVING uniqExact(event_timestamp) > 1
  );
  ```

  - 선택 근거: 4단계에서 충돌 행은 `FINAL`로도 2행이 남는 것을 확인했으므로 `gold.orders_deduplicated`로도 셀 수 있지만 `FINAL` 비용만 더해진다. `grafana_reader`에 `silver` 권한을 주지 않는다.
  - 비교한 대안: 대시보드 패널로 매 갱신마다 실행하면 [충돌 감지 정책](04-clickhouse-storage.md#이벤트-식별과-테이블-기준)의 "대시보드 갱신마다 실행하지 않는다"에 어긋나고 1분 갱신마다 1시간 그룹 집계가 돈다.
  - 제약: 1시간·24시간 점검 비용은 지속 부하에서 측정하지 않았다. 5단계 측정에서 서버 시간·메모리를 기록하고 주기를 조정한다.
- 경보 상태는 Grafana 내부 DB에 남는다. 볼륨을 지우면 이력이 사라지며, v1은 이력 보존을 요구하지 않는다.
- 비교한 대안: Slack·Webhook 연락처는 외부 서비스·비밀값이 추가된다. 로컬 단일 사용자 환경이라 화면 확인으로 충분하며, 필요해지면 연락처 프로비저닝만 추가한다.

## 처리지연 측정과 표시

[공통 측정 원칙](01-log-generator.md#공통-측정-원칙)을 따른다. 다음 지표를 구분해 표시한다.

| 지표 | 측정 기준 | 해석 |
| --- | --- | --- |
| 단계별 처리시간 | 생성기·Kafka 전송·Spark 배치·ClickHouse INSERT 지표 | 평균·p50·p95·p99와 측정 건수·실패 건수 표시 |
| 데이터 최신성 | 조회 기준 현재 UTC 시각 − 조회 범위 내 최신 `event_timestamp` | 최신 데이터가 얼마나 오래됐는지 나타내며 개별 이벤트 처리지연과 다름 |
| 조회 응답시간 | 실제 대시보드 조회 요청 시작 → 응답 수신 | 조회·네트워크 지연. 서버 측은 `query_log`의 `grafana_reader` 행, 클라이언트 측은 브라우저 계측으로 경계를 구분 |
| 화면 반영 지연 | 이벤트 생성 → 해당 이벤트 또는 이를 포함한 집계의 화면 반영 | 자동 갱신 대기·조회·렌더링을 포함하며 별도 브라우저 계측으로 검증 |

- 대시보드 자동 갱신 주기, 조회 시간 범위, 집계 간격, 캐시 설정을 측정 결과에 함께 기록한다. 데이터가 없는 경우 최신성을 0으로 표시하지 않고 데이터 없음으로 구분한다.
- 화면 반영 지연은 샘플 이벤트를 포함한 검증 패널과 브라우저 계측으로 확인한다. 서버 쿼리 완료나 응답 수신을 화면 반영 완료로 간주하지 않는다. 실제 화면 반영을 계측하지 않으면 해당 수치를 측정값으로 표시하지 않는다.
- Grafana 표시 지연과 데이터 최신성을 분리한다. 집계 패널은 집계 윈도우 대기까지 포함할 수 있으므로 해당 조건을 명시한다.
- Kafka backlog는 offset 기반 건수와 시간 기반 지연을 구분해 표시한다. Spark 배치 시간, 실제 EPS, 적재 실패·재시도와 함께 보며 병목을 판단한다.
- 서비스 간 시계 오차·측정 불가·미발견 이벤트를 누락시키지 않고 별도 지표로 표시한다. 구간별 백분위수의 단순 평균이나 겹치는 단계 지연의 합계를 종단 간 지연으로 표시하지 않는다.

## 완료 기준

측정 부하는 3·4단계와 같은 5,000 EPS 지속 부하(15분 이상)로 고정하고, 두 대시보드를 기본 갱신 주기로 연 상태에서 측정한다. 아래 목표는 4단계 실측을 기준으로 정한 초기값이며 첫 측정 결과로 다시 확인한다.

| 지표 | 목표 | 근거 |
| --- | --- | --- |
| 4단계 완료 기준 전체 | 대시보드를 연 상태에서도 [4단계 목표](04-clickhouse-storage.md#완료-기준)(ACK EPS, 트리거 시간, Kafka → 콜백 p95, INSERT p95, 15분 조회 p95, part 수)를 모두 통과 | 대시보드·경보 조회가 같은 ClickHouse CPU 2개를 쓴다. 조회 부하가 적재 목표를 깨면 안 된다 |
| 주문 대시보드 패널 쿼리 서버 시간 p95(15분 범위) | 1초 이하 | 4단계 15분 조회 목표와 같다 |
| 주문 대시보드 1시간 범위 패널 쿼리 서버 시간 p95 | 측정 후 확정 | 1시간 범위는 미측정. 결과에 따라 지원 범위를 줄이거나 조합 D 도입을 다시 검토한다 |
| 운영 대시보드 패널 쿼리 서버 시간 p95 | 1초 이하 | 5초 갱신 안에 끝나야 다음 갱신과 겹치지 않는다 |
| 충돌 점검 쿼리 서버 시간(1시간·24시간) | 측정 후 확정 | 평가 주기 조정 근거로 쓴다 |
| Grafana 컨테이너 | 측정 동안 OOM·재시작 없음, 메모리 최대치 기록 | 512MiB 제한 확인 |

기능 확인 항목은 아래와 같다.

- 빈 볼륨에서 `docker compose up`만으로 데이터 소스·대시보드 2개·경보 규칙이 생성되고 데이터 소스 상태 확인이 통과한다. `grafana_reader`로 `silver` 조회·쓰기 쿼리가 거부된다.
- 최신성·화면 반영 지연의 구분, 갱신 주기와 조회 조건 표시, 데이터 없음·측정 불가 표시를 확인한다. 갱신 주기를 바꿔 화면 대기 영향과 데이터 처리 지연이 구분되는지 검증한다.
- 주문 표본의 기대 집계와 화면 수치를 대조하고 중복 재처리 시 주문 수·매출이 부풀지 않는지 확인한다.
- 단계별 지연·장애를 주입해 해당 패널과 실패·재시도·backlog 변화를 검증한다. Spark 측 JDBC write action 시간과 `system.query_log`의 서버 측 INSERT 처리시간의 측정 경계를 구분해 표시하는지 확인한다.
- 경보: Python worker SIGSTOP으로 진행 공백 경보가 Driver 감시 종료(약 90초)보다 먼저 뜨는지, 재시작 후 재시작 경보가 뜨는지, 권한 회수로 INSERT 실패·배치 실패 경보가 뜨는지, 충돌 행 주입으로 1시간·전체 충돌 경보가 뜨는지 확인한다. 생성기를 멈춘 상태에서는 진행 공백 경보가 뜨지 않아야 한다. 주입한 시험 행은 확인 후 삭제한다.
