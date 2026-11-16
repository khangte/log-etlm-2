# 프로젝트 개요

- 상태: 초안
- 적용 범위: 제품 v1

## 목표와 범위

대량 이벤트 로그를 10초 이내에 처리하는 근실시간 파이프라인을 구축한다. 처리량과 종단 간 지연은 통합 후 측정한다.

`Python Producer → Kafka → Spark Structured Streaming → ClickHouse → Grafana`

v1은 주문 완료 단일 분석 이벤트 도메인이다. health_check는 필터링 검증용이다. v2는 다중 이벤트 도메인으로 확장한다. 제품 버전과 이벤트 schema_version은 별도로 관리한다.

추후 파이프라인이 안정화되면 apps/collector(FastAPI)를 앞단에 추가하고, 기존 log_generator를 HTTP 요청을 쏘는 트래픽 생성기(또는 tests/load의 Locust 스크립트)로 전환을 고려한다.

초기 생성 부하는 전체 로그 합계 5,000 EPS(Events Per Second)로 한다. 정상 주문의 직렬화된 UTF-8 페이로드는 대략 1,000B 규모를 참고하되 크기 조건은 두지 않으며 패딩 없이 생성한다. EPS에는 정상·health_check·불량 로그를 모두 포함한다. 실제 페이로드 크기와 B/s를 측정하고 줄바꿈은 페이로드 크기에서 제외한다. 세부 스키마와 검증 기준은 [로그 생성기](01-log-generator.md)에서 정의한다.

생성 데이터는 실제 회원과 연결되지 않는 합성 데이터로 한정한다. 사용자 식별에는 합성 `user_id`를 사용하고 이름·이메일·전화번호·상세 주소·IP는 로그에 포함하지 않는다. 지역·기기 정보는 집계 검증용 합성값만 사용한다.

## 개발 단계

개발에 앞서 [개발환경](00-development-environment.md)의 도구, 실행 방식과 설정 관리 기준을 따른다. 환경은 각 단계에 필요한 범위부터 구성한다.

1. [로그 생성기](01-log-generator.md)
2. [Kafka 연동](02-kafka-ingestion.md)
3. [Spark 스트림 처리](03-stream-processing.md)
4. [ClickHouse 적재](04-clickhouse-storage.md)
5. [Grafana 시각화](05-grafana-dashboard.md)
6. 상세 세팅 조정: 측정 결과를 바탕으로 범위를 정의한다.

각 단계는 PRD 보완 → 구현 → 검증 순서로 진행한다.

## 기술 선정 근거


### 실시간 이벤트 로그 파이프라인 기술 선정 근거 (Architecture Decision Record)

> **Architecture Flow:** `Python Producer` ➔ `Apache Kafka` ➔ `Spark Structured Streaming` ➔ `ClickHouse` ➔ `Grafana`

### 1. 메시지 브로커: Apache Kafka

**대안 비교:** `RabbitMQ`, `Redis Streams / Pub-Sub`

| 비교 항목                 | Apache Kafka (채택)                                                    | RabbitMQ                                        | Redis Streams / Pub-Sub                             |
| :------------------------ | :--------------------------------------------------------------------- | :---------------------------------------------- | :-------------------------------------------------- |
| **데이터 보존 및 재처리** | 디스크 순차 기록으로 소비 후에도 데이터 유지 (Offset 기반 재처리 가능) | 컨슈머 소비 즉시 큐에서 메시지 삭제             | 메모리 기반으로 장기 보관 시 비용 폭증 및 유실 위험 |
| **처리량 (Throughput)**   | 대용량 스트림에 특화 (초당 수십만 건 이상 처리)                        | 복잡한 라우팅에 강하나 대용량 적재 시 병목 발생 | 매우 빠르나 단일 노드 메모리 한계 존재              |
| **확장성**                | 다수의 컨슈머 그룹이 동일 토픽을 독립적으로 구독 가능                  | 1:1 또는 특정 라우팅 바인딩 구조 중심           | 대용량 분산 파티셔닝 및 영속성 관리 취약            |

- **선정 근거:**
  - `RabbitMQ`는 메시지 전달 후 데이터가 삭제되어 뒷단(Spark) 장애 발생 시 과거 로그를 재처리(Replay)하기 어렵고, `Redis`는 메모리 한계로 인해 트래픽 폭주 시 데이터 유실 위험이 있습니다.
  - **Apache Kafka**는 디스크 기반의 압도적인 처리량과 오프셋(Offset) 관리를 통한 데이터 재처리 기능을 제공하여, 수집부와 처리부의 결합도를 낮추고(Decoupling) 무손실 파이프라인을 보장하기 위해 선택했습니다.

### 2. 스트림 처리 엔진: Spark Structured Streaming

**대안 비교:** `Apache Flink`, `Kafka Streams`

| 비교 항목                  | Spark Structured Streaming (채택)                               | Apache Flink                                           | Kafka Streams                                              |
| :------------------------- | :-------------------------------------------------------------- | :----------------------------------------------------- | :--------------------------------------------------------- |
| **처리 모델 및 지연 시간** | **마이크로 배치** (수 초 단위). 대시보드 목적의 근실시간성 충족 | 순수 이벤트 단위 스트리밍 (밀리초 단위). 초저지연 강점 | 경량 라이브러리 형태. 복잡한 분산 연산 한계                |
| **배치(Batch) 코드 통합**  | 기존 배치 처리와 **동일한 DataFrame/SQL API** 공유              | 스트리밍과 배치 API 이원화로 학습 및 운영 난이도 상승  | 별도 클러스터 없이 앱 내장형으로 실행되어 대규모 확장 제한 |
| **생태계 및 유지보수**     | Python(PySpark) 지원이 성숙하고 레퍼런스가 풍부함               | 주로 Java/Scala 기반 운영 권장                         | Java/Scala 전용                                            |

- **선정 근거:**
  - `Apache Flink`는 밀리초(ms) 단위의 초저지연 처리에 유리하지만 클러스터 운영 복잡도가 높습니다. 본 시스템의 목표는 **5~10초 단위의 실시간 지표 모니터링**이므로 마이크로 배치 아키텍처로도 요구사항을 완벽히 충족합니다.
  - **Spark Structured Streaming**은 중첩 JSON 파싱, 배열 데이터 분리(`explode`), 노이즈 필터링 등 무거운 전처리를 분산 환경에서 안정적으로 수행하며, 추후 과거 데이터 재집계(Batch) 파이프라인과 동일한 코드 베이스를 재사용할 수 있어 개발 및 운영 효율성 면에서 채택했습니다.

### 3. 분석용 데이터베이스(OLAP): ClickHouse

**대안 비교:** `RDBMS (MySQL/PostgreSQL)`, `Elasticsearch`, `Cloud DW (BigQuery)`

| 비교 항목                  | ClickHouse (채택)                                                   | RDBMS (MySQL 등)                                                | Elasticsearch                                                     | Cloud DW (BigQuery)                                               |
| :------------------------- | :------------------------------------------------------------------ | :-------------------------------------------------------------- | :---------------------------------------------------------------- | :---------------------------------------------------------------- |
| **저장 구조 및 집계 속도** | **열 기반(Columnar)** 저장. 수억 건의 `GROUP BY` 연산 0.1초 내 처리 | **행 기반(Row)** 저장. 대용량 로그 집계 시 디스크 I/O 병목 발생 | 역인덱스 기반. 텍스트 검색엔 강하나 수치 집계 시 메모리 소모 과다 | 대용량 분석엔 적합하나 쿼리당 과금 및 수 초 단위의 응답 지연 발생 |
| **압축률 및 자원 효율**    | 동일 컬럼 데이터 연속 배치로 **압축률이 매우 높음**                 | 인덱스 용량이 원본 데이터보다 커지는 현상 발생                  | 원본 JSON과 인덱스를 모두 저장해 디스크 점유율이 높음             | 로컬/온프레미스 환경 구축 불가 (클라우드 종속)                    |

- **선정 근거:**
  - `MySQL` 같은 행 기반 RDBMS는 수천만 건 이상의 로그 집계 시 심각한 쿼리 지연이 발생하며, `Elasticsearch`는 전문 검색(Full-text Search)에는 강하지만 수치 기반 시계열 집계 시 메모리 및 스토리지 비용이 과도합니다.
  - **ClickHouse**는 열 기반 저장 구조와 벡터화(Vectorized) 연산을 통해 압도적인 데이터 압축률과 0.1초 이내의 초저지연 OLAP 집계 성능을 제공하므로, 다차원 실시간 필터링(Slice & Dice)이 요구되는 대시보드 백엔드로 가장 적합했습니다.

### 4. 시각화 및 모니터링: Grafana

**대안 비교:** `Apache Superset / Tableau`, `Kibana`

| 비교 항목             | Grafana (채택)                                                  | Apache Superset / Tableau                                       | Kibana                                              |
| :-------------------- | :-------------------------------------------------------------- | :-------------------------------------------------------------- | :-------------------------------------------------- |
| **실시간 갱신 주기**  | **1초~5초 단위의 초단기 자동 새로고침** 및 스트리밍 차트 최적화 | 최소 10초~1분 단위 갱신 위주. 무거운 비즈니스 리포트 탐색용     | Elasticsearch 전용 도구로 ClickHouse 직접 연동 불가 |
| **운영 및 알람 통합** | 임계치 초과 시 **Slack/Webhook 실시간 알람** 기본 내장          | 단순 시각화(BI)에 집중되어 실시간 장애/이상 징후 알림 기능 미흡 | Elastic 스택에 강하게 종속됨                        |

- **선정 근거:**
  - `Superset`이나 `Tableau`는 비개발자의 다차원 데이터 탐색(Self-service BI)에는 유리하지만, 5초 이내의 빠른 시계열 지표 변화 관측에는 무겁습니다.
  - **Grafana**는 초단위 자동 갱신과 시계열 데이터 렌더링에 최적화되어 있으며, ClickHouse 공식 플러그인 연동은 물론 특정 지표 급감·급증 시 즉각 대응할 수 있는 **실시간 알람(Alerting) 기능**까지 단일 도구로 통합할 수 있어 선택했습니다.


## 예정 폴더 구조

앱은 `log_generator`, `streaming`으로 구분한다. 현재는 로그 생성기만 구현하며 streaming은 디렉터리 자리만 확보한다. 환경변수 접두사 `PRODUCER_`는 기존 실행 설정과의 호환을 위해 유지한다.


```
log-etlm-2/
├── README.md
├── AGENTS.md
├── .gitignore
├── .env.example
├── compose.yaml
│
├── apps/
│   ├── log_generator/                 # 합성 로그 생성 및 전송
│   │   ├── pyproject.toml
│   │   ├── uv.lock
│   │   ├── .python-version
│   │   ├── src/log_generator/
│   │   │   ├── __init__.py
│   │   │   ├── main.py
│   │   │   ├── config.py
│   │   │   ├── generator.py
│   │   │   ├── pools.py
│   │   │   └── outputs/
│   │   │       ├── __init__.py
│   │   │       ├── file.py             # stdout은 main.py에서 연결
│   │   │       ├── kafka.py            # 2단계에서 추가
│   │   │       └── http.py             # FastAPI 도입 시 추가
│   │   └── tests/
│   │
│   └── streaming/                     # 3단계에서 구현
│       └── src/streaming/
│           ├── main.py
│           ├── config.py
│           ├── source.py
│           ├── schemas.py
│           ├── transforms.py
│           └── sink.py
│
├── contracts/
│   └── events/
│       ├── order_completed.v1.schema.json
│       └── examples/
│           └── order_completed.v1.json
│
├── infra/
│   ├── kafka/
│   │   └── init-topics.sh
│   ├── spark/
│   │   └── spark-defaults.conf
│   ├── clickhouse/
│   │   └── config.xml
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/
│       │   └── dashboards/
│       └── dashboards/
│
├── sql/
│   └── clickhouse/
│       ├── migrations/
│       │   └── 001_create_events.sql
│       └── queries/
│           └── event_latency.sql
│
├── scripts/
│   ├── submit-streaming.sh
│   └── smoke-test.sh
│
├── tests/
│   ├── integration/
│   └── load/
│
├── docs/
│   ├── prd/
│   ├── architecture.md
│   ├── decisions/
│   └── runbooks/
│       ├── local-development.md
│       └── replay.md
│
└── .github/
    └── workflows/
        └── ci.yml
```
