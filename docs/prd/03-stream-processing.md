# Spark 스트림 처리

- 상태: 초안
- 적용 범위: 제품 v1

## 목표

Kafka에서 로그를 읽어 주문 로그를 검증하고 변환한다.

## 범위 및 후속 정의 사항

health_check를 필터링하고 주문 필수 필드를 검증한다. 정상 주문은 평탄화하고 items를 분리한다. 불량 로그는 원문과 오류 사유를 포함해 `dlq.ecommerce.orders.v1` 토픽으로 격리한다. 분리 전 이벤트 기준으로 처리 건수를 확인한다. DLQ 상세 스키마와 재처리 구현 방식은 이 단계에서 확정한다.

입력 계약은 [로그 생성기](01-log-generator.md)의 중첩 스키마를 따른다. JSON 파싱 후 `event_name`으로 health_check를 먼저 제외하고 주문 스키마를 검증한다. health_check에는 `context`·`properties`가 없다. JSON 문법 오류는 파싱 실패 대상으로 분리한다.

## 입력·DLQ 및 장애 처리

[Kafka 연동](02-kafka-ingestion.md)의 원본·DLQ 계약을 따른다. 아래는 3단계 구현 요구사항이며 아직 구현하지 않았다.

- 원본 `raw.ecommerce.orders.v1`의 3개 파티션을 읽고, 파싱·주문 검증 실패는 별도 토픽 `dlq.ecommerce.orders.v1`의 1개 파티션으로 보낸다. 원본과 DLQ 파티션을 일대일 대응시키지 않는다.
- 정상 health_check는 제외 건수로 집계하고 DLQ에 보내지 않는다. 정상 주문은 ClickHouse 적재 경로로 전달한다.
- DLQ에는 원문 bytes를 Base64로 보존하고 원본 topic·partition·offset·Kafka timestamp, 오류 코드·설명, 실패 단계, 감지 시각, 처리기 버전을 포함한다.
- 원본 topic·partition·offset의 조합을 DLQ 메시지 키 및 중복 판별 기준으로 사용한다. 키 지정 자체가 중복을 제거하지 않으며, 재시도에 따른 DLQ 중복과 정상 적재 중복을 고려해 처리한다.
- ClickHouse 일시 장애는 배치 실패·재시도로 복구한다. DLQ 기록 실패도 해당 배치를 성공 처리하지 않고 재시도한다. 여러 출력의 원자적 완료는 가정하지 않는다.
- 최초 실행은 `startingOffsets=earliest`, 재시작은 체크포인트 위치를 따른다. `failOnDataLoss=true`로 필요한 offset의 소실을 드러낸다.
- 원본 소비 쿼리는 DLQ를 구독하지 않는다. DLQ 재처리는 별도 경로·체크포인트로 실행하며 구체적인 재투입·중복 처리 방식은 구현 전 확정한다.

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
- `current_timestamp()`를 변환 전후에 추가해 처리시간을 계산하지 않는다. 같은 쿼리에서 같은 값을 반환하므로 연산별 완료 시각이 아니다.
- `event_timestamp`, Kafka 추가 시각·topic·partition·offset을 후속 저장 컬럼으로 보존한다. items 분리 이후에도 지연 분포는 원본 이벤트 기준으로 집계해 상품 개수에 따른 중복 가중을 피한다.
- health_check 제외·정상 주문·불량 격리 건수를 구분한다. 파싱 실패는 Kafka 메타데이터로 추적하고 생성 기준 지연 계산 불가 건수를 기록한다.

참고: [Spark 진행 정보](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#monitoring-streaming-queries), [current_timestamp 의미](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_timestamp.html).

## 완료 기준

해당 단계에 착수할 때 지연 목표값·측정 구간·샘플링 설정을 포함한 정량 기준을 확정한다. 아래 계측은 요구사항이며 아직 구현·검증하지 않았다.

불량 3종의 DLQ 격리·원문 복원·원본 위치 추적, health_check 제외, DLQ 쓰기 실패 시 배치 재시도와 중복 판별, 체크포인트 재개 및 offset 소실 시 실패를 검증한다. 배치 진행 정보와 offset 범위의 연결, 실제 action 기준 시간, 쓰기 시간과 전체 시간의 중복 방지, 이벤트 기준 집계를 검증한다. 소비 지연과 처리 지연을 각각 주입해 지표를 확인한다.
