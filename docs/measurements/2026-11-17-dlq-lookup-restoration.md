# DLQ 조회·원문 bytes 복원 검증

- 상태: 검증 완료
- 적용 범위: 제품 v1, Spark Structured Streaming 3단계의 기존 Kafka DLQ 조회·원문 복원
- 검증 시각: 2026-11-17 06:42:01 UTC
- 기준: [Spark PRD의 DLQ 기록 이후 처리 범위](../prd/03-stream-processing.md#dlq-기록-이후의-처리-범위), DLQ 메시지 스키마와 표준 불량 3종의 오류 코드
- 결과 원본: [검증 JSON](2026-11-17-dlq-lookup-restoration.json)

## 방법

실행 중인 Kafka의 `dlq.ecommerce.orders.v1` 파티션 0을 별도 소비 도구로 조회했다. 조회 시작 시 low offset은 2552, high offset(배타적)은 477433이었다. 마지막 2,000건 범위의 시작 offset 475433부터 읽어 첫 3건에서 불량 3종을 찾았다. 이 검증은 기존 생성기 데이터로 수행했으며 `contracts/samples` 파일을 새로 발행하지 않았다.

각 레코드에 대해 다음 조건을 검사했다.

1. DLQ JSON의 필드 집합·스키마 버전, 오류 단계·코드·경로와 오류 설명·처리기 버전이 존재한다.
2. 메시지 키가 `source_topic:source_partition:source_offset`과 같다.
3. `raw_value_base64`를 `base64.b64decode(..., validate=True)`로 엄격하게 디코딩할 수 있다.
4. 원본 토픽의 해당 partition·offset을 직접 읽어 복원 bytes와 전체 바이트열이 같다. 두 값의 SHA-256도 결과 JSON에 기록했다.
5. 원본 Kafka timestamp 종류가 LogAppendTime이고 UTC 밀리초 시각이 DLQ의 `source_timestamp`와 같다.
6. 원본에 실제로 JSON 문법 오류, `properties.order_id` 누락, 문자열 `properties.total_amount`가 각각 존재한다.

## 결과

| 오류 코드 | DLQ offset | 원본 위치 | 복원 크기 | 오류 경로 | 결과 |
| --- | ---: | --- | ---: | --- | --- |
| `SCHEMA_REQUIRED` | 475433 | `raw.ecommerce.orders.v1:2:3447551` | 828 bytes | `properties.order_id` | 모든 조건 통과 |
| `SCHEMA_TYPE` | 475434 | `raw.ecommerce.orders.v1:2:3447579` | 886 bytes | `properties.total_amount` | 모든 조건 통과 |
| `INVALID_JSON` | 475435 | `raw.ecommerce.orders.v1:2:3447581` | 855 bytes | `null`（JSON 파싱 실패） | 모든 조건 통과 |

파싱 실패의 `error_path`는 실제 계약에 따라 `null`이다. 최초 조회 스크립트에서 이를 `$`로 가정해 해당 종류를 찾지 못했으며, 구현의 `_result` 기본값과 PRD의 nullable 경로 계약을 확인한 뒤 검증 스크립트를 바로잡아 재실행했다. 제품 코드 변경은 없었다.

장애 검증 담당자가 공유한 DLQ offset 476997~477432의 같은 키 218쌍은 시나리오 C의 커밋 실패 후 재실행으로 생긴 의도된 중복이다. 이번 대표 레코드 3건은 해당 구간 밖이며, 이 검증은 전체 DLQ의 중복률이나 중복 제거 여부를 판정하지 않는다.

## 재실행

저장소 루트에서 기존 생성기 가상환경의 `confluent-kafka`를 사용한다.

```bash
apps/log_generator/.venv/bin/python docs/measurements/verify_dlq_lookup.py \
  --bootstrap-servers localhost:9092 \
  --dlq-topic dlq.ecommerce.orders.v1 \
  --scan-limit 2000
```

도구는 `assign`으로 명시한 offset을 읽고 `enable.auto.commit=false`, `enable.auto.offset.store=false`, `allow.auto.create.topics=false`를 적용한다. `subscribe`, `commit`, `store_offsets`, 데이터 발행, 토픽 설정 변경, 컨테이너 조작을 하지 않는다. 사용한 API는 설치된 `confluent-kafka`의 `Consumer.assign` 문서로 확인했다. 공유 Spark PRD·체크포인트·정상 출력도 변경하지 않았다.

보관 기간 경과로 원본이 삭제됐거나 최근 조회 범위에 불량 3종이 모두 없으면 재실행은 실패할 수 있다. 필요하면 원본이 보관된 동안 `--scan-limit`을 늘려 조회한다. 이번 결과는 실환경의 기존 불량 3종 대표 레코드에 대한 증거이며 전체 DLQ 전수 검사, null·빈 bytes·비 UTF-8 입력의 실환경 검증, 자동 재처리·수정 데이터 재투입은 포함하지 않는다.
