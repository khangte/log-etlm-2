# 데이터 계약

- 상태: 초안
- 적용 범위: 제품 v1 / `schema_version` 1.0.0
- 요구사항 원문: [로그 생성기 PRD](../docs/prd/01-log-generator.md)

Producer가 만드는 Kafka 메시지 값(UTF-8 JSON 한 줄)의 계약이다. 생성기 테스트와 후속 Spark 검증이 같은 파일을 기준으로 삼는다.

## 스키마

| 파일 | 대상 |
| --- | --- |
| `schemas/order_completed.1.0.0.schema.json` | `event_name=order_completed` |
| `schemas/health_check.1.0.0.schema.json` | `event_name=health_check` |

검증 순서는 JSON 파싱 → `event_name`으로 스키마 선택 → 스키마 검증 → 아래 의미 규칙 검증이다.

Spark의 지원 이벤트·버전 분류, 타입 변환, 정상 주문 1건당 1행의 평탄화 매핑과 Kafka 추적 컬럼은 [Spark PRD](../docs/prd/03-stream-processing.md#파싱이벤트-분류-규칙)에서 정의한다. 이 출력 매핑은 원본 JSON 계약을 변경하지 않는다. 정상 health_check도 해당 계약을 검증한 뒤 제외한다.

### 스키마 밖 의미 규칙

JSON Schema로 표현하지 않으므로 별도 코드로 검증한다.

1. `total_amount == sum(items[].price × items[].quantity) - discount_amount`
2. `discount_amount <= sum(items[].price × items[].quantity)`
3. 같은 `product_id`는 상품 풀과 이름·카테고리·가격이 일치한다.
4. `region`·`city`는 지역 풀에 정의된 조합이다.

## 표준 샘플

`samples/`의 각 파일은 실제 출력과 같은 공백 없는 한 줄 직렬화 + LF다. 표의 크기는 LF를 뺀 페이로드 바이트 수다.

| 파일 | 기대 결과 | 크기(B) |
| --- | --- | --- |
| `valid_order_completed.json` | 통과. PRD 정상 주문 예시와 동일 값 | 872 |
| `valid_health_check.json` | 통과. 주문 분석에서는 필터링 대상 | 149 |
| `invalid_malformed_json.json` | JSON 파싱 실패. 최종 `}`를 `!`로 교체 | 858 |
| `invalid_missing_order_id.json` | 파싱 성공, `properties.order_id` 누락으로 스키마 실패 | 808 |
| `invalid_amount_type.json` | 파싱 성공, `total_amount` 타입 오류로 스키마 실패 | 869 |

불량 샘플은 의도한 오류 하나만 포함한다. 불량 2종의 원본 주문(`invalid_malformed_json`과 같은 값)은 금액 규칙을 만족한다.

## 변경 규칙

스키마를 바꾸면 `schema_version`을 올리고 새 버전 파일을 추가한다. PRD 스키마 표, 스키마 파일, 샘플, 이 문서를 함께 갱신한다.
