# Kafka 연동

- 상태: 구현 중
- 적용 범위: 제품 v1

## 목표

로그 생성기의 출력을 Kafka 토픽으로 전송한다.

[로그 생성기](01-log-generator.md)의 직렬화된 UTF-8 페이로드를 메시지 값으로 그대로 전송한다. 파일용 LF 구분자는 포함하지 않고 이중 JSON 직렬화를 하지 않는다. 초기 입력 목표는 전체 5,000 EPS다. 정상 주문 페이로드는 대략 1,000B 규모를 참고하되 크기를 고정하지 않고 실제 바이트 수를 측정한다.

## 범위 및 후속 정의 사항

2단계는 로컬 Kafka 실행 환경과 원본·DLQ 토픽 생성, Kafka UI, Producer의 원본 전송을 구현한다. Spark의 검증·DLQ 기록·조회·원문 복원 검증은 3단계에서 구현한다. DLQ 데이터의 자동 재처리와 수정 데이터 재투입 기능은 후속 작업으로 보류한다. 아래 설정은 구현 기준이며, 자원·성능 최적값은 실측으로 확인한다.

### 로컬 실행 구성

- Docker Desktop의 WSL2 통합을 이용하고 루트 `compose.yaml`에서 실행한다.
- 공식 JVM 이미지 `apache/kafka:4.4.0`를 고정한다. 단일 노드가 broker와 controller를 겸하는 KRaft 방식이다.
- WSL 접속은 `localhost:${KAFKA_HOST_PORT}`, Compose 내부 접속은 `kafka:19092`로 구분한다. 호스트 포트는 기본 예시 9092이며 루프백에만 게시한다.
- Kafka 컨테이너는 CPU 2개, 메모리 2GiB, JVM heap 512MiB로 시작한다. 토픽 초기화 컨테이너는 메모리 512MiB를 사용한다.
- named volume에 데이터와 KRaft 메타데이터를 보존한다. 재시작 시 유지하며 명시적 볼륨 삭제로 초기화한다. 볼륨은 이미지에서 `appuser` 소유로 준비된 `/var/lib/kafka/data`에 마운트하고 `KAFKA_LOG_DIRS`도 같은 경로로 지정한다(`/tmp` 하위 경로에 마운트하면 root 소유 볼륨이라 브로커가 쓰기 권한 오류로 기동하지 못한다).
- healthcheck 성공 후 일회성 초기화 서비스가 토픽을 생성한다. 자동 토픽 생성은 끈다.
- 애플리케이션 토픽은 원본과 DLQ의 2개다. Kafka 내부 관리 토픽은 별도다. `.env`의 `KAFKA_TOPIC`은 `raw.ecommerce.orders.v1`, 추가할 `KAFKA_DLQ_TOPIC`은 `dlq.ecommerce.orders.v1`로 지정한다. 점은 토픽명의 실제 문자이며 백슬래시는 포함하지 않는다.
- 두 토픽 모두 복제 계수 1, `min.insync.replicas=1`, `message.timestamp.type=LogAppendTime`, delete 정책을 사용한다. 세그먼트는 128MiB 또는 5분에 롤링한다.

| 항목 | 원본 | DLQ |
| --- | --- | --- |
| 토픽명 | `raw.ecommerce.orders.v1` | `dlq.ecommerce.orders.v1` |
| 파티션 수 | 3 | 1 |
| 보관 기간 | 1시간 (`retention.ms=3600000`) | 6시간 (`retention.ms=21600000`) |
| 파티션당 보관 크기 | 8GiB (`retention.bytes=8589934592`) | 12GiB (`retention.bytes=12884901888`) |
| 토픽 전체 보관 크기 기준 | 24GiB | 12GiB |
| 메시지 키 | 미지정 | 원본 topic·partition·offset을 조합한 식별자 |

보관 기간과 크기 중 먼저 적용되는 조건으로 정리한다. 삭제는 세그먼트 단위로 비동기 처리되므로 정확한 디스크 상한이나 보관 시간 보장은 아니다.

- 단일 노드이므로 브로커 장애 중 처리가 중단되며 디스크 손실에 대한 복제 보호는 없다.
- 토픽 초기화는 기존 토픽을 변경하지 않는다. 설정 변경은 별도 alter 명령 또는 데이터 초기화로 적용한다.

### Kafka UI

- 루트 `compose.yaml`에 `kafka-ui` 서비스를 추가한다. Kafbat UI의 `ghcr.io/kafbat/kafka-ui` 이미지를 사용하고, 정확한 릴리스 태그는 구현 시 확인해 고정한다. `latest`는 사용하지 않는다.
- 브로커 healthcheck 성공 후 시작한다. 클러스터 이름은 `local`, `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:19092`로 내부 리스너에 연결한다.
- 웹 접속은 `http://localhost:8080`으로 한다. `.env.example`에 `KAFKA_UI_HOST_PORT=8080`을 추가하고, Compose 포트는 `127.0.0.1:${KAFKA_UI_HOST_PORT}:8080`으로 게시한다. 포트 충돌 시 호스트 포트만 변경한다.
- `mem_limit: 1g`, `cpus: 1`, JVM heap 상한 512MiB로 시작한다. UI의 JVM과 메시지 조회에 여유를 두기 위한 초기 예산이며, 사용량을 측정해 조정한다. Kafka 2GiB와 합쳐 상주 컨테이너 메모리 제한 합계는 3GiB이고, 초기화 실행 중에는 최대 512MiB의 제한 예산이 추가된다. 실제 메모리를 전부 미리 예약하는 것은 아니다.
- UI에서 브로커 연결 상태, 두 토픽의 파티션·보관 설정, 파티션별 offset 범위와 메시지 표본을 확인한다. DLQ 메시지 검증은 Spark의 기록 구현 이후 수행한다.
- 클러스터 설정은 Compose 환경변수로 관리한다. 동적 설정 편집은 끄고 별도 영속 볼륨은 두지 않는다. 초기 로컬 환경에는 별도 로그인 인증을 구성하지 않는다.
- 대량 메시지 조회는 브로커 부하에 영향을 줄 수 있으므로 성능 측정 중에는 UI 메시지 탐색을 중지하고 UI 실행 여부를 측정 결과에 기록한다.

참고: [Kafbat UI 시작 안내](https://ui.docs.kafbat.io/overview/getting-started), [공식 Compose 구성 예시](https://github.com/kafbat/kafka-ui/blob/main/documentation/compose/kafbat-ui.yaml).

### 로컬 설정값의 선택 근거

아래 수치는 [개발환경](00-development-environment.md)의 PC RAM 16GB·WSL 상한 12GB·CPU 8개와 초기 5,000 EPS를 바탕으로 정한 시작값이다. 자원·보관·병렬도 수치는 벤치마크로 확정한 최적값이 아니며, 실제 사용량과 지연을 측정해 조정한다.

| 항목 | 선택 근거 및 조정 기준 |
| --- | --- |
| Docker Desktop + 루트 Compose | 사용자가 선택한 WSL 실행 환경을 따르며, 서비스와 네트워크·볼륨 구성을 같은 파일로 재현한다. |
| 공식 JVM 이미지 `4.4.0` 고정 | 2026-11 기준 최신 정식 릴리스를 명시해 실행 환경을 재현한다. 이후 버전 변경은 설정·클라이언트 호환 검증과 함께 수행한다. JVM 이미지는 실험적 native 이미지 대신 일반 JVM 실행 경로를 사용하기 위한 선택이다. |
| 단일 노드 KRaft | 단일 WSL 환경에서 Kafka가 Spark·ClickHouse·Grafana와 제한된 메모리를 공유하므로, Kafka는 브로커와 컨트롤러를 겸하는 단일 노드로 구성해 메모리 사용을 줄인다. 고가용성·복제 장애 검증이 필요해지면 가용 자원을 확보한 뒤 노드를 늘린다. |
| 호스트 9092 / 내부 19092 | 9092는 익숙한 Kafka 접속 포트이고 19092는 내부 리스너 구분용이다. 포트 숫자에 성능상 의미는 없다. 메타데이터로 반환하는 주소도 접속 위치에 맞아야 하므로 리스너를 분리한다. 루프백 게시로 로컬 개발 범위를 유지한다. |
| CPU 2개 / 메모리 2GiB | WSL 자원 일부를 Kafka에 배정하고 Spark·ClickHouse·Grafana 및 개발 도구에 여유를 남긴다. CPU 제한 도달, 메모리 부족, ACK 지연 증가가 관찰되면 조정한다. 처리량 보장 수치가 아니다. |
| JVM heap 512MiB | 작은 개발 토픽에서 시작할 heap 예산이다. 컨테이너 2GiB 중 나머지는 JVM 비heap·네이티브 메모리·파일 캐시 등에 사용할 여유이며 고정 배분은 아니다. GC와 메모리 사용량을 확인해 조정한다. |
| 초기화 컨테이너 512MiB | 토픽 생성 CLI 한 번을 실행할 별도 예산이다. CLI heap 상한은 256MiB로 두고 JVM의 추가 메모리 여유를 남긴다. |
| named volume | 컨테이너 교체·재시작과 데이터 수명을 분리해 재처리 검증에 사용한다. 백업이나 디스크 장애 보호 수단은 아니다. |
| healthcheck 후 명시적 토픽 생성 | 브로커 준비 이전의 생성 실패와 자동 생성 시 의도치 않은 기본 설정 적용을 피한다. 기존 토픽을 유지해 반복 기동에 따른 데이터 삭제를 방지한다. |
| 원본·DLQ 토픽 분리 | `raw.ecommerce.orders.v1`은 가공 전 주문 도메인 입력, `dlq.ecommerce.orders.v1`은 처리 불가능 데이터를 담는다. 보관·소비·재처리를 독립 관리한다. health_check도 필터링 검증을 위해 원본에 함께 보낸다. 토픽명의 v1과 이벤트 schema_version은 별개다. |
| 원본 3 / DLQ 1 파티션 | 원본은 평균 약 1,667 EPS/파티션으로 병렬 읽기를 검증하고, DLQ는 현재 불량률 5%인 약 250 EPS를 기준으로 시작한다. 서로 다른 토픽의 파티션 개수나 번호를 맞출 필요는 없다. 파티션 증가가 단일 브로커의 CPU·디스크 성능을 늘리지는 않으며 전체 순서는 보장하지 않는다. |
| 복제 계수 1 / 최소 ISR 1 | 브로커가 1개이므로 사용 가능한 복제본 수에 맞춘다. 이 환경의 `acks=all`은 단일 ISR의 확인이며 다중 복제 내구성 검증이 아니다. |
| `LogAppendTime` | Producer 생성 시각과 별개로 브로커 기록 기준의 timestamp를 얻어 생성 → Kafka 지연을 계산한다. ACK·복제 완료 시각과는 구분한다. |
| delete 정책 / 원본 1시간·DLQ 6시간 | 원본은 최근 입력 재처리용이고 DLQ는 오류 조사와 수정 후 재처리를 위해 더 오래 남긴다. 두 토픽 모두 개별 로그를 보존하므로 compaction은 사용하지 않는다. |
| 원본 24GiB / DLQ 12GiB | 기존 생성기의 유형별 실측 크기와 5% 불량률로 계산한 보관량에 여유를 둔 시작 예산이다. 아래 계산을 따른다. |
| 세그먼트 128MiB / 5분 | 크기 또는 시간에 따라 로그를 나누어 작은 개발 데이터에서도 보관 정책의 동작을 관찰하기 위한 값이다. 작은 세그먼트는 파일 수를 늘리므로 파일 수와 디스크 사용량을 보고 조정한다. |

토픽 보관·세그먼트·timestamp의 동작은 [Kafka 4.4 토픽 설정](https://kafka.apache.org/44/configuration/topic-configs/)을 기준으로 한다. 용량과 시간 추산은 이 프로젝트의 부하 가정에서 계산한 값이다.

### 부하·보관 용량 계산

[로그 생성기 실측](01-log-generator.md)의 정상 주문 865.4B, health_check 149B, 불량 주문 852.3B와 비율 85%·10%·5%를 사용한다. DLQ 크기는 아직 실측이 아니며 원문 Base64와 오류 메타데이터를 합쳐 건당 1,700B로 가정한다. 파티션별 보관 크기는 균등 분산과 현재 불량률을 가정한다.

- 원본 평균: `865.4 × 0.85 + 149 × 0.10 + 852.3 × 0.05 ≈ 793B`.
- 원본 유입: `5,000 × 793 ≈ 3.97MB/s`, 1시간 약 14.3GB. 30% 여유를 더하면 약 18.6GB로, 전체 24GiB 보관 기준 안에 들어간다.
- DLQ 유입: `5,000 × 0.05 × 1,700 = 0.425MB/s`, 6시간 약 9.2GB. 30% 여유를 더하면 약 11.9GB로, 12GiB 보관 기준 안에 들어간다.
- 두 토픽 보관 크기 기준 합계는 36GiB다. 세그먼트·인덱스·내부 토픽 등의 여유를 고려해 Kafka 데이터용 디스크 여유 공간은 최소 50GiB를 권장한다. 이는 여유 공간 확인 기준이며 볼륨 quota는 아니다. Docker Desktop의 WSL 가상 디스크는 데이터·볼륨을 삭제해도 자동으로 줄지 않으므로 Windows 측 디스크 여유도 함께 확인한다.
- 압축 이득은 계산에 포함하지 않는다. 실제 평균 크기·압축률·불량률·파티션 편중에 따라 보관 가능 시간이 달라진다. 30%는 초기 예산 여유이며 최악의 경우를 보장하는 상한이 아니다.
- 필요 파티션 수는 목표 EPS를 파티션당 지속 처리 가능한 실측 EPS로 나눠 올림한 값을 참고하되 소비 병렬도와 편중도 함께 확인한다. 현재 파티션당 실측 처리량이 없어 3/1을 최적값으로 확정하지 않는다.
- Kafka는 `mem_limit: 2g`, heap 512MiB, CPU 2개를 유지한다. 보관 데이터는 디스크에 저장하며 토픽 수나 보관 용량에 비례해 heap을 늘리지 않는다. 메모리 부족·GC·CPU 제한과 지연의 관계를 측정해 조정한다.

### DLQ 및 offset 관리

DLQ는 원본의 특정 파티션이 아닌 독립 토픽이다. Producer는 정상·health_check·불량 원문을 원본에 전송하고 Spark가 파싱·검증 후 분기한다.

| 상황 | 처리 |
| --- | --- |
| JSON 문법 오류, 주문 필수 필드 누락, 타입·값 검증 실패 | DLQ 기록 |
| 정상 health_check | 필터링 및 제외 건수 집계 |
| Producer의 Kafka 연결·전송 실패 | 재시도 후 최종 실패 시 비정상 종료 |
| ClickHouse 일시 장애 | 배치 실패·재시도로 복구; 데이터 불량으로 DLQ에 보내지 않음 |
| DLQ 쓰기 실패 | 해당 배치를 성공 처리하지 않고 재시도 |

DLQ에는 원문 bytes의 Base64, 원본 topic·partition·offset·Kafka timestamp, 오류 코드·설명, 실패 단계, 감지 시각과 처리기 버전을 저장한다. 원본 위치 식별자를 메시지 키와 중복 판별 기준으로 사용한다. Kafka 키 지정만으로 중복이 제거되지는 않으며 배치 재시도로 DLQ 중복이 발생할 수 있다. 상세 스키마와 오류 코드는 3단계에서 정의한다. 3단계의 DLQ 범위는 기록·조회·원문 복원 검증까지이며, 자동 재처리와 수정 데이터 재투입 기능은 보류한다. 상세 범위는 [Spark PRD](03-stream-processing.md#dlq-기록-이후의-처리-범위)를 따른다.

Offset은 Kafka가 파티션별로 부여하는 위치다. 원본과 DLQ의 offset은 독립적이며, DLQ에는 자신의 위치와 별개로 원본 위치를 보존한다. 보관 만료 후에도 원본 offset은 0으로 되돌아가지 않는다.

Spark 최초 실행은 `startingOffsets=earliest`, 재시작은 체크포인트 위치를 사용한다. 보관 만료로 필요한 offset이 없으면 `failOnDataLoss=true`로 실패를 드러낸다. 원본 보관이 1시간이므로 Spark가 1시간 넘게 중단되면 재시작이 실패하는 것이 의도된 동작이다. 복구는 유실 구간을 기록한 뒤 해당 쿼리의 체크포인트를 삭제하고 `startingOffsets=earliest`로 다시 시작한다. 원본 소비 쿼리는 원본 토픽만 구독하며 DLQ는 별도 재처리 경로로 관리한다. 의도적인 재처리는 별도 체크포인트와 시작 위치를 지정한다.

### Producer 연동 설정

| 항목 | 제안 | 선택 근거 |
| --- | --- | --- |
| 클라이언트 | `confluent-kafka` 비동기 Producer, 라이브러리 버전은 구현 시 고정 | librdkafka 기반 배치·재시도·delivery 콜백을 활용하고, 5,000 EPS에서 매 건 ACK를 기다리는 직렬 전송을 피한다. 실제 목표 달성은 부하 검증으로 확인한다. |
| 출력 모드 | 기존 stdout/file에 kafka 추가, 직렬화된 bytes를 LF 없이 전송 | 동일한 생성 데이터를 출력 대상별로 비교할 수 있다. Kafka 레코드가 메시지 경계를 제공하므로 파일 구분자 LF는 필요하지 않다. 원본 불량 bytes도 그대로 보존한다. |
| 메시지 구성 | 정상·health_check·불량 모두 같은 원본 토픽, key 미지정으로 분산; 파싱은 Spark에서 수행 | 생성기에서 파싱·분류하면 검증용 불량 입력이 유실되거나 바뀔 수 있다. 현재 사용자별 순서 요구가 없으므로 키를 강제하지 않는다. 키 미지정은 순간적인 균등 분배를 보장하지 않으며 파티션 편중을 측정한다. |
| 전달 설정 | `acks=all`, `enable.idempotence=true`; 클라이언트 재시도 중 중복 억제, 프로세스 재실행·후속 저장까지 exactly-once를 보장하지 않음 | 브로커 확인을 성공 기준으로 삼고 클라이언트 재시도로 인한 중복을 억제한다. 단일 ISR 구성에서는 다른 브로커의 복제 확인은 없다. |
| 배치·압축 | `linger.ms=5`, `compression.type=lz4`로 시작하고 실제 처리량·CPU 사용량 측정 | 5ms는 배치를 모을 시간을 주면서 짧은 대기를 유지하기 위한 값이다. 5,000 EPS라면 평균 약 25건이 생성되는 시간이며 실제 배치는 파티션·스케줄링에 따라 달라진다. LZ4는 JSON의 반복 필드 압축을 시도할 초기 후보이며 무압축 대비 CPU·전송량·지연을 비교한다. |
| 실패 처리 | `delivery.timeout.ms=30000`, 큐 포화 시 poll하며 최대 30초 대기; 최종 실패 시 생성 중단 및 비정상 종료 | 일시적 단절의 재시도 시간을 허용하면서 개발 중 장애가 무한 대기로 보이지 않게 한다. delivery 제한과 큐 등록 대기는 별도 구간이므로 총 대기가 30초를 넘을 수 있다. 실패 후 계속 생성하면 전달되지 않은 로그가 늘어나므로 중단한다. |
| 클라이언트 오류 | `error_cb` 등록; `fatal()` 오류는 즉시 생성 중단 및 비정상 종료, 그 외 오류(예: 전체 브로커 연결 끊김)는 stderr 기록·건수 집계 후 delivery 제한으로 판단 | 브로커 단절과 idempotence fatal 오류는 개별 메시지가 아닌 `error_cb`로 전달되어 delivery 콜백만으로는 놓친다. fatal 이후 Producer는 더 이상 전송할 수 없다. |
| 종료 | 정상 종료·Ctrl+C에서 최대 30초 flush, 실패·미전달 건수를 stderr에 기록 | 비동기 큐에 남은 로그의 전송 기회를 주면서 종료 대기를 제한한다. flush는 전달 제한시간을 갱신하지 않으며 전송 성공을 보장하지 않는다. 실패 또는 잔여 건이 있으면 성공 종료로 처리하지 않는다. |
| 관측 | 생성·큐 등록·ACK 성공·실패·미전달 건수 분리, ACK 지연 p50/p95/p99와 큐 길이 집계 | 큐 등록을 전송 성공으로 오인하지 않도록 한다. p50은 일반적인 지연, p95/p99는 느린 구간, 큐 길이는 적체를 드러낸다. |
| 재시도 관측 | `statistics.interval.ms=1000`으로 librdkafka 통계 콜백을 켜고 재시도 지표 수집; delivery 콜백 실패와 구분 | 이 값을 설정하지 않으면 통계 콜백이 호출되지 않는다. 1초는 생성기 상태 출력 주기와 맞춘 값이다. 최종 성공 전에 일어난 재시도는 최종 delivery 결과만으로 파악할 수 없다. 브로커·요청 단위 통계이므로 메시지별 정확한 재시도 건수로 해석하지 않는다. |
| 성능 기준 | 5초 준비 후 60초 측정, ACK 기준 5,000 EPS의 ±5%, ACK 지연 p95 100ms 이하 | 2단계 완료 기준으로 사용한다. 준비 5초는 초기 연결·메타데이터 조회의 영향을 줄이기 위한 가정이며 준비 완료 여부도 확인한다. 60초는 1단계 생성기와 비교할 약 30만 건의 관측 구간이다. ±5%는 초기 스케줄링·측정 오차 허용 범위이고, 100ms는 종단 간 10초 목표 중 수집 구간에 배정한 초기 지연 예산이다. Kafka의 보장 성능이나 60초 안정성만으로 장시간 안정성을 입증하는 기준은 아니다. |

Producer 설정의 동작은 [librdkafka 설정 문서](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)와 [동작 설명](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_INTRODUCTION.html)을 참고한다. 이 문서의 30초 제한과 성능 목표는 프로젝트의 초기 선택값이며 라이브러리 기본값 또는 공식 권장 성능을 의미하지 않는다.

성능 기준은 아직 달성 결과가 아니다. 프로세스 종료 후 미전달 로그의 자동 복구 저장은 별도 구현이 필요하며 이번 제안에 포함하지 않는다.

참고: [공식 Docker 문서](https://kafka.apache.org/44/getting-started/docker/), [공식 단일 노드 구성 예시](https://github.com/apache/kafka/blob/4.4.0/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml).

## 생성·수정할 파일 구조

아래는 2단계 구현 시 사용할 구조다. `[신규]`는 생성할 파일, `[수정]`은 기존 파일의 변경을 뜻한다. 이 목록은 구현 계획이며 파일 생성 또는 구현 완료를 의미하지 않는다.

```text
log-etlm-2/
├── compose.yaml                              # [수정] 브로커·초기화·Kafka UI·볼륨·자원 제한
├── .env.example                              # [수정] Kafka/UI 접속·원본/DLQ 토픽·Producer 설정 예시
├── infra/
│   └── kafka/
│       └── init-topics.sh                     # [신규] 원본 3개·DLQ 1개 파티션과 보관 설정 적용
├── apps/
│   └── log_generator/
│       ├── pyproject.toml                    # [수정] confluent-kafka 의존성 고정
│       ├── uv.lock                           # [수정] 의존성 잠금 갱신
│       ├── src/log_generator/
│       │   ├── config.py                     # [수정] Kafka 출력 설정 읽기·검증
│       │   ├── main.py                       # [수정] 출력 연결·종료·통계 집계
│       │   └── outputs/
│       │       └── kafka_producer.py                  # [신규] 비동기 전송·콜백·poll·flush·오류 처리
│       └── tests/
│           ├── test_generator.py             # [수정] Kafka 설정 검증과 기존 출력 회귀 확인
│           └── test_kafka_producer.py                 # [신규] 브로커 없이 전송·오류·종료 동작 검증
├── tests/
│   └── integration/
│       └── test_kafka_ingestion.py            # [신규] 실제 브로커의 원본 보존·timestamp·전송 검증
└── docs/
    ├── prd/
    │   ├── 02-kafka-ingestion.md              # [수정] 2단계 요구사항과 검증 기준
    │   └── 03-stream-processing.md            # [수정] 원본 입력·DLQ 계약 연계
    └── runbooks/
        └── local-development.md              # [수정] 실행·토픽 확인·부하·장애·초기화 절차
```

| 파일·구성 | 구현 책임 |
| --- | --- |
| `compose.yaml` / `infra/kafka/init-topics.sh` | 초기화 스크립트를 초기화 컨테이너에 읽기 전용으로 마운트해 실행한다. 기존 Compose의 인라인 토픽 생성 명령을 옮겨 두 토픽 생성 설정을 한 곳에서 관리한다. 기존 토픽은 삭제하거나 설정을 자동 변경하지 않는다. |
| `compose.yaml`의 `kafka-ui` | UI 이미지·내부 접속 주소·호스트 포트·자원 제한을 정의한다. 별도 UI 앱 소스나 설정 파일은 생성하지 않는다. |
| `.env.example` / `config.py` | Compose의 `KAFKA_TOPIC=raw.ecommerce.orders.v1`, `KAFKA_DLQ_TOPIC=dlq.ecommerce.orders.v1`과 앱 접두사 `PRODUCER_`를 쓰는 Producer 설정을 구분한다. `KAFKA_UI_HOST_PORT`는 Compose에서 사용하고 Producer 설정으로 읽지 않는다. Producer 설정 키는 `PRODUCER_KAFKA_BOOTSTRAP_SERVERS`(WSL 접속 주소)·`PRODUCER_KAFKA_TOPIC`·`PRODUCER_KAFKA_LINGER_MS`·`PRODUCER_KAFKA_COMPRESSION`·`PRODUCER_KAFKA_DELIVERY_TIMEOUT_MS`·`PRODUCER_KAFKA_QUEUE_FULL_TIMEOUT_SEC`·`PRODUCER_KAFKA_FLUSH_TIMEOUT_SEC`이며 코드 기본값 없이 `.env.example`에 모두 명시한다. `PRODUCER_KAFKA_TOPIC`은 `${KAFKA_TOPIC}`을 참조해 토픽명을 한 곳에서 관리한다. `acks=all`·`enable.idempotence=true`·`statistics.interval.ms=1000`은 코드에 고정한다. 실제 `.env`는 로컬 실행 설정이며 Git에 포함하지 않는다. |
| `outputs/kafka_producer.py` / `main.py` | Kafka 클라이언트 동작은 `kafka_producer.py`에 두고 실행 흐름·공통 통계는 기존 main.py를 확장한다. 생성기의 원본 bytes는 다시 직렬화하지 않는다. |
| 단위 테스트 | 클라이언트 대체 객체로 큐 포화·delivery 실패·fatal 오류·flush 잔여 건과 성공 집계를 확인한다. 기존 stdout/file 동작도 유지되는지 확인한다. |
| 통합 테스트 / runbook | 소량의 정상·health_check·불량 bytes를 실제로 송수신해 값·메타데이터를 확인한다. 60초 부하 측정, 브로커 중단·지연 주입과 재기동 검증 명령 및 결과는 runbook에 기록한다. |

Kafka 데이터는 저장소 안에 파일로 생성하지 않고 Docker의 `kafka-data` named volume에 저장한다. `.venv/`, 캐시와 대량 측정 로그도 Git에 포함하지 않는다.

3단계의 Spark 검증·DLQ 기록·조회·원문 복원 검증 코드는 이번 단계에서 생성하지 않는다. DLQ 자동 재처리와 수정 데이터 재투입 기능은 3단계에서도 보류한다. DLQ 상세 스키마와 샘플, streaming 앱의 구체적인 파일 구조는 3단계 PRD 보완 시 정의한다. 2단계에서는 DLQ 토픽 생성과 설정 확인까지만 수행한다.

## 처리지연 측정

[공통 측정 원칙](01-log-generator.md#공통-측정-원칙)을 따른다. 직렬화된 원본 페이로드는 변경하지 않는다.

| 기준점·지표 | 기록 위치·계산 | 의미 |
| --- | --- | --- |
| ACK 지연 | 성공·실패 delivery 콜백의 `msg.latency()` | librdkafka가 측정한 전송 API 호출부터 delivery report 생성까지의 시간. 클라이언트 대기·전송·브로커 확인을 포함하고 애플리케이션의 콜백 poll 대기는 포함하지 않음. 전송 API 반환은 성공 ACK가 아님 |
| 브로커 추가 시각 (`kafka_append_timestamp`) | 토픽의 `message.timestamp.type=LogAppendTime` 설정 후 레코드 timestamp 사용 | 브로커 로그 추가 기준이며 복제 완료·ACK 수신 시각과 다름 |
| 생성 → Kafka 지연 | `kafka_append_timestamp - event_timestamp` | 생성 이후 직렬화·클라이언트 대기·전송을 포함 |

- 2단계에서는 Spark 없이 생성 → Kafka 지연을 계산한다. Producer는 성공 delivery 콜백의 `msg.timestamp()`가 `TIMESTAMP_LOG_APPEND_TIME` 종류일 때 그 값을 사용하고, 콜백 시점에 이미 가진 원본 `event_timestamp`와 비교한다. 콜백이 이 종류를 반환하는지는 구현 시 실측으로 확인하며, 반환하지 않으면 콘솔 소비자의 `print.timestamp=true` 출력으로 표본을 검증한다.
- `CreateTime` timestamp를 브로커 도착 시각으로 사용하지 않는다. 소비 시 timestamp 종류를 확인한다. 같은 배치의 레코드는 같은 append 시각을 받으므로 이벤트별 도착 시각은 근사치다.
- `event_timestamp`는 밀리초 절삭값이고 append 시각도 배치 단위이며 WSL 절전 후 시계가 어긋날 수 있어 지연이 음수로 계산될 수 있다. 음수 건수와 최솟값을 별도 집계하고 백분위 계산에서 0으로 보정하지 않는다.
- 지연 값은 개별 저장하지 않고 값별 건수(히스토그램)로 집계해 실행 시간과 무관하게 메모리를 유지한다. 생성 → Kafka 지연은 ms 정수 그대로, ACK 지연은 0.1ms 단위로 반올림해 센다. 백분위는 nearest-rank 방식이다.
- `acks` 설정을 측정 결과에 함께 남기며 성공의 의미는 전달 보장 설정에 맞춘다.
- ACK 시각은 이미 전송한 메시지에 넣지 않고 생성기 운영 지표로 집계한다. 전송 직전 UTC 시각이 추가로 필요하면 Kafka 헤더를 사용하며 원본 JSON은 재직렬화하지 않는다.
- topic·partition·offset을 후속 처리까지 보존해 원문 파싱 실패도 추적할 수 있게 한다. JSON을 해석할 수 없는 로그는 생성 기준 지연을 계산할 수 없는 건수로 별도 집계한다.
- 클라이언트 대기열 크기와 재시도·실패 건수를 함께 관찰한다. Kafka의 체류·소비 대기는 Spark 측정과 연결한다.

참고: [Kafka 토픽 timestamp 설정](https://kafka.apache.org/44/configuration/topic-configs/).

## Spark 연계 지속 부하의 후속 관측 (2026-11-17)

[Spark 지속 부하 결과](03-stream-processing.md#지속-부하-측정-결과-2026-11-17)에서 120초 준비 + 600초 본 측정의 실제 ACK는 4,947.65 EPS였다. 전체 720초의 ACK 지연 p95는 12.6ms, delivery 최종 실패·재시도·클라이언트 오류는 각각 0이다. 이는 기존 완료 기준의 5초 준비 + 60초 단독 검증과 다른 구간이며 대체하지 않는다.

입력 종료를 위해 SIGINT를 보냈을 때 생성기가 `output flush failed: 1 messages undelivered after flush`로 exit 1을 반환했다. 큐 등록 3,564,479·ACK 3,564,478·Kafka offset 증가 3,564,479로 종료 집계가 1건 어긋난다. 실제 유실이나 원인을 단정하지 않으며 SIGINT·delivery 콜백·ACK 카운터·flush 종료 경로를 별도 조사해야 한다. 이번 측정에서는 코드를 수정하지 않았다.

## 완료 기준

아래 항목을 모두 충족하면 2단계를 완료로 처리한다. 성능 기준에 미달하면 원인과 실측치를 기록하고 완료로 처리하지 않는다. 실행 검증 결과는 로컬 runbook에 기록한다.

1. **토픽:** 두 토픽의 이름·파티션 수·보관 설정이 이 문서와 일치하고, 재기동 후 데이터가 유지된다.
2. **원본 보존:** 소비한 메시지 값이 생성기 출력 bytes와 일치하고 LF가 포함되지 않는다.
3. **성능:** 5초 준비 후 60초 측정에서 ACK 기준 5,000 EPS의 ±5%, `msg.latency()` 기준 ACK 지연 p95 100ms 이하를 만족한다. 측정 중 Kafka 컨테이너의 CPU·메모리 사용량을 함께 기록한다.
4. **timestamp:** 레코드 timestamp 종류가 LogAppendTime이고, 생성 → Kafka 지연 p50/p95/p99와 음수 건수를 산출한다.
5. **실패 처리:** 브로커 중단 시 delivery 제한 후 비정상 종료하고 실패·미전달 건수를 stderr에 기록한다. `error_cb` 오류와 통계 콜백의 재시도 지표가 집계된다.
6. **지연 주입:** 네트워크 지연이나 브로커 일시 중지를 주입했을 때 ACK 지연·큐 길이 지표가 증가한다.

7. **Kafka UI:** 로컬 웹 접속과 브로커 연결이 성공하고, 두 토픽의 파티션 수·보관 설정·offset 범위 및 원본 메시지 표본을 확인할 수 있다. UI 이미지 고정 버전과 메모리 사용량을 기록한다.
