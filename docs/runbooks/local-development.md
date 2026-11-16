# 로컬 개발 runbook

- 상태: 구현 중
- 적용 범위: 제품 v1 / 1단계 로그 생성기까지

명령은 WSL Ubuntu에서 실행한다. 단계가 추가되면 이 문서를 갱신한다.

## 1. 환경 준비

```bash
cp .env.example .env            # 프로젝트 루트. Producer·Compose가 함께 읽는 실행 설정 파일
cd apps/log_generator
uv sync                         # .venv 생성, 실행 의존성(python-dotenv)과 dev 의존성(jsonschema) 포함
```

## 2. 테스트

```bash
cd apps/log_generator
uv run python -m unittest discover -s tests -v
```

## 3. 생성기 실행

프로젝트 루트 `.env`를 자동으로 읽는다. 셸에서 지정한 환경변수가 `.env`보다 우선하며, 코드 기본값은 없다. 모든 설정 키를 명시해야 하고 누락·필수값 공백은 종료 코드 2로 거부한다. seed는 빈 값으로 무작위를, stdout 출력 경로는 빈 값으로 미사용을 지정한다. `.env.example`은 실행 시 읽지 않는다. `.env`가 없어도 모든 키를 환경변수로 제공하면 실행할 수 있다.

```bash
cd apps/log_generator

# stdout, 기본 300,000건
uv run log-generator

# 파일 출력. 기존 파일이 있으면 종료 코드 2로 거부한다.
PRODUCER_OUTPUT=file PRODUCER_OUTPUT_PATH=/tmp/events.jsonl uv run log-generator

# 무제한 실행. Ctrl+C로 종료하면 최종 통계를 stderr에 쓴다.
PRODUCER_MAX_EVENTS=0 uv run log-generator > /dev/null
```

stderr에는 상태 주기마다 `"status": "running"` JSON이, 종료 시 최종 통계 JSON이 한 줄씩 기록된다. 주기 상태의 `interval_events`·`interval_eps`는 직전 상태 출력 이후 구간의 건수·속도다.

```bash
# 로그는 버리고 1초마다 생성 건수만 확인
PRODUCER_MAX_EVENTS=0 PRODUCER_STATUS_INTERVAL_SEC=1 uv run log-generator > /dev/null
```

## 4. 성능 측정 (PRD 기준 6)

```bash
cd apps/log_generator
PRODUCER_OUTPUT=file PRODUCER_OUTPUT_PATH=/tmp/perf.jsonl PRODUCER_MAX_EVENTS=0 PRODUCER_STATUS_INTERVAL_SEC=5 \
  timeout -s INT 67 /usr/bin/time -v .venv/bin/log-generator 2> /tmp/perf.err
```

- 첫 번째 `running` 상태(약 5초)와 13번째 상태(약 65초)의 `events_written` 차이를 경과 시간 차이로 나눠 측정 EPS를 구한다.
- `/usr/bin/time` 출력에서 CPU 사용률과 `Maximum resident set size`를 기록한다.
- `timeout`이 SIGINT를 보내므로 셸의 종료 코드는 124다. 생성기 자체의 종료 코드는 130이다.

## 5. 데이터 초기화

생성 파일은 지정한 출력 경로에만 생긴다. 다시 실행하기 전에 해당 파일을 지운다.

```bash
rm -f /tmp/events.jsonl /tmp/perf.jsonl /tmp/perf.err
```
