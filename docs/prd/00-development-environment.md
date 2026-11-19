# 개발환경

- 상태: 완료 (환경 검증 결과 기록, swap 입출력은 미측정)
- 적용 범위: 제품 v1
- 환경 구성 기준은 합의되었으며, 실제 설치 및 실행 검증은 단계별로 진행한다.

## 실행 환경

전체 파이프라인은 단일 PC의 로컬 WSL2 환경에서 개발하고 실행한다.

| 항목 | 구성 |
| --- | --- |
| PC 전체 RAM | 16GB |
| WSL 배포판 | Ubuntu 24.04 LTS |
| WSL 메모리 상한 | 12GB |
| WSL swap | 4GB |
| WSL 사용 가능 CPU | 논리 프로세서 8개 |
| Docker | 기존 Docker Desktop의 WSL2 백엔드 및 Ubuntu 통합 사용 |
| 서비스 실행 | WSL 터미널에서 `docker compose` 사용 |
| Producer Python | 3.12 계열 |
| Python 의존성 관리 | uv |
| 프로젝트 경로 | `/home/kang/projects/log-etlm-2` |

WSL 메모리 상한과 swap은 Windows 사용자 홈의 `.wslconfig`에서 관리한다. 아래 내용은 설정 기준이며, 이 문서 작성으로 실제 설정이 변경되지는 않는다.

```ini
[wsl2]
memory=12GB
swap=4GB
```

메모리는 필요에 따라 사용하며 12GB는 상한이다. swap 4GB는 디스크를 사용하는 보조 공간이다. 성능 측정 시 swap 입출력과 Windows의 메모리 사용량도 확인한다.

시간 동기는 WSL/Hyper-V 쪽 동기 하나만 사용한다. Ubuntu의 `systemd-timesyncd`는 비활성화한다(`sudo systemctl disable --now systemd-timesyncd`). 두 동기가 함께 켜져 있으면 서로 반대로 보정해 단조 시계 속도가 수천 ppm까지 흔들린다([WSL 시계 차이 조사](04-clickhouse-storage.md#wsl-시계-차이-조사-2026-11-18)). 장시간 측정 전에는 Windows에서 `w32tm /query /status`로 동기 상태를 확인하고, 미동기면 `w32tm /resync`를 실행한다.

## Docker 실행 방식

- Docker Desktop에서 WSL2 백엔드를 사용하고, 현재 Ubuntu 배포판의 WSL Integration을 활성화한다.
- WSL 내부에 별도 Docker Engine을 설치하지 않는다.
- Kafka, Spark, ClickHouse, Grafana는 루트 `compose.yaml`에 단계별로 추가한다.
- 호스트에서 실행하는 Producer와 컨테이너 내부 서비스의 접속 주소를 구분한다.
- 서비스별 CPU·메모리 제한은 해당 연동 단계에서 정의한다. Windows와 WSL 내부 개발 도구가 사용할 여유를 고려한다.

## Python 및 의존성 관리

- Producer는 Python 3.12 계열을 사용하고, 정확한 패치 버전은 구현 착수 시 고정한다.
- uv로 Python 버전, 가상환경과 의존성을 관리한다.
- 앱별 `pyproject.toml`, `uv.lock`, `.python-version`을 Git에 포함한다.
- 앱별 `.venv/`는 Git에서 제외한다.
- 의존성 설치는 앱 디렉터리에서 `uv sync`, 실행은 `uv run`을 기준으로 한다.
- Spark의 Python·PySpark·Java·Kafka 커넥터 버전은 [Spark PRD의 3단계 구현 결정](03-stream-processing.md#3단계-구현-결정)에 고정했다. Spark 앱(`apps/streaming`)은 uv 대신 공식 Spark 이미지의 Python과 Dockerfile에 고정한 패키지를 사용하며 컨테이너 안에서 실행·테스트한다.

```text
apps/log_generator/
├── .python-version
├── pyproject.toml
├── uv.lock
├── .venv/             # Git 제외
├── src/
└── tests/
```

## 단계별 환경 구성

1. 로그 생성기: WSL의 Python 가상환경에서 실행하고 표준 출력·파일로 검증한다.
2. Kafka: Docker Desktop으로 브로커를 실행하고 Producer를 연결한다.
3. Spark: 컨테이너 실행 환경과 체크포인트 저장 위치를 구성한다.
4. ClickHouse: 컨테이너와 영속 데이터 볼륨을 추가한다.
5. Grafana: 데이터 소스와 대시보드를 추가하고 전체 흐름을 확인한다.
6. 상세 세팅 조정: 처리량, 지연과 자원 사용량을 측정해 설정을 조정한다.

## 설정 및 데이터 관리

- 환경변수 목록과 개발용 예시는 `.env.example`에 기록한다. 실제 `.env`는 Git에서 제외한다.
- `.env`는 실행 환경(로컬·도커·테스트 등)별로 달라지는 외부 설정값의 저장소이고, 루트 `.env.example`은 그 예시이자 공통 템플릿이다. 앱의 `config.py`는 환경변수를 읽어 타입 변환·검증을 적용한 파이썬 객체로 만드는 통로다.
- `.env.example`에는  앱별 변수를 `PRODUCER_`처럼 앱 접두사를 붙이고 단계별로 추가한다.
- 로컬 실행 전 `cp .env.example .env`로 `.env`를 만든다. `.env`가 Producer와 Compose가 함께 쓰는 유일한 실행 설정 파일이며 `.env.example`은 실행 시 읽지 않는다.
- Producer는 `python-dotenv`로 프로젝트 루트 `.env`를 자동으로 읽는다. 앱 디렉터리에서 `uv run log-generator`로 실행하며 별도 `--env-file` 옵션은 필요 없다. 우선순위는 환경변수 > `.env`이다. `.env`가 없으면 환경변수만 사용한다. 코드 기본값은 두지 않으며 설정 키 누락·필수값 공백은 시작 전에 거부한다. 빈 seed와 stdout 모드의 빈 출력 경로는 명시적으로 허용한다.
- Compose는 루트 `.env`를 자동으로 읽는다. 컨테이너에 전달할 변수는 `compose.yaml`에 명시한다.
- 컨테이너 이미지와 라이브러리 버전은 도입 단계에서 고정한다.
- Kafka·ClickHouse 데이터와 Spark 체크포인트는 볼륨에 저장하고 Git에서 제외한다.
- 생성 로그, 가상환경과 캐시도 Git에서 제외한다. 검증용 소량 샘플은 별도로 관리한다.
- 실제 실행 명령과 데이터 초기화 절차는 구현 시 `docs/runbooks/local-development.md`에 기록한다.

## 환경 검증 기준

- WSL에서 `docker version`과 `docker compose version`으로 연결 및 사용 가능 여부를 확인한다.
- 앱 디렉터리에서 `uv sync`와 `uv run python --version`으로 환경을 재현한다.
- 서비스는 해당 개발 단계에서 시작·종료·재시작을 검증한다.
- 전체 통합 시 메모리 부족, 지속적인 swap 입출력, 처리량과 종단 간 지연을 확인한다.
- 전체 서비스 구동 가능 여부와 목표 성능 달성 여부를 구분해 기록한다.

### 환경 검증 결과 (2026-11-19)

- 구동: Kafka·Kafka UI·Spark·ClickHouse·Grafana를 함께 띄운 상태에서 5,000 EPS 17분(5단계)·60분(진단) 실행이 끝까지 동작했다. 앱 환경은 각 단계에서 `uv sync`로 재현했다.
- 목표 성능: [4단계 완료 기준](04-clickhouse-storage.md#완료-기준)을 통과했다. 종단 간 지연은 [개요](overview.md#v1-목표-판정-2026-11-19)를 따른다.
- 메모리: 60분 실행 직후 WSL 사용 7,637MiB/11,963MiB(가용 4,326MiB), Spark 컨테이너 최대 약 2.34GiB/3GiB였다. OOM은 없었다.
- swap 입출력: 측정하지 않았다.
- 로컬 환경 요인: WSL2 VM 전체 정지(60분에 1~2회, 약 15초)가 관측됐다([진단](../measurements/2026-11-19-spark-latency-spike-diagnosis.md)). Windows 쪽 원인은 확인하지 않았으며, 로컬 측정에서 지연이 튈 때는 생성기 상태 로그 공백으로 VM 정지 여부부터 가린다.
