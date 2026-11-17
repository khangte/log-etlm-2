"""Kafka 주문 로그를 검증·평탄화하고 정상 출력과 DLQ로 나누는 Spark 스트리밍 패키지."""

# DLQ processor_version에 기록한다. 분류 규칙이 바뀌면 올린다.
__version__ = "0.1.0"
