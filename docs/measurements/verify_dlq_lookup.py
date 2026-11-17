"""기존 Kafka DLQ의 불량 3종을 조회하고 원본 offset의 bytes와 대조한다.

토픽·설정을 변경하지 않으며 소비 offset도 저장하거나 커밋하지 않는다.
"""

import argparse
import base64
import hashlib
import json
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition

EXPECTED = {
    "INVALID_JSON": ("parse", None),
    "SCHEMA_REQUIRED": ("schema", "properties.order_id"),
    "SCHEMA_TYPE": ("schema", "properties.total_amount"),
}
FIELDS = {
    "dlq_schema_version", "source_topic", "source_partition", "source_offset",
    "source_timestamp", "raw_value_base64", "error_stage", "error_code",
    "error_path", "error_message", "event_name", "schema_version",
    "detected_at", "processor_version",
}


def read_message(consumer, deadline):
    """지정 시간이 지나기 전에 실제 메시지 한 건을 읽고 소비 오류를 드러낸다."""
    while time.monotonic() < deadline:
        message = consumer.poll(min(1, max(0, deadline - time.monotonic())))
        if message is not None:
            if message.error():
                raise RuntimeError(str(message.error()))
            return message
    raise TimeoutError("Kafka message read timed out")


def verify(args):
    """DLQ의 고정 조회 범위에서 대표 레코드를 찾고 원본 bytes·위치를 확인한다."""
    config = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": "dlq-lookup-verification-" + uuid.uuid4().hex,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
    }
    consumer = Consumer(config)
    try:
        partition = TopicPartition(args.dlq_topic, 0)
        low, high = consumer.get_watermark_offsets(partition, timeout=10)
        start = max(low, high - args.scan_limit)
        consumer.assign([TopicPartition(args.dlq_topic, 0, start)])
        selected = {}
        scanned = 0
        deadline = time.monotonic() + 30
        while len(selected) < len(EXPECTED) and scanned < high - start:
            message = read_message(consumer, deadline)
            if message.offset() >= high:
                break
            scanned += 1
            record = json.loads(message.value())
            code = record.get("error_code")
            if code not in EXPECTED or code in selected:
                continue
            stage, path = EXPECTED[code]
            if record.get("error_path") != path:
                continue
            assert set(record) == FIELDS, record.keys()
            assert record["dlq_schema_version"] == "1.0.0"
            assert record["error_stage"] == stage
            assert record["error_message"] and record["processor_version"]
            key = f'{record["source_topic"]}:{record["source_partition"]}:{record["source_offset"]}'
            assert message.key() == key.encode("utf-8")
            selected[code] = {
                "dlq_partition": message.partition(), "dlq_offset": message.offset(),
                "key": key, "record": record,
            }
        assert set(selected) == set(EXPECTED), f"Missing error classes: {set(EXPECTED) - set(selected)}"

        for code, sample in selected.items():
            record = sample["record"]
            source = TopicPartition(record["source_topic"], record["source_partition"], record["source_offset"])
            consumer.assign([source])
            original = read_message(consumer, time.monotonic() + 15)
            assert (original.topic(), original.partition(), original.offset()) == (source.topic, source.partition, source.offset)
            restored = base64.b64decode(record["raw_value_base64"], validate=True)
            assert restored == original.value(), "Restored bytes differ from original Kafka value"
            assert original.timestamp()[0] == 2, "Source timestamp must be LogAppendTime"
            timestamp = datetime.fromtimestamp(original.timestamp()[1] / 1000, timezone.utc)
            assert timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z") == record["source_timestamp"]
            if code == "INVALID_JSON":
                try:
                    json.loads(restored)
                except json.JSONDecodeError:
                    pass
                else:
                    raise AssertionError("Source is not malformed JSON")
            else:
                properties = json.loads(restored)["properties"]
                if code == "SCHEMA_REQUIRED":
                    assert "order_id" not in properties
                else:
                    assert isinstance(properties["total_amount"], str)
            sample.update({
                "restored_byte_length": len(restored),
                "original_sha256": hashlib.sha256(original.value()).hexdigest(),
                "restored_sha256": hashlib.sha256(restored).hexdigest(),
                "bytes_equal": True, "source_position_equal": True,
                "source_timestamp_equal": True, "strict_base64_decode": True,
            })
        return {
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "bootstrap_servers": args.bootstrap_servers,
            "dlq_topic": args.dlq_topic,
            "consumer_config": config,
            "snapshot": {"partition": 0, "low": low, "high_exclusive": high, "scan_start": start, "records_scanned": scanned},
            "scope": "Existing generated anomalies; no new input, no reprocessing, no topic/config/checkpoint changes",
            "samples": selected,
            "result": "passed",
        }
    finally:
        consumer.close()


def main():
    """접속 주소와 조회 한도를 받아 검증 결과를 JSON으로 출력한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--dlq-topic", default="dlq.ecommerce.orders.v1")
    parser.add_argument("--scan-limit", type=int, default=2000)
    args = parser.parse_args()
    if args.scan_limit < 1:
        parser.error("--scan-limit must be positive")
    print(json.dumps(verify(args), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
