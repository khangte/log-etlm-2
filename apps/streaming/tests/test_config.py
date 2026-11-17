"""streaming.config.load_config 단위 테스트: 기본값 없음, 필수값 누락, 형식 오류."""

import pytest

from streaming.config import Config, ConfigError, load_config

VALID = {
    "SPARK_KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
    "KAFKA_TOPIC": "raw.ecommerce.orders.v1",
    "KAFKA_DLQ_TOPIC": "dlq.ecommerce.orders.v1",
    "SPARK_STARTING_OFFSETS": "earliest",
    "SPARK_MAX_OFFSETS_PER_TRIGGER": "50000",
    "SPARK_TRIGGER_INTERVAL": "5 seconds",
    "SPARK_SHUFFLE_PARTITIONS": "3",
    "SPARK_CHECKPOINT_DIR": "/opt/spark/checkpoints/orders",
    "SPARK_OUTPUT_DIR": "/opt/spark/output/orders",
    "SPARK_LATENCY_METRICS_ENABLED": "true",
}


def test_유효한_설정을_읽는다():
    """모든 값이 있으면 Config 로 변환된다."""
    c = load_config(dict(VALID))
    assert isinstance(c, Config)
    assert (c.bootstrap_servers, c.topic, c.dlq_topic) == ("kafka:19092", "raw.ecommerce.orders.v1",
                                                            "dlq.ecommerce.orders.v1")
    assert (c.max_offsets_per_trigger, c.shuffle_partitions, c.trigger_interval) == (50000, 3, "5 seconds")
    assert (c.checkpoint_dir, c.output_dir, c.starting_offsets) == (
        "/opt/spark/checkpoints/orders", "/opt/spark/output/orders", "earliest")


def test_latest_허용():
    """starting offsets latest 허용."""
    assert load_config({**VALID, "SPARK_STARTING_OFFSETS": "latest"}).starting_offsets == "latest"


@pytest.mark.parametrize("key", sorted(VALID))
def test_필수값_누락(key):
    """키가 없으면 키 이름이 포함된 ConfigError."""
    env = dict(VALID)
    del env[key]
    with pytest.raises(ConfigError, match=key):
        load_config(env)


@pytest.mark.parametrize("key", sorted(VALID))
@pytest.mark.parametrize("blank", ["", "   "])
def test_빈값_거부(key, blank):
    """빈 문자열·공백은 누락과 같다."""
    with pytest.raises(ConfigError, match=key):
        load_config({**VALID, key: blank})


@pytest.mark.parametrize("key", ["SPARK_MAX_OFFSETS_PER_TRIGGER", "SPARK_SHUFFLE_PARTITIONS"])
@pytest.mark.parametrize("bad", ["abc", "1.5", "0", "-1", "5 seconds"])
def test_정수_형식_오류(key, bad):
    """양의 정수가 아니면 ConfigError."""
    with pytest.raises(ConfigError, match=key):
        load_config({**VALID, key: bad})


@pytest.mark.parametrize("bad", ["Earliest", "none", "0", "{\"a\":{\"0\":1}}"])
def test_starting_offsets_허용값(bad):
    """earliest/latest 외 값 거부."""
    with pytest.raises(ConfigError, match="SPARK_STARTING_OFFSETS"):
        load_config({**VALID, "SPARK_STARTING_OFFSETS": bad})


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("false", False)])
def test_지연_계측_켬_끔(raw, expected):
    """true/false를 불리언으로 읽는다."""
    assert load_config({**VALID, "SPARK_LATENCY_METRICS_ENABLED": raw}).latency_metrics_enabled is expected


@pytest.mark.parametrize("bad", ["True", "FALSE", "1", "0", "yes", "off"])
def test_지연_계측_허용값(bad):
    """소문자 true/false 외 값 거부."""
    with pytest.raises(ConfigError, match="SPARK_LATENCY_METRICS_ENABLED"):
        load_config({**VALID, "SPARK_LATENCY_METRICS_ENABLED": bad})


def test_빈_매핑은_프로세스_환경을_읽지_않는다(monkeypatch):
    """매핑({})을 주면 os.environ 값을 쓰지 않는다."""
    for k, v in VALID.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(ConfigError):
        load_config({})


def test_None이면_프로세스_환경을_읽는다(monkeypatch):
    """인자 없으면 os.environ 을 사용한다."""
    for k, v in VALID.items():
        monkeypatch.setenv(k, v)
    assert load_config().topic == VALID["KAFKA_TOPIC"]


def test_Config는_불변():
    """frozen dataclass."""
    c = load_config(dict(VALID))
    with pytest.raises(Exception):
        c.topic = "x"
