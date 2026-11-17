"""streaming.transforms.classify 단위 테스트. PRD '파싱·평탄화 검증 기준'과 경계 입력을 검증한다."""

import json

import pytest

from conftest import dump, sample_bytes
from streaming import transforms
from streaming.transforms import LONG_PATHS, VALIDATORS, classify

EXPECTED_KEYS = {"outcome", "event_name", "schema_version", "error_stage", "error_code", "error_path",
                 "error_message"}


def dlq(result, stage, code, path=None):
    """결과가 불량이고 단계·코드·경로가 기대와 같은지 확인한다."""
    assert result["outcome"] == "dlq", result
    assert (result["error_stage"], result["error_code"], result["error_path"]) == (stage, code, path), result
    assert result["error_message"]


@pytest.fixture(autouse=True)
def checked_fast_valid(monkeypatch):
    """이 모듈의 모든 classify 입력에서 1단 "유효" 판정마다 엄격 검증기도 유효인지 대조한다."""
    fast_valid = transforms._fast_valid

    def _checked(key, doc):
        """1단 판정을 돌려주되 엄격 판정과 어긋나면(불량 통과) 실패시킨다."""
        valid = fast_valid(key, doc)
        assert not valid or VALIDATORS[key].is_valid(doc), f"1단만 유효로 판정: {doc!r}"
        return valid

    monkeypatch.setattr(transforms, "_fast_valid", _checked)


class TestTwoStageValidation:
    """1단(jsonschema-rs)과 2단(엄격 검증기) 판정 일치."""

    def test_소수점_정수는_1단을_건너뛰고_불량(self, mutate):
        # jsonschema-rs는 15000.0을 integer로 허용하므로 소수 감지로 1단을 건너뛰어야 한다.
        raw = mutate(lambda d: d["properties"]["items"][0].__setitem__("price", 15000.0))
        dlq(classify(raw), "schema", "SCHEMA_TYPE", "properties.items[0].price")

    @pytest.mark.parametrize("ts", ["0000-11-16T06:08:00.123Z", "2026-11-16T23:59:60.000Z"])
    def test_1단이_허용하는_시각은_불량(self, mutate, ts):
        # jsonschema-rs date-time은 연도 0000·초 60을 허용한다. 기존 형식 검사기 재확인으로 거부한다.
        dlq(classify(mutate(lambda d: d.__setitem__("event_timestamp", ts))), "schema", "SCHEMA_FORMAT",
            "event_timestamp")

    def test_생성기_이벤트_판정_일치(self):
        # 생성기의 정상·불량 혼합 이벤트 전체를 대조한다. 불일치는 _CheckedFast가 실패시킨다.
        import random
        from log_generator.generator import EventGenerator
        from log_generator.pools import build_user_pool

        gen = EventGenerator(random.Random(1), build_user_pool(1000))
        outcomes = {classify(gen.next_event().payload)["outcome"] for _ in range(3000)}
        assert outcomes == {"order", "excluded", "dlq"}


class TestStandardSamples:
    """표준 샘플 5종의 분류 결과."""

    def test_정상_주문_검증_결과(self):
        """정상 주문은 오류 없이 분류 메타데이터만 반환한다."""
        r = classify(sample_bytes("valid_order_completed.json"))
        assert set(r) == EXPECTED_KEYS
        assert r["outcome"] == "order"
        assert (r["event_name"], r["schema_version"]) == ("order_completed", "1.0.0")
        assert r["error_stage"] is r["error_code"] is r["error_path"] is r["error_message"] is None

    def test_정상_health_check는_제외된다(self):
        """정상 health_check: excluded, 오류 없음."""
        r = classify(sample_bytes("valid_health_check.json"))
        assert r["outcome"] == "excluded"
        assert (r["event_name"], r["schema_version"]) == ("health_check", "1.0.0")
        assert r["error_code"] is None

    def test_불량_JSON_파싱_실패(self):
        """malformed JSON: parse / INVALID_JSON."""
        dlq(classify(sample_bytes("invalid_malformed_json.json")), "parse", "INVALID_JSON")

    def test_불량_order_id_누락(self):
        """order_id 누락: schema / SCHEMA_REQUIRED / properties.order_id."""
        r = classify(sample_bytes("invalid_missing_order_id.json"))
        dlq(r, "schema", "SCHEMA_REQUIRED", "properties.order_id")
        assert (r["event_name"], r["schema_version"]) == ("order_completed", "1.0.0")

    def test_불량_금액_타입_오류(self):
        """total_amount 문자열: schema / SCHEMA_TYPE / properties.total_amount."""
        dlq(classify(sample_bytes("invalid_amount_type.json")), "schema", "SCHEMA_TYPE", "properties.total_amount")

    def test_bytearray_입력도_처리한다(self):
        """bytearray 입력은 bytes와 같은 결과."""
        assert classify(bytearray(sample_bytes("valid_order_completed.json")))["outcome"] == "order"

    def test_끝_개행이_있는_입력도_정상이다(self):
        """JSON 뒤 LF는 공백이므로 정상."""
        assert classify(sample_bytes("valid_order_completed.json") + b"\n")["outcome"] == "order"


class TestParseBoundaries:
    """파싱 단계 경계 입력."""

    def test_null_value(self):
        """None: NULL_VALUE."""
        dlq(classify(None), "parse", "NULL_VALUE")

    def test_빈_bytes(self):
        """b'': EMPTY_VALUE. null과 구분된다."""
        dlq(classify(b""), "parse", "EMPTY_VALUE")

    @pytest.mark.parametrize("raw", [b"\xff\xfe{}", b'{"a":"\xc3\x28"}', b"\x80"])
    def test_잘못된_UTF8(self, raw):
        """잘못된 UTF-8: INVALID_UTF8."""
        dlq(classify(raw), "parse", "INVALID_UTF8")

    def test_BOM은_JSON_실패(self):
        """UTF-8 BOM이 앞에 있으면 INVALID_JSON."""
        dlq(classify(b"\xef\xbb\xbf" + sample_bytes("valid_health_check.json")), "parse", "INVALID_JSON")

    @pytest.mark.parametrize("raw", [b"   ", b"\n", b"{", b'{"a":1,}', b"{'a':1}", b"{a:1}", b"// c\n{}",
                                      b"{} {}", b'{"a":1}x', b'{"a":1}\x00'])
    def test_JSON_문법_오류(self, raw):
        """공백만·문법 오류·관대한 확장 문법·여분 토큰은 INVALID_JSON."""
        dlq(classify(raw), "parse", "INVALID_JSON")

    @pytest.mark.parametrize("raw", [b'{"a":NaN}', b'{"a":Infinity}', b'{"a":-Infinity}'])
    def test_비표준_상수_거부(self, raw):
        """NaN/Infinity: INVALID_JSON."""
        dlq(classify(raw), "parse", "INVALID_JSON")

    def test_중복_키_거부(self):
        """중복 키: INVALID_JSON."""
        raw = sample_bytes("valid_health_check.json").replace(b'"event_name"', b'"event_id":"x","event_name"', 1)
        dlq(classify(raw), "parse", "INVALID_JSON")

    def test_중첩_중복_키_거부(self, mutate):
        """중첩 객체 안의 중복 키도 INVALID_JSON."""
        raw = sample_bytes("valid_order_completed.json").replace(b'"os":"iOS"', b'"os":"iOS","os":"x"', 1)
        dlq(classify(raw), "parse", "INVALID_JSON")

    def test_깊은_중첩은_예외없이_불량(self):
        """매우 깊은 중첩도 RecursionError 없이 불량 분류."""
        r = classify(b"[" * 100000 + b"]" * 100000)
        assert r["outcome"] == "dlq"

    def test_매우_긴_숫자는_예외없이_분류(self):
        """5000자리 정수가 예외를 던지지 않는다."""
        r = classify(b'{"event_name":"health_check","schema_version":"1.0.0","x":' + b"9" * 5000 + b"}")
        assert r["outcome"] == "dlq"

    @pytest.mark.parametrize("raw", [b"[]", b"[1,2]", b'"str"', b"123", b"true", b"null", b"1.5"])
    def test_비객체_JSON은_스키마_위반(self, raw):
        """유효 JSON이지만 객체가 아니면 schema / SCHEMA_TYPE / $."""
        dlq(classify(raw), "schema", "SCHEMA_TYPE", "$")


class TestContractSelection:
    """계약 선택 단계."""

    @pytest.mark.parametrize("doc, path", [
        ({}, "event_name"),
        ({"schema_version": "1.0.0"}, "event_name"),
        ({"event_name": None, "schema_version": "1.0.0"}, "event_name"),
        ({"event_name": 1, "schema_version": "1.0.0"}, "event_name"),
        ({"event_name": "health_check"}, "schema_version"),
        ({"event_name": "health_check", "schema_version": 1}, "schema_version"),
        ({"event_name": "health_check", "schema_version": None}, "schema_version"),
    ])
    def test_분류_필드_누락_타입오류(self, doc, path):
        """event_name·schema_version 누락/비문자열: contract / CONTRACT_FIELD_INVALID."""
        dlq(classify(dump(doc)), "contract", "CONTRACT_FIELD_INVALID", path)

    @pytest.mark.parametrize("name, ver", [
        ("order_completed", "2.0.0"), ("order_completed", "1.0"), ("order_completed", ""),
        ("unknown_event", "1.0.0"), ("ORDER_COMPLETED", "1.0.0"), ("health_check", "1.0.1"),
        ("order_completed ", "1.0.0"),
    ])
    def test_미지원_이벤트_버전(self, name, ver):
        """미지원 조합: CONTRACT_UNSUPPORTED, 식별 가능한 이름·버전 보존, 경로 없음."""
        r = classify(dump({"event_name": name, "schema_version": ver}))
        dlq(r, "contract", "CONTRACT_UNSUPPORTED")
        assert (r["event_name"], r["schema_version"]) == (name, ver)

    def test_지원_조합은_두_가지뿐(self):
        """VALIDATORS 키."""
        assert set(VALIDATORS) == {("order_completed", "1.0.0"), ("health_check", "1.0.0")}

    def test_필드_누락시_식별값_일부만_보존(self):
        """schema_version 누락 시 event_name은 보존된다."""
        r = classify(dump({"event_name": "health_check"}))
        assert r["event_name"] == "health_check" and r["schema_version"] is None


class TestHealthCheck:
    """health_check도 계약 검증을 받는다."""

    def hc(self, **over):
        """표준 health_check에 필드를 덮어쓴 bytes."""
        doc = json.loads(sample_bytes("valid_health_check.json"))
        doc.update(over)
        return dump(doc)

    def test_추가_필드는_불량(self):
        """정의 외 필드: SCHEMA_ADDITIONALPROPERTIES, 경로는 해당 필드."""
        dlq(classify(self.hc(context={})), "schema", "SCHEMA_ADDITIONALPROPERTIES", "context")
        dlq(classify(self.hc(properties={})), "schema", "SCHEMA_ADDITIONALPROPERTIES", "properties")

    def test_필수_누락은_불량(self):
        """event_id 누락: SCHEMA_REQUIRED / event_id."""
        doc = json.loads(sample_bytes("valid_health_check.json"))
        del doc["event_id"]
        dlq(classify(dump(doc)), "schema", "SCHEMA_REQUIRED", "event_id")

    def test_잘못된_event_id(self):
        """event_id 형식 오류: SCHEMA_PATTERN."""
        dlq(classify(self.hc(event_id="abc")), "schema", "SCHEMA_PATTERN", "event_id")

    def test_event_id_null(self):
        """null 값: SCHEMA_TYPE."""
        dlq(classify(self.hc(event_id=None)), "schema", "SCHEMA_TYPE", "event_id")

    def test_잘못된_시각(self):
        """잘못된 날짜: 제외되지 않고 불량."""
        r = classify(self.hc(event_timestamp="2026-02-30T06:08:00.456Z"))
        assert r["outcome"] == "dlq" and r["error_path"] == "event_timestamp"


class TestSchemaViolations:
    """주문 스키마 위반 경계."""

    def test_정의_외_최상위_필드(self, mutate):
        """최상위 정의 외 필드."""
        dlq(classify(mutate(lambda d: d.update(extra=1))), "schema", "SCHEMA_ADDITIONALPROPERTIES", "extra")

    def test_정의_외_중첩_필드(self, mutate):
        """중첩 객체(context.device)의 정의 외 필드에도 additionalProperties=false."""
        r = classify(mutate(lambda d: d["context"]["device"].update(extra="x")))
        dlq(r, "schema", "SCHEMA_ADDITIONALPROPERTIES", "context.device.extra")

    def test_items_원소_정의_외_필드(self, mutate):
        """items[0] 정의 외 필드 경로."""
        r = classify(mutate(lambda d: d["properties"]["items"][0].update(extra="x")))
        dlq(r, "schema", "SCHEMA_ADDITIONALPROPERTIES", "properties.items[0].extra")

    @pytest.mark.parametrize("path", [
        ["event_id"], ["event_timestamp"], ["context", "user_id"], ["context", "is_logged_in"],
        ["context", "device", "os"], ["context", "network", "city"], ["context", "page", "utm_source"],
        ["properties", "currency"], ["properties", "total_amount"], ["properties", "items", 0, "price"],
        ["properties", "items", 0, "quantity"], ["properties", "items", 0, "product_id"],
    ])
    def test_모든_필수_필드_누락(self, order_doc, path):
        """필수 필드 누락은 SCHEMA_REQUIRED이고 경로가 누락 필드까지 이어진다."""
        parent = order_doc
        for p in path[:-1]:
            parent = parent[p]
        del parent[path[-1]]
        r = classify(dump(order_doc))
        text = ""
        for p in path:
            text += f"[{p}]" if isinstance(p, int) else (f".{p}" if text else p)
        dlq(r, "schema", "SCHEMA_REQUIRED", text)

    @pytest.mark.parametrize("path", [
        ["event_id"], ["event_timestamp"], ["context"], ["context", "user_id"], ["context", "is_logged_in"],
        ["context", "device", "os"], ["properties", "order_id"], ["properties", "total_amount"],
        ["properties", "discount_amount"], ["properties", "items"], ["properties", "items", 0, "price"],
    ])
    def test_null_값은_거부(self, order_doc, path):
        """null 허용 안 함."""
        parent = order_doc
        for p in path[:-1]:
            parent = parent[p]
        parent[path[-1]] = None
        r = classify(dump(order_doc))
        assert r["outcome"] == "dlq" and r["error_stage"] == "schema", r

    @pytest.mark.parametrize("field, value, code", [
        ("event_timestamp", "2026-02-30T06:08:00.123Z", "SCHEMA_FORMAT"),
        ("event_timestamp", "2026-13-01T06:08:00.123Z", "SCHEMA_FORMAT"),
        ("event_timestamp", "2026-11-16T25:08:00.123Z", "SCHEMA_FORMAT"),
        ("event_timestamp", "2026-11-16T06:08:60.123Z", "SCHEMA_FORMAT"),
        ("event_timestamp", "0000-11-16T06:08:00.123Z", "SCHEMA_FORMAT"),
        ("event_timestamp", "2025-02-29T06:08:00.123Z", "SCHEMA_FORMAT"),
        ("event_timestamp", "2026-11-16T06:08:00Z", "SCHEMA_PATTERN"),
        ("event_timestamp", "2026-11-16T06:08:00.12Z", "SCHEMA_PATTERN"),
        ("event_timestamp", "2026-11-16T06:08:00.123456Z", "SCHEMA_PATTERN"),
        ("event_timestamp", "2026-11-16T06:08:00.123+09:00", "SCHEMA_PATTERN"),
        ("event_timestamp", "2026-11-16 06:08:00.123Z", "SCHEMA_FORMAT"),
        ("event_timestamp", "2026-11-16t06:08:00.123z", "SCHEMA_PATTERN"),
        ("event_timestamp", 1763273280123, "SCHEMA_TYPE"),
        ("event_timestamp", "", "SCHEMA_FORMAT"),
    ])
    def test_event_timestamp_형식(self, mutate, field, value, code):
        """event_timestamp 형식·달력 시각 검증."""
        r = classify(mutate(lambda d: d.update({field: value})))
        dlq(r, "schema", code, "event_timestamp")

    def test_윤년_2월_29일은_정상(self, mutate):
        """2028-02-29는 실제 달력 시각이므로 정상."""
        r = classify(mutate(lambda d: d.update(event_timestamp="2028-02-29T23:59:59.999Z")))
        assert r["outcome"] == "order"

    def test_event_timestamp_경계값(self, mutate):
        """0001-01-01 과 9999-12-31 도 변환된다."""
        for ts in ("0001-01-01T00:00:00.000Z", "9999-12-31T23:59:59.999Z"):
            r = classify(mutate(lambda d: d.update(event_timestamp=ts)))
            assert r["outcome"] == "order", (ts, r)

    @pytest.mark.parametrize("eid", [
        "550e8400-e29b-11d4-a716-446655440000",  # 버전 1
        "550e8400-e29b-41d4-c716-446655440000",  # variant
        "550E8400-E29B-41D4-A716-446655440000",  # 대문자
        "550e8400e29b41d4a716446655440000",
        " 550e8400-e29b-41d4-a716-446655440000",
        "",
    ])
    def test_event_id_형식(self, mutate, eid):
        """UUID v4 소문자 형식이 아니면 SCHEMA_PATTERN."""
        dlq(classify(mutate(lambda d: d.update(event_id=eid))), "schema", "SCHEMA_PATTERN", "event_id")

    @pytest.mark.parametrize("field", ["event_id", "event_timestamp"])
    def test_끝_개행이_붙은_값은_거부해야_한다(self, mutate, field):
        """Python `$`는 끝 개행 앞에도 매치된다. ECMA-262 정규식 기준 계약은 이를 거부해야 한다."""
        base = json.loads(sample_bytes("valid_order_completed.json"))[field]
        r = classify(mutate(lambda d: d.update({field: base + "\n"})))
        assert r["outcome"] == "dlq", f"{field}에 끝 개행을 붙여도 {r['outcome']} 로 수용됨: {r.get('order') and r['order'][field]!r}"

    @pytest.mark.parametrize("section, field, value", [
        ("properties", "order_id", "ord_550e8400e29b41d4a716446655440000\n"),
        ("context", "user_id", "usr_00001\n"),
        ("context", "session_id", "sess_550e8400e29b41d4a716446655440000\n"),
    ])
    def test_끝_개행_다른_식별자(self, mutate, section, field, value):
        """식별자 pattern에도 끝 개행이 거부되어야 한다."""
        r = classify(mutate(lambda d: d[section].update({field: value})))
        assert r["outcome"] == "dlq", f"{field} 끝 개행이 수용됨"

    @pytest.mark.parametrize("value", ["amount", "27000", True, False, 27000.0, 2.7e4, [], {}, None])
    def test_total_amount_타입(self, mutate, value):
        """숫자 문자열·불리언·소수점 표기(27000.0)·배열·객체·null은 SCHEMA_TYPE."""
        r = classify(mutate(lambda d: d["properties"].update(total_amount=value)))
        dlq(r, "schema", "SCHEMA_TYPE", "properties.total_amount")

    def test_원문_27000_0_표기(self):
        """원문 텍스트가 27000.0 인 경우."""
        raw = sample_bytes("valid_order_completed.json").replace(b'"total_amount":27000', b'"total_amount":27000.0')
        dlq(classify(raw), "schema", "SCHEMA_TYPE", "properties.total_amount")

    def test_원문_지수_표기_정수형(self):
        """원문 2.7e4 도 정수로 수용하지 않는다."""
        raw = sample_bytes("valid_order_completed.json").replace(b'"total_amount":27000', b'"total_amount":2.7e4')
        dlq(classify(raw), "schema", "SCHEMA_TYPE", "properties.total_amount")

    @pytest.mark.parametrize("value", ["true", "false", 1, 0, "True", None])
    def test_is_logged_in_타입(self, mutate, value):
        """불리언 강제 변환 없음."""
        r = classify(mutate(lambda d: d["context"].update(is_logged_in=value)))
        dlq(r, "schema", "SCHEMA_TYPE", "context.is_logged_in")

    @pytest.mark.parametrize("price", ["15000", 15000.5, True, 0, -1])
    def test_price_타입_범위(self, mutate, price):
        """price: 문자열·소수·불리언은 SCHEMA_TYPE, 0 이하는 SCHEMA_MINIMUM."""
        r = classify(mutate(lambda d: d["properties"]["items"][0].update(price=price)))
        code = "SCHEMA_MINIMUM" if price in (0, -1) and price is not True else "SCHEMA_TYPE"
        dlq(r, "schema", code, "properties.items[0].price")

    @pytest.mark.parametrize("q, code", [(0, "SCHEMA_MINIMUM"), (-1, "SCHEMA_MINIMUM"), (1.5, "SCHEMA_TYPE"),
                                          ("2", "SCHEMA_TYPE"), (True, "SCHEMA_TYPE")])
    def test_quantity(self, mutate, q, code):
        """quantity 범위·타입."""
        r = classify(mutate(lambda d: d["properties"]["items"][0].update(quantity=q)))
        dlq(r, "schema", code, "properties.items[0].quantity")

    @pytest.mark.parametrize("field", ["total_amount", "discount_amount"])
    def test_음수_금액(self, mutate, field):
        """total/discount 음수는 SCHEMA_MINIMUM."""
        r = classify(mutate(lambda d: d["properties"].update({field: -1})))
        dlq(r, "schema", "SCHEMA_MINIMUM", f"properties.{field}")

    def test_items_빈_배열(self, mutate):
        """items 0건: SCHEMA_MINITEMS / properties.items."""
        dlq(classify(mutate(lambda d: d["properties"].update(items=[]))), "schema", "SCHEMA_MINITEMS",
            "properties.items")

    def test_items_2건(self, mutate):
        """items 2건: SCHEMA_MAXITEMS / properties.items."""
        dlq(classify(mutate(lambda d: d["properties"]["items"].append(dict(d["properties"]["items"][0])))),
            "schema", "SCHEMA_MAXITEMS", "properties.items")

    def test_items_null(self, mutate):
        """items null: SCHEMA_TYPE."""
        dlq(classify(mutate(lambda d: d["properties"].update(items=None))), "schema", "SCHEMA_TYPE",
            "properties.items")

    def test_items_비배열(self, mutate):
        """items가 객체: SCHEMA_TYPE."""
        dlq(classify(mutate(lambda d: d["properties"].update(items={}))), "schema", "SCHEMA_TYPE",
            "properties.items")

    def test_items_원소_비객체(self, mutate):
        """items[0]이 문자열: SCHEMA_TYPE / properties.items[0]."""
        dlq(classify(mutate(lambda d: d["properties"].update(items=["x"]))), "schema", "SCHEMA_TYPE",
            "properties.items[0]")

    @pytest.mark.parametrize("field, value, code", [
        ("payment_method", "cash", "SCHEMA_ENUM"), ("payment_method", "CREDIT_CARD", "SCHEMA_ENUM"),
        ("currency", "USD", "SCHEMA_CONST"), ("currency", "krw", "SCHEMA_CONST"),
        ("order_id", "ord_xyz", "SCHEMA_PATTERN"), ("order_id", "", "SCHEMA_PATTERN"),
    ])
    def test_properties_코드값(self, mutate, field, value, code):
        """enum/const/pattern 위반."""
        dlq(classify(mutate(lambda d: d["properties"].update({field: value}))), "schema", code,
            f"properties.{field}")

    @pytest.mark.parametrize("field, value, code", [
        ("user_tier", "vip", "SCHEMA_ENUM"), ("user_tier", "GOLD", "SCHEMA_ENUM"),
        ("user_id", "usr_1", "SCHEMA_PATTERN"), ("user_id", "u1", "SCHEMA_PATTERN"),
        ("session_id", "sess_ABC", "SCHEMA_PATTERN"),
    ])
    def test_context_코드값(self, mutate, field, value, code):
        """context 코드값 위반."""
        dlq(classify(mutate(lambda d: d["context"].update({field: value}))), "schema", code, f"context.{field}")

    def test_country_소문자(self, mutate):
        """country 대소문자 보정 없음."""
        dlq(classify(mutate(lambda d: d["context"]["network"].update(country="kr"))), "schema", "SCHEMA_PATTERN",
            "context.network.country")

    def test_빈_문자열_필드(self, mutate):
        """non_empty_string: 빈 값은 SCHEMA_MINLENGTH."""
        dlq(classify(mutate(lambda d: d["context"]["device"].update(os=""))), "schema", "SCHEMA_MINLENGTH",
            "context.device.os")

    def test_URL_쿼리_거부(self, mutate):
        """current_url에 쿼리 문자열이 있으면 SCHEMA_PATTERN."""
        dlq(classify(mutate(lambda d: d["context"]["page"].update(current_url="/a?x=1"))), "schema",
            "SCHEMA_PATTERN", "context.page.current_url")

    def test_event_name_const_불일치는_계약선택_단계(self, mutate):
        """order_completed 문서에 health_check 이름만 넣으면 health_check 계약으로 검증되어 불량."""
        r = classify(mutate(lambda d: d.update(event_name="health_check")))
        dlq(r, "schema", "SCHEMA_ADDITIONALPROPERTIES", "context")

    def test_다중_오류는_1건만_기록(self, mutate):
        """오류가 여러 개여도 결과는 불량 1건(best_match)이고 예외가 없다."""
        def f(d):
            d["event_id"] = "x"
            d["properties"]["currency"] = "USD"
            d["context"]["user_tier"] = "gold"
        r = classify(mutate(f))
        assert r["outcome"] == "dlq" and r["error_stage"] == "schema"

    def test_공백_문자열_보정_없음(self, mutate):
        """문자열 앞뒤 공백은 보정하지 않고 enum 위반으로 거부."""
        dlq(classify(mutate(lambda d: d["properties"].update(payment_method=" kakao_pay"))), "schema",
            "SCHEMA_ENUM", "properties.payment_method")


class TestSemantic:
    """의미 규칙 위반."""

    def test_할인_상한_초과(self, mutate):
        """할인 > 상품 합계: SEMANTIC_DISCOUNT_EXCEEDS."""
        r = classify(mutate(lambda d: d["properties"].update(discount_amount=30001, total_amount=0)))
        dlq(r, "semantic", "SEMANTIC_DISCOUNT_EXCEEDS", "properties.discount_amount")

    def test_할인_상한_경계_동일은_정상(self, mutate):
        """할인 == 상품 합계, total 0 은 정상."""
        r = classify(mutate(lambda d: d["properties"].update(discount_amount=30000, total_amount=0)))
        assert r["outcome"] == "order"

    def test_금액_불일치(self, mutate):
        """total != 합계-할인: SEMANTIC_AMOUNT_MISMATCH."""
        r = classify(mutate(lambda d: d["properties"].update(total_amount=27001)))
        dlq(r, "semantic", "SEMANTIC_AMOUNT_MISMATCH", "properties.total_amount")

    def test_수량_변경시_금액_불일치(self, mutate):
        """quantity만 바꿔 합계가 어긋남."""
        r = classify(mutate(lambda d: d["properties"]["items"][0].update(quantity=3)))
        dlq(r, "semantic", "SEMANTIC_AMOUNT_MISMATCH", "properties.total_amount")

    @pytest.mark.parametrize("patch", [
        {"product_id": "prod_9999"},
        {"product_name": "Other"},
        {"category": "home"},
    ])
    def test_상품_불일치(self, mutate, patch):
        """풀에 없는 ID·이름/카테고리 불일치: SEMANTIC_PRODUCT_MISMATCH / properties.items[0]."""
        r = classify(mutate(lambda d: d["properties"]["items"][0].update(patch)))
        dlq(r, "semantic", "SEMANTIC_PRODUCT_MISMATCH", "properties.items[0]")

    def test_상품_가격_불일치(self, mutate):
        """가격이 풀과 다르면 (금액은 일관) PRODUCT_MISMATCH."""
        def f(d):
            d["properties"]["items"][0]["price"] = 16000
            d["properties"].update(total_amount=29000)
        dlq(classify(mutate(f)), "semantic", "SEMANTIC_PRODUCT_MISMATCH", "properties.items[0]")

    @pytest.mark.parametrize("patch", [
        {"region": "Busan"},  # KR/Busan/Geumcheon-gu
        {"city": "Nowhere"},
        {"country": "US"},
    ])
    def test_지역_불일치(self, mutate, patch):
        """지역 풀에 없는 조합: SEMANTIC_LOCATION_MISMATCH / context.network."""
        r = classify(mutate(lambda d: d["context"]["network"].update(patch)))
        dlq(r, "semantic", "SEMANTIC_LOCATION_MISMATCH", "context.network")

    def test_의미_검증은_스키마_통과_후(self, mutate):
        """스키마 위반이 있으면 의미 규칙보다 스키마 단계가 먼저."""
        r = classify(mutate(lambda d: d["properties"].update(total_amount=1, currency="USD")))
        assert r["error_stage"] == "schema"

    def test_모든_상품_풀_조합은_정상(self, mutate):
        """상품 풀 10종 × 지역 풀 7종이 모두 정상으로 통과한다(풀 동기화 확인)."""
        from log_generator.pools import LOCATIONS, PRODUCTS
        for p in PRODUCTS:
            def f(d, p=p):
                d["properties"]["items"][0].update(product_id=p.product_id, product_name=p.product_name,
                                                     category=p.category, price=p.price, quantity=1)
                d["properties"].update(total_amount=p.price, discount_amount=0)
            assert classify(mutate(f))["outcome"] == "order", p
        for loc in LOCATIONS:
            r = classify(mutate(lambda d, loc=loc: d["context"]["network"].update(
                country=loc.country, region=loc.region, city=loc.city)))
            assert r["outcome"] == "order", loc


class TestConversion:
    """Long 범위 초과와 타입 변환."""

    def test_Long_최댓값은_정상(self, mutate):
        """2^63-1 총액은 변환 단계 통과(의미 규칙 불일치가 먼저이므로 불량이어도 CONVERSION이 아님을 확인)."""
        r = classify(mutate(lambda d: d["properties"].update(total_amount=2**63 - 1)))
        assert r["error_code"] == "SEMANTIC_AMOUNT_MISMATCH"

    def test_total_amount_Long_초과_자체는_의미규칙이_먼저(self, mutate):
        """상품 풀 가격이 고정이라 total_amount 단독 초과는 금액 불일치로 분류된다."""
        r = classify(mutate(lambda d: d["properties"].update(total_amount=2**63)))
        dlq(r, "semantic", "SEMANTIC_AMOUNT_MISMATCH", "properties.total_amount")

    def test_quantity_Long_초과(self, mutate):
        """quantity 가 Long 범위를 넘고 금액이 일관되면 CONVERSION_LONG_RANGE(매핑 순서상 첫 초과 컬럼 경로)."""
        q = 2**63
        price = 15000
        subtotal = price * q

        def f(d):
            d["properties"]["items"][0]["quantity"] = q
            d["properties"].update(discount_amount=0, total_amount=subtotal)
        r = classify(mutate(f))
        dlq(r, "conversion", "CONVERSION_LONG_RANGE", "properties.total_amount")  # 컬럼 순서상 첫 초과 컬럼

    def test_total_amount_Long_초과_변환(self, mutate):
        """합계가 맞는 거대 수량: 변환 실패는 첫 초과 컬럼(total_amount) 기준."""
        q = 2**63

        def f(d):
            d["properties"]["items"][0]["quantity"] = q
            d["properties"].update(discount_amount=0, total_amount=15000 * q)
        r = classify(mutate(f))
        assert r["error_stage"] == "conversion" and r["error_code"] == "CONVERSION_LONG_RANGE"

    def test_discount_amount_Long_초과(self, mutate):
        """discount 와 합계가 모두 Long 초과."""
        q = 2**62

        def f(d):
            d["properties"]["items"][0]["quantity"] = q
            d["properties"].update(discount_amount=2**63, total_amount=15000 * q - 2**63)
        r = classify(mutate(f))
        assert r["error_code"] == "CONVERSION_LONG_RANGE", r

    def test_Long_범위_안_최대_수량은_정상(self, mutate):
        """합계가 Long을 넘지 않는 거대 수량."""
        q = (2**63 - 1) // 15000

        def f(d):
            d["properties"]["items"][0]["quantity"] = q
            d["properties"].update(discount_amount=0, total_amount=15000 * q)
        r = classify(mutate(f))
        assert r["outcome"] == "order"

    def test_Long_컬럼_상수(self):
        """Long 범위 검증 경로는 4개."""
        assert set(LONG_PATHS) == {"total_amount", "discount_amount", "price", "quantity"}

    def test_UTF8_한글_값_보존(self, mutate):
        """비ASCII 문자열(os)도 계약 검증을 통과한다."""
        r = classify(mutate(lambda d: d["context"]["device"].update(os="아이폰🍎")))
        assert r["outcome"] == "order"

    def test_이스케이프_유니코드(self, mutate):
        """\\u 이스케이프는 디코딩 후 계약 검증을 통과한다."""
        raw = sample_bytes("valid_order_completed.json").replace(b'"os":"iOS"', b'"os":"\\uc544\\uc774"')
        assert classify(raw)["outcome"] == "order"
