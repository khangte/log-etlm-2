"""수정 전(커밋 e9184c6) Python jsonschema 계약 검증 cProfile (PRD 03 '처리량 병목: 분류 단계 계약 검증' 보완).

정상 주문 1건 검증에 시간이 어디에 쓰이는지 확인한다. Spark 이미지 Python에서 수정 전 코드로 실행한다.
  B=$(base64 -w0 docs/measurements/profile_jsonschema_validation.py)
  git archive e9184c6 apps/streaming/src apps/log_generator/src contracts \
    | docker run --rm -i -e B=$B --entrypoint bash log-etlm-2-spark:latest -c \
      'mkdir -p /tmp/o && cd /tmp/o && tar x && echo $B | base64 -d > p.py && \
       PYTHONPATH=apps/streaming/src:apps/log_generator/src:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip python3 p.py'
"""

import cProfile
import json
import pstats
import random
import time
from collections import defaultdict

from jsonschema.exceptions import best_match
from log_generator.generator import EventGenerator, serialize
from log_generator.pools import build_user_pool
from streaming.transforms import VALIDATORS, classify

N = 20_000


def best_of(fn, rounds: int = 5) -> float:
    """fn을 rounds번 실행해 가장 빠른 회차의 µs/건을 돌려준다(첫 회차 워밍업 영향 제거)."""
    best = float("inf")
    for _ in range(rounds):
        t = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t)
    return best / N * 1e6


def group(path: str, func: str) -> str:
    """cProfile 항목을 비용 범주로 묶는다."""
    if "referencing" in path or func in ("_validate_reference", "ref", "dynamicRef"):
        return "$ref 참조 해석 (referencing)"
    if "rfc3339" in path or "_format" in path:
        return "format 검사 (date-time)"
    if func == "_pattern" or path.endswith("re/__init__.py") or path.endswith("sre_compile.py") or "_ecma_pattern" in func:
        return "pattern 정규식"
    if "_types" in path or func == "is_type":
        return "타입 검사 (type checker)"
    if "_keywords" in path or "_legacy_keywords" in path:
        return "키워드 함수 (properties, required 등)"
    if path.endswith("jsonschema/validators.py"):
        return "검증기 순회 (iter_errors, descend)"
    if "jsonschema" in path:
        return "jsonschema 기타"
    return "기타 (builtins 등)"


def main() -> None:
    """정상 주문 N건으로 시간 측정과 cProfile을 실행해 JSON으로 출력한다."""
    g = EventGenerator(random.Random(1), build_user_pool(500))
    raw = [serialize(g.order()) for _ in range(N)]
    docs = [json.loads(v) for v in raw]
    validator = VALIDATORS[("order_completed", "1.0.0")]

    timing = {
        "classify_us": round(best_of(lambda: [classify(v) for v in raw]), 1),
        "best_match_iter_errors_us": round(best_of(lambda: [best_match(validator.iter_errors(d)) for d in docs]), 1),
        "is_valid_us": round(best_of(lambda: [validator.is_valid(d) for d in docs]), 1),
    }
    try:
        import jsonschema_rs

        rs = jsonschema_rs.Draft202012Validator(validator.schema, validate_formats=True)
        timing["jsonschema_rs_is_valid_us"] = round(best_of(lambda: [rs.is_valid(d) for d in docs]), 2)
    except ImportError:
        pass

    prof = cProfile.Profile()
    prof.enable()
    for d in docs:
        best_match(validator.iter_errors(d))
    prof.disable()
    st = pstats.Stats(prof)
    total = st.total_tt
    calls = sum(v[1] for v in st.stats.values())

    by_group: dict[str, float] = defaultdict(float)
    for (path, _line, func), (_cc, _nc, tt, _ct, _callers) in st.stats.items():
        by_group[group(path, func)] += tt
    top = sorted(st.stats.items(), key=lambda kv: kv[1][2], reverse=True)[:15]
    cum = sorted(st.stats.items(), key=lambda kv: kv[1][3], reverse=True)[:15]

    def row(kv, key: int) -> dict:
        """pstats 항목 1개를 JSON 행으로 바꾼다."""
        (path, line, func), (cc, nc, tt, ct, _c) = kv
        return {"func": f"{path.split('site-packages/')[-1]}:{line}({func})", "calls_per_doc": round(nc / N, 1),
                "tottime_share": round(tt / total, 3), "cumtime_share": round(ct / total, 3)}

    print(json.dumps({
        "n": N, "doc": "order_completed 1.0.0, EventGenerator(seed=1), user pool 500, 모두 유효",
        "timing_best_of_5": timing,
        "profile": {"total_sec_with_profiler": round(total, 2), "function_calls_per_doc": round(calls / N, 1),
                    "by_group_share": {k: round(v / total, 3) for k, v in sorted(by_group.items(), key=lambda kv: -kv[1])},
                    "top_tottime": [row(kv, 2) for kv in top], "top_cumtime": [row(kv, 3) for kv in cum]},
    }, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
