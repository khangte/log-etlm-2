"""불량률별 classify() 단독 벤치(포트폴리오 보완 측정 2, PRD 03 '처리량 병목: 분류 단계 계약 검증' 후속).

두 단계로 실행한다.
  1) 페이로드 생성(호스트, 생성기 venv):
     apps/log_generator/.venv/bin/python bench_classify_bad_rate.py gen <out.jsonl>
     정상 주문·health_check·불량 3종(malformed_json, missing_order_id, invalid_amount_type)을 따로 만든다.
  2) 측정(Spark 이미지 Python): python3 bench_classify_bad_rate.py run <out.jsonl>
     불량률 r마다 정상 이벤트 일부를 불량으로 바꾼 20,000건 묶음을 classify()에 넣어 µs/건을 잰다.
     정상 쪽은 생성기 비율(주문:health_check = 85:10)을 유지한다. 불량은 3종을 같은 비율로 섞는다.
"""

import json
import sys
import time

N = 20_000
RATES = (0.0, 0.05, 0.20, 0.50, 1.0)
ANOMALIES = ("malformed_json", "missing_order_id", "invalid_amount_type")


def gen(path: str) -> None:
    """생성기 함수로 종류별 페이로드를 만들어 jsonl로 저장한다."""
    import random

    from log_generator.generator import EventGenerator, serialize
    from log_generator.pools import build_user_pool

    g = EventGenerator(random.Random(1), build_user_pool(500))
    with open(path, "w") as f:
        for _ in range(N):
            f.write(json.dumps({"kind": "normal_order", "v": serialize(g.order()).decode()}) + "\n")
        for _ in range(N):
            f.write(json.dumps({"kind": "health_check", "v": serialize(g.health_check()).decode()}) + "\n")
        for a in ANOMALIES:
            for _ in range(N):
                f.write(json.dumps({"kind": a, "v": g.invalid_order(a).payload.decode()}) + "\n")


def bench(values: list[bytes], rounds: int = 5) -> float:
    """classify()를 rounds번 반복해 가장 빠른 회차의 µs/건을 돌려준다(첫 회차 워밍업 영향 제거)."""
    from streaming.transforms import classify

    best = float("inf")
    for _ in range(rounds):
        t = time.perf_counter()
        for v in values:
            classify(v)
        best = min(best, time.perf_counter() - t)
    return best / len(values) * 1e6


def run(path: str) -> None:
    """불량률별·종류별 µs/건을 JSON으로 출력한다."""
    pools: dict[str, list[bytes]] = {}
    for line in open(path):
        r = json.loads(line)
        pools.setdefault(r["kind"], []).append(r["v"].encode())

    by_kind = {k: round(bench(v[:5000]), 1) for k, v in pools.items()}
    by_rate = []
    for rate in RATES:
        bad = int(N * rate)
        good = N - bad
        orders = good * 85 // 95
        values = pools["normal_order"][:orders] + pools["health_check"][: good - orders]
        values += [pools[ANOMALIES[i % 3]][i // 3] for i in range(bad)]
        us = bench(values)
        by_rate.append({"bad_rate": rate, "us_per_event": round(us, 1), "events_per_sec_core": round(1e6 / us)})
    print(json.dumps({"n": N, "by_kind_us": by_kind, "by_rate": by_rate}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    {"gen": gen, "run": run}[sys.argv[1]](sys.argv[2])
