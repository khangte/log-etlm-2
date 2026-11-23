"""생성기 ops.producer_stats 행 변환·구간 집계·HTTP 전송 검증."""

import io
import json
import re
import threading
import time
import unittest
import urllib.parse
from collections import Counter
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from log_generator.config import ClickHouseConfig, LoadProfile, Segment, load_config
from log_generator.main import run
from log_generator.ops_metrics import ProducerStatsWriter, producer_stats_row
from test_generator import ENV_VALUES
from test_kafka_producer import event, make

SCHEMA = Path(__file__).resolve().parents[3] / "infra" / "clickhouse" / "schema.sql"


def schema_columns(table: str) -> list[str]:
    """schema.sql의 CREATE TABLE 본문에서 컬럼 이름을 순서대로 읽는다."""
    body = re.search(rf"CREATE TABLE IF NOT EXISTS {re.escape(table)}\s*\((.*?)\n\)", SCHEMA.read_text(), re.S)
    return [line.split()[0] for line in body.group(1).strip().splitlines() if line.strip()]


class FakeWriter:
    """submit된 행과 close 호출을 기록한다."""

    def __init__(self):
        """빈 기록으로 시작한다."""
        self.rows, self.closed, self.failed_rows = [], False, 0

    def submit(self, row):
        """행을 기록한다."""
        self.rows.append(row)

    def close(self):
        """닫힘을 기록한다."""
        self.closed = True


class ProducerStatsRowTests(unittest.TestCase):
    def test_row_matches_schema_columns(self):
        out, _, _ = make()
        row = producer_stats_row(0.0, "r", 5000, 0, out.interval_metrics(), Counter())
        self.assertEqual(list(row), schema_columns("ops.producer_stats"))
        # 측정 대상이 없으면 건수 0, 분위수 0으로 둔다.
        self.assertEqual((row["ack_latency_ms_count"], row["ack_latency_ms_p99"]), (0, 0.0))
        self.assertEqual(row["interval_start"], "1970-01-01T00:00:00.000+00:00")

    def test_interval_metrics_are_per_interval_and_reset(self):
        out, producer, _ = make()
        for ms in (100, 200):
            out.send(*event(ms))
        out.sample_queue()
        out.poll()
        first = out.interval_metrics()
        self.assertEqual((first["acked"], first["failed"], first["queue_len_max"]), (2, 0, 2))
        self.assertEqual(first["gen_to_kafka_ms"], Counter({900: 1, 800: 1}))
        out.send(*event(500))
        out.poll()
        second = out.interval_metrics()
        self.assertEqual((second["acked"], second["gen_to_kafka_ms"].total()), (1, 1))
        self.assertEqual(out.metrics(detail=True)["gen_to_kafka_samples"], 3)  # 전체 누적은 유지


class RunWritesProducerStatsTests(unittest.TestCase):
    def test_run_submits_intervals_that_sum_to_totals_and_closes_writer(self):
        config = load_config(ENV_VALUES | {"PRODUCER_OUTPUT": "kafka", "PRODUCER_EPS": "1000",
                                           "PRODUCER_MAX_EVENTS": "1500", "PRODUCER_SEED": "3"})
        out, _, _ = make()
        writer, err = FakeWriter(), io.StringIO()
        stats, status = run(config, None, err, out, writer=writer)
        self.assertEqual(status, "completed")
        self.assertTrue(writer.closed)
        self.assertGreaterEqual(len(writer.rows), 2)  # 1.5초: 1초 구간 + 마지막 부분 구간
        self.assertEqual(sum(r["generated"] for r in writer.rows), 1500)
        self.assertEqual(sum(r["acked"] for r in writer.rows), 1500)
        self.assertEqual(sum(r["gen_serialize_us_count"] for r in writer.rows), 1500)
        self.assertEqual(len({r["run_id"] for r in writer.rows}), 1)
        self.assertEqual(writer.rows[-1]["undelivered"], 0)
        final = json.loads(err.getvalue().splitlines()[-1])
        self.assertEqual(final["producer_stats"]["run_id"], writer.rows[0]["run_id"])

    def test_rows_record_current_segment_target_eps(self):
        profile = LoadProfile(Path("test.csv"), (Segment(1, 1000, "a"), Segment(1, 2000, "b")))
        config = replace(load_config(ENV_VALUES | {"PRODUCER_OUTPUT": "kafka", "PRODUCER_MAX_EVENTS": "0",
                                                   "PRODUCER_SEED": "3"}), profile=profile)
        out, _, _ = make()
        writer = FakeWriter()
        stats, status = run(config, None, io.StringIO(), out, writer=writer)
        self.assertEqual(status, "completed")
        self.assertEqual(writer.rows[0]["target_eps"], 1000)
        self.assertEqual(writer.rows[-1]["target_eps"], 2000)
        self.assertEqual(sum(r["generated"] for r in writer.rows), stats.events)
        self.assertEqual(sum(r["acked"] for r in writer.rows), stats.events)


class WriterHttpTests(unittest.TestCase):
    def serve(self, code: int) -> tuple[HTTPServer, list]:
        """요청을 기록하고 code로 응답하는 로컬 HTTP 서버를 띄운다."""
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                """경로·헤더·본문을 기록하고 정해진 코드로 응답한다."""
                body = self.rfile.read(int(self.headers["Content-Length"]))
                requests.append((self.path, self.headers, body))  # 헤더 이름은 대소문자 무시로 조회
                self.send_response(code)
                self.end_headers()

            def log_message(self, *args):
                """테스트 출력에 접근 로그를 남기지 않는다."""

        server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        return server, requests

    def test_sends_json_each_row_with_async_insert_and_generator_account(self):
        server, requests = self.serve(200)
        writer = ProducerStatsWriter(ClickHouseConfig(f"http://127.0.0.1:{server.server_port}", "pw"), io.StringIO())
        writer.submit({"a": 1})
        writer.submit({"a": 2})
        writer.close()
        self.assertEqual(writer.failed_rows, 0)
        sent = [json.loads(line) for _, _, body in requests for line in body.splitlines()]
        self.assertEqual(sent, [{"a": 1}, {"a": 2}])
        path, headers, _ = requests[0]
        params = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
        self.assertEqual(params["query"], ["INSERT INTO ops.producer_stats FORMAT JSONEachRow"])
        self.assertEqual((params["async_insert"], params["wait_for_async_insert"]), (["1"], ["1"]))
        self.assertEqual((headers["X-ClickHouse-User"], headers["X-ClickHouse-Key"]), ("generator_writer", "pw"))

    def test_batches_rows_within_flush_interval(self):
        server, requests = self.serve(200)
        writer = ProducerStatsWriter(ClickHouseConfig(f"http://127.0.0.1:{server.server_port}", "pw"),
                                     io.StringIO(), flush_interval=0.5)
        for i in range(3):  # 간격 안의 행은 INSERT 1회로 묶인다
            writer.submit({"a": i})
            time.sleep(0.05)
        time.sleep(0.8)  # 간격이 지나 첫 묶음이 전송된다
        writer.submit({"a": 3})
        writer.close()  # 종료 때는 간격을 기다리지 않고 남은 행을 보낸다
        bodies = [[json.loads(line)["a"] for line in body.splitlines()] for _, _, body in requests]
        self.assertEqual(bodies, [[0, 1, 2], [3]])

    def test_failure_only_warns(self):
        server, _ = self.serve(500)
        err = io.StringIO()
        writer = ProducerStatsWriter(ClickHouseConfig(f"http://127.0.0.1:{server.server_port}", "pw"), err)
        writer.submit({"a": 1})
        writer.close()
        self.assertEqual(writer.failed_rows, 1)
        self.assertIn("producer_stats insert failed", err.getvalue())


if __name__ == "__main__":
    unittest.main()
