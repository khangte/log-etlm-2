"""멈춤 감시 스레드가 이벤트 공백이 timeout을 넘을 때만 exit 1을 호출하는지 검증한다."""

from unittest.mock import MagicMock

from streaming.monitoring import ProgressLogger, StallWatchdog


class FakeClock:
    """테스트에서 시각을 직접 옮기는 단조 시계."""

    def __init__(self):
        """0초에서 시작한다."""
        self.now = 0.0

    def __call__(self):
        """현재 시각을 돌려준다."""
        return self.now


def make(timeout=90):
    """가짜 시계·종료 함수를 넣은 감시 스레드와 기록 목록을 만든다."""
    clock, exits, heartbeat = FakeClock(), [], {}
    watchdog = StallWatchdog(heartbeat, {"query_id": "q"}, timeout, clock=clock, exit_fn=exits.append)
    return watchdog, clock, exits, heartbeat


def test_timeout_이내면_종료하지_않는다():
    """시작 시각 기준 공백이 timeout 이하면 종료하지 않는다."""
    watchdog, clock, exits, _ = make()
    clock.now = 90
    assert watchdog.check() is False
    assert exits == []


def test_공백이_timeout을_넘으면_exit_1():
    """마지막 이벤트 뒤 timeout을 넘기면 exit_fn(1)."""
    watchdog, clock, exits, heartbeat = make()
    heartbeat["last_event_at"] = 10
    clock.now = 100.5
    assert watchdog.check() is True
    assert exits == [1]


def test_리스너_진행·유휴_이벤트가_기준_시각을_갱신한다(monkeypatch):
    """진행·유휴 이벤트가 오면 공백이 다시 0부터 센다."""
    clock = FakeClock()
    monkeypatch.setattr("streaming.monitoring.time.monotonic", clock)
    context = {}
    listener = ProgressLogger(context)
    exits = []
    watchdog = StallWatchdog(listener.heartbeat, {}, 90, clock=clock, exit_fn=exits.append)
    progress = MagicMock()
    progress.progress.json = "{}"
    for handler, event in ((listener.onQueryProgress, progress), (listener.onQueryIdle, None)):
        clock.now += 80
        handler(event)
        clock.now += 80
        assert watchdog.check() is False
    assert exits == [] and context == {}  # 감시 시각은 배치 지표로 퍼지지 않는다.


def test_stop하면_run이_끝난다():
    """정상 종료 경로에서 stop()으로 스레드가 끝난다."""
    watchdog, _, exits, _ = make()
    watchdog.interval = 0.01
    watchdog.start()
    watchdog.stop()
    watchdog.join(timeout=1)
    assert not watchdog.is_alive() and exits == []
