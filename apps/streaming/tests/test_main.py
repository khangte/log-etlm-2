"""쿼리 시작·실행 실패 시에도 Driver 자원을 정리하고 원래 오류를 전파하는지 검증한다."""

from unittest.mock import MagicMock

import pytest

from streaming import main


@pytest.mark.parametrize("failure", [None, "source", "start", "await"])
def test_실행_종료와_실패_시_자원_정리(monkeypatch, failure):
    """정상 종료와 입력·쿼리 시작·실행 실패 모두 리스너·세션을 정리한다."""
    spark, source, writer, query = (MagicMock() for _ in range(4))
    source.writeStream = writer
    for method in ("queryName", "outputMode", "foreachBatch", "option", "trigger"):
        getattr(writer, method).return_value = writer
    writer.start.return_value = query
    query.isActive = True
    read_kafka = MagicMock(return_value=source)
    if failure == "source":
        read_kafka.side_effect = RuntimeError("source failed")
    elif failure == "start":
        writer.start.side_effect = RuntimeError("start failed")
    elif failure == "await":
        query.awaitTermination.side_effect = RuntimeError("await failed")
    monkeypatch.setattr(main, "load_config", MagicMock())
    monkeypatch.setattr(main, "build_spark", lambda config: spark)
    monkeypatch.setattr(main, "read_kafka", read_kafka)
    if failure:
        with pytest.raises(RuntimeError, match=f"{failure} failed"):
            main.main()
    else:
        main.main()
    listener = spark.streams.addListener.call_args.args[0]
    spark.streams.removeListener.assert_called_once_with(listener)
    spark.stop.assert_called_once_with()
    if failure not in ("source", "start"):
        query.stop.assert_called_once_with()
    else:
        query.stop.assert_not_called()
