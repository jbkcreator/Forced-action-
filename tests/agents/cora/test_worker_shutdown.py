from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

from src.agents.cora import worker
from tests.agents.cora.helpers import new_consumer_name


def test_request_stop_flips_flag():
    w = worker.Worker(consumer_name=new_consumer_name())
    assert w._stop is False
    w.request_stop()
    assert w._stop is True


def test_run_forever_exits_immediately_when_already_stopped():
    w = worker.Worker(consumer_name=new_consumer_name())
    w.request_stop()
    start = time.monotonic()
    w.run_forever(block_ms=100)
    assert time.monotonic() - start < 1.0


def test_run_forever_stops_promptly_on_signal_from_another_thread(monkeypatch):
    monkeypatch.setattr(worker, "run_cora_main", MagicMock(return_value={"terminal_status": "completed"}))
    w = worker.Worker(consumer_name=new_consumer_name())
    thread = threading.Thread(target=w.run_forever, kwargs={"block_ms": 100})
    thread.start()
    time.sleep(0.05)
    w.request_stop()
    thread.join(timeout=3)
    assert not thread.is_alive()


def test_main_loop_survives_a_transient_exception(monkeypatch):
    """A Redis/handler blip inside one iteration must not crash the whole worker process."""
    from src.agents.cora import queue

    call_count = {"n": 0}

    def flaky_read_batch(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("simulated transient Redis hiccup")
        return []

    monkeypatch.setattr(queue, "read_batch", flaky_read_batch)
    w = worker.Worker(consumer_name=new_consumer_name())
    thread = threading.Thread(target=w.run_forever, kwargs={"block_ms": 50})
    thread.start()
    time.sleep(0.2)
    w.request_stop()
    thread.join(timeout=3)
    assert not thread.is_alive()
    assert call_count["n"] >= 2  # the loop kept going after the first iteration raised
