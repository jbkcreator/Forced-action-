"""T-12: transcript ingest runs in its own worker, never inside the CDR poller tick."""
from __future__ import annotations

import inspect
from collections import Counter
from contextlib import contextmanager

import pytest

from config import lending_dispositions as cfg
from src.lending import cdr_poller, transcript_worker


def _settings(enabled: bool):
    return lambda: type("S", (), {"lending_transcript_ingest_enabled": enabled})()


def test_poller_tick_no_longer_runs_transcript_ingest():
    assert not hasattr(cdr_poller, "transcript_step")
    assert "transcript" not in inspect.getsource(cdr_poller.main).lower()


def test_worker_has_its_own_lock_key_and_5_second_interval():
    assert cfg.TRANSCRIPT_WORKER_LOCK_KEY != cfg.CDR_POLL_LOCK_KEY
    assert cfg.TRANSCRIPT_POLL_SECONDS == 5


def test_cycle_idles_while_the_switch_is_off(monkeypatch):
    monkeypatch.setattr(transcript_worker, "get_settings", _settings(False))
    monkeypatch.setattr(transcript_worker, "get_dialer", lambda: pytest.fail("dialer must not be built"))
    assert transcript_worker.run_cycle() == Counter()


def test_cycle_runs_one_batch_when_on(monkeypatch):
    calls = {}
    monkeypatch.setattr(transcript_worker, "get_settings", _settings(True))
    monkeypatch.setattr(transcript_worker, "get_dialer", lambda: "dialer")

    class Db:
        def commit(self):
            calls["commit"] = True

    @contextmanager
    def session():
        yield Db()

    def ingest(db, dialer, *, now, limit):
        calls.update(dialer=dialer, limit=limit)
        return Counter({cfg.TRANSCRIPT_DONE: 1})

    monkeypatch.setattr(transcript_worker, "lending_session", session)
    monkeypatch.setattr(transcript_worker, "run_transcript_ingest", ingest)
    assert transcript_worker.run_cycle() == Counter({cfg.TRANSCRIPT_DONE: 1})
    assert calls == {"dialer": "dialer", "limit": cfg.TRANSCRIPT_BATCH_SIZE, "commit": True}


def test_cycle_without_a_dialer_does_nothing(monkeypatch):
    monkeypatch.setattr(transcript_worker, "get_settings", _settings(True))
    monkeypatch.setattr(transcript_worker, "get_dialer", lambda: None)
    assert transcript_worker.run_cycle() == Counter()


def test_loop_survives_a_failing_cycle(monkeypatch):
    ticks = []

    def boom():
        ticks.append(1)
        raise RuntimeError("x")

    def sleep(_s):
        if len(ticks) >= 2:
            transcript_worker._stop(None, None)

    monkeypatch.setattr(transcript_worker, "run_cycle", boom)
    monkeypatch.setattr(transcript_worker, "_acquire_lock", lambda: True)
    monkeypatch.setattr(transcript_worker.time, "sleep", sleep)
    monkeypatch.setattr(transcript_worker, "_running", True)
    assert transcript_worker.main([]) == 0
    assert len(ticks) == 2


def test_once_runs_a_single_cycle(monkeypatch):
    ticks = []
    monkeypatch.setattr(transcript_worker, "run_cycle", lambda: ticks.append(1) or Counter())
    monkeypatch.setattr(transcript_worker, "_acquire_lock", lambda: True)
    monkeypatch.setattr(transcript_worker, "_running", True)
    assert transcript_worker.main(["--once"]) == 0
    assert ticks == [1]


def test_second_instance_exits_when_the_lock_is_held(monkeypatch):
    monkeypatch.setattr(transcript_worker, "_acquire_lock", lambda: False)
    monkeypatch.setattr(transcript_worker, "run_cycle", lambda: pytest.fail("must not run"))
    assert transcript_worker.main(["--once"]) == 3
