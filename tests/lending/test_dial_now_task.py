"""Ops check (D1/D7): which launch queue the caller should be on now, and its line setting."""
from __future__ import annotations

from contextlib import contextmanager

from config import lending_queues as q
from src.tasks import lending_dial_now as task


class FakeDb:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *_a, **_kw):
        rows = self.rows

        class R:
            def all(self):
                return rows
        return R()


def _patch(monkeypatch, rows):
    @contextmanager
    def fake_db():
        yield FakeDb(rows)
    monkeypatch.setattr(task, "get_db_context", fake_db)


def test_points_at_partners_while_any_remain(monkeypatch, caplog):
    _patch(monkeypatch, [(q.PARTNERS, 4), (q.VERIFIED_MATURITY, 500)])
    caplog.set_level("INFO")
    assert task.main([]) == 0
    assert task.dial_now_summary({q.PARTNERS: 4, q.VERIFIED_MATURITY: 500}) == {
        "queue": q.PARTNERS, "max_lines": 3,
        "active_by_queue": {q.PARTNERS: 4, q.TRANSACTION_READY: 0, q.BUILDERS: 0, q.VERIFIED_MATURITY: 500},
    }


def test_maturity_only_when_everything_above_is_empty_and_at_five_lines():
    assert task.dial_now_summary({q.VERIFIED_MATURITY: 525})["queue"] == q.VERIFIED_MATURITY
    assert task.dial_now_summary({q.VERIFIED_MATURITY: 525})["max_lines"] == 5


def test_nothing_loaded_returns_no_queue_and_exit_code_signals_it(monkeypatch):
    _patch(monkeypatch, [])
    assert task.dial_now_summary({})["queue"] is None
    assert task.main([]) == task.EXIT_NOTHING_TO_DIAL
