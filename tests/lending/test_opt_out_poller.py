"""PR 318 re-review #10: the GHL DND sync runs in its own transaction, after the poll commits. No database."""
from __future__ import annotations

from contextlib import contextmanager

import pytest

from src.lending import opt_out_poller as poller
from src.lending.compliance import PollResult


@pytest.fixture
def events(monkeypatch):
    log = []

    @contextmanager
    def fake_db():
        log.append("begin")
        yield object()
        log.append("commit")

    monkeypatch.setattr(poller, "get_db_context", fake_db)
    return log


def test_the_poll_commits_before_the_ghl_sync_starts(monkeypatch, events):
    monkeypatch.setattr(poller, "poll_fa_opt_outs", lambda db, sync_ghl: events.append(f"poll sync_ghl={sync_ghl}") or PollResult(1, 0))
    monkeypatch.setattr(poller, "sync_ghl_dnd", lambda db: events.append("ghl"))
    poller.run_once()
    assert events == ["begin", "poll sync_ghl=False", "commit", "begin", "ghl", "commit"]


def test_a_ghl_failure_does_not_fail_the_cycle_or_undo_the_polled_opt_outs(monkeypatch, events):
    monkeypatch.setattr(poller, "poll_fa_opt_outs", lambda db, sync_ghl: PollResult(2, 0))

    def boom(db):
        raise RuntimeError("ghl 500")

    monkeypatch.setattr(poller, "sync_ghl_dnd", boom)
    assert poller.run_once().new_opt_outs == 2
    assert events[:3] == ["begin", "commit", "begin"]        # the poll's transaction committed first


def test_no_ghl_sync_when_another_poller_holds_the_lock(monkeypatch, events):
    monkeypatch.setattr(poller, "poll_fa_opt_outs", lambda db, sync_ghl: PollResult(0, 0, skipped_locked=True))
    monkeypatch.setattr(poller, "sync_ghl_dnd", lambda db: events.append("ghl"))
    poller.run_once()
    assert "ghl" not in events
