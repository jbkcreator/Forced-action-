"""PR 318 re-review #6: contacts pushed before a mid-load abort are recorded. No database."""
from __future__ import annotations

from src.lending import dialer_load


def test_pushed_contacts_are_stored_and_committed_on_abort(monkeypatch):
    stored, commits = [], []
    monkeypatch.setattr(dialer_load, "_store_chunk", lambda db, run_id, chunk, active: stored.append(list(chunk)))
    dialer_load._record_pushed_before_abort(None, "run-1", [("item", 7)], {}, lambda: commits.append(1))
    assert stored == [[("item", 7)]] and commits == [1]


def test_nothing_is_stored_when_no_contact_was_pushed_since_the_last_commit(monkeypatch):
    monkeypatch.setattr(dialer_load, "_store_chunk", lambda *a: (_ for _ in ()).throw(AssertionError))
    dialer_load._record_pushed_before_abort(None, "run-1", [], {}, lambda: None)


def test_a_failure_while_recording_does_not_mask_the_original_error(monkeypatch, caplog):
    def boom(*a):
        raise RuntimeError("db down")

    monkeypatch.setattr(dialer_load, "_store_chunk", boom)
    with caplog.at_level("ERROR"):
        dialer_load._record_pushed_before_abort(None, "run-1", [("item", 7)], {}, lambda: None)
    assert "not recorded" in caplog.text
