"""R1 0.6: exhausted is permanent for calling lists until a new inbound. No database: the SQL runs against a scripted
fake session (the window count itself is Postgres; its shape is checked in the rolled-back run)."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from config.lending_compliance import MAX_ATTEMPTS_TOTAL
from src.lending import compliance
from src.lending import exhausted as ex

NOW = datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc)  # a Thursday
ANA, BEN = "+18135557001", "+18135557002"


class _Result:
    def __init__(self, rows=(), rowcount=0):
        self._rows, self.rowcount = list(rows), rowcount

    def fetchall(self):
        return self._rows


class _Db:
    def __init__(self, counts=None, marked=(), clear_rows=1, fail=False):
        self.counts, self.marked_rows, self.clear_rows, self.fail = counts or {}, set(marked), clear_rows, fail
        self.writes: list[tuple] = []

    @contextmanager
    def begin_nested(self):
        yield

    def execute(self, stmt, params=None):
        if self.fail:
            raise RuntimeError("database error with +18135557001 in it")
        if stmt is ex._WINDOW_COUNTS:
            return _Result([(p, n) for p, n in self.counts.items() if p in params["phones"]])
        if stmt is ex._MARKED:
            return _Result([(p,) for p in self.marked_rows if p in params["phones"]])
        if stmt is ex._OVER_CAP_NOW:
            return _Result([(p, n) for p, n in self.counts.items() if n >= params["cap"]])
        self.writes.append((stmt, params))
        return _Result(rowcount=self.clear_rows if stmt is ex._CLEAR else 1)


def test_business_days_skip_weekends():
    assert ex.business_days_ago(NOW, 10) == datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)


def test_a_phone_over_the_cap_in_the_window_is_exhausted():
    db = _Db(counts={ANA: MAX_ATTEMPTS_TOTAL, BEN: MAX_ATTEMPTS_TOTAL - 1})
    assert ex.exhausted_phones(db, [ANA, BEN], NOW) == {ANA}


def test_a_marked_phone_stays_exhausted_after_its_attempts_age_out():
    """Ana was called 6 times in October: the window is empty now, but the mark keeps her off every later list."""
    db = _Db(counts={}, marked={ANA})
    assert ex.exhausted_phones(db, [ANA, BEN], NOW) == {ANA}
    assert ex.is_exhausted(db, "(813) 555-7001", NOW)
    assert not ex.is_exhausted(db, BEN, NOW)


def test_the_list_gate_uses_the_shared_rule():
    db = _Db(marked={ANA})
    assert compliance._attempt_history_exhausted_phones(db, [ANA, BEN], NOW) == {ANA}


def test_marking_writes_one_upsert_per_phone():
    db = _Db()
    assert ex.mark(db, {ANA: 6, BEN: 7}, NOW) == 2
    assert [p["phone"] for s, p in db.writes if s is ex._MARK] == [ANA, BEN]


def test_the_after_call_hook_marks_and_never_raises():
    db = _Db(counts={ANA: 6})
    assert ex.mark_one(db, ANA, NOW)
    (stmt, params), = db.writes
    assert stmt is ex._MARK and params["attempts"] == 6
    assert ex.mark_one(_Db(fail=True), ANA, NOW) is False
    assert ex.mark_one(None, ANA, NOW) is False


def test_the_watch_marks_every_phone_over_the_cap():
    db = _Db(counts={ANA: 6, BEN: 2})
    assert ex.mark_over_cap(db, NOW) == 1
    assert [p["phone"] for s, p in db.writes if s is ex._MARK] == [ANA]


def test_a_new_inbound_clears_the_mark_with_its_reason():
    db = _Db()
    assert ex.clear_for_new_inbound(db, "813-555-7001", ex.CLEARED_BY_WEBSITE, NOW)
    (stmt, params), = db.writes
    assert stmt is ex._CLEAR and params == {"phone": ANA, "now": NOW, "reason": ex.CLEARED_BY_WEBSITE}


def test_an_inbound_reset_is_recorded_even_with_no_mark_yet():
    """Review F6: a phone over the cap the watch has not marked yet still keeps the inquiry's reset."""
    sql = " ".join(str(ex._CLEAR).split())
    assert sql.startswith("INSERT INTO lending.exhausted_phones") and "ON CONFLICT (phone) DO UPDATE" in sql
    assert "cleared_at < EXCLUDED.cleared_at" in sql  # a late older inbound never moves the reset backwards


def test_a_mark_is_written_only_when_calls_since_the_latest_reset_reach_the_cap():
    """Review F6: an inquiry between the watch's count and its write wins; the WHERE recounts on the locked row."""
    sql = " ".join(str(ex._MARK).split())
    assert "COALESCE(lending.exhausted_phones.cleared_at" in sql and ">= :cap" in sql
    db = _Db()
    ex.mark(db, {ANA: 6}, NOW)
    (_, params), = db.writes
    assert params["cap"] == ex.MAX_ATTEMPTS_TOTAL
    assert params["since"] == ex.business_days_ago(NOW, ex.ATTEMPT_HISTORY_BUSINESS_DAYS)


def test_clearing_never_raises_and_ignores_a_missing_phone():
    assert ex.clear_for_new_inbound(_Db(fail=True), ANA, ex.CLEARED_BY_LENDINGFLOW, NOW) is False
    assert ex.clear_for_new_inbound(_Db(), None, ex.CLEARED_BY_LENDINGFLOW, NOW) is False
    assert ex.clear_for_new_inbound(_Db(clear_rows=0), ANA, ex.CLEARED_BY_LENDINGFLOW, NOW) is False


def test_the_after_call_hook_writes_the_mark_when_the_history_cap_is_reached(monkeypatch):
    marked = []
    monkeypatch.setattr(compliance, "_attempt_cap", lambda db, phone, now: SimpleNamespace(allowed=True))
    monkeypatch.setattr(compliance, "_attempt_history_exhausted_phones", lambda db, phones, now: set(phones))
    monkeypatch.setattr(compliance, "_flag_nurture", lambda db, phones: None)
    monkeypatch.setattr(compliance, "_close_exhausted_load_row", lambda db, phone: None)
    monkeypatch.setattr(ex, "mark_one", lambda db, phone, now: marked.append(phone) or True)
    compliance.on_attempt_recorded(object(), ANA, now=NOW, dialer_remover=lambda p, *, reason: None)
    assert marked == [ANA]


def test_a_lendingflow_intake_lifts_the_mark_before_it_locks_the_contact(monkeypatch):
    """Review (round 2) F4: the after-call hook locks the exhausted row, then the contact. Intake must take them in the
    same order, or a deadlock can abort the reset and the new inquiry saves with the phone still blocked."""
    from src.lending import lendingflow

    order = []

    class Stop(Exception):
        pass

    def contact(db, phone, email):
        order.append("contact")
        raise Stop

    monkeypatch.setattr(lendingflow, "_record_duplicate", lambda *a, **k: None)
    monkeypatch.setattr(lendingflow, "_upsert_contact", contact)
    monkeypatch.setattr(ex, "clear_for_new_inbound", lambda *a, **k: order.append("clear"))
    parsed = SimpleNamespace(phone=ANA, email=None, vendor_lead_id="v1")
    with pytest.raises(Stop):
        lendingflow.save_lead(object(), parsed, {}, now=NOW)
    assert order == ["clear", "contact"]
