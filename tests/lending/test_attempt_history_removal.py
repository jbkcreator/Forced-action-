"""PR 318 re-review #4: the 6-attempt removal closes the load row only if the dialer confirmed it.
No database: the DB-touching helpers are patched out."""
from __future__ import annotations

from datetime import datetime, timezone

NOON_LOCAL = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)


def _history_cap_hook(monkeypatch, remover):
    """Drives on_attempt_recorded to the total-history branch without a database."""
    from types import SimpleNamespace
    from src.lending import compliance as c

    closed = []
    monkeypatch.setattr(c, "_attempt_cap", lambda db, phone, now: SimpleNamespace(allowed=True))
    monkeypatch.setattr(c, "_attempt_history_exhausted_phones", lambda db, phones, now: set(phones))
    monkeypatch.setattr(c, "_flag_nurture", lambda db, phones: None)
    monkeypatch.setattr(c, "_close_exhausted_load_row", lambda db, phone: closed.append(phone))
    result = c.on_attempt_recorded(None, "+18135557001", now=NOON_LOCAL, dialer_remover=remover)
    return result, closed


def test_history_cap_closes_the_load_row_when_the_dialer_removal_succeeds(monkeypatch):
    result, closed = _history_cap_hook(monkeypatch, lambda phone, *, reason: None)
    assert result.allowed is False and closed == ["+18135557001"]


def test_history_cap_leaves_the_load_row_active_when_the_dialer_removal_fails(monkeypatch):
    def failing(phone, *, reason):
        raise RuntimeError("batchdialer 500")

    result, closed = _history_cap_hook(monkeypatch, failing)
    assert result.allowed is False       # still blocked at dial time
    assert closed == []                   # row stays active so the sweep/weekly job can retry


def _hook(monkeypatch, *, cap_allowed, exhausted):
    """on_attempt_recorded with its DB helpers patched; returns (result, nurture, closed, holds)."""
    from types import SimpleNamespace
    from src.lending import compliance as c

    nurture, closed, holds = [], [], []
    blocked = SimpleNamespace(allowed=cap_allowed, reason="ATTEMPT_CAP_REACHED")
    monkeypatch.setattr(c, "_attempt_cap", lambda db, phone, now: blocked)
    monkeypatch.setattr(c, "_attempt_history_exhausted_phones", lambda db, phones, now: set(phones) if exhausted else set())
    monkeypatch.setattr(c, "_flag_nurture", lambda db, phones: nurture.extend(phones))
    monkeypatch.setattr(c, "_close_exhausted_load_row", lambda db, phone: closed.append(phone))
    monkeypatch.setattr(c, "_open_holds", lambda db, reasons: holds.append(dict(reasons)))
    result = c.on_attempt_recorded(None, "+18135557001", now=NOON_LOCAL, dialer_remover=lambda p, *, reason: None)
    return result, nurture, closed, holds


def test_the_6th_attempt_that_is_also_the_3rd_in_24h_still_flags_nurture_and_closes_the_row(monkeypatch):
    """Finding 7: the 24h-cap branch used to return first, so the nurture flag was skipped."""
    from config.lending_compliance import ReasonCode
    result, nurture, closed, holds = _hook(monkeypatch, cap_allowed=False, exhausted=True)
    assert result.reason == ReasonCode.ATTEMPT_HISTORY_EXCEEDED
    assert nurture == ["+18135557001"] and closed == ["+18135557001"] and holds == []


def test_the_24h_cap_alone_still_opens_a_temporary_hold(monkeypatch):
    result, nurture, closed, holds = _hook(monkeypatch, cap_allowed=False, exhausted=False)
    assert result.allowed is False and nurture == [] and closed == []
    assert len(holds) == 1
