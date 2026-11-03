"""PR #400 review fixes: batched matcher writes, locked + guarded call updates, retry de-dupe, no starvation by
unmatchable events, no exception text in the webhook body, gate flag split from consent capture."""
from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone

import pytest

from src.lending import call_pipeline as cp
from src.lending import ghl_disposition_match as m

NOW = datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc)


class _Result:
    def __init__(self, rows=(), scalar=None):
        self.rows, self._scalar = list(rows), scalar

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def scalar(self):
        return self._scalar

    def scalar_one(self):
        return self._scalar

    def first(self):
        return self.rows[0] if self.rows else None


class _Nested:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _MatchDb:
    def __init__(self, events, calls):
        self.events, self.calls, self.executed = events, calls, []

    def begin_nested(self):
        return _Nested()

    def execute(self, sql, params=None):
        s = " ".join(str(sql).split())
        self.executed.append((s, params))
        if s.startswith("UPDATE"):
            return _Result()
        if "FROM lending.ghl_disposition_events" in s:
            return _Result(self.events)
        return _Result(self.calls)


def _ev(i, **kw):
    return {"id": i, "phone": f"+1813555014{i}", "code": "BOOKED", "ghl_call_id": None, "called_at": None,
            "received_at": NOW, "caller_name": None, "gate_failed": None, **kw}


def _call(i, phone, **kw):
    return {"id": i, "phone": phone, "dialer_call_id": f"ghl:m{i}", "call_started_at": NOW - timedelta(minutes=2),
            "disposition": None, "unfunded_cause": None, **kw}


# ---- 1. one statement per table, whatever the batch size -----------------------------------------------------------

def test_matched_outcomes_are_written_in_one_statement_per_table():
    db = _MatchDb([_ev(1), _ev(2)], [_call(7, "+18135550141"), _call(8, "+18135550142")])
    assert m.attach_pending(db, NOW) == [7, 8]
    updates = [(s, p) for s, p in db.executed if s.startswith("UPDATE")]
    assert len(updates) == 2
    (call_sql, call_params), (event_sql, event_params) = updates
    assert "lending.call_dispositions" in call_sql and [p["id"] for p in call_params] == [7, 8]
    assert "lending.ghl_disposition_events" in event_sql and [p["id"] for p in event_params] == [1, 2]


# ---- 2. candidate calls are locked, and the write re-checks the call ------------------------------------------------

def test_candidate_calls_are_locked_in_a_fixed_order():
    db = _MatchDb([_ev(1)], [_call(7, "+18135550141")])
    m.attach_pending(db, NOW)
    (candidates,) = [s for s, _ in db.executed if s.startswith("SELECT") and "FROM lending.call_dispositions" in s
                     and "ghl_disposition_events" not in s]
    assert "ORDER BY id" in candidates and candidates.endswith("FOR UPDATE")


def test_the_call_write_never_overwrites_another_outcome_or_a_dnc():
    sql = " ".join(str(m._SET_CALL).split())
    assert "(disposition IS NULL OR dialer_call_id = :exact)" in sql
    assert "(disposition IS DISTINCT FROM :dnc OR :d = :dnc)" in sql


def test_exact_is_set_only_for_an_event_that_names_its_call():
    db = _MatchDb([_ev(1), _ev(2, ghl_call_id="msg-9")],
                  [_call(7, "+18135550141"), _call(8, "+18135550142", dialer_call_id="ghl:msg-9")])
    m.attach_pending(db, NOW)
    call_params = next(p for s, p in db.executed if s.startswith("UPDATE lending.call_dispositions"))
    assert [p["exact"] for p in call_params] == [None, "ghl:msg-9"]
    assert all(p["dnc"] == "DNC_REQUEST" for p in call_params)


# ---- 3. a webhook retry is stored once --------------------------------------------------------------------------------

class _RecordDb:
    def __init__(self, existing=None):
        self.existing, self.executed = existing, []

    def execute(self, sql, params=None):
        s = " ".join(str(sql).split())
        self.executed.append((s, params))
        if "pg_advisory_xact_lock" in s:
            return _Result()
        if s.startswith("SELECT id FROM lending.ghl_disposition_events"):
            return _Result(scalar=self.existing)
        return _Result(scalar=99)


def _parsed(**extra):
    return m.parse_event({"disposition": "Requested Appointment", "phone": "8135550142", "user": "Josh Kantor", **extra})


CALLED = "2026-11-03T14:05:00Z"


def test_a_first_delivery_is_inserted():
    db = _RecordDb()
    assert m.record_event(db, _parsed(), {}) == (99, False)
    assert any(s.startswith("INSERT INTO lending.ghl_disposition_events") for s, _ in db.executed)


def test_a_repeated_delivery_returns_the_stored_event_without_inserting():
    db = _RecordDb(existing=42)
    assert m.record_event(db, _parsed(called_at=CALLED), {}) == (42, True)
    assert not any(s.startswith("INSERT") for s, _ in db.executed)
    lock, find = db.executed[0], db.executed[1]
    assert "pg_advisory_xact_lock" in lock[0]  # serialises concurrent retries of the same delivery
    assert "IS NOT DISTINCT FROM" in find[0] and find[1]["minutes"] == m.GHL_DISPOSITION_DEDUPE_MINUTES


# ---- 4. unmatchable events do not hold the batch ---------------------------------------------------------------------

def test_only_events_that_have_a_matchable_call_are_read():
    sql = " ".join(str(m._PENDING).split())
    assert "EXISTS (SELECT 1 FROM lending.call_dispositions" in sql
    assert sql.endswith("FOR UPDATE OF e SKIP LOCKED")


# ---- 5. no exception text in the 422 body ----------------------------------------------------------------------------

def test_invalid_event_carries_a_fixed_reason():
    with pytest.raises(m.InvalidDispositionEvent) as info:
        m.parse_event({"phone": "8135550142"})
    assert info.value.reason in m.INVALID_EVENT_REASONS


def test_the_webhook_422_detail_is_the_fixed_reason(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from pydantic import SecretStr

    from config.settings import get_settings
    from src.api import lending_ghl_router as r
    from src.api.deps import get_db

    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr("s"), raising=False)

    def parse(body):
        raise m.InvalidDispositionEvent("internal detail that must not leak", reason="disposition is required")

    monkeypatch.setattr(r, "parse_disposition_event", parse)
    app = FastAPI()
    app.include_router(r.router)
    app.dependency_overrides[get_db] = lambda: object()
    resp = TestClient(app).post("/webhooks/lending/ghl-disposition", json={}, headers={"X-Webhook-Secret": "s"})
    assert resp.status_code == 422 and resp.json() == {"detail": "disposition is required"}


# ---- 6. gate flag is its own step, sharing one contact-field read -------------------------------------------------

class _PipeDb:
    def __init__(self, row):
        self.row, self.sql = row, []

    def execute(self, sql, params=None):
        s = " ".join(str(sql).split())
        self.sql.append((s, params))
        return _Result(rows=[self.row] if s.startswith("SELECT phone") else [])


class _Dialer:
    def __init__(self, fields):
        self.fields, self.reads = fields, 0

    def get_contact_customfields(self, contact_id, quick=False):
        self.reads += 1
        return self.fields


def _bd_event():
    from src.lending.dispositions import DialerCallEvent
    fields = {f.name: None for f in dataclasses.fields(DialerCallEvent)}
    return DialerCallEvent(**{**fields, "call_id": "bd-1", "contact_id": "k-1", "direction": "outbound", "duration": 60})


def test_capture_consent_no_longer_writes_the_gate_flag():
    db = _PipeDb(None)
    assert cp.capture_consent(db, _bd_event(), "+18135550142", "x", None, fields={"gate_failed": "yes"})
    assert not [s for s, _ in db.sql if "gate_failed" in s]


def test_capture_gate_flag_sets_the_column_only_when_ticked():
    db = _PipeDb(None)
    cp.capture_gate_flag(db, 5, {"gate_failed": "Yes"})
    cp.capture_gate_flag(db, 6, {"gate_failed": "no"})
    cp.capture_gate_flag(db, 7, None)
    assert [p for s, p in db.sql if "SET gate_failed = true" in s] == [{"id": 5}]


def test_the_check_reads_the_contact_fields_once_for_consent_and_gate():
    row = ("+18135550142", "x", "outbound", "k-1", 60)
    db, dialer = _PipeDb(row), _Dialer({"text_consent": "yes", "gate_failed": "yes"})
    recorded = []
    import src.lending.call_pipeline as mod
    orig = mod.record_consent
    mod.record_consent = lambda *a, **k: recorded.append(a[2])
    try:
        cp._check_consent(db, _bd_event(), 5, dialer)
    finally:
        mod.record_consent = orig
    assert dialer.reads == 1 and recorded == ["on_call_yes"]
    assert [p for s, p in db.sql if "SET gate_failed = true" in s] == [{"id": 5}]
    assert any("consent_checked_at = clock_timestamp()" in s for s, _ in db.sql)


def test_a_failed_field_read_stamps_nothing():
    class _Broken(_Dialer):
        def get_contact_customfields(self, contact_id, quick=False):
            raise RuntimeError("down")

    db = _PipeDb(("+18135550142", "x", "outbound", "k-1", 60))
    cp._check_consent(db, _bd_event(), 5, _Broken({}))
    assert not any("consent_checked_at" in s and s.startswith("UPDATE") for s, _ in db.sql)


def test_without_a_call_identity_every_delivery_is_stored():
    """No call id and no call time: a retry and a second real call look the same, so nothing is discarded."""
    db = _RecordDb(existing=42)
    assert m.record_event(db, _parsed(), {}) == (99, False)
    assert not any("pg_advisory_xact_lock" in s or s.startswith("SELECT id FROM") for s, _ in db.executed)
    assert any(s.startswith("INSERT INTO lending.ghl_disposition_events") for s, _ in db.executed)
