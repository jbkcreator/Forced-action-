"""GHL call outcomes against Postgres (throwaway TEST_DATABASE_URL only): retry de-dupe, the write guard, and the
pending read skipping outcomes that have no call yet."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from migrations.apply_lending_call_gate_failed import apply_to as apply_gate_failed
from migrations.apply_lending_ghl_disposition_events import apply_to as apply_events
from src.lending import ghl_disposition_match as m


@pytest.fixture
def db(lending_db):
    conn = lending_db.connection()
    apply_gate_failed(conn)
    apply_events(conn)
    return lending_db


def _phone():
    return f"+1813{uuid.uuid4().int % 10_000_000:07d}"


def _call(db, phone, started, *, disposition=None, call_id=None):
    return db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, talk_duration_sec, "
        "call_started_at, call_ended_at, disposition_at, disposition, raw_event) "
        "VALUES (:id, 'ghl', 'outbound', :p, 60, :s, :s, :s, :d, '{}') RETURNING id"),
        {"id": call_id or f"ghl:t-{uuid.uuid4().hex}", "p": phone, "s": started, "d": disposition}).scalar()


def _disposition(db, call_id):
    return db.execute(text("SELECT disposition FROM lending.call_dispositions WHERE id = :id"), {"id": call_id}).scalar()


def _event(phone, label="Requested Appointment", **extra):
    return m.parse_event({"disposition": label, "phone": phone, "user": "Caller A", **extra})


def test_a_retried_delivery_is_stored_once(db):
    phone = _phone()
    first = m.record_event(db, _event(phone), {})
    again = m.record_event(db, _event(phone), {})
    assert first[1] is False and again == (first[0], True)
    assert db.execute(text("SELECT count(*) FROM lending.ghl_disposition_events WHERE phone = :p"),
                      {"p": phone}).scalar() == 1


def test_a_retry_does_not_land_on_a_second_call(db):
    now = datetime.now(timezone.utc)
    phone = _phone()
    older, newer = _call(db, phone, now - timedelta(minutes=10)), _call(db, phone, now - timedelta(minutes=2))
    m.record_event(db, _event(phone), {})
    assert m.attach_pending(db, now) == [newer]
    m.record_event(db, _event(phone), {})  # GHL retries the same webhook
    assert m.attach_pending(db, now) == []
    assert _disposition(db, newer) == "BOOKED" and _disposition(db, older) is None


def test_the_write_never_replaces_another_outcome_or_a_dnc(db):
    now = datetime.now(timezone.utc)
    phone = _phone()
    taken = _call(db, phone, now, disposition="CALLBACK_REQUESTED")
    dnc = _call(db, phone, now, disposition="DNC_REQUEST", call_id="ghl:dnc-1")
    base = {"v": "x", "cause": None, "gate": None, "caller": None, "unknown_caller": "GHL (unknown)", "dnc": "DNC_REQUEST"}
    db.execute(m._SET_CALL, [{**base, "d": "BOOKED", "id": taken, "exact": None},
                             {**base, "d": "BOOKED", "id": dnc, "exact": "ghl:dnc-1"}])
    assert _disposition(db, taken) == "CALLBACK_REQUESTED" and _disposition(db, dnc) == "DNC_REQUEST"


def test_outcomes_with_no_call_do_not_hold_back_newer_ones(db, monkeypatch):
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(m, "GHL_DISPOSITION_MATCH_BATCH", 2)
    for _ in range(3):  # older outcomes whose calls never arrive
        m.record_event(db, _event(_phone()), {})
    phone = _phone()
    call = _call(db, phone, now - timedelta(minutes=1))
    m.record_event(db, _event(phone, label="Follow Up"), {})
    assert m.attach_pending(db, now) == [call]
    assert _disposition(db, call) == "CALLBACK_REQUESTED"
