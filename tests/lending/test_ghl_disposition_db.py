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
    return f"+181372{uuid.uuid4().int % 100_000:05d}"  # exchange 72x: always a valid number


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
    phone, called = _phone(), datetime.now(timezone.utc).isoformat()
    first = m.record_event(db, _event(phone, called_at=called), {})
    again = m.record_event(db, _event(phone, called_at=called), {})
    assert first[1] is False and again == (first[0], True)
    assert db.execute(text("SELECT count(*) FROM lending.ghl_disposition_events WHERE phone = :p"),
                      {"p": phone}).scalar() == 1


def test_a_retry_does_not_land_on_a_second_call(db):
    now = datetime.now(timezone.utc)
    phone = _phone()
    older, newer = _call(db, phone, now - timedelta(minutes=10)), _call(db, phone, now - timedelta(minutes=2))
    called = (now - timedelta(minutes=2)).isoformat()
    m.record_event(db, _event(phone, called_at=called), {})
    assert m.attach_pending(db, now) == [newer]
    m.record_event(db, _event(phone, called_at=called), {})  # GHL retries the same webhook
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


def _markers(db, call_id):
    return tuple(db.execute(text("SELECT caller_name, sheet_synced_disposition, slack_posted_disposition "
                                 "FROM lending.call_dispositions WHERE id = :id"), {"id": call_id}).one())


def test_two_separate_calls_with_the_same_outcome_and_no_call_identity_both_keep_it(db):
    now = datetime.now(timezone.utc)
    phone = _phone()
    first, second = _call(db, phone, now - timedelta(minutes=8)), _call(db, phone, now - timedelta(minutes=3))
    m.record_event(db, _event(phone), {})
    assert m.attach_pending(db, now) == [second]
    assert m.record_event(db, _event(phone), {})[1] is False  # a second real outcome, not a retry
    assert m.attach_pending(db, now) == [first]
    assert _disposition(db, first) == _disposition(db, second) == "BOOKED"
    assert db.execute(text("SELECT count(*) FROM lending.ghl_disposition_events WHERE phone = :p"),
                      {"p": phone}).scalar() == 2


def test_a_caller_named_by_a_later_outcome_is_redelivered(db):
    now = datetime.now(timezone.utc)
    phone = _phone()
    call = _call(db, phone, now, disposition="BOOKED", call_id="ghl:named-1")
    db.execute(text("UPDATE lending.call_dispositions SET caller_name = 'GHL (unknown)', "
                    "sheet_synced_disposition = 'BOOKED', slack_posted_disposition = 'BOOKED' WHERE id = :id"),
               {"id": call})
    m.record_event(db, m.parse_event({"disposition": "Requested Appointment", "phone": phone, "user": "Josh Kantor",
                                      "call_id": "named-1"}), {})
    assert m.attach_pending(db, now) == [call]
    assert _markers(db, call) == ("Josh Kantor", None, None)  # Sheet + Slack are behind again: redelivered


def _import_row(call_id, phone, now, caller):
    return {"call_id": call_id, "source": "ghl", "direction": "outbound", "phone": phone, "seat": "ghl",
            "caller_name": caller, "tag": "ghl", "raw_outcome": "ghl:completed", "talk": 60, "started": now,
            "ended": now, "raw_event": "{}", "unknown_outcome": "ghl:unknown", "unknown_caller": "GHL (unknown)"}


def test_a_reimport_names_an_unknown_caller_and_redelivers_it(db):
    from src.lending import ghl_call_import as imp

    now = datetime.now(timezone.utc)
    phone, call_id = _phone(), f"ghl:re-{uuid.uuid4().hex}"
    db.execute(imp._UPSERT, _import_row(call_id, phone, now, "GHL (unknown)"))
    db.execute(text("UPDATE lending.call_dispositions SET disposition = 'BOOKED', sheet_synced_disposition = 'BOOKED', "
                    "slack_posted_disposition = 'BOOKED' WHERE dialer_call_id = :c"), {"c": call_id})
    assert db.execute(imp._UPSERT, _import_row(call_id, phone, now, "Josh Kantor")).first() is not None
    row_id = db.execute(text("SELECT id FROM lending.call_dispositions WHERE dialer_call_id = :c"),
                        {"c": call_id}).scalar()
    assert _markers(db, row_id) == ("Josh Kantor", None, None)
    # a known name is never replaced, and an unchanged call is not touched
    assert db.execute(imp._UPSERT, _import_row(call_id, phone, now, "David Willis")).first() is None
    assert _markers(db, row_id)[0] == "Josh Kantor"
