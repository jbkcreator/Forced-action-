"""Go Live G12: every call row carries the recording-disclosure flag; gaps are listable."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

NOW = datetime(2026, 10, 1, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _call(db, ended_at, logged=False):
    cid = f"test-{uuid.uuid4().hex}"
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, call_ended_at, "
                    "recording_disclosure_logged, raw_event) VALUES (:c, '+18135558501', 'outbound', :t, :l, '{}')"),
               {"c": cid, "t": ended_at, "l": logged})
    return cid


def test_marking_a_call_sets_its_disclosure_flag(db):
    from src.lending.call_log import mark_disclosure_logged
    cid = _call(db, NOW)
    assert mark_disclosure_logged(db, cid) is True
    flag = db.execute(text("SELECT recording_disclosure_logged FROM lending.call_dispositions "
                           "WHERE dialer_call_id = :c"), {"c": cid}).scalar()
    assert flag is True


def test_marking_an_unknown_call_reports_false(db):
    from src.lending.call_log import mark_disclosure_logged
    assert mark_disclosure_logged(db, "no-such-call") is False


def test_calls_missing_the_flag_are_listed_within_the_window(db):
    from src.lending.call_log import calls_missing_disclosure
    old = _call(db, NOW - timedelta(days=3))
    missing = _call(db, NOW - timedelta(hours=2))
    _call(db, NOW - timedelta(hours=1), logged=True)
    found = calls_missing_disclosure(db, since=NOW - timedelta(days=1))
    assert missing in found and old not in found


# ── Attempt rows from BatchDialer call records (so the cap works without pushed events) ──

def _record(call_id, phone="(813) 555-8502", direction="outbound", status="no-answer", ended="2026-10-01T14:00:00Z"):
    return {"id": call_id, "phoneNumber": phone, "direction": direction, "status": status, "endedAt": ended}


def test_call_records_become_attempt_rows_once(db):
    from src.lending.call_log import record_call_attempts
    rows = [_record(f"cdr-{uuid.uuid4().hex[:6]}"), _record(f"cdr-{uuid.uuid4().hex[:6]}", status="answered")]
    assert record_call_attempts(db, rows) == ["+18135558502", "+18135558502"]
    assert record_call_attempts(db, rows) == []           # replay inserts nothing
    n = db.execute(text("SELECT count(*) FROM lending.call_dispositions WHERE phone = '+18135558502' "
                        "AND direction = 'outbound' AND call_ended_at IS NOT NULL")).scalar()
    assert n == 2


def test_records_without_an_id_phone_or_end_time_are_ignored(db):
    from src.lending.call_log import record_call_attempts
    assert record_call_attempts(db, [_record(None), _record("x-1", phone=""), _record("x-2", ended=None)]) == []


def test_an_existing_row_written_by_the_webhook_is_left_as_is(db):
    from src.lending.call_log import record_call_attempts
    cid = f"cdr-{uuid.uuid4().hex[:6]}"
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, caller_seat, raw_event) "
                    "VALUES (:c, '+18135558502', 'outbound', 'seat-a1', '{}')"), {"c": cid})
    assert record_call_attempts(db, [_record(cid)]) == []
    assert db.execute(text("SELECT caller_seat FROM lending.call_dispositions WHERE dialer_call_id = :c"),
                      {"c": cid}).scalar() == "seat-a1"
