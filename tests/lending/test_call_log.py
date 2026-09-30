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
    db.execute(text("INSERT INTO lending.call_dispositions (aircall_call_id, phone, direction, call_ended_at, "
                    "recording_disclosure_logged, raw_event) VALUES (:c, '+18135558501', 'outbound', :t, :l, '{}')"),
               {"c": cid, "t": ended_at, "l": logged})
    return cid


def test_marking_a_call_sets_its_disclosure_flag(db):
    from src.lending.call_log import mark_disclosure_logged
    cid = _call(db, NOW)
    assert mark_disclosure_logged(db, cid) is True
    flag = db.execute(text("SELECT recording_disclosure_logged FROM lending.call_dispositions "
                           "WHERE aircall_call_id = :c"), {"c": cid}).scalar()
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
