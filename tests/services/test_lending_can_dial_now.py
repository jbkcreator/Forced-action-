"""WP-W0-3 can-dial-now: calling window + 3 attempts / rolling 24h (spec §3.1)."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.lending_compliance import ReasonCode

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

PHONE = "+18135558001"
# 2026-09-29 is EDT (UTC-4): 12:00Z == 08:00 local.
NOON_LOCAL = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    session = Session(bind=conn)
    # Dev 4 owns call_dispositions; contract = one row per attempt with phone + call_ended_at.
    session.execute(text(
        "CREATE TABLE IF NOT EXISTS lending.call_dispositions "
        "(phone varchar(20) NOT NULL, call_ended_at timestamptz NOT NULL, disposition varchar(30))"
    ))
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _check(db, now):
    from src.services.lending_compliance import can_dial_now
    return can_dial_now(PHONE, db, now=now)


def _attempt(db, ended_at, disposition=None):
    db.execute(
        text("INSERT INTO lending.call_dispositions (phone, call_ended_at, disposition) VALUES (:p, :t, :d)"),
        {"p": PHONE, "t": ended_at, "d": disposition},
    )


@pytest.mark.parametrize("utc_hour,utc_min,allowed", [
    (11, 59, False),   # 07:59 local
    (12, 0, True),     # 08:00 local
    (23, 59, True),    # 19:59 local
    (0, 0, False),     # 20:00 local (next UTC day handled below)
])
def test_calling_window_edges(db, utc_hour, utc_min, allowed):
    day = 30 if (utc_hour, utc_min) == (0, 0) else 29
    result = _check(db, datetime(2026, 9, day, utc_hour, utc_min, tzinfo=timezone.utc))
    assert result.allowed is allowed
    if not allowed:
        assert result.reason == ReasonCode.OUTSIDE_CALL_WINDOW


def test_third_attempt_allowed_fourth_blocked_even_without_disposition(db):
    _attempt(db, NOON_LOCAL - timedelta(hours=3), disposition=None)   # no-answer still counts
    _attempt(db, NOON_LOCAL - timedelta(hours=2), disposition="LEFT_VOICEMAIL")
    assert _check(db, NOON_LOCAL).allowed          # 2 attempts so far -> third is fine

    _attempt(db, NOON_LOCAL - timedelta(hours=1), disposition=None)
    blocked = _check(db, NOON_LOCAL)                # 3 done -> fourth blocked
    assert blocked.reason == ReasonCode.ATTEMPT_CAP_REACHED


def test_block_ends_once_oldest_attempt_is_older_than_24h(db):
    _attempt(db, NOON_LOCAL - timedelta(hours=25), None)
    _attempt(db, NOON_LOCAL - timedelta(hours=2), None)
    _attempt(db, NOON_LOCAL - timedelta(hours=1), None)
    assert _check(db, NOON_LOCAL).allowed


def test_recipient_timezone_is_lending_owned_and_conservative_for_850():
    from src.services.lending_compliance import recipient_timezone
    assert recipient_timezone("+18505551234").key == "America/Chicago"
    assert recipient_timezone("+14045551234").key == "America/New_York"  # Georgia
    assert recipient_timezone("+18135551234", zip_code="33602").key == "America/New_York"
