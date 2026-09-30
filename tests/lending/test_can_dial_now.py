"""WP-W0-3 can-dial-now: calling window + 3 attempts / rolling 24h (spec §3.1)."""
from __future__ import annotations

import os
import uuid
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
        "(phone varchar(20), direction varchar(10), call_ended_at timestamptz NOT NULL, disposition varchar(30))"
    ))
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _check(db, now):
    from src.lending.compliance import can_dial_now
    return can_dial_now(PHONE, db, now=now)


def _attempt(db, ended_at, disposition=None, direction="outbound"):
    db.execute(
        text("INSERT INTO lending.call_dispositions (aircall_call_id, phone, direction, call_ended_at, disposition, raw_event) "
             "VALUES (:cid, :p, :dir, :t, :d, '{}')"),
        {"cid": f"test-{uuid.uuid4().hex}", "p": PHONE, "dir": direction, "t": ended_at, "d": disposition},
    )


@pytest.mark.parametrize("utc_hour,utc_min,allowed", [
    (12, 59, False),   # 08:59 ET, before the 09:00 ET start
    (13, 0, True),     # 09:00 ET
    (23, 14, True),    # 19:14 ET
    (23, 15, False),   # 19:15 ET client hard stop
])
def test_calling_window_edges_are_the_eastern_hard_stop(db, utc_hour, utc_min, allowed):
    result = _check(db, datetime(2026, 9, 29, utc_hour, utc_min, tzinfo=timezone.utc))
    assert result.allowed is allowed
    if not allowed:
        assert result.reason == ReasonCode.OUTSIDE_CALL_WINDOW


def test_central_number_is_also_held_to_its_local_8_to_8(db):
    from src.lending.compliance import can_dial_now
    central = "+18505551234"  # 850 -> Central
    ok = can_dial_now(central, db, now=datetime(2026, 9, 29, 22, 30, tzinfo=timezone.utc))   # 17:30 CT / 18:30 ET
    assert ok.allowed
    late = can_dial_now(central, db, now=datetime(2026, 9, 29, 23, 20, tzinfo=timezone.utc))  # 18:20 CT / 19:20 ET
    assert late.reason == ReasonCode.OUTSIDE_CALL_WINDOW


def test_shift_groups_split_the_eastern_day(db):
    from src.lending.compliance import can_dial_now
    at_1500 = datetime(2026, 9, 29, 19, 0, tzinfo=timezone.utc)   # 15:00 ET
    at_1230 = datetime(2026, 9, 29, 16, 30, tzinfo=timezone.utc)  # 12:30 ET
    assert can_dial_now(PHONE, db, now=at_1230, seat_group="A").allowed
    assert can_dial_now(PHONE, db, now=at_1230, seat_group="B").reason == ReasonCode.OUTSIDE_CALL_WINDOW
    assert can_dial_now(PHONE, db, now=at_1500, seat_group="A").reason == ReasonCode.OUTSIDE_CALL_WINDOW
    assert can_dial_now(PHONE, db, now=at_1500, seat_group="B").allowed
    assert can_dial_now(PHONE, db, now=at_1500).allowed   # no group -> widest window


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
    from src.lending.compliance import recipient_timezone
    assert recipient_timezone("+18505551234").key == "America/Chicago"
    assert recipient_timezone("+14045551234").key == "America/New_York"  # Georgia
    assert recipient_timezone("+18135551234", zip_code="33602").key == "America/New_York"


class FakeDialer:
    def __init__(self):
        self.removed, self.reasons = [], []

    def __call__(self, phone, *, reason):
        self.removed.append(phone)
        self.reasons.append(reason)


def test_on_attempt_recorded_pulls_contact_at_the_cap_only(db):
    from src.lending.compliance import on_attempt_recorded

    dialer = FakeDialer()
    _attempt(db, NOON_LOCAL - timedelta(hours=2))
    _attempt(db, NOON_LOCAL - timedelta(hours=1))
    assert on_attempt_recorded(db, PHONE, now=NOON_LOCAL, dialer_remover=dialer).allowed
    assert dialer.removed == []

    _attempt(db, NOON_LOCAL - timedelta(minutes=5))
    result = on_attempt_recorded(db, PHONE, now=NOON_LOCAL, dialer_remover=dialer)
    assert result.reason == ReasonCode.ATTEMPT_CAP_REACHED
    assert dialer.removed == [PHONE]
    assert dialer.reasons == ["attempt_cap"]


def test_on_attempt_recorded_ignores_missing_phone(db):
    from src.lending.compliance import on_attempt_recorded

    assert on_attempt_recorded(db, None, now=NOON_LOCAL, dialer_remover=FakeDialer()) is None


def test_inbound_calls_do_not_count_toward_the_cap(db):
    for h in (3, 2, 1):
        _attempt(db, NOON_LOCAL - timedelta(hours=h), direction="inbound")
    assert _check(db, NOON_LOCAL).allowed


def test_missing_direction_counts_toward_the_cap(db):
    for h in (3, 2, 1):
        _attempt(db, NOON_LOCAL - timedelta(hours=h), direction=None)
    assert _check(db, NOON_LOCAL).reason == ReasonCode.ATTEMPT_CAP_REACHED
