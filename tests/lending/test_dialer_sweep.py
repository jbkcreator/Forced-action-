"""WP-W0-3 dialer enforcement sweep: contacts leave the Aircall queue outside
8 AM–8 PM recipient local time or at 3 attempts / rolling 24 h, and come back when
the rule allows it again. Opted-out numbers are never restored.

Aircall calls are fakes (Developer 3's final signatures). No Tracerfy.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

PHONE = "+18135557001"      # Tampa, Eastern
OTHER = "+18135557002"
# 2026-09-29 is EDT (UTC-4)
NOON_LOCAL = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)
NINE_PM_LOCAL = datetime(2026, 9, 30, 1, 0, tzinfo=timezone.utc)
NEXT_9AM_LOCAL = datetime(2026, 9, 30, 13, 0, tzinfo=timezone.utc)


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    session = Session(bind=conn)
    session.execute(text(
        "CREATE TABLE IF NOT EXISTS lending.call_dispositions "
        "(phone varchar(20), direction varchar(10), call_ended_at timestamptz NOT NULL, disposition varchar(30))"
    ))
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


class FakeAircall:
    def __init__(self, fail_remove=False, fail_restore=False):
        self.removed, self.restored = [], []
        self.fail_remove, self.fail_restore = fail_remove, fail_restore

    def remove(self, phone, *, reason):
        if self.fail_remove:
            raise RuntimeError("aircall 500")
        self.removed.append((phone, reason))

    def restore(self, phone):
        if self.fail_restore:
            raise RuntimeError("aircall 500")
        self.restored.append(phone)


def _sweep(db, now, aircall, loaded=(PHONE,)):
    from src.lending.compliance import sweep_dialer_pool
    return sweep_dialer_pool(
        db, now=now, dialer_remover=aircall.remove, dialer_restorer=aircall.restore,
        loaded_phones=lambda _db: list(loaded),
    )


def _attempt(db, ended_at):
    db.execute(text("INSERT INTO lending.call_dispositions (phone, direction, call_ended_at) "
                    "VALUES (:p, 'outbound', :t)"), {"p": PHONE, "t": ended_at})


def _hold(db, phone=PHONE):
    return db.execute(text("SELECT reason, released_at, release_reason FROM lending.dialer_holds "
                           "WHERE phone = :p ORDER BY id DESC LIMIT 1"), {"p": phone}).fetchone()


def test_inside_window_under_cap_nothing_happens(db):
    aircall = FakeAircall()
    result = _sweep(db, NOON_LOCAL, aircall)
    assert (result.pulled, result.restored) == (0, 0)
    assert aircall.removed == [] and _hold(db) is None


def test_after_8pm_contact_is_pulled_then_restored_next_morning(db):
    aircall = FakeAircall()
    assert _sweep(db, NINE_PM_LOCAL, aircall).pulled == 1
    assert aircall.removed == [(PHONE, "call_window")]
    assert _hold(db).reason == "call_window"

    # Developer 3's remove marks the load row inactive, so the loaded list no longer has it.
    assert _sweep(db, NINE_PM_LOCAL + timedelta(minutes=5), aircall, loaded=()).pulled == 0
    assert _sweep(db, NEXT_9AM_LOCAL, aircall, loaded=()).restored == 1
    assert aircall.restored == [PHONE]
    assert _hold(db).released_at is not None


def test_contact_at_the_cap_is_pulled_and_restored_after_24h(db):
    aircall = FakeAircall()
    first = NOON_LOCAL - timedelta(hours=3)
    for h in (3, 2, 1):
        _attempt(db, NOON_LOCAL - timedelta(hours=h))

    _sweep(db, NOON_LOCAL, aircall)
    assert aircall.removed == [(PHONE, "attempt_cap")]

    # 8 PM passed and the cap is still live — stays held
    assert _sweep(db, NOON_LOCAL + timedelta(hours=4), aircall, loaded=()).restored == 0
    # next day 13:00 local: oldest attempt > 24 h old, window open
    assert _sweep(db, first + timedelta(hours=24, minutes=1), aircall, loaded=()).restored == 1


def test_opted_out_contact_is_never_restored(db):
    aircall = FakeAircall()
    _sweep(db, NINE_PM_LOCAL, aircall)
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                    "VALUES (:p, 'OPT_OUT', 'test')"), {"p": PHONE})

    assert _sweep(db, NEXT_9AM_LOCAL, aircall, loaded=()).restored == 0
    assert aircall.restored == []
    hold = _hold(db)
    assert hold.released_at is not None and hold.release_reason == "suppressed"


def test_failed_removal_retries_next_sweep(db):
    assert _sweep(db, NINE_PM_LOCAL, FakeAircall(fail_remove=True)).pulled == 0
    assert _hold(db) is None
    ok = FakeAircall()
    assert _sweep(db, NINE_PM_LOCAL + timedelta(minutes=1), ok).pulled == 1


def test_failed_restore_keeps_the_hold_for_the_next_sweep(db):
    aircall = FakeAircall()
    _sweep(db, NINE_PM_LOCAL, aircall)
    assert _sweep(db, NEXT_9AM_LOCAL, FakeAircall(fail_restore=True), loaded=()).restored == 0
    assert _hold(db).released_at is None
    assert _sweep(db, NEXT_9AM_LOCAL + timedelta(minutes=1), aircall, loaded=()).restored == 1


def test_already_held_contact_is_not_pulled_twice(db):
    aircall = FakeAircall()
    _sweep(db, NINE_PM_LOCAL, aircall)
    _sweep(db, NINE_PM_LOCAL + timedelta(minutes=1), aircall)  # still in loaded list (e.g. removal not reflected)
    assert aircall.removed == [(PHONE, "call_window")]


def test_cap_hook_records_a_hold_so_the_sweep_restores_it(db):
    from src.lending.compliance import on_attempt_recorded

    aircall = FakeAircall()
    for h in (3, 2, 1):
        _attempt(db, NOON_LOCAL - timedelta(hours=h))
    on_attempt_recorded(db, PHONE, now=NOON_LOCAL, dialer_remover=aircall.remove)
    assert _hold(db).reason == "attempt_cap"

    assert _sweep(db, NOON_LOCAL + timedelta(hours=21, minutes=1), aircall, loaded=()).restored == 1


def test_second_sweep_skips_while_the_first_holds_the_lock(db):
    from src.lending.compliance import SWEEP_LOCK_KEY

    other = create_engine(os.environ["DATABASE_URL"]).connect()
    try:
        other.execute(text("SELECT pg_advisory_lock(:k)"), {"k": SWEEP_LOCK_KEY})
        assert _sweep(db, NINE_PM_LOCAL, FakeAircall()).skipped_locked is True
    finally:
        other.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": SWEEP_LOCK_KEY})
        other.close()
