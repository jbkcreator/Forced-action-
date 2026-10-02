"""WP-GL-9 schema: decision columns and the day slot (skipped texts free it)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from migrations.apply_lending_gl9_text_back import apply_to
from src.lending.dispositions import queue_missed_call

PHONE = "+18135558601"
ENDED = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)  # 11:00 ET


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def _status(db, call_id):
    return db.execute(text("SELECT status FROM lending.missed_call_events WHERE dialer_call_id = :c"), {"c": call_id}).scalar()


def test_migration_is_idempotent_and_adds_the_decision_columns(db):
    apply_to(db.connection())
    cols = {r[0] for r in db.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'lending' AND table_name = 'missed_call_events'"))}
    assert {"decided_at", "template_key", "provider_message_id"} <= cols


def test_a_pending_event_holds_the_days_slot(db):
    assert queue_missed_call(db, "c1", PHONE, None, None, ENDED) == "pending"
    assert queue_missed_call(db, "c2", PHONE, None, None, ENDED) == "duplicate_day"


@pytest.mark.parametrize("terminal", ["skipped_no_consent", "skipped_late", "dry_run", "failed", "skipped_quiet_hours"])
def test_a_skipped_failed_or_dry_run_event_frees_the_slot(db, terminal):
    queue_missed_call(db, "c1", PHONE, None, None, ENDED)
    db.execute(text("UPDATE lending.missed_call_events SET status = :s WHERE dialer_call_id = 'c1'"), {"s": terminal})
    assert queue_missed_call(db, "c2", PHONE, None, None, ENDED) == "pending"


@pytest.mark.parametrize("holding", ["sent", "sending", "send_unknown"])
def test_sent_sending_and_unknown_events_keep_the_slot(db, holding):
    queue_missed_call(db, "c1", PHONE, None, None, ENDED)
    db.execute(text("UPDATE lending.missed_call_events SET status = :s WHERE dialer_call_id = 'c1'"), {"s": holding})
    assert queue_missed_call(db, "c2", PHONE, None, None, ENDED) == "duplicate_day"
