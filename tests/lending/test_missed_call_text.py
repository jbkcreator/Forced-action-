"""WP-GL-9 missed-call text: detect no-answers, decide, send through the consent-gated SMS path."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.lending.missed_call_text import MissedCall, parse_cdr, render_text

NOW = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)   # 11:00 ET
PHONE = "+18135558601"


def _cdr(**over):
    row = {"id": "cdr-1", "phoneNumber": "(813) 555-8601", "status": "no-answer", "direction": "outbound",
           "callerId": "+18135550999", "endedAt": "2026-10-05T14:59:30Z"}
    row.update(over)
    return row


# ── Parsing call records (field names confirmed by the first real call) ──

def test_an_unanswered_outbound_call_becomes_a_missed_call():
    call = parse_cdr(_cdr())
    assert call == MissedCall(call_id="cdr-1", phone=PHONE, caller_id_number="+18135550999",
                              ended_at=datetime(2026, 10, 5, 14, 59, 30, tzinfo=timezone.utc))


@pytest.mark.parametrize("over", [{"status": "answered"}, {"direction": "inbound"}, {"id": None}])
def test_answered_inbound_or_unidentified_calls_are_ignored(over):
    assert parse_cdr(_cdr(**over)) is None


def test_text_names_the_property_and_always_carries_stop_language():
    assert "123 Main St" in render_text("123 Main St, Tampa FL 33602")
    for body in (render_text("123 Main St"), render_text(None)):
        assert "Reply STOP" in body and len(body) <= 320


# ── Decision + send (DB) ──

needs_db = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")


@pytest.fixture
def db():
    from migrations.apply_lending_missed_call_texts import apply_to
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    apply_to(conn)
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


class Sender:
    def __init__(self, allow=True):
        self.sent, self.allow = [], allow

    def __call__(self, to, body):
        self.sent.append((to, body))
        return self.allow


def _call(call_id=None, phone=PHONE, minutes_ago=0.5):
    return MissedCall(call_id=call_id or f"c-{uuid.uuid4().hex[:8]}", phone=phone, caller_id_number=None,
                      ended_at=NOW - timedelta(minutes=minutes_ago))


def _outcomes(db):
    return [r[0] for r in db.execute(text(
        "SELECT outcome FROM lending.missed_call_texts WHERE phone = :p ORDER BY id"), {"p": PHONE})]


@needs_db
def test_sends_once_and_logs_the_send(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call()], sender=sender, enabled=True, now=NOW)
    assert len(sender.sent) == 1 and sender.sent[0][0] == PHONE
    assert _outcomes(db) == ["sent"]


@needs_db
def test_one_text_per_person_per_day(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call(), _call()], sender=sender, enabled=True, now=NOW)
    assert len(sender.sent) == 1
    assert _outcomes(db) == ["sent", "skipped_daily_cap"]


@needs_db
def test_the_same_call_is_never_processed_twice(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    call = _call(call_id="dup-1")
    process_missed_calls(db, [call], sender=sender, enabled=True, now=NOW)
    process_missed_calls(db, [call], sender=sender, enabled=True, now=NOW)
    assert len(sender.sent) == 1 and _outcomes(db) == ["sent"]


@needs_db
def test_a_suppressed_number_is_never_texted(db):
    from src.lending.missed_call_text import process_missed_calls
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                    "VALUES (:p, 'OPT_OUT', 'test')"), {"p": PHONE})
    sender = Sender()
    process_missed_calls(db, [_call()], sender=sender, enabled=True, now=NOW)
    assert sender.sent == [] and _outcomes(db) == ["skipped_suppressed"]


@needs_db
def test_a_consent_block_from_the_sms_gate_is_logged_as_no_consent(db):
    from src.lending.missed_call_text import process_missed_calls
    process_missed_calls(db, [_call()], sender=Sender(allow=False), enabled=True, now=NOW)
    assert _outcomes(db) == ["skipped_no_consent"]


@needs_db
def test_disabled_sends_nothing_and_logs_a_dry_run(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call()], sender=sender, enabled=False, now=NOW)
    assert sender.sent == [] and _outcomes(db) == ["dry_run"]


@needs_db
def test_a_call_older_than_the_window_is_skipped_as_late(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call(minutes_ago=10)], sender=sender, enabled=True, now=NOW)
    assert sender.sent == [] and _outcomes(db) == ["skipped_late"]


@needs_db
def test_one_poll_cycle_reads_call_records_and_decides_new_no_answers(db):
    from src.lending.missed_call_poller import run_cycle
    fresh = (NOW - timedelta(seconds=20)).isoformat()
    records = {"items": [_cdr(id="p-1", endedAt=fresh), _cdr(id="p-2", status="answered", endedAt=fresh)]}
    http = lambda method, path, json=None: records
    counts = run_cycle(db, http=http, enabled=False, now=NOW)
    assert counts == {"dry_run": 1}
    assert _outcomes(db) == ["dry_run"]


@needs_db
def test_a_second_poller_skips_while_the_first_holds_the_lock(db):
    from src.lending.missed_call_poller import run_cycle
    from config.lending_missed_call import POLL_LOCK_KEY
    other = db.get_bind().engine.connect()
    try:
        other.execute(text("SELECT pg_advisory_lock(:k)"), {"k": POLL_LOCK_KEY})
        assert run_cycle(db, http=lambda *a, **k: pytest.fail("must not poll"), enabled=False, now=NOW) is None
    finally:
        other.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": POLL_LOCK_KEY})
        other.close()
