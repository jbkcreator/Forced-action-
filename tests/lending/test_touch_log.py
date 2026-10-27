"""The per-phone touch log reads calls, text decisions and outcomes together (Q66)."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending.consent import record_consent
from src.lending.dispositions import queue_missed_call
from src.lending.text_back import process_pending
from src.lending.touch_log import touches

PHONE = "+17275550199"
TEXT_NOW = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)  # 11:00 ET: inside the text-back window


@pytest.fixture
def tdb(lending_db):
    from migrations.apply_lending_gl9_text_back import apply_to as apply_text_back
    from migrations.apply_lending_lender_routing import apply_to as apply_routing
    from migrations.apply_lending_lender_rule_values import apply_to as apply_rule_values
    from migrations.apply_lending_lendingflow import apply_to as apply_lf
    from migrations.apply_lending_lendingflow_enrichment import apply_to as apply_enrichment
    from migrations.apply_lending_partner_door import apply_to as apply_partner_door
    from src.lending.models import LendingContact

    conn = lending_db.get_bind()
    LendingContact.__table__.create(bind=conn, checkfirst=True)
    for apply in (apply_lf, apply_enrichment, apply_rule_values, apply_partner_door, apply_routing, apply_text_back):
        apply(conn)
    return lending_db


def _call(db, direction: str, outcome: str, minutes_ago: int, *, phone: str = PHONE) -> str:
    call_id = f"touch-{uuid.uuid4().hex}"
    at = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, disposition_raw, call_started_at,"
                    " call_ended_at, raw_event, call_source) VALUES (:id, :d, :p, :o, :t, :t, '{}'::jsonb, 'ghl')"),
               {"id": call_id, "d": direction, "p": phone, "o": outcome, "t": at})
    return call_id


def _queue_text(db, call_id: str, *, phone: str = PHONE) -> None:
    """Queue the missed-call event the way the CDR poller does (``queue_missed_call``)."""
    ended = TEXT_NOW - timedelta(seconds=10)
    db.execute(text("UPDATE lending.call_dispositions SET call_ended_at = :e WHERE dialer_call_id = :c"), {"e": ended, "c": call_id})
    queue_missed_call(db, call_id, phone, None, {"property_address": "1 Main St, Tampa FL"}, ended)


def _decide_text(db, call_id: str, *, minutes_ago: int, consent: bool = True, phone: str = PHONE) -> None:
    """Run the active text-back processor on the queued event, then place its decision ``minutes_ago`` in the past."""
    _queue_text(db, call_id, phone=phone)
    if consent:
        record_consent(db, phone, "on_call_yes")
    process_pending(db, sender=lambda *_args, **_kwargs: "msg-1", enabled=True, number="+18135550100", now=TEXT_NOW)
    at = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    db.execute(text("UPDATE lending.missed_call_events SET created_at = :t, decided_at = :t WHERE dialer_call_id = :c"),
               {"t": at, "c": call_id})


def test_calls_texts_stage_entries_and_routing_come_back_newest_first(tdb):
    inbound = _call(tdb, "inbound", "ghl:no-answer", 50)
    outbound = _call(tdb, "outbound", "ghl:completed", 40)
    _decide_text(tdb, inbound, minutes_ago=30)
    tdb.execute(text("INSERT INTO lending.ghl_stage_events (ghl_opportunity_id, stage_name, stage_key, phone, event_at, raw_event)"
                     " VALUES ('opp-1', 'Booked', 'booked', :p, now() - interval '20 minutes', '{}'::jsonb)"), {"p": PHONE})
    lead = tdb.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, loan_type, property_state, ghl_status,"
        " suppressed, raw_payload, consent_status) VALUES ('TOUCH-1', 'touch-hash-1', :p, 'DSCR_RENTAL', 'FL', 'synced', false,"
        " '{}'::jsonb, 'present') RETURNING id"), {"p": PHONE}).scalar_one()
    tdb.execute(text("INSERT INTO lending.file_routing_events (lead_id, lender_name, action, created_at)"
                     " VALUES (:l, 'Acme Lending', 'routed', now() - interval '10 minutes')"), {"l": lead})
    found = touches(tdb, "(727) 555-0199")  # any format phone_utils understands
    assert [(t.kind, t.direction, t.outcome) for t in found] == [
        ("routing", None, "routed"), ("stage", None, "Booked"), ("text", "outbound", "sent"),
        ("call", "outbound", "ghl:completed"), ("call", "inbound", "ghl:no-answer")]
    assert {t.ref for t in found} >= {inbound, outbound, "opp-1", str(lead)}


def test_a_text_that_was_withheld_shows_why(tdb):
    call_id = _call(tdb, "outbound", "ghl:no-answer", 5)
    _decide_text(tdb, call_id, minutes_ago=1, consent=False)
    texts = [t for t in touches(tdb, PHONE) if t.kind == "text"]
    assert [(t.direction, t.outcome, t.ref) for t in texts] == [("outbound", "skipped_no_consent", call_id)]


def test_a_text_still_waiting_to_be_decided_shows_as_pending(tdb):
    call_id = _call(tdb, "outbound", "ghl:no-answer", 5)
    _queue_text(tdb, call_id)
    texts = [t for t in touches(tdb, PHONE) if t.kind == "text"]
    assert [(t.outcome, t.ref) for t in texts] == [("pending", call_id)]
    assert texts[0].at is not None


def test_a_same_day_duplicate_is_in_the_history_next_to_the_text_that_was_sent(tdb):
    first = _call(tdb, "outbound", "ghl:no-answer", 20)
    second = _call(tdb, "outbound", "ghl:no-answer", 10)
    _decide_text(tdb, first, minutes_ago=15)
    _queue_text(tdb, second)  # same ET day as the first, which holds the day's slot
    outcomes = {t.ref: t.outcome for t in touches(tdb, PHONE) if t.kind == "text"}
    assert outcomes == {first: "sent", second: "duplicate_day"}


def test_another_phone_is_not_included(tdb):
    _call(tdb, "inbound", "ghl:completed", 5, phone="+17275550177")
    assert touches(tdb, PHONE) == []


def test_an_invalid_or_empty_phone_returns_nothing(tdb):
    assert touches(tdb, "") == [] and touches(tdb, "abc") == []


def test_the_limit_keeps_the_newest(tdb):
    for minutes in (30, 20, 10):
        _call(tdb, "inbound", "ghl:completed", minutes)
    assert len(touches(tdb, PHONE, limit=2)) == 2
