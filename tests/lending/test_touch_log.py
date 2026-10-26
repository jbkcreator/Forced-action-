"""The per-phone touch log reads calls, text decisions and outcomes together (Q66)."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending.touch_log import touches

PHONE = "+17275550199"


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


def test_calls_texts_stage_entries_and_routing_come_back_newest_first(tdb):
    inbound = _call(tdb, "inbound", "ghl:no-answer", 50)
    outbound = _call(tdb, "outbound", "ghl:completed", 40)
    tdb.execute(text("INSERT INTO lending.missed_call_texts (dialer_call_id, phone, event_date_et, outcome, created_at)"
                     " VALUES (:id, :p, current_date, 'sent', now() - interval '30 minutes')"), {"id": inbound, "p": PHONE})
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


def test_another_phone_is_not_included(tdb):
    _call(tdb, "inbound", "ghl:completed", 5, phone="+17275550177")
    assert touches(tdb, PHONE) == []


def test_an_invalid_or_empty_phone_returns_nothing(tdb):
    assert touches(tdb, "") == [] and touches(tdb, "abc") == []


def test_the_limit_keeps_the_newest(tdb):
    for minutes in (30, 20, 10):
        _call(tdb, "inbound", "ghl:completed", minutes)
    assert len(touches(tdb, PHONE, limit=2)) == 2
