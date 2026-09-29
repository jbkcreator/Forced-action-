"""Pilot rollup counts on fixtures. Real Postgres via fresh_db (rolled back)."""
from __future__ import annotations

import pytest
from sqlalchemy import text

import config.lead_ownership as cfg
import src.services.campaign_pilot_rollup as rollup_mod
from src.core.models import Property
from src.services.campaign_pilot_rollup import CampaignRollup, pilot_rollup
from src.services.lead_ownership import claim_ownership

A, B = "test_rollup_a", "test_rollup_b"


@pytest.fixture(autouse=True)
def _priority(monkeypatch):
    monkeypatch.setattr(cfg, "CAMPAIGN_PRIORITY", (A, B))
    monkeypatch.setattr(rollup_mod, "CAMPAIGN_PRIORITY", (A, B))


def _person(session, campaign: str) -> str:
    p = str(session.execute(text(
        "INSERT INTO fa_max_persons (source, source_reference) VALUES ('maturity', 'test-rollup-' || gen_random_uuid()) "
        "RETURNING person_id")).scalar_one())
    claim_ownership(session, person_id=p, campaign=campaign, source="property_radar")
    return p


def _relay(session, person_id: str, status: str, thread_id: str | None = None) -> None:
    session.execute(text(
        "INSERT INTO relay_approval_queue (idempotency_key, channel, recipient, payload, status, person_id, thread_id) "
        "VALUES (:k, 'email', 'test@example.com', '{}'::jsonb, :s, CAST(:p AS uuid), :t)"),
        {"k": f"test-rollup-{person_id}-{status}-{thread_id}", "s": status, "p": person_id, "t": thread_id})


def _concierge(session, person_id: str, classification: str) -> None:
    session.execute(text(
        "INSERT INTO fa_max_concierge_log (person_id, inbound_channel, classification) "
        "VALUES (CAST(:p AS uuid), 'email', :c)"), {"p": person_id, "c": classification})


def _booking(session, person_id: str, status: str) -> None:
    session.execute(text(
        "INSERT INTO fa_max_bookings (booking_ref, calendar_id, attendee_email, topic, starts_at, ends_at, "
        "status, person_id) VALUES (:r, 'test-cal', 'test@example.com', 'test', now() + interval '1 day', "
        "now() + interval '1 day 30 minutes', :s, CAST(:p AS uuid))"),
        {"r": f"test-rollup-{person_id}-{status}", "s": status, "p": person_id})


def _lost(session, thread_id: str, reason: str) -> None:
    session.execute(text(
        "INSERT INTO agent_lane_opportunity_outcomes (opportunity_thread_id, outcome, reason_code, coded_by) "
        "VALUES (:t, 'lost', :r, 'test')"), {"t": thread_id, "r": reason})


def _dial_call(session, person_id: str, parcel: str) -> None:
    prop = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id="test-rollup")
    session.add(prop)
    session.flush()
    session.execute(text(
        "INSERT INTO fa_max_property_associations (person_id, property_id, role) "
        "VALUES (CAST(:p AS uuid), :prop, 'subject')"), {"p": person_id, "prop": prop.id})
    session.execute(text(
        "INSERT INTO dial_list_touch (property_id, action, generation_date) VALUES (:prop, 'dial_called', CURRENT_DATE)"),
        {"prop": prop.id})


def test_zero_rows_before_any_activity(fresh_db):
    assert pilot_rollup(fresh_db) == [CampaignRollup(A), CampaignRollup(B)]


def test_counts_each_measure_as_distinct_people(fresh_db):
    p1, p2, p3 = _person(fresh_db, A), _person(fresh_db, A), _person(fresh_db, B)

    _relay(fresh_db, p1, "sent", "TEST-OPP-1")
    _relay(fresh_db, p1, "sent", "TEST-OPP-1b")          # same person twice -> reach 1
    _relay(fresh_db, p2, "pending")                        # not sent -> not reach
    _dial_call(fresh_db, p2, "TEST-ROLLUP-PARCEL-1")       # call made -> reach
    _concierge(fresh_db, p1, "interested")
    _concierge(fresh_db, p2, "wrong_person")               # a reply AND wrong person
    _booking(fresh_db, p1, "confirmed")
    _booking(fresh_db, p2, "cancelled")                    # cancelled -> not booked
    _lost(fresh_db, "TEST-OPP-1", "loan_paid_off")
    _relay(fresh_db, p3, "sent", "TEST-OPP-3")
    _lost(fresh_db, "TEST-OPP-3", "wrong_contact")

    rows = {r.campaign: r for r in pilot_rollup(fresh_db)}
    assert rows[A] == CampaignRollup(A, reach=2, reply=2, booked=1, wrong_person=1, paid_off=1)
    assert rows[B] == CampaignRollup(B, reach=1, reply=0, booked=0, wrong_person=1, paid_off=0)


def test_blocked_campaign_gets_no_credit(fresh_db):
    p = _person(fresh_db, A)
    claim_ownership(fresh_db, person_id=p, campaign=B, source="property_radar")  # B blocked by A
    _relay(fresh_db, p, "sent")
    rows = {r.campaign: r for r in pilot_rollup(fresh_db)}
    assert (rows[A].reach, rows[B].reach) == (1, 0)


def test_event_credited_to_owner_at_event_time(fresh_db):
    p = _person(fresh_db, B)
    fresh_db.execute(text(
        "UPDATE lead_campaign_assignments SET created_at = now() - interval '10 days' "
        "WHERE person_id = CAST(:p AS uuid)"), {"p": p})
    _relay(fresh_db, p, "sent", "TEST-OPP-EARLY")
    fresh_db.execute(text(
        "UPDATE relay_approval_queue SET created_at = now() - interval '5 days' WHERE thread_id = 'TEST-OPP-EARLY'"))
    fresh_db.execute(text(
        "UPDATE lead_campaign_assignments SET status = 'preempted', displaced_by = :a, "
        "ended_at = now() - interval '2 days' WHERE person_id = CAST(:p AS uuid)"), {"p": p, "a": A})
    fresh_db.execute(text(
        "INSERT INTO lead_campaign_assignments (person_id, campaign, source, status, created_at) "
        "VALUES (CAST(:p AS uuid), :a, 'property_radar', 'active', now() - interval '2 days')"), {"p": p, "a": A})
    _booking(fresh_db, p, "confirmed")  # now -> owned by A

    rows = {r.campaign: r for r in pilot_rollup(fresh_db)}
    assert (rows[B].reach, rows[B].booked) == (1, 0)
    assert (rows[A].reach, rows[A].booked) == (0, 1)
