"""T-12: background enrichment card, fit presentation, routing, deal-facts webhook, sweep."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from config.lender_matrix import LenderRules
from src.lending.contracts import LoanType, RoutingTag
from src.lending.db import get_lending_db
from src.lending.enrichment import service
from src.lending.enrichment.fit_card import evaluate_fit, parse_band_bounds
from src.lending.enrichment.ports import CompsView, FakeComps, FakeDealThread, FakeStreetView
from src.lending.enrichment.property_facts import PropertyFacts, PropertyMatch
from src.lending.enrichment.routing import route_lead
from src.lending.enrichment.webhook import router
from src.lending.lendingflow import parse_lendingflow, save_lead

SECRET = "ghl-secret"
FIXTURE = Path(__file__).parent / "fixtures" / "lendingflow_fake.json"
TODAY = date(2026, 10, 9)


# --------------------------------------------------------------------------- routing

@pytest.mark.parametrize("close, tag, reason", [
    (TODAY + timedelta(days=30), RoutingTag.FULL_MACHINE, "address_and_close_date_within_window"),  # exactly 30
    (TODAY + timedelta(days=31), RoutingTag.NURTURE, "close_date_beyond_window"),
    (TODAY, RoutingTag.FULL_MACHINE, "address_and_close_date_within_window"),
    (TODAY - timedelta(days=1), RoutingTag.NURTURE, "close_date_in_past"),
    (None, RoutingTag.NURTURE, "missing_close_date"),
])
def test_routing_close_date_window(close, tag, reason):
    decision = route_lead("12 Main St", close, today=TODAY)
    assert (decision.tag, decision.reason) == (tag, reason)


@pytest.mark.parametrize("address", [None, "", "   "])
def test_routing_missing_address_is_nurture_even_with_a_close_date(address):
    decision = route_lead(address, TODAY + timedelta(days=5), today=TODAY)
    assert (decision.tag, decision.reason) == (RoutingTag.NURTURE, "missing_address")


# --------------------------------------------------------------------------- fit presentation

def _rules(key, name, *, verified=True, floor=None, points="0.02", spread="0.08", loan_types=()):
    return LenderRules(
        key=key, name=name, verified=verified, loan_types=frozenset(loan_types), credit_floor=floor,
        origination_points=Decimal(points), rate_spread=Decimal(spread),
    )


def _matrix(*, backflip_cost="0.09", all_verified=True):
    return (
        _rules("generic_backflip_flip", "Backflip (Fix & Flip)", floor=640, points="0.03", spread=backflip_cost,
               loan_types=[LoanType.FIX_AND_FLIP]),
        _rules("generic_backflip_construction", "Backflip (Ground-Up)", floor=680,
               loan_types=[LoanType.GROUND_UP_CONSTRUCTION]),
        _rules("rcn_capital", "RCN Capital", points="0.01", spread="0.05"),
        _rules("easy_street", "Easy Street Capital", points="0.015", spread="0.06"),
        _rules("kiavi_affiliate", "Kiavi Affiliate", points="0.02", spread="0.07", verified=all_verified),
        _rules("abl", "Asset Based Lending (ABL)", points="0.025", spread="0.09"),
    )


def _lead(**over):
    base = {"loan_type": "FIX_AND_FLIP", "loan_amount": 400000, "property_state": "FL", "credit_band": "700-739",
            "credit_band_min_fico": 700}
    base.update(over)
    return base


def test_score_is_percent_of_lenders_that_fit():
    view = evaluate_fit(_lead(), address=None, matrix=_matrix())
    # Backflip's two rule rows are one lender: 5 lenders, all 5 fit
    assert view.score == 100
    matrix = _matrix()[:2] + tuple(_rules(k, k, floor=800) for k in ("rcn_capital", "easy_street", "kiavi_affiliate", "abl"))
    # only Backflip fits: 1 of 5
    view = evaluate_fit(_lead(), address=None, matrix=matrix)
    assert view.score == 20


def test_backflip_ranks_first_even_when_more_expensive():
    view = evaluate_fit(_lead(), address=None, matrix=_matrix(backflip_cost="0.12"))
    assert view.ranked[0] == "Backflip"
    assert view.ranked[1:] == ["RCN Capital", "Easy Street Capital", "Kiavi Affiliate", "Asset Based Lending (ABL)"]  # cheapest first


def test_every_other_lender_is_shown_when_backflip_does_not_fit():
    view = evaluate_fit(_lead(credit_band="600-639", credit_band_min_fico=600), address=None, matrix=_matrix())
    assert "Backflip" not in view.ranked
    assert view.ranked == ["RCN Capital", "Easy Street Capital", "Kiavi Affiliate", "Asset Based Lending (ABL)"]


def test_score_withheld_while_any_lender_is_unverified():
    view = evaluate_fit(_lead(), address=None, matrix=_matrix(all_verified=False))
    assert view.evaluated and view.score is None
    assert view.note == "Lender rules pending confirmation"


def test_credit_band_straddling_a_floor_asks_for_confirmation():
    view = evaluate_fit(_lead(credit_band="620-679", credit_band_min_fico=620), address=None, matrix=_matrix())
    assert view.straddles == ["Backflip: credit needs confirmation on the call"]


def test_open_ended_band_never_straddles():
    view = evaluate_fit(_lead(credit_band="740+", credit_band_min_fico=740), address=None, matrix=_matrix())
    assert view.straddles == []


def test_missing_core_fields_are_reported_not_scored():
    view = evaluate_fit(_lead(loan_type=None, loan_amount=None), address=None, matrix=_matrix())
    assert not view.evaluated and view.score is None
    assert view.missing_fields == ["loan_type", "loan_amount"]


@pytest.mark.parametrize("raw, bounds", [("620-679", (620, 679)), ("620 to 679", (620, 679)), ("740+", None), (None, None)])
def test_parse_band_bounds(raw, bounds):
    assert parse_band_bounds(raw) == bounds


# --------------------------------------------------------------------------- facts SQL

@pytest.fixture
def enr_db(lending_db):
    from migrations.apply_lending_lendingflow import apply_to as apply_lf
    from migrations.apply_lending_lendingflow_enrichment import apply_to as apply_enr

    conn = lending_db.get_bind()
    apply_lf(conn)
    apply_enr(conn)
    return lending_db


def test_facts_query_runs_and_returns_none_for_a_missing_property(enr_db):
    from src.lending.enrichment.property_facts import load_property_facts

    assert load_property_facts(enr_db, -1) is None


# --------------------------------------------------------------------------- service

def _payload(**over):
    body = json.loads(FIXTURE.read_text(encoding="utf-8"))
    body.update(over)
    return body


def _make_lead(db, *, address=None, state="Florida", loan_type="Fix & Flip", emitted=True, vendor="LF-1", phone="(727) 555-0100"):
    payload = _payload(lead_id=vendor, phone=phone, state=state, loan_type=loan_type, email=f"{vendor}@example.com")
    if address:
        payload["property_address"] = address
    result = save_lead(db, parse_lendingflow(payload), payload)
    if emitted:
        db.execute(text("UPDATE lending.lendingflow_leads SET event_emitted_at = now() WHERE id = :i"), {"i": result.lead_id})
    db.commit()
    return result.lead_id


@pytest.fixture
def facts(monkeypatch):
    """A confident property match with known facts, so the tests do not depend on the properties table."""
    monkeypatch.setattr(service, "match_property", lambda *a, **k: PropertyMatch(42, 100, "hillsborough"))
    monkeypatch.setattr(service, "load_property_facts", lambda *a, **k: PropertyFacts(
        sunbiz_standing="ACTIVE", officers=[{"name": "A Officer", "title": "MGR"}], deeds=[], deed_count=2, permits=[],
        permit_count=1))


def _row(db, lead_id):
    return db.execute(text("SELECT * FROM lending.lendingflow_enrichment WHERE lead_id = :i"), {"i": lead_id}).mappings().one()


def test_card_shows_the_three_facts_and_fit(enr_db, facts):
    lead_id = _make_lead(enr_db, address="12 Main St")
    thread = FakeDealThread()
    service.ensure_row(enr_db, lead_id)
    outcome = service.enrich_lead(enr_db, lead_id, thread=thread, now=datetime(2026, 10, 9, 16, tzinfo=timezone.utc))
    row = _row(enr_db, lead_id)
    assert outcome.status == "ready" and row["status"] == "ready"
    assert row["property_id"] == 42 and row["match_confidence"] == 100
    card = row["card"]["facts"]
    assert card["sunbiz_standing"] == "ACTIVE" and card["officers"][0]["name"] == "A Officer"
    assert card["deed_count"] == 2 and card["permit_count"] == 1
    assert row["fit"]["evaluated"] is True
    assert row["routing_tag"] == "NURTURE" and row["routing_reason"] == "missing_close_date"
    assert thread.posts == []  # NURTURE and no call-captured address: nothing is posted


def test_full_machine_sets_priority_and_posts_once(enr_db, facts):
    lead_id = _make_lead(enr_db, address="12 Main St")
    service.record_deal_facts(enr_db, phone="(727) 555-0100", address=None, target_close_date=date(2026, 11, 8), source="booking_form")
    enr_db.commit()
    thread = FakeDealThread()
    now = datetime(2026, 10, 9, 16, tzinfo=timezone.utc)
    service.enrich_lead(enr_db, lead_id, thread=thread, now=now)
    row = _row(enr_db, lead_id)
    assert (row["routing_tag"], row["closer_priority"]) == ("FULL_MACHINE", True)
    assert len(thread.posts) == 1 and "FULL_MACHINE" in thread.posts[0][1]
    service.record_deal_facts(enr_db, phone="(727) 555-0100", address=None, target_close_date=date(2026, 11, 8), source="booking_form")
    enr_db.commit()
    service.enrich_lead(enr_db, lead_id, thread=thread, now=now)  # same facts again: quiet update, no second post
    assert len(thread.posts) == 1


def test_call_captured_address_posts_street_view_and_comps_into_the_thread(enr_db, facts):
    lead_id = _make_lead(enr_db)
    service.record_deal_facts(enr_db, phone="(727) 555-0100", address="99 Oak Ave", target_close_date=None, source="slack_form")
    enr_db.commit()
    thread, view = FakeDealThread(), FakeStreetView({"99 Oak Ave": "https://maps.example/pano/1"})
    comps = FakeComps(CompsView(True, None, Decimal("300000"), Decimal("320000"), Decimal("340000"), "high", 4,
                                [{"sale_price": "310000", "sale": "2026-05", "sqft": 1500}]))
    service.enrich_lead(enr_db, lead_id, thread=thread, street_view=view, comps=comps)
    assert view.calls == ["99 Oak Ave"] and comps.calls == [42]
    assert len(thread.posts) == 2
    assert thread.posts[1][0] == "fake-thread-1"  # the Street View / comps post replies in the card's thread
    assert "pano/1" in thread.posts[1][1] and "$320,000" in thread.posts[1][1]
    assert _row(enr_db, lead_id)["thread_ts"] == "fake-thread-1"


def test_no_property_match_says_so_and_never_invents_facts(enr_db, monkeypatch):
    monkeypatch.setattr(service, "match_property", lambda *a, **k: None)
    lead_id = _make_lead(enr_db, address="1 Nowhere Rd")
    service.ensure_row(enr_db, lead_id)
    service.enrich_lead(enr_db, lead_id, thread=FakeDealThread())
    card = _row(enr_db, lead_id)["card"]
    assert card["property_matched"] is False and card["facts"] is None
    assert card["coverage_note"] == "no confident property match"


def test_failed_post_marks_the_row_failed_for_the_sweep(enr_db, facts):
    lead_id = _make_lead(enr_db)
    service.record_deal_facts(enr_db, phone="(727) 555-0100", address="99 Oak Ave", target_close_date=None, source="slack_form")
    enr_db.commit()
    outcome = service.enrich_lead(enr_db, lead_id, thread=FakeDealThread(error=RuntimeError("slack down")),
                                  street_view=FakeStreetView(), comps=FakeComps())
    row = _row(enr_db, lead_id)
    assert outcome.status == "failed" and row["status"] == "failed" and row["last_error"] == "deal_thread_post_failed"
    assert row["routing_tag"] is not None  # the card itself was saved


def test_a_running_row_is_not_claimed_twice(enr_db, facts):
    lead_id = _make_lead(enr_db)
    service.ensure_row(enr_db, lead_id)
    enr_db.execute(text("UPDATE lending.lendingflow_enrichment SET status = 'running', last_attempt_at = now() WHERE lead_id = :i"), {"i": lead_id})
    enr_db.commit()
    assert service.enrich_lead(enr_db, lead_id, thread=FakeDealThread()).status == "busy"


def test_suppressed_lead_is_skipped(enr_db, facts):
    lead_id = _make_lead(enr_db)
    enr_db.execute(text("UPDATE lending.lendingflow_leads SET suppressed = true WHERE id = :i"), {"i": lead_id})
    service.ensure_row(enr_db, lead_id)
    enr_db.commit()
    assert service.enrich_lead(enr_db, lead_id, thread=FakeDealThread()).status == "skipped"


def test_sweep_creates_rows_for_missed_events_and_retries_failures(enr_db, facts, monkeypatch):
    missed = _make_lead(enr_db, vendor="LF-A", phone="(727) 555-0111")
    not_emitted = _make_lead(enr_db, vendor="LF-B", phone="(727) 555-0122", emitted=False)
    ran = service.run_sweep(enr_db, thread=FakeDealThread())
    assert ran == 1 and _row(enr_db, missed)["status"] == "ready"
    assert enr_db.execute(text("SELECT count(*) FROM lending.lendingflow_enrichment WHERE lead_id = :i"), {"i": not_emitted}).scalar() == 0
    enr_db.execute(text("UPDATE lending.lendingflow_enrichment SET status = 'failed', attempts = 1, "
                        "last_attempt_at = now() - interval '2 minutes' WHERE lead_id = :i"), {"i": missed})
    enr_db.commit()
    assert service.run_sweep(enr_db, thread=FakeDealThread()) == 1  # past the 1-minute first backoff
    enr_db.execute(text("UPDATE lending.lendingflow_enrichment SET status = 'failed', attempts = 8, "
                        "last_attempt_at = now() - interval '1 day' WHERE lead_id = :i"), {"i": missed})
    enr_db.commit()
    assert service.run_sweep(enr_db, thread=FakeDealThread()) == 0  # attempts capped


def test_event_handler_is_a_no_op_while_the_flag_is_off(enr_db, monkeypatch):
    monkeypatch.setattr(service, "enabled", lambda: False)
    service.on_lead_created(object())  # would raise on .lead_id if it did any work


# --------------------------------------------------------------------------- webhook

@pytest.fixture
def client(enr_db, monkeypatch):
    from src.lending.enrichment import webhook as hook

    settings = hook.get_settings()
    monkeypatch.setattr(settings, "lending_enrichment_enabled", True)
    monkeypatch.setattr(settings, "lending_ghl_webhook_secret", SecretStr(SECRET))
    scheduled = []
    monkeypatch.setattr(hook, "_enrich_in_background", lambda lead_id: scheduled.append(lead_id))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_lending_db] = lambda: enr_db
    c = TestClient(app)
    c.scheduled = scheduled
    return c


def _post(client, body, secret=SECRET):
    return client.post("/webhooks/lending/lendingflow-deal-facts", json=body,
                       headers={"X-Webhook-Secret": secret} if secret else {})


def test_webhook_stores_the_facts_and_queues_a_rebuild(client, enr_db):
    lead_id = _make_lead(enr_db)
    response = _post(client, {"phone": "727-555-0100", "target_close_date": "2026-11-01", "source": "booking_form"})
    assert response.status_code == 200 and client.scheduled == [lead_id]
    row = _row(enr_db, lead_id)
    assert row["target_close_date"] == date(2026, 11, 1) and row["status"] == "pending" and row["facts_source"] == "booking_form"
    _post(client, {"phone": "727-555-0100", "property_address": " 99  Oak Ave ", "source": "slack_form"})
    row = _row(enr_db, lead_id)
    assert row["captured_address"] == "99 Oak Ave" and row["target_close_date"] == date(2026, 11, 1)  # earlier fact kept


@pytest.mark.parametrize("body, status", [
    ({"phone": "727-555-0100", "source": "booking_form"}, 422),
    ({"phone": "727-555-0100", "target_close_date": "soon", "source": "booking_form"}, 422),
    ({"phone": "727-555-0100", "target_close_date": "2026-11-01", "source": "other"}, 422),
    ({"phone": "727-555-0999", "target_close_date": "2026-11-01", "source": "booking_form"}, 404),
])
def test_webhook_rejects_bad_requests(client, enr_db, body, status):
    _make_lead(enr_db)
    assert _post(client, body).status_code == status


def test_webhook_auth_and_flag(client, enr_db, monkeypatch):
    body = {"phone": "727-555-0100", "target_close_date": "2026-11-01", "source": "booking_form"}
    assert _post(client, body, secret="wrong").status_code == 401
    assert _post(client, body, secret=None).status_code == 401
    from src.lending.enrichment import webhook as hook

    monkeypatch.setattr(hook.get_settings(), "lending_enrichment_enabled", False)
    assert _post(client, body).status_code == 503
