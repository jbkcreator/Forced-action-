"""Closer Cockpit backend tests (Sprint S1b).

Pure-function tagging tests run anywhere. API/webhook tests use the real-PG
`fresh_db` fixture (rolled back per test); the webhook signature tests reject
before any DB write.
"""
import hashlib
import hmac
import json

import pytest

from src.services.closer_call_tagging import _coerce_tags, _extract_json


# ── Tagging: JSON extraction ────────────────────────────────────────────────

def test_extract_json_plain():
    assert _extract_json('{"a": 1}') == {"a": 1}


def test_extract_json_fenced():
    assert _extract_json('```json\n{"a": 1}\n```') == {"a": 1}


def test_extract_json_embedded_prose():
    assert _extract_json('Here are the tags: {"a": 1}. Done.')["a"] == 1


def test_extract_json_garbage_returns_none():
    assert _extract_json("no json at all") is None


# ── Tagging: coercion to controlled vocab ───────────────────────────────────

def test_coerce_filters_unknown_objections_and_resolution():
    raw = {
        "objections": ["price_too_high", "totally_made_up"],
        "objection_resolved": "weird_value",
        "call_outcome": "committed",
        "follow_ups": [{"commitment": "email pricing"}, {"nope": 1}],
    }
    t = _coerce_tags(raw)
    assert t["objections"] == ["price_too_high"]
    assert t["objection_resolved"] == "none"          # invalid -> none
    assert t["call_outcome"] == "committed"
    assert t["follow_ups"] == [{"commitment": "email pricing", "due_date": None}]


def test_coerce_invalid_outcome_becomes_none():
    assert _coerce_tags({"call_outcome": "bogus"})["call_outcome"] is None


def test_coerce_empty():
    t = _coerce_tags({})
    assert t == {"objections": [], "objection_resolved": "none",
                 "call_outcome": None, "follow_ups": []}


# ── API / webhook ───────────────────────────────────────────────────────────

@pytest.fixture
def client(fresh_db):
    from fastapi.testclient import TestClient

    from src.api.admin_router import get_current_admin
    from src.api.deps import get_db
    from src.api.main import app

    app.dependency_overrides[get_db] = lambda: fresh_db
    app.dependency_overrides[get_current_admin] = lambda: {"sub": "test-admin"}
    yield TestClient(app)
    app.dependency_overrides.clear()


def _make_subscriber(session) -> int:
    from src.core.models import Subscriber
    s = Subscriber(
        stripe_customer_id="cus_test_closer_calls",
        tier="pro",
        vertical="roofing",
        county_id="hillsborough",
    )
    session.add(s)
    session.flush()
    return s.id


def test_correlate_unknown_subscriber_404(client):
    r = client.post("/api/admin/closer-calls", json={
        "aircall_call_id": "tc_corr_404", "subscriber_id": 999999999,
    })
    assert r.status_code == 404


def test_correlate_then_idempotent(client, fresh_db):
    sid = _make_subscriber(fresh_db)
    body = {"aircall_call_id": "tc_corr_idem", "subscriber_id": sid, "dialed_e164": "813-555-1234"}
    r1 = client.post("/api/admin/closer-calls", json=body)
    assert r1.status_code == 201
    assert r1.json()["aircall_call_id"] == "tc_corr_idem"
    assert r1.json()["status"] == "pending"
    r2 = client.post("/api/admin/closer-calls", json=body)
    assert r2.status_code == 200  # idempotent return-existing
    assert r2.json()["id"] == r1.json()["id"]


def test_feedback_validation_and_happy(client, fresh_db):
    sid = _make_subscriber(fresh_db)
    from src.core.models import CloserCall
    row = CloserCall(aircall_call_id="tc_fb", subscriber_id=sid)
    fresh_db.add(row)
    fresh_db.flush()

    # invalid objection_type -> 422
    bad = client.post(f"/api/admin/closer-calls/{row.id}/feedback",
                      json={"objection_type": "nonsense"})
    assert bad.status_code == 422

    # invalid rating -> 422
    bad2 = client.post(f"/api/admin/closer-calls/{row.id}/feedback",
                       json={"lead_quality_rating": 9})
    assert bad2.status_code == 422

    # valid -> 200
    ok = client.post(f"/api/admin/closer-calls/{row.id}/feedback", json={
        "objection_type": "price_too_high", "pitch_variant": "roi_first",
        "lead_quality_rating": 4,
    })
    assert ok.status_code == 200
    assert ok.json()["lead_quality_rating"] == 4


def test_feedback_missing_call_404(client):
    r = client.post("/api/admin/closer-calls/987654321/feedback",
                    json={"lead_quality_rating": 3})
    assert r.status_code == 404


def test_webhook_missing_signature_401(client):
    r = client.post("/webhooks/aircall", json={"event": "call.ended", "data": {"id": 1}})
    assert r.status_code == 401


def test_webhook_bad_signature_401(client):
    r = client.post(
        "/webhooks/aircall",
        data=json.dumps({"event": "call.ended", "data": {"id": 1}}),
        headers={"X-Aircall-Signature": "deadbeef", "Content-Type": "application/json"},
    )
    assert r.status_code == 401


def test_webhook_valid_signature_ignored_event_200(client):
    """A correctly-signed but irrelevant event returns 200 without DB writes."""
    from config.settings import get_settings
    token = get_settings().aircall_webhook_token
    if not token:
        pytest.skip("AIRCALL_WEBHOOK_TOKEN not configured")
    body = json.dumps({"event": "call.created", "data": {"id": 999}}).encode()
    sig = hmac.new(token.get_secret_value().encode(), body, hashlib.sha256).hexdigest()
    r = client.post("/webhooks/aircall", data=body,
                    headers={"X-Aircall-Signature": sig, "Content-Type": "application/json"})
    assert r.status_code == 200
    assert r.json() == {"ok": True}
