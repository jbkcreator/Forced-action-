"""
E2E tests for Phase 3 A1: Loss Autopsy Engine.

Hits the live server at http://localhost:8001 with real Claude API calls
and real DB state. Run with:

    PYTHONPATH=. .venv/Scripts/python.exe -m pytest \
        tests/scenarios/test_e2e_loss_autopsy.py -v -m scenario
"""
from __future__ import annotations

import pytest
import requests
from sqlalchemy import text as sa_text

from config.settings import settings
from src.core.database import get_db_context

BASE = "http://localhost:8001"

_VALID_REJECTION_REASONS = {
    "PRICING_TOO_HIGH",
    "COMPETITOR_WON",
    "UNDERWRITING_REJECTED",
    "CONTACT_EXHAUSTED",
    "TIMELINE_MISMATCH",
    "PROPERTY_CONDITION",
    "OWNER_UNRESPONSIVE",
    "BROKER_CONFLICT",
    "UNKNOWN",
}

# Module-level state shared between ordered test functions
_s: dict = {}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def admin_headers():
    """Login to the live server and return bearer headers."""
    r = requests.post(
        f"{BASE}/api/admin/login",
        json={
            "username": settings.admin_username,
            "password": settings.admin_password.get_secret_value(),
        },
        timeout=10,
    )
    assert r.status_code == 200, f"Admin login failed: {r.text}"
    token = r.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture(scope="module")
def real_property_id():
    """Return a Gold+ property_id with a distress score from the live DB."""
    with get_db_context() as db:
        pid = db.execute(
            sa_text("""
                SELECT ds.property_id
                FROM distress_scores ds
                WHERE ds.lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                ORDER BY ds.score_date DESC
                LIMIT 1
            """)
        ).scalar_one_or_none()
    assert pid is not None, "No Gold+ property found in DB — run CDS scoring first"
    return pid


@pytest.fixture(scope="module")
def subscriber_feed_uuid():
    """Return the event_feed_uuid of the first active subscriber."""
    with get_db_context() as db:
        fuuid = db.execute(
            sa_text("""
                SELECT event_feed_uuid FROM subscribers
                WHERE event_feed_uuid IS NOT NULL AND tier IS NOT NULL
                LIMIT 1
            """)
        ).scalar_one_or_none()
    assert fuuid is not None, "No subscriber with feed_uuid found in DB"
    return fuuid


@pytest.fixture(scope="module")
def real_subscriber_id():
    """Return a subscriber id for direct DB inserts in T6."""
    with get_db_context() as db:
        sid = db.execute(
            sa_text("SELECT id FROM subscribers WHERE tier IS NOT NULL LIMIT 1")
        ).scalar_one_or_none()
    assert sid is not None, "No subscriber found in DB"
    return sid


# ---------------------------------------------------------------------------
# T1 — Manual admin trigger: GHOSTED_SLA (smoke test)
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t1_admin_trigger_ghosted_sla(admin_headers, real_property_id):
    """POST /api/admin/loss-autopsies/trigger → 201, row in DB, Claude cost logged."""
    r = requests.post(
        f"{BASE}/api/admin/loss-autopsies/trigger",
        json={"property_id": real_property_id, "trigger_reason": "GHOSTED_SLA"},
        headers=admin_headers,
        timeout=30,
    )
    assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"

    body = r.json()
    assert "id" in body
    assert body["trigger_reason"] == "GHOSTED_SLA"
    assert body["primary_rejection_reason"] in _VALID_REJECTION_REASONS, (
        f"Unexpected reason: {body['primary_rejection_reason']}"
    )
    assert body["cora_behavior_adjustment"], "cora_behavior_adjustment must be non-empty"
    assert body["claude_cost_usd"] is not None and body["claude_cost_usd"] > 0

    # Verify DB row
    autopsy_id = body["id"]
    with get_db_context() as db:
        row = db.execute(
            sa_text("SELECT * FROM loss_autopsies WHERE id = :id"),
            {"id": autopsy_id},
        ).mappings().first()
    assert row is not None, "loss_autopsies row not found in DB"
    assert row["trigger_reason"] == "GHOSTED_SLA"
    assert row["raw_context"] and row["raw_context"] != {}
    assert row["model_response"] and "tool_input" in row["model_response"]
    assert float(row["claude_cost_usd"]) > 0

    # Verify api_usage_logs entry
    with get_db_context() as db:
        log_row = db.execute(
            sa_text("""
                SELECT id FROM api_usage_logs
                WHERE task_type = 'loss_autopsy'
                ORDER BY created_at DESC LIMIT 1
            """)
        ).scalar_one_or_none()
    assert log_row is not None, "api_usage_logs entry for loss_autopsy not found"

    # Persist for later tests
    _s["t1_autopsy_id"] = autopsy_id
    _s["property_id"] = real_property_id


# ---------------------------------------------------------------------------
# T2 — List endpoint
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t2_list_loss_autopsies(admin_headers):
    """GET /api/admin/loss-autopsies?property_id=X → items include T1 row."""
    property_id = _s["property_id"]
    r = requests.get(
        f"{BASE}/api/admin/loss-autopsies",
        params={"property_id": property_id},
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] >= 1
    ids = [item["id"] for item in body["items"]]
    assert _s["t1_autopsy_id"] in ids, "T1 autopsy not found in list response"


# ---------------------------------------------------------------------------
# T3 — Detail endpoint
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t3_detail_loss_autopsy(admin_headers):
    """GET /api/admin/loss-autopsies/{id} → raw_context contains property data."""
    autopsy_id = _s["t1_autopsy_id"]
    r = requests.get(
        f"{BASE}/api/admin/loss-autopsies/{autopsy_id}",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert isinstance(body["raw_context"], dict) and body["raw_context"]
    assert isinstance(body["model_response"], dict)
    assert "tool_input" in body["model_response"]
    # Context gathering fired for the right property
    assert body["raw_context"].get("property", {}).get("id") == _s["property_id"]


# ---------------------------------------------------------------------------
# T4 — Deal-capture path (CLOSED_LOST)
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t4_deal_capture_closed_lost(subscriber_feed_uuid, real_property_id):
    """POST /api/deal-capture with bucket=skip → deal_outcome + loss_autopsy rows created."""
    r = requests.post(
        f"{BASE}/api/deal-capture",
        json={
            "feed_uuid": subscriber_feed_uuid,
            "property_id": real_property_id,
            "deal_size_bucket": "skip",
        },
        timeout=30,
    )
    assert r.status_code == 201, f"deal-capture failed: {r.text}"

    # Find the deal_outcome row
    with get_db_context() as db:
        deal_row = db.execute(
            sa_text("""
                SELECT id FROM deal_outcomes
                WHERE property_id = :pid AND pipeline_stage = 'closed_lost'
                ORDER BY created_at DESC LIMIT 1
            """),
            {"pid": real_property_id},
        ).mappings().first()
    assert deal_row is not None, "deal_outcomes row with pipeline_stage=closed_lost not found"
    deal_outcome_id = deal_row["id"]

    # Find the corresponding loss_autopsy
    with get_db_context() as db:
        autopsy_row = db.execute(
            sa_text("""
                SELECT id, trigger_reason, primary_rejection_reason
                FROM loss_autopsies
                WHERE deal_outcome_id = :did
            """),
            {"did": deal_outcome_id},
        ).mappings().first()
    assert autopsy_row is not None, "loss_autopsies row for deal_outcome_id not found"
    assert autopsy_row["trigger_reason"] == "CLOSED_LOST"
    assert autopsy_row["primary_rejection_reason"] in _VALID_REJECTION_REASONS

    _s["t4_deal_outcome_id"] = deal_outcome_id


# ---------------------------------------------------------------------------
# T5 — Idempotency guard
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t5_idempotency_guard(admin_headers, real_property_id):
    """Triggering again for the same deal_outcome_id returns 409."""
    deal_outcome_id = _s["t4_deal_outcome_id"]
    r = requests.post(
        f"{BASE}/api/admin/loss-autopsies/trigger",
        json={
            "property_id": real_property_id,
            "trigger_reason": "CLOSED_LOST",
            "deal_outcome_id": deal_outcome_id,
        },
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 409, f"Expected 409 (idempotency), got {r.status_code}: {r.text}"

    # Confirm still exactly one row for this deal_outcome_id
    with get_db_context() as db:
        count = db.execute(
            sa_text("SELECT count(*) FROM loss_autopsies WHERE deal_outcome_id = :did"),
            {"did": deal_outcome_id},
        ).scalar()
    assert count == 1, f"Expected 1 autopsy row, found {count}"


# ---------------------------------------------------------------------------
# T6 — Operator CRM PATCH path
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t6_operator_crm_patch(admin_headers, real_subscriber_id, real_property_id):
    """PATCH /api/admin/deals/{id} to closed_lost → loss_autopsy row created."""
    # Seed a deal_outcome in 'lead' stage so we can PATCH it
    with get_db_context() as db:
        deal_id = db.execute(
            sa_text("""
                INSERT INTO deal_outcomes
                    (subscriber_id, property_id, deal_size_bucket, pipeline_stage, created_at)
                VALUES
                    (:sid, :pid, 'skip', 'lead', now())
                RETURNING id
            """),
            {"sid": real_subscriber_id, "pid": real_property_id},
        ).scalar_one()

    # PATCH to closed_lost
    r = requests.patch(
        f"{BASE}/api/admin/deals/{deal_id}",
        json={"pipeline_stage": "closed_lost"},
        headers=admin_headers,
        timeout=30,
    )
    assert r.status_code == 200, f"PATCH failed: {r.text}"
    body = r.json()
    assert body["to_stage"] == "closed_lost"

    # Assert loss_autopsy row was created
    with get_db_context() as db:
        autopsy_row = db.execute(
            sa_text("""
                SELECT trigger_reason, primary_rejection_reason
                FROM loss_autopsies
                WHERE deal_outcome_id = :did
            """),
            {"did": deal_id},
        ).mappings().first()
    assert autopsy_row is not None, "loss_autopsies row not created after PATCH"
    assert autopsy_row["trigger_reason"] == "CLOSED_LOST"
    assert autopsy_row["primary_rejection_reason"] in _VALID_REJECTION_REASONS
