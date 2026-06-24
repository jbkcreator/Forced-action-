"""
E2E tests for Phase 3 A5: Pre-Decision Snapshot Service.

Hits the live server at http://localhost:8001 with real DB state.
No Claude API calls — zero inference cost.

Run with:
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest \
        tests/scenarios/test_e2e_snapshots.py -v -m scenario
"""
from __future__ import annotations

import pytest
import requests
from sqlalchemy import text as sa_text

from config.settings import settings
from src.core.database import get_db_context

BASE = "http://localhost:8001"

# Shared state between ordered tests
_s: dict = {}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def admin_headers():
    r = requests.post(
        f"{BASE}/api/admin/login",
        json={
            "username": settings.admin_username,
            "password": settings.admin_password.get_secret_value(),
        },
        timeout=10,
    )
    assert r.status_code == 200, f"Admin login failed: {r.text}"
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


@pytest.fixture(scope="module")
def feed_uuid():
    """Return a valid event_feed_uuid for a subscriber with a known property."""
    with get_db_context() as db:
        row = db.execute(
            sa_text("""
                SELECT s.event_feed_uuid, s.vertical, ds.property_id
                FROM subscribers s
                JOIN distress_scores ds ON ds.county_id = s.county_id
                WHERE s.status = 'active'
                  AND ds.lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                  AND ds.vertical_scores IS NOT NULL
                ORDER BY ds.score_date DESC
                LIMIT 1
            """)
        ).mappings().one_or_none()
    if row is None:
        pytest.skip("No active subscriber with scored Gold+ property found")
    _s["feed_uuid"] = str(row["event_feed_uuid"])
    _s["vertical"] = row["vertical"]
    _s["property_id"] = row["property_id"]
    return _s["feed_uuid"]


# ---------------------------------------------------------------------------
# T1 — deal-capture (non-skip) creates funded snapshot
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t1_deal_capture_funded_creates_snapshot(feed_uuid, admin_headers):
    """deal-capture with a non-skip bucket creates a snapshot with outcome_status=funded."""
    r = requests.post(
        f"{BASE}/api/deal-capture",
        json={
            "feed_uuid": feed_uuid,
            "property_id": _s["property_id"],
            "deal_size_bucket": "10_25k",
            "deal_amount": 15000,
        },
        timeout=60,  # win graphic + autopsy side effects can be slow
    )
    assert r.status_code == 201, r.text
    deal_outcome_id = r.json().get("deal_outcome_id") or r.json().get("id")

    with get_db_context() as db:
        row = db.execute(
            sa_text("""
                SELECT id, outcome_status, all_vertical_scores, runner_up_verticals,
                       selected_vertical, resolved_at
                FROM pre_decision_snapshots
                WHERE property_id = :pid
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"pid": _s["property_id"]},
        ).mappings().one_or_none()

    assert row is not None, "No snapshot row found after deal-capture"
    assert row["outcome_status"] == "funded"
    assert row["resolved_at"] is not None
    assert isinstance(row["all_vertical_scores"], dict)
    assert len(row["all_vertical_scores"]) == 6
    assert row["selected_vertical"] == _s["vertical"]

    _s["snapshot_id"] = str(row["id"])
    _s["funded_property_id"] = _s["property_id"]


# ---------------------------------------------------------------------------
# T2 — deal-capture (skip/CLOSED_LOST) creates lost snapshot
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t2_deal_capture_skip_creates_lost_snapshot(feed_uuid, admin_headers):
    """deal-capture with deal_size_bucket=skip creates outcome_status=lost snapshot."""
    with get_db_context() as db:
        row = db.execute(
            sa_text("""
                SELECT s.event_feed_uuid, s.vertical, ds.property_id
                FROM subscribers s
                JOIN distress_scores ds ON ds.county_id = s.county_id
                WHERE s.status = 'active'
                  AND ds.lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                  AND ds.property_id != :skip_pid
                ORDER BY ds.score_date DESC
                LIMIT 1
            """),
            {"skip_pid": _s["property_id"]},
        ).mappings().one_or_none()

    if row is None:
        pytest.skip("No second property available for skip test")

    skip_pid = row["property_id"]
    r = requests.post(
        f"{BASE}/api/deal-capture",
        json={
            "feed_uuid": str(row["event_feed_uuid"]),
            "property_id": skip_pid,
            "deal_size_bucket": "skip",
        },
        timeout=60,  # skip path triggers loss autopsy (Claude call)
    )
    assert r.status_code == 201, r.text

    with get_db_context() as db:
        snap = db.execute(
            sa_text("""
                SELECT outcome_status, runner_up_verticals
                FROM pre_decision_snapshots
                WHERE property_id = :pid
                ORDER BY created_at DESC LIMIT 1
            """),
            {"pid": skip_pid},
        ).mappings().one_or_none()

    assert snap is not None, "No snapshot for skip deal"
    assert snap["outcome_status"] == "lost"
    assert snap["runner_up_verticals"] is not None
    assert len(snap["runner_up_verticals"]) <= 3

    _s["skip_property_id"] = skip_pid


# ---------------------------------------------------------------------------
# T3 — GET list filtered by property_id returns T1 row
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t3_list_filter_by_property_id(admin_headers):
    """GET /api/admin/snapshots?property_id=... returns the T1 snapshot."""
    if "funded_property_id" not in _s:
        pytest.skip("T1 did not complete — skipping T3")
    r = requests.get(
        f"{BASE}/api/admin/snapshots",
        params={"property_id": _s["funded_property_id"]},
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] >= 1
    ids = [item["id"] for item in body["items"]]
    assert _s["snapshot_id"] in ids


# ---------------------------------------------------------------------------
# T4 — GET detail returns full raw_context and matching selected_vertical
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t4_get_detail(admin_headers):
    """GET /api/admin/snapshots/{id} returns raw_context and correct selected_vertical."""
    if "snapshot_id" not in _s:
        pytest.skip("T1 did not complete — skipping T4")
    r = requests.get(
        f"{BASE}/api/admin/snapshots/{_s['snapshot_id']}",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["raw_context"] is not None
    assert body["raw_context"] != {}
    assert body["selected_vertical"] == _s["vertical"]
    assert body["all_vertical_scores"] is not None
    assert len(body["all_vertical_scores"]) == 6


# ---------------------------------------------------------------------------
# T5 — Idempotency: same deal_outcome_id produces exactly 1 snapshot row
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t5_idempotent_snapshot(admin_headers):
    """Snapshot capture is idempotent — DB has exactly 1 row per deal_outcome_id."""
    if "funded_property_id" not in _s:
        pytest.skip("T1 did not complete — skipping T5")
    r = requests.get(
        f"{BASE}/api/admin/snapshots",
        params={"property_id": _s["funded_property_id"]},
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200
    items = r.json()["items"]
    # All items for this property should have distinct deal_outcome_ids
    deal_outcome_ids = [i["deal_outcome_id"] for i in items if i["deal_outcome_id"]]
    assert len(deal_outcome_ids) == len(set(deal_outcome_ids)), (
        "Duplicate deal_outcome_ids found — idempotency violated"
    )


# ---------------------------------------------------------------------------
# T6 — CRM PATCH to closed_lost updates snapshot outcome_status
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t6_crm_patch_creates_snapshot(admin_headers):
    """CRM PATCH pipeline_stage=closed_lost on a deal with no snapshot creates one with outcome_status=lost."""
    with get_db_context() as db:
        # Find a deal_outcome with no snapshot yet (pre-A5 rows)
        row = db.execute(
            sa_text("""
                SELECT d.id AS deal_id, d.property_id
                FROM deal_outcomes d
                LEFT JOIN pre_decision_snapshots pds ON pds.deal_outcome_id = d.id
                WHERE pds.id IS NULL
                  AND d.pipeline_stage NOT IN ('closed_lost', 'declined', 'closed_won')
                  AND d.property_id IS NOT NULL
                ORDER BY d.created_at DESC
                LIMIT 1
            """)
        ).mappings().one_or_none()

    if row is None:
        pytest.skip("No deal_outcome without a snapshot in a non-terminal stage")

    deal_id = row["deal_id"]
    property_id = row["property_id"]

    r = requests.patch(
        f"{BASE}/api/admin/deals/{deal_id}",
        json={"pipeline_stage": "closed_lost"},
        headers=admin_headers,
        timeout=30,
    )
    assert r.status_code == 200, r.text

    with get_db_context() as db:
        snap = db.execute(
            sa_text("""
                SELECT outcome_status, resolved_at, property_id
                FROM pre_decision_snapshots
                WHERE deal_outcome_id = :did
            """),
            {"did": deal_id},
        ).mappings().one_or_none()

    assert snap is not None, "No snapshot created by CRM PATCH"
    assert snap["outcome_status"] == "lost"
    assert snap["resolved_at"] is not None
    assert snap["property_id"] == property_id
