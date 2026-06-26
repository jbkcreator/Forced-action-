"""
E2E tests for Sprint 4.6: Underwriting Reason-Code Feedback Integration.

Hits the live server at http://localhost:8001 with real DB state.
No external API calls are made.

Run with:
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest \
        tests/scenarios/test_e2e_underwriting.py -v -m scenario
"""
from __future__ import annotations

import pytest
import requests
from sqlalchemy import text as sa_text

from config.scoring import UNDERWRITING_REASON_SIGNAL_NUDGES, VERTICAL_WEIGHTS
from config.settings import settings
from src.core.database import get_db_context

BASE = "http://localhost:8001"

# Shared test state
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
def target_parcel():
    """Return a parcel_id + property_id from a Gold+ scored property."""
    with get_db_context() as db:
        row = db.execute(
            sa_text("""
                SELECT p.parcel_id, p.id AS property_id, ds.final_cds_score, ds.lead_tier
                FROM distress_scores ds
                JOIN properties p ON p.id = ds.property_id
                WHERE ds.lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                  AND ds.qualified = TRUE
                ORDER BY ds.score_date DESC
                LIMIT 1
            """)
        ).first()
    assert row is not None, "No scored Gold+ property found — run CDS engine first"
    return {"parcel_id": row.parcel_id, "property_id": row.property_id,
            "baseline_score": float(row.final_cds_score), "baseline_tier": row.lead_tier}


@pytest.fixture(scope="module", autouse=True)
def cleanup(target_parcel):
    """Remove underwriting_feedback rows and any underwriting override rows after tests."""
    yield
    with get_db_context() as db:
        db.execute(
            sa_text("DELETE FROM underwriting_feedback WHERE property_id = :pid"),
            {"pid": target_parcel["property_id"]},
        )
        db.execute(
            sa_text(
                "DELETE FROM scoring_weight_overrides WHERE source = 'underwriting_feedback'"
            )
        )
    from src.services.heuristic_loader import invalidate_cache
    invalidate_cache()


# ---------------------------------------------------------------------------
# T1 — 404 for unknown parcel
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t1_unknown_parcel_returns_404(admin_headers):
    r = requests.post(
        f"{BASE}/api/loans/underwriting-feedback",
        json={"parcel_id": "DOES-NOT-EXIST-00000", "reason_code": "ltv_too_high"},
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 404, r.text


# ---------------------------------------------------------------------------
# T2 — 422 for invalid reason_code
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t2_invalid_reason_code_returns_422(admin_headers, target_parcel):
    r = requests.post(
        f"{BASE}/api/loans/underwriting-feedback",
        json={"parcel_id": target_parcel["parcel_id"], "reason_code": "bad_code"},
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 422, r.text


# ---------------------------------------------------------------------------
# T3 — successful feedback submission
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t3_submit_feedback_succeeds(admin_headers, target_parcel):
    r = requests.post(
        f"{BASE}/api/loans/underwriting-feedback",
        json={
            "parcel_id":     target_parcel["parcel_id"],
            "reason_code":   "structural_damage",
            "reason_detail": "Foundation cracking identified during appraisal",
            "lender_id":     "LENDER_E2E_001",
            "loan_amount":   285000.0,
        },
        headers=admin_headers,
        timeout=30,
    )
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["status"] == "ok"
    assert body["parcel_id"] == target_parcel["parcel_id"]
    assert body["reason_code"] == "structural_damage"
    assert body["nudges_applied"] == len(UNDERWRITING_REASON_SIGNAL_NUDGES["structural_damage"])
    assert "new_cds_score" in body
    assert "new_lead_tier" in body
    assert "vertical_scores" in body

    # Score should not have increased after a decline signal
    assert body["new_cds_score"] <= target_parcel["baseline_score"] + 2.0, (
        f"Score unexpectedly rose from {target_parcel['baseline_score']} → {body['new_cds_score']}"
    )

    _s["score_after_structural"] = body["new_cds_score"]
    _s["tier_after_structural"] = body["new_lead_tier"]


# ---------------------------------------------------------------------------
# T4 — weight overrides visible in admin API
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t4_overrides_written_to_weight_table(admin_headers):
    r = requests.get(
        f"{BASE}/api/admin/scoring/weight-overrides",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()

    uw_rows = [
        item for item in body["items"]
        if item["source"] == "underwriting_feedback"
    ]
    assert len(uw_rows) == len(UNDERWRITING_REASON_SIGNAL_NUDGES["structural_damage"]), (
        f"Expected {len(UNDERWRITING_REASON_SIGNAL_NUDGES['structural_damage'])} "
        f"underwriting override rows, got {len(uw_rows)}"
    )

    # Every override must have a negative delta (penalty)
    for row in uw_rows:
        assert row["delta"] < 0, f"Expected negative delta for {row['vertical']}/{row['signal_type']}, got {row['delta']}"
        # effective_weight must stay in [0, 100]
        if row["effective_weight"] is not None:
            assert 0 <= row["effective_weight"] <= 100


# ---------------------------------------------------------------------------
# T5 — audit record persisted in underwriting_feedback table
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t5_audit_row_persisted(target_parcel):
    with get_db_context() as db:
        rows = db.execute(
            sa_text("""
                SELECT reason_code, lender_id, loan_amount
                FROM underwriting_feedback
                WHERE property_id = :pid
                ORDER BY submitted_at DESC
            """),
            {"pid": target_parcel["property_id"]},
        ).fetchall()

    assert len(rows) >= 1
    row = rows[0]
    assert row.reason_code == "structural_damage"
    assert row.lender_id == "LENDER_E2E_001"
    assert float(row.loan_amount) == pytest.approx(285000.0)


# ---------------------------------------------------------------------------
# T6 — GET history endpoint
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t6_get_feedback_history(admin_headers, target_parcel):
    r = requests.get(
        f"{BASE}/api/loans/underwriting-feedback/{target_parcel['parcel_id']}",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["parcel_id"] == target_parcel["parcel_id"]
    assert body["total"] >= 1
    assert len(body["feedback"]) == body["total"]

    first = body["feedback"][0]
    assert first["reason_code"] == "structural_damage"
    assert first["lender_id"] == "LENDER_E2E_001"
    assert first["loan_amount"] == pytest.approx(285000.0)
    assert first["submitted_at"] is not None


# ---------------------------------------------------------------------------
# T7 — second decline compounds the penalty
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t7_second_decline_compounds(admin_headers, target_parcel):
    r = requests.post(
        f"{BASE}/api/loans/underwriting-feedback",
        json={
            "parcel_id":   target_parcel["parcel_id"],
            "reason_code": "flood_zone",
            "lender_id":   "LENDER_E2E_002",
        },
        headers=admin_headers,
        timeout=30,
    )
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["nudges_applied"] == len(UNDERWRITING_REASON_SIGNAL_NUDGES["flood_zone"])

    # Score after two declines should not be higher than after one
    score_after_second = body["new_cds_score"]
    assert score_after_second <= _s.get("score_after_structural", target_parcel["baseline_score"]) + 2.0

    # History should now have 2 rows
    r2 = requests.get(
        f"{BASE}/api/loans/underwriting-feedback/{target_parcel['parcel_id']}",
        headers=admin_headers,
        timeout=10,
    )
    assert r2.json()["total"] == 2


# ---------------------------------------------------------------------------
# T8 — GET 404 for unknown parcel
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t8_get_unknown_parcel_404(admin_headers):
    r = requests.get(
        f"{BASE}/api/loans/underwriting-feedback/DOES-NOT-EXIST-99999",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 404, r.text


# ---------------------------------------------------------------------------
# T9 — no auth returns 401
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t9_no_auth_returns_401(target_parcel):
    r = requests.post(
        f"{BASE}/api/loans/underwriting-feedback",
        json={"parcel_id": target_parcel["parcel_id"], "reason_code": "ltv_too_high"},
        timeout=10,
    )
    assert r.status_code in (401, 403), r.text
