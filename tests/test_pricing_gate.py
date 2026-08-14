"""
Tests for pricing_truth gate on GET /api/deal-room/{token}.

No real DB or Stripe calls — DB (via dependency override) and pricing_truth are mocked.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


def _fake_db_row():
    """Simulate a deal_rooms DB row."""
    return SimpleNamespace(
        token="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        prospect_name="Jane Doe",
        zip_code="33601",
        tier="starter",
        job_value=5000.0,
        held_at=None,
        expires_at=None,
        converted_at=None,
    )


def _make_db_override(row):
    """Return a FastAPI dependency override that yields a mock session."""
    def _override():
        result_mock = MagicMock()
        result_mock.fetchone.return_value = row
        db_mock = MagicMock()
        db_mock.execute.return_value = result_mock
        yield db_mock
    return _override


TOKEN = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


# ---------------------------------------------------------------------------
# pricing_truth.check returns mismatch → 503
# ---------------------------------------------------------------------------

def test_get_deal_room_pricing_mismatch_returns_503():
    from src.api.main import app
    from src.api.deps import get_db

    mismatch = {"surface": "subscription", "tier": "starter_founding",
                 "displayed_cents": 60000, "stripe_cents": 59900}
    pt_result = {"ok": False, "mismatches": [mismatch]}

    app.dependency_overrides[get_db] = _make_db_override(_fake_db_row())
    try:
        with patch("src.services.pricing_truth.check", return_value=pt_result):
            resp = TestClient(app).get(f"/api/deal-room/{TOKEN}")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert resp.status_code == 503
    body = resp.json()
    assert body["detail"]["detail"] == "Pricing inconsistency detected"
    assert body["detail"]["mismatches"] == [mismatch]


# ---------------------------------------------------------------------------
# pricing_truth.check returns ok=True → 200
# ---------------------------------------------------------------------------

def test_get_deal_room_pricing_ok_returns_200():
    from src.api.main import app
    from src.api.deps import get_db

    pt_result = {"ok": True, "mismatches": []}

    app.dependency_overrides[get_db] = _make_db_override(_fake_db_row())
    try:
        with patch("src.services.pricing_truth.check", return_value=pt_result), \
             patch("src.api.deal_room_router.get_lead_pool", return_value=[]), \
             patch("src.api.deal_room_router._distress_type_map", return_value={}):
            resp = TestClient(app).get(f"/api/deal-room/{TOKEN}")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert resp.status_code == 200
    body = resp.json()
    assert body["token"] == TOKEN
    assert body["prospect_name"] == "Jane Doe"


# ---------------------------------------------------------------------------
# pricing_truth.check raises exception → 503 (fail closed)
# ---------------------------------------------------------------------------

def test_get_deal_room_pricing_exception_returns_503():
    from src.api.main import app
    from src.api.deps import get_db

    app.dependency_overrides[get_db] = _make_db_override(_fake_db_row())
    try:
        with patch("src.services.pricing_truth.check", side_effect=RuntimeError("Stripe down")):
            resp = TestClient(app).get(f"/api/deal-room/{TOKEN}")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert resp.status_code == 503
    body = resp.json()
    assert body["detail"]["detail"] == "Pricing inconsistency detected"
    assert body["detail"]["mismatches"] == []
