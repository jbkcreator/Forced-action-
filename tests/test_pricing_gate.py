"""
Tests for the advisory pricing_truth check on GET /api/deal-room/{token}.

The check is advisory (option C): it logs broken price config but must never
block the deal-room from loading. No real DB or Stripe calls — DB (via
dependency override) and pricing_truth are mocked.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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


def _get_room(pt_patch):
    from src.api.main import app
    from src.api.deps import get_db

    app.dependency_overrides[get_db] = _make_db_override(_fake_db_row())
    try:
        with pt_patch, \
             patch("src.api.deal_room_router.get_lead_pool", return_value=[]), \
             patch("src.api.deal_room_router._distress_type_map", return_value={}):
            return TestClient(app).get(f"/api/deal-room/{TOKEN}")
    finally:
        app.dependency_overrides.pop(get_db, None)


# ---------------------------------------------------------------------------
# pricing_truth reports problems → still 200 (advisory, does NOT block)
# ---------------------------------------------------------------------------

def test_get_deal_room_pricing_problem_does_not_block():
    pt_result = {"ok": False, "problems": [
        {"name": "annual_lock", "surface": "subscription", "tier": "annual_lock",
         "price_id": "price_dead", "reason": "not_found"},
    ]}
    resp = _get_room(patch("src.services.pricing_truth.check", return_value=pt_result))
    assert resp.status_code == 200
    assert resp.json()["token"] == TOKEN


# ---------------------------------------------------------------------------
# pricing_truth ok → 200
# ---------------------------------------------------------------------------

def test_get_deal_room_pricing_ok_returns_200():
    pt_result = {"ok": True, "problems": []}
    resp = _get_room(patch("src.services.pricing_truth.check", return_value=pt_result))
    assert resp.status_code == 200
    body = resp.json()
    assert body["token"] == TOKEN
    assert body["prospect_name"] == "Jane Doe"


# ---------------------------------------------------------------------------
# pricing_truth raises → still 200 (advisory failure is swallowed)
# ---------------------------------------------------------------------------

def test_get_deal_room_pricing_exception_does_not_block():
    resp = _get_room(patch("src.services.pricing_truth.check",
                           side_effect=RuntimeError("Stripe down")))
    assert resp.status_code == 200
    assert resp.json()["token"] == TOKEN
