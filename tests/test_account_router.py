"""End-to-end test for the reference route gated by require_tier (fa B1-02)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.account_router import router
from src.services.account_auth import get_current_account
from src.core.database import get_db


def _make_client(tier: str):
    app = FastAPI()
    app.include_router(router)

    account = MagicMock()
    account.account_id = "11111111-1111-1111-1111-111111111111"

    app.dependency_overrides[get_current_account] = lambda: account
    app.dependency_overrides[get_db] = lambda: MagicMock()

    patcher = patch("src.middleware.tier_gate.get_account_tier", return_value=tier)
    patcher.start()
    return TestClient(app), patcher


def test_starter_account_gets_403_on_investor_pro_route():
    client, patcher = _make_client("starter")
    try:
        resp = client.get("/api/account/investor-pro-ping")
        assert resp.status_code == 403
        assert resp.json() == {"detail": "Your plan does not include access to this feature"}
    finally:
        patcher.stop()


def test_investor_pro_account_gets_200_on_investor_pro_route():
    client, patcher = _make_client("investor_pro")
    try:
        resp = client.get("/api/account/investor-pro-ping")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}
    finally:
        patcher.stop()


def test_full_chain_jwt_to_account_to_tier_to_gate_returns_200():
    """Drives the real chain: JWT decode -> get_current_account DB lookup ->
    get_account_tier DB fallback join -> require_tier gate -> HTTP response.

    Only src.core.database.get_db is overridden; get_current_account and
    get_account_tier both run for real.
    """
    from src.services.subscriber_auth import create_access_token

    with patch("src.services.subscriber_auth._subscriber_secret", return_value="test-key"):
        token = create_access_token(1, "feed-uuid-not-used-here")

    account_row = MagicMock()
    account_row.account_id = "11111111-1111-1111-1111-111111111111"
    account_row.status = "active"

    tier_row = MagicMock()
    tier_row.tier = "investor_pro"

    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [account_row, tier_row]

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: db

    with patch("src.services.subscriber_auth._subscriber_secret", return_value="test-key"), \
         patch("src.services.entitlement_service.redis_available", return_value=False):
        client = TestClient(app)
        resp = client.get(
            "/api/account/investor-pro-ping",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
