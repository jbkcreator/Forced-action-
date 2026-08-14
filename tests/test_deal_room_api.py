"""
HTTP-layer tests for:
  - GET  /api/deal-room/{token}         (ticket 04 — public read endpoint)
  - POST /api/admin/deal-room           (ticket 05 — admin generator)

No Postgres required — DB and service calls are mocked.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    from src.api.main import app
    return TestClient(app)


@pytest.fixture
def admin_headers(monkeypatch):
    from config.settings import settings
    from pydantic import SecretStr
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    monkeypatch.setattr(settings, "admin_password", SecretStr("test-admin-pass"))
    from src.api.admin_router import create_access_token
    return {"Authorization": f"Bearer {create_access_token({'sub': 'admin', 'scope': 'admin'})}"}


_VALID_BODY = {
    "prospect_name": "Test Prospect",
    "prospect_email": "prospect@example.com",
    "zip_code": "33601",
    "tier": "starter",
    "job_value": 5000.0,
    "close_rate": 0.3,
}


def _fake_deal_room():
    return SimpleNamespace(token="fake-token-abc123")


def _mock_db():
    """Return a MagicMock session with commit() as a no-op."""
    db = MagicMock()
    db.commit.return_value = None
    return db


# ---------------------------------------------------------------------------
# Auth guard
# ---------------------------------------------------------------------------


class TestAuth:
    def test_unauthenticated_rejected(self, client):
        r = client.post("/api/admin/deal-room", json=_VALID_BODY)
        assert r.status_code in (401, 403)


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------


class TestCreateDealRoom:
    def test_success_returns_both_urls(self, client, admin_headers, monkeypatch):
        from src.api.main import app
        from src.api.deps import get_db

        db = _mock_db()
        app.dependency_overrides[get_db] = lambda: db

        try:
            with (
                patch(
                    "src.api.deal_room_router.get_lead_pool",
                    return_value=[{"id": 1, "address": "123 Main St"}],
                ),
                patch(
                    "src.api.deal_room_router.create_deal_room",
                    return_value=_fake_deal_room(),
                ),
                patch(
                    "src.api.deal_room_router.get_settings",
                    return_value=SimpleNamespace(app_base_url="https://forcedactionleads.com"),
                ),
            ):
                r = client.post("/api/admin/deal-room", json=_VALID_BODY, headers=admin_headers)
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert r.status_code == 201
        body = r.json()
        assert "deal_room_url" in body
        assert "prefilled_checkout_url" in body
        assert "fake-token-abc123" in body["deal_room_url"]
        assert "fake-token-abc123" in body["prefilled_checkout_url"]
        assert "start_tier=starter" in body["prefilled_checkout_url"]
        assert "zip=33601" in body["prefilled_checkout_url"]

    def test_deal_room_url_uses_base_url(self, client, admin_headers):
        from src.api.main import app
        from src.api.deps import get_db

        db = _mock_db()
        app.dependency_overrides[get_db] = lambda: db

        try:
            with (
                patch("src.api.deal_room_router.get_lead_pool", return_value=[]),
                patch(
                    "src.api.deal_room_router.create_deal_room",
                    return_value=_fake_deal_room(),
                ),
                patch(
                    "src.api.deal_room_router.get_settings",
                    return_value=SimpleNamespace(app_base_url="https://forcedactionleads.com/"),
                ),
            ):
                r = client.post("/api/admin/deal-room", json=_VALID_BODY, headers=admin_headers)
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert r.status_code == 201
        # Trailing slash on base_url should not produce double slash
        assert "//deal-room" not in r.json()["deal_room_url"]


# ---------------------------------------------------------------------------
# Non-available ZIP → 409
# ---------------------------------------------------------------------------


class TestUnavailableZip:
    def test_unavailable_zip_returns_409(self, client, admin_headers):
        from fastapi import HTTPException

        from src.api.main import app
        from src.api.deps import get_db

        db = _mock_db()
        app.dependency_overrides[get_db] = lambda: db

        try:
            with (
                patch("src.api.deal_room_router.get_lead_pool", return_value=[]),
                patch(
                    "src.api.deal_room_router.create_deal_room",
                    side_effect=HTTPException(
                        status_code=409,
                        detail="ZIP 33601 is not available for a hold deposit (current status: held).",
                    ),
                ),
                patch(
                    "src.api.deal_room_router.get_settings",
                    return_value=SimpleNamespace(app_base_url="https://forcedactionleads.com"),
                ),
            ):
                r = client.post("/api/admin/deal-room", json=_VALID_BODY, headers=admin_headers)
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert r.status_code == 409
        assert "not available" in r.json()["detail"]


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_invalid_tier_rejected(self, client, admin_headers):
        body = {**_VALID_BODY, "tier": "dominator"}
        r = client.post("/api/admin/deal-room", json=body, headers=admin_headers)
        assert r.status_code == 422

    def test_invalid_zip_rejected(self, client, admin_headers):
        body = {**_VALID_BODY, "zip_code": "ABC12"}
        r = client.post("/api/admin/deal-room", json=body, headers=admin_headers)
        assert r.status_code == 422

    def test_invalid_email_rejected(self, client, admin_headers):
        body = {**_VALID_BODY, "prospect_email": "not-an-email"}
        r = client.post("/api/admin/deal-room", json=body, headers=admin_headers)
        assert r.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/deal-room/{token} — ticket 04
# ---------------------------------------------------------------------------

from datetime import datetime, timedelta, timezone  # noqa: E402


def _make_db_row(
    *,
    token: str = "test-token-abc",
    prospect_name: str = "Jane Roofer",
    zip_code: str = "33601",
    tier: str = "starter",
    job_value: float = 5000.0,
    held_at: datetime | None = None,
    expires_at: datetime | None = None,
    converted_at: datetime | None = None,
):
    """Return a SimpleNamespace that mimics a SQLAlchemy Row for deal_rooms."""
    return SimpleNamespace(
        token=token,
        prospect_name=prospect_name,
        zip_code=zip_code,
        tier=tier,
        job_value=job_value,
        held_at=held_at,
        expires_at=expires_at,
        converted_at=converted_at,
    )


def _db_with_row(row):
    """Return a mock Session whose execute().fetchone() returns `row`."""
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = row
    return db


class TestGetDealRoom:
    """Tests for GET /api/deal-room/{token}."""

    def _override_db(self, app, db):
        from src.api.deps import get_db
        app.dependency_overrides[get_db] = lambda: db
        return db

    def _clear_db(self, app):
        from src.api.deps import get_db
        app.dependency_overrides.pop(get_db, None)

    def test_unknown_token_returns_404(self, client):
        from src.api.main import app

        db = MagicMock()
        db.execute.return_value.fetchone.return_value = None
        self._override_db(app, db)

        try:
            r = client.get("/api/deal-room/no-such-token")
        finally:
            self._clear_db(app)

        assert r.status_code == 404

    def test_pre_hold_returns_200_with_correct_shape(self, client, monkeypatch):
        from src.api.main import app

        row = _make_db_row()
        db = _db_with_row(row)
        self._override_db(app, db)

        try:
            with patch("src.api.deal_room_router.get_lead_pool", return_value=[]):
                r = client.get(f"/api/deal-room/{row.token}")
        finally:
            self._clear_db(app)

        assert r.status_code == 200
        body = r.json()
        assert body["token"] == row.token
        assert body["prospect_name"] == row.prospect_name
        assert body["zip"] == row.zip_code
        assert body["tier"] == row.tier
        assert body["hold_state"] == "available"
        assert body["held_at"] is None
        assert body["expires_at"] is None
        assert isinstance(body["properties"], list)
        assert isinstance(body["roi"], list)
        assert len(body["roi"]) == 3  # starter, pro, founder

    def test_held_state_returned_correctly(self, client):
        from src.api.main import app

        now = datetime.now(timezone.utc)
        row = _make_db_row(
            held_at=now - timedelta(hours=1),
            expires_at=now + timedelta(hours=47),
        )
        db = _db_with_row(row)
        self._override_db(app, db)

        try:
            with patch("src.api.deal_room_router.get_lead_pool", return_value=[]):
                r = client.get(f"/api/deal-room/{row.token}")
        finally:
            self._clear_db(app)

        assert r.status_code == 200
        assert r.json()["hold_state"] == "held"
        assert r.json()["held_at"] is not None

    def test_expired_state_returned_correctly(self, client):
        from src.api.main import app

        now = datetime.now(timezone.utc)
        row = _make_db_row(
            held_at=now - timedelta(hours=50),
            expires_at=now - timedelta(hours=2),
        )
        db = _db_with_row(row)
        self._override_db(app, db)

        try:
            with patch("src.api.deal_room_router.get_lead_pool", return_value=[]):
                r = client.get(f"/api/deal-room/{row.token}")
        finally:
            self._clear_db(app)

        assert r.status_code == 200
        assert r.json()["hold_state"] == "expired"

    def test_properties_snapshot_not_in_response(self, client):
        from src.api.main import app

        row = _make_db_row()
        db = _db_with_row(row)
        self._override_db(app, db)

        try:
            with patch("src.api.deal_room_router.get_lead_pool", return_value=[]):
                r = client.get(f"/api/deal-room/{row.token}")
        finally:
            self._clear_db(app)

        assert "properties_snapshot" not in r.json()

    def test_roi_covers_starter_pro_founder(self, client):
        from src.api.main import app

        row = _make_db_row(job_value=9700.0)
        db = _db_with_row(row)
        self._override_db(app, db)

        try:
            with patch("src.api.deal_room_router.get_lead_pool", return_value=[]):
                r = client.get(f"/api/deal-room/{row.token}")
        finally:
            self._clear_db(app)

        roi = r.json()["roi"]
        tiers = {entry["tier"] for entry in roi}
        assert tiers == {"starter", "pro", "founder"}

    def test_roi_payback_multiple_computed(self, client):
        from src.api.main import app

        row = _make_db_row(job_value=970.0)  # starter multiple = 970/97 = 10.0
        db = _db_with_row(row)
        self._override_db(app, db)

        try:
            with patch("src.api.deal_room_router.get_lead_pool", return_value=[]):
                r = client.get(f"/api/deal-room/{row.token}")
        finally:
            self._clear_db(app)

        starter_roi = next(e for e in r.json()["roi"] if e["tier"] == "starter")
        assert starter_roi["payback_multiple"] == 10.0
        assert starter_roi["price"] == 97

    def test_angi_comparison_uses_config_constant(self, client):
        from src.api.main import app

        row = _make_db_row()
        db = _db_with_row(row)
        self._override_db(app, db)

        fake_settings = SimpleNamespace(angi_shared_lead_cost=99)

        try:
            with (
                patch("src.api.deal_room_router.get_lead_pool", return_value=[]),
                patch("src.api.deal_room_router.get_settings", return_value=fake_settings),
            ):
                r = client.get(f"/api/deal-room/{row.token}")
        finally:
            self._clear_db(app)

        for entry in r.json()["roi"]:
            assert "$99" in entry["angi_comparison"]
