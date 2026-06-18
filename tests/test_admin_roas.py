"""
HTTP-layer tests for GET /api/admin/roas (S2 — ROAS by campaign).

JWT-protected; aggregation + ROAS math verified against a mocked DB session so
no Postgres is needed. Mirrors tests/test_revenue_signal_admin.py conventions.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


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
    return {"Authorization": f"Bearer {create_access_token({'sub': 'admin'})}"}


def _override_db(rows):
    """Return (sess, cleanup) and install a get_db override returning `rows`."""
    from src.api.main import app
    from src.api.deps import get_db
    sess = MagicMock()
    sess.execute.return_value.mappings.return_value.all.return_value = rows
    app.dependency_overrides[get_db] = lambda: sess

    def cleanup():
        app.dependency_overrides.pop(get_db, None)
    return sess, cleanup


def _row(campaign_id=None, utm_campaign=None, total_revenue=0.0, purchase_count=0):
    return {
        "campaign_key": campaign_id or utm_campaign or "unattributed",
        "campaign_id": campaign_id,
        "utm_campaign": utm_campaign,
        "total_revenue": total_revenue,
        "purchase_count": purchase_count,
    }


# ── Auth ─────────────────────────────────────────────────────────────────────

class TestAuth:
    def test_no_token_rejected(self, client):
        r = client.get("/api/admin/roas")
        assert r.status_code in (401, 403)


# ── Aggregation + ROAS math ──────────────────────────────────────────────────

class TestRoas:
    def test_groups_and_computes_roas(self, client, admin_headers):
        rows = [
            _row(campaign_id="fa_test_001", utm_campaign="fa_test_campaign",
                 total_revenue=300.0, purchase_count=3),
            _row(campaign_id=None, utm_campaign=None, total_revenue=99.0, purchase_count=1),
        ]
        sess, cleanup = _override_db(rows)
        try:
            r = client.get("/api/admin/roas?ad_spend=100", headers=admin_headers)
        finally:
            cleanup()
        assert r.status_code == 200
        body = r.json()
        assert len(body) == 2
        first = body[0]
        assert first["campaign_id"] == "fa_test_001"
        assert first["utm_campaign"] == "fa_test_campaign"
        assert first["total_revenue"] == 300.0
        assert first["purchase_count"] == 3
        assert first["ad_spend"] == 100.0
        assert first["roas"] == 3.0

    def test_roas_null_when_spend_missing(self, client, admin_headers):
        rows = [_row(campaign_id="c1", total_revenue=150.0, purchase_count=2)]
        sess, cleanup = _override_db(rows)
        try:
            r = client.get("/api/admin/roas", headers=admin_headers)
        finally:
            cleanup()
        assert r.status_code == 200
        body = r.json()
        assert body[0]["roas"] is None
        assert body[0]["ad_spend"] == 0.0

    def test_roas_null_when_spend_zero(self, client, admin_headers):
        rows = [_row(campaign_id="c1", total_revenue=150.0, purchase_count=2)]
        sess, cleanup = _override_db(rows)
        try:
            r = client.get("/api/admin/roas?ad_spend=0", headers=admin_headers)
        finally:
            cleanup()
        assert r.status_code == 200
        assert r.json()[0]["roas"] is None

    def test_filters_forwarded_as_named_binds(self, client, admin_headers):
        sess, cleanup = _override_db([])
        try:
            r = client.get(
                "/api/admin/roas?campaign_id=fa_test_001&utm_campaign=fa_test_campaign"
                "&start_date=2026-06-01&end_date=2026-06-30",
                headers=admin_headers,
            )
        finally:
            cleanup()
        assert r.status_code == 200
        params = sess.execute.call_args.args[1]
        assert params["campaign_id"] == "fa_test_001"
        assert params["utm_campaign"] == "fa_test_campaign"
        assert params["start_date"] == "2026-06-01"
        assert params["end_date"] == "2026-06-30"

    def test_empty_result_is_empty_list(self, client, admin_headers):
        sess, cleanup = _override_db([])
        try:
            r = client.get("/api/admin/roas?ad_spend=50", headers=admin_headers)
        finally:
            cleanup()
        assert r.status_code == 200
        assert r.json() == []
