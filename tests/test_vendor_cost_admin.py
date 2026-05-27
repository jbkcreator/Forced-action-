"""
Admin API vendor cost pause endpoint tests.

Integration tests against real Postgres (fresh_db) with JWT auth mocked.

Run:
    pytest tests/test_vendor_cost_admin.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from src.core.models import VendorCostPause


def _make_pause(db, *, vendor="claude", pause_target="ap_lite_sweep", status="active"):
    now = datetime.now(timezone.utc)
    p = VendorCostPause(
        vendor=vendor,
        pause_target=pause_target,
        reason="test reason for pause",
        status=status,
        paused_at=now,
        auto_resume_at=now + timedelta(hours=24),
        created_by="test",
        metadata_json={},
        today_cost_usd=15.0,
        threshold_usd=10.0,
        anomaly_score=2.5,
    )
    db.add(p)
    db.flush()
    return p


def _admin_token():
    """Return a valid JWT for the test admin user."""
    import time
    from config.settings import get_settings
    from jose import jwt

    settings = get_settings()
    raw = settings.admin_jwt_secret
    secret = raw.get_secret_value() if raw else "test-secret-for-tests-only"
    return jwt.encode(
        {"sub": "testadmin", "exp": int(time.time()) + 3600},
        secret,
        algorithm="HS256",
    )


@pytest.fixture
def client(fresh_db):
    """TestClient with DB dependency overridden to use fresh_db."""
    from src.api.main import app
    from src.api.admin_router import get_db

    app.dependency_overrides[get_db] = lambda: fresh_db
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers():
    return {"Authorization": f"Bearer {_admin_token()}"}


# ============================================================================
# GET /api/admin/vendor-cost/pauses
# ============================================================================


class TestListVendorCostPauses:
    def test_returns_active_pauses_by_default(self, client, fresh_db, auth_headers):
        _make_pause(fresh_db, vendor="claude", pause_target="ap_lite_sweep")
        _make_pause(fresh_db, vendor="claude", pause_target="bundle_dispatcher", status="auto_resumed")
        fresh_db.flush()

        resp = client.get("/api/admin/vendor-cost/pauses", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        targets = [p["pause_target"] for p in data]
        assert "ap_lite_sweep" in targets
        assert "bundle_dispatcher" not in targets  # not active

    def test_status_all_returns_all_rows(self, client, fresh_db, auth_headers):
        _make_pause(fresh_db, vendor="claude", pause_target="ap_lite_sweep")
        _make_pause(fresh_db, vendor="claude", pause_target="bundle_dispatcher", status="auto_resumed")
        fresh_db.flush()

        resp = client.get("/api/admin/vendor-cost/pauses?status=all", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        targets = [p["pause_target"] for p in data]
        assert "ap_lite_sweep" in targets
        assert "bundle_dispatcher" in targets

    def test_empty_when_no_pauses(self, client, fresh_db, auth_headers):
        resp = client.get("/api/admin/vendor-cost/pauses", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == []

    def test_requires_auth(self, client):
        resp = client.get("/api/admin/vendor-cost/pauses")
        assert resp.status_code == 401

    def test_limit_respected(self, client, fresh_db, auth_headers):
        for i in range(5):
            _make_pause(fresh_db, vendor="claude", pause_target=f"target_{i}")
        fresh_db.flush()

        resp = client.get("/api/admin/vendor-cost/pauses?status=all&limit=3", headers=auth_headers)
        assert resp.status_code == 200
        assert len(resp.json()) <= 3


# ============================================================================
# POST /api/admin/vendor-cost/pauses/{id}/resume
# ============================================================================


class TestResumeVendorCostPause:
    def test_manual_resume_active_pause(self, client, fresh_db, auth_headers):
        p = _make_pause(fresh_db)
        pause_id = p.id

        resp = client.post(
            f"/api/admin/vendor-cost/pauses/{pause_id}/resume",
            json={"reason": "manually cleared after investigation"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        fresh_db.expire(p)
        assert p.status == "manually_resumed"
        assert p.resumed_by is not None
        assert "testadmin" in p.resumed_by

    def test_resume_requires_reason_min_5_chars(self, client, fresh_db, auth_headers):
        p = _make_pause(fresh_db)

        resp = client.post(
            f"/api/admin/vendor-cost/pauses/{p.id}/resume",
            json={"reason": "ok"},
            headers=auth_headers,
        )
        assert resp.status_code == 422  # validation error

    def test_resume_already_resumed_returns_409(self, client, fresh_db, auth_headers):
        p = _make_pause(fresh_db, status="auto_resumed")

        resp = client.post(
            f"/api/admin/vendor-cost/pauses/{p.id}/resume",
            json={"reason": "already done"},
            headers=auth_headers,
        )
        assert resp.status_code == 409

    def test_resume_nonexistent_returns_404(self, client, fresh_db, auth_headers):
        resp = client.post(
            "/api/admin/vendor-cost/pauses/999999/resume",
            json={"reason": "does not exist"},
            headers=auth_headers,
        )
        assert resp.status_code == 404

    def test_requires_auth(self, client, fresh_db):
        p = _make_pause(fresh_db)
        resp = client.post(
            f"/api/admin/vendor-cost/pauses/{p.id}/resume",
            json={"reason": "unauthorized attempt"},
        )
        assert resp.status_code == 401


# ============================================================================
# GET /api/admin/vendor-cost/pauses/{id}/skipped
# ============================================================================


class TestListSkippedActions:
    def test_returns_skipped_logs(self, client, fresh_db, auth_headers):
        from src.core.models import ApiUsageLog

        p = _make_pause(fresh_db)
        fresh_db.add(ApiUsageLog(
            service="claude",
            task_type="ap_lite_sweep",
            pause_target="ap_lite_sweep",
            cost_usd=0.0,
            blocked_by_pause=True,
            block_reason="pause: test",
            created_at=p.paused_at + timedelta(seconds=1),  # ensure >= paused_at filter
        ))
        fresh_db.flush()

        resp = client.get(f"/api/admin/vendor-cost/pauses/{p.id}/skipped", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        # Endpoint returns {"rows": [...], "total_skipped": N, ...}
        rows = data if isinstance(data, list) else data.get("rows", [])
        assert len(rows) >= 1
        assert rows[0]["block_reason"] == "pause: test"
        assert data.get("total_skipped", len(rows)) >= 1

    def test_nonexistent_pause_returns_404(self, client, auth_headers):
        resp = client.get("/api/admin/vendor-cost/pauses/999999/skipped", headers=auth_headers)
        assert resp.status_code == 404

    def test_requires_auth(self, client, fresh_db):
        p = _make_pause(fresh_db)
        resp = client.get(f"/api/admin/vendor-cost/pauses/{p.id}/skipped")
        assert resp.status_code == 401
