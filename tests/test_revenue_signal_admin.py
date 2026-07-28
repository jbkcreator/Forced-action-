"""
fa037 — HTTP-layer tests for GET /api/admin/subscribers/{id}/revenue-signal.

Mirrors the pattern in tests/test_lifecycle_autonomy_admin_endpoints.py:
  - JWT-protected (no token → 401/403)
  - happy path returns the full payload + history
  - missing subscriber → 404
  - subscriber exists but no UserSegment yet → safe-default body
  - history rows ordered newest-first
  - query param `history_limit` validated (1..200)
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ──────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────

@pytest.fixture
def client():
    from src.api.main import app
    return TestClient(app)


@pytest.fixture
def admin_token(monkeypatch):
    from config.settings import settings
    from pydantic import SecretStr
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    monkeypatch.setattr(settings, "admin_password", SecretStr("test-admin-pass"))

    from src.api.admin_router import create_access_token
    return create_access_token({"sub": "admin"})


@pytest.fixture
def auth_headers(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


def _fake_db_ctx(fake_session):
    @contextmanager
    def _ctx():
        yield fake_session
    return _ctx


def _row(**kwargs):
    return SimpleNamespace(**kwargs)


# ──────────────────────────────────────────────────────────────────────────
# Auth
# ──────────────────────────────────────────────────────────────────────────

class TestAuth:

    def test_no_token_rejected(self, client):
        r = client.get("/api/admin/subscribers/1/revenue-signal")
        assert r.status_code in (401, 403)


# ──────────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────────

class TestHappyPath:

    def test_returns_full_payload_with_history(self, client, auth_headers):
        sess = MagicMock()
        # db.get(Subscriber, ...) returns a row (truthy).
        sess.get.return_value = _row(id=42, email="ops@example.com")

        # The endpoint also runs a fetchall() for history rows.
        now = datetime(2026, 5, 25, 12, 0, tzinfo=timezone.utc)
        history_rows = [
            _row(
                action_type="wallet_txn", old_score=50, new_score=82, delta=32,
                band="very_high",
                metadata={"amount": 5, "txn_type": "debit"},
                created_at=now,
            ),
            _row(
                action_type="invoice_paid", old_score=40, new_score=50, delta=10,
                band="medium", metadata=None,
                created_at=now - timedelta(hours=4),
            ),
        ]
        sess.execute.return_value.fetchall.return_value = history_rows

        # get_revenue_signal_score is patched so we don't have to build the
        # entire user_segments fake result chain — its dict shape is already
        # locked by tests in test_revenue_signal_score.py.
        snapshot = {
            "score": 82, "band": "very_high",
            "breakdown": {
                "spend_velocity": 20, "engagement_recency": 18,
                "wallet_lock_status": 15, "lead_interaction_rate": 17,
                "zip_competition": 8,
            },
            "reasons": ["wallet tier: power", "active within last 7 days"],
            "updated_at": now.isoformat(),
            "last_significant_action_at": now.isoformat(),
            "last_action": "wallet_txn",
        }
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.revenue_signal.get_revenue_signal_score",
                   return_value=snapshot):
            r = client.get(
                "/api/admin/subscribers/42/revenue-signal", headers=auth_headers,
            )

        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert body["subscriber_id"] == 42
        assert body["score"] == 82
        assert body["band"] == "very_high"
        assert body["last_action"] == "wallet_txn"
        assert "spend_velocity" in body["breakdown"]
        assert len(body["history"]) == 2
        # Newest first.
        assert body["history"][0]["action_type"] == "wallet_txn"
        assert body["history"][0]["delta"] == 32
        assert body["history"][1]["action_type"] == "invoice_paid"


# ──────────────────────────────────────────────────────────────────────────
# 404 — subscriber missing
# ──────────────────────────────────────────────────────────────────────────

class TestNotFound:

    def test_unknown_subscriber_returns_404(self, client, auth_headers):
        sess = MagicMock()
        sess.get.return_value = None   # subscriber not found

        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)):
            r = client.get(
                "/api/admin/subscribers/99999/revenue-signal", headers=auth_headers,
            )
        assert r.status_code == 404
        body = r.json()
        assert body["detail"]["error"] == "not_found"
        assert "99999" in body["detail"]["message"]


# ──────────────────────────────────────────────────────────────────────────
# Safe-default — subscriber exists but no UserSegment row
# ──────────────────────────────────────────────────────────────────────────

class TestSafeDefault:

    def test_no_user_segment_returns_zero_score(self, client, auth_headers):
        sess = MagicMock()
        sess.get.return_value = _row(id=7, email="new@example.com")
        sess.execute.return_value.fetchall.return_value = []   # no history

        # Real safe default from get_revenue_signal_score.
        from src.services.revenue_signal import WEIGHTS
        safe_default = {
            "score": 0, "band": "low",
            "breakdown": {k: 0 for k in WEIGHTS},
            "reasons": ["no significant signals yet"],
            "updated_at": None,
            "last_significant_action_at": None,
            "last_action": None,
        }
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.revenue_signal.get_revenue_signal_score",
                   return_value=safe_default):
            r = client.get(
                "/api/admin/subscribers/7/revenue-signal", headers=auth_headers,
            )
        assert r.status_code == 200
        body = r.json()
        assert body["score"] == 0
        assert body["band"] == "low"
        assert body["history"] == []
        assert body["last_action"] is None
        assert body["revenue_signal_updated_at"] is None


# ──────────────────────────────────────────────────────────────────────────
# history_limit query param
# ──────────────────────────────────────────────────────────────────────────

class TestHistoryLimitParam:

    def test_limit_forwarded_to_sql(self, client, auth_headers):
        sess = MagicMock()
        sess.get.return_value = _row(id=42)
        sess.execute.return_value.fetchall.return_value = []

        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.revenue_signal.get_revenue_signal_score",
                   return_value={
                       "score": 0, "band": "low",
                       "breakdown": {}, "reasons": [],
                       "updated_at": None, "last_significant_action_at": None,
                       "last_action": None,
                   }):
            r = client.get(
                "/api/admin/subscribers/42/revenue-signal?history_limit=5",
                headers=auth_headers,
            )
        assert r.status_code == 200
        # Last execute() call carries our limit param.
        call = sess.execute.call_args
        assert call.args[1]["limit"] == 5

    def test_limit_above_max_rejected(self, client, auth_headers):
        r = client.get(
            "/api/admin/subscribers/42/revenue-signal?history_limit=999",
            headers=auth_headers,
        )
        assert r.status_code == 422

    def test_limit_zero_rejected(self, client, auth_headers):
        r = client.get(
            "/api/admin/subscribers/42/revenue-signal?history_limit=0",
            headers=auth_headers,
        )
        assert r.status_code == 422
