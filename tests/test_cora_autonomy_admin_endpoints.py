"""
fa036 — HTTP-layer integration tests for the Cora autonomy admin endpoints.

Covers the 4 endpoints added to src/api/admin_router.py:
  POST /api/admin/cora-playbook/{id}/adopt
  POST /api/admin/cora-playbook/{id}/reject
  POST /api/admin/cora-playbook/{id}/retire
  GET  /api/admin/cora-autonomy

Verified for each:
  - 401 without JWT (auth required)
  - 200 with valid JWT + valid id (happy path) + correct response shape
  - 200 with valid JWT but stale playbook (idempotent no-op semantics)
  - 404 when the playbook id doesn't exist at all
  - 422/400 on malformed body where applicable

The endpoints use `with get_db_context()` directly (not the `get_db`
FastAPI dependency), so we patch the context manager in the admin_router
namespace. `transition_status` is patched per-test to simulate
adopt/reject/retire results without touching Postgres.
"""

from __future__ import annotations

from contextlib import contextmanager
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
    """Return a context manager that yields the given fake session."""
    @contextmanager
    def _ctx():
        yield fake_session
    return _ctx


def _fake_session_with(*, status_lookup=None):
    """Build a fake SQLAlchemy session where `.execute(...).first()` returns
    a row carrying `status_lookup`, or None if status_lookup is None.

    Only the row's `.status` attribute is read by the endpoint when
    `transition_status` returns False.
    """
    sess = MagicMock()
    if status_lookup is None:
        sess.execute.return_value.first.return_value = None
    else:
        sess.execute.return_value.first.return_value = SimpleNamespace(status=status_lookup)
    return sess


# ---------------------------------------------------------------------------
# Auth — 401 without JWT
# ---------------------------------------------------------------------------

class TestAuthRequired:

    def test_adopt_requires_auth(self, client):
        r = client.post("/api/admin/cora-playbook/1/adopt", json={"actor": "dev"})
        assert r.status_code in (401, 403)

    def test_reject_requires_auth(self, client):
        r = client.post("/api/admin/cora-playbook/1/reject", json={"actor": "dev"})
        assert r.status_code in (401, 403)

    def test_retire_requires_auth(self, client):
        r = client.post("/api/admin/cora-playbook/1/retire", json={"actor": "dev"})
        assert r.status_code in (401, 403)

    def test_cora_autonomy_get_requires_auth(self, client):
        r = client.get("/api/admin/cora-autonomy")
        assert r.status_code in (401, 403)


# ---------------------------------------------------------------------------
# Adopt
# ---------------------------------------------------------------------------

class TestAdoptEndpoint:

    def test_happy_path_returns_200_and_adopted(self, client, auth_headers):
        sess = MagicMock()
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.playbook_writer.transition_status", return_value=True) as mock_t:
            r = client.post(
                "/api/admin/cora-playbook/42/adopt",
                headers=auth_headers,
                json={"actor": "dev@heu.ai"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert body["id"] == 42
        assert body["status"] == "adopted"
        # transition_status was called with the right kwargs.
        call_kwargs = mock_t.call_args.kwargs
        assert call_kwargs["to_status"] == "adopted"
        assert call_kwargs["actor"] == "dev@heu.ai"

    def test_idempotent_when_already_adopted(self, client, auth_headers):
        """Re-adopting an already-adopted row → 200, status reflects current
        state, plus a 'note' explaining no transition happened."""
        sess = _fake_session_with(status_lookup="adopted")
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.playbook_writer.transition_status", return_value=False):
            r = client.post(
                "/api/admin/cora-playbook/42/adopt",
                headers=auth_headers,
                json={"actor": "dev@heu.ai"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "adopted"
        assert "note" in body

    def test_404_when_playbook_missing(self, client, auth_headers):
        sess = _fake_session_with(status_lookup=None)
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.playbook_writer.transition_status", return_value=False):
            r = client.post(
                "/api/admin/cora-playbook/9999/adopt",
                headers=auth_headers,
                json={"actor": "dev@heu.ai"},
            )
        assert r.status_code == 404
        assert r.json()["detail"]["error"] == "not_found"

    def test_422_when_actor_missing(self, client, auth_headers):
        r = client.post(
            "/api/admin/cora-playbook/42/adopt",
            headers=auth_headers,
            json={},   # missing required 'actor'
        )
        assert r.status_code == 422


# ---------------------------------------------------------------------------
# Reject
# ---------------------------------------------------------------------------

class TestRejectEndpoint:

    def test_happy_path_returns_200_with_reason(self, client, auth_headers):
        sess = MagicMock()
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.playbook_writer.transition_status", return_value=True) as mock_t:
            r = client.post(
                "/api/admin/cora-playbook/42/reject",
                headers=auth_headers,
                json={"actor": "dev@heu.ai", "reason": "not enough lift"},
            )
        assert r.status_code == 200
        assert r.json()["status"] == "rejected"
        assert mock_t.call_args.kwargs["reason"] == "not enough lift"

    def test_reject_without_reason_is_allowed(self, client, auth_headers):
        sess = MagicMock()
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.playbook_writer.transition_status", return_value=True) as mock_t:
            r = client.post(
                "/api/admin/cora-playbook/42/reject",
                headers=auth_headers,
                json={"actor": "dev@heu.ai"},
            )
        assert r.status_code == 200
        assert mock_t.call_args.kwargs["reason"] is None

    def test_404_when_playbook_missing(self, client, auth_headers):
        sess = _fake_session_with(status_lookup=None)
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.playbook_writer.transition_status", return_value=False):
            r = client.post(
                "/api/admin/cora-playbook/9999/reject",
                headers=auth_headers,
                json={"actor": "dev@heu.ai"},
            )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Retire
# ---------------------------------------------------------------------------

class TestRetireEndpoint:

    def test_happy_path_returns_200(self, client, auth_headers):
        sess = MagicMock()
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.playbook_writer.transition_status", return_value=True) as mock_t:
            r = client.post(
                "/api/admin/cora-playbook/42/retire",
                headers=auth_headers,
                json={"actor": "dev@heu.ai"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "retired"
        assert mock_t.call_args.kwargs["to_status"] == "retired"

    def test_no_transition_when_not_adopted(self, client, auth_headers):
        """Retire on a 'recommended' (not yet adopted) row → 200 + note."""
        sess = _fake_session_with(status_lookup="recommended")
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.playbook_writer.transition_status", return_value=False):
            r = client.post(
                "/api/admin/cora-playbook/42/retire",
                headers=auth_headers,
                json={"actor": "dev@heu.ai"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "recommended"
        assert "note" in body

    def test_404_when_playbook_missing(self, client, auth_headers):
        sess = _fake_session_with(status_lookup=None)
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)), \
             patch("src.services.playbook_writer.transition_status", return_value=False):
            r = client.post(
                "/api/admin/cora-playbook/9999/retire",
                headers=auth_headers,
                json={"actor": "dev@heu.ai"},
            )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# GET /cora-autonomy
# ---------------------------------------------------------------------------

class TestCoraAutonomyGet:

    def _session_with_cards(self, n_cards):
        """Build a fake session returning n synthetic learning_card rows."""
        from datetime import date, timedelta
        rows = []
        for i in range(n_cards):
            rows.append(SimpleNamespace(
                card_date=date(2026, 5, 25) - timedelta(days=7 * i),
                summary_text=f"Cora autonomy: {80 - i}% autonomous, n/a overridden, 2 adopted, +1 net playbooks",
                data_json={
                    "autonomous_pct": 80 - i,
                    "overridden_pct": None,
                    "recommended_adoptions": 2,
                    "net_new_playbooks": 1,
                },
            ))
        sess = MagicMock()
        sess.execute.return_value.fetchall.return_value = rows
        return sess

    def test_returns_latest_plus_history(self, client, auth_headers):
        sess = self._session_with_cards(n_cards=3)
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)):
            r = client.get("/api/admin/cora-autonomy", headers=auth_headers)
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert body["latest"] is not None
        assert body["latest"]["card_date"] == "2026-05-25"
        assert body["latest"]["metrics"]["autonomous_pct"] == 80
        assert len(body["history"]) == 3
        # History is newest-first.
        assert body["history"][0]["card_date"] > body["history"][1]["card_date"]

    def test_returns_empty_when_no_cards(self, client, auth_headers):
        sess = self._session_with_cards(n_cards=0)
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)):
            r = client.get("/api/admin/cora-autonomy", headers=auth_headers)
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert body["latest"] is None
        assert body["history"] == []

    def test_weeks_query_param_respects_range(self, client, auth_headers):
        sess = self._session_with_cards(n_cards=2)
        with patch("src.api.admin_router.get_db_context", _fake_db_ctx(sess)):
            r = client.get("/api/admin/cora-autonomy?weeks=4", headers=auth_headers)
        assert r.status_code == 200
        # Query was issued with weeks=4 in params.
        call = sess.execute.call_args
        assert call.args[1]["weeks"] == 4

    def test_weeks_param_validated_to_max_52(self, client, auth_headers):
        r = client.get("/api/admin/cora-autonomy?weeks=999", headers=auth_headers)
        assert r.status_code == 422

    def test_weeks_param_validated_min_1(self, client, auth_headers):
        r = client.get("/api/admin/cora-autonomy?weeks=0", headers=auth_headers)
        assert r.status_code == 422
