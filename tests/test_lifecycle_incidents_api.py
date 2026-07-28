"""
HTTP-level tests for the Lifecycle self-healing incident management endpoints.

Covers:
  GET  /api/admin/lifecycle-incidents
  GET  /api/admin/lifecycle-incidents/{id}
  POST /api/admin/lifecycle-incidents/{id}/acknowledge
  POST /api/admin/lifecycle-incidents/{id}/resolve
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
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
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def admin_token(monkeypatch):
    from config.settings import settings
    from pydantic import SecretStr
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    monkeypatch.setattr(settings, "admin_password", SecretStr("test-admin-pass"))
    from src.api.admin_router import create_access_token
    return create_access_token({"sub": "admin-test"})


@pytest.fixture
def auth(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


def _db_ctx(fake_session):
    @contextmanager
    def _ctx():
        yield fake_session
    return _ctx


def _mock_session(*, all_rows=None, first_row=None):
    """Build a MagicMock session whose execute chain returns the supplied data."""
    sess = MagicMock()
    exec_result = sess.execute.return_value
    exec_result.mappings.return_value.all.return_value = all_rows or []
    exec_result.mappings.return_value.first.return_value = first_row
    return sess


def _incident_row(**overrides):
    base = {
        "id": 1,
        "metric_name": "reply_rate",
        "county_id": "hillsborough",
        "feature_name": "fomo_graph",
        "severity": "yellow",
        "observed_value": 0.04,
        "threshold_value": 0.05,
        "baseline_value": 0.10,
        "breach_started": datetime(2026, 5, 1, tzinfo=timezone.utc),
        "breach_resolved": None,
        "duration_hours": None,
        "action_taken": "no_op",
        "action_details": None,
        "decision_id": None,
        "created_at": datetime(2026, 5, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 5, 1, tzinfo=timezone.utc),
        "_total": 1,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Auth — all endpoints require JWT
# ---------------------------------------------------------------------------

class TestAuthRequired:

    def test_list_requires_auth(self, client):
        r = client.get("/api/admin/lifecycle-incidents")
        assert r.status_code in (401, 403)

    def test_detail_requires_auth(self, client):
        r = client.get("/api/admin/lifecycle-incidents/1")
        assert r.status_code in (401, 403)

    def test_acknowledge_requires_auth(self, client):
        r = client.post("/api/admin/lifecycle-incidents/1/acknowledge", json={})
        assert r.status_code in (401, 403)

    def test_resolve_requires_auth(self, client):
        r = client.post("/api/admin/lifecycle-incidents/1/resolve", json={})
        assert r.status_code in (401, 403)


# ---------------------------------------------------------------------------
# GET /api/admin/lifecycle-incidents
# ---------------------------------------------------------------------------

class TestListLifecycleIncidents:

    def test_returns_200_and_paginated_shape(self, client, auth):
        row = _incident_row()
        sess = _mock_session(all_rows=[row])
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/lifecycle-incidents", headers=auth)
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert body["limit"] == 50
        assert body["offset"] == 0
        assert len(body["data"]) == 1
        assert body["data"][0]["id"] == 1
        assert "_total" not in body["data"][0]

    def test_empty_result_returns_total_zero(self, client, auth):
        sess = _mock_session(all_rows=[])
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/lifecycle-incidents", headers=auth)
        assert r.status_code == 200
        assert r.json()["total"] == 0
        assert r.json()["data"] == []

    def test_severity_filter_rejected_if_invalid(self, client, auth):
        r = client.get("/api/admin/lifecycle-incidents?severity=critical", headers=auth)
        assert r.status_code == 422

    def test_action_taken_filter_rejected_if_invalid(self, client, auth):
        r = client.get("/api/admin/lifecycle-incidents?action_taken=unknown_action", headers=auth)
        assert r.status_code == 422

    def test_inverted_date_range_rejected(self, client, auth):
        r = client.get(
            "/api/admin/lifecycle-incidents"
            "?date_from=2026-05-10T00:00:00Z&date_to=2026-05-01T00:00:00Z",
            headers=auth,
        )
        assert r.status_code == 422

    def test_open_only_filter_passes_through(self, client, auth):
        sess = _mock_session(all_rows=[])
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/lifecycle-incidents?open_only=true", headers=auth)
        assert r.status_code == 200
        # Verify the SQL included the open_only predicate
        sql_text = str(sess.execute.call_args[0][0])
        assert "breach_resolved IS NULL" in sql_text

    def test_severity_filter_included_in_sql(self, client, auth):
        sess = _mock_session(all_rows=[])
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/lifecycle-incidents?severity=red", headers=auth)
        assert r.status_code == 200
        call_params = sess.execute.call_args[0][1]
        assert call_params.get("severity") == "red"


# ---------------------------------------------------------------------------
# GET /api/admin/lifecycle-incidents/{incident_id}
# ---------------------------------------------------------------------------

class TestGetLifecycleIncident:

    def test_returns_200_and_full_row(self, client, auth):
        row = {k: v for k, v in _incident_row().items() if k != "_total"}
        sess = _mock_session(first_row=row)
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/lifecycle-incidents/1", headers=auth)
        assert r.status_code == 200
        assert r.json()["id"] == 1
        assert r.json()["metric_name"] == "reply_rate"

    def test_404_when_not_found(self, client, auth):
        sess = _mock_session(first_row=None)
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/lifecycle-incidents/9999", headers=auth)
        assert r.status_code == 404
        assert "9999" in r.json()["detail"]


# ---------------------------------------------------------------------------
# POST /api/admin/lifecycle-incidents/{incident_id}/acknowledge
# ---------------------------------------------------------------------------

class TestAcknowledgeIncident:

    def _ack_sess(self, *, action_taken="no_op", action_details=None, breach_resolved=None):
        """Session where first() returns the incident and execute() is called again for UPDATE."""
        fetch_row = {
            "id": 1,
            "action_taken": action_taken,
            "action_details": action_details,
            "breach_resolved": breach_resolved,
        }
        sess = MagicMock()
        # First call (SELECT) → first_row; subsequent calls (UPDATE) → ignored
        sess.execute.return_value.mappings.return_value.first.return_value = fetch_row
        return sess

    def test_acknowledges_no_op_incident_and_upgrades_to_human_escalated(self, client, auth):
        sess = self._ack_sess(action_taken="no_op")
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.post(
                "/api/admin/lifecycle-incidents/1/acknowledge",
                headers=auth,
                json={"notes": "Looking into it", "acknowledged_by": "dev@heu.ai"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["action_taken"] == "human_escalated"
        assert body["upgraded"] is True
        assert body["action_details"]["acknowledgement"]["notes"] == "Looking into it"
        assert body["action_details"]["acknowledgement"]["acknowledged_by"] == "dev@heu.ai"

    def test_preserves_stronger_action_when_not_no_op(self, client, auth):
        sess = self._ack_sess(action_taken="auto_paused")
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.post(
                "/api/admin/lifecycle-incidents/1/acknowledge",
                headers=auth,
                json={},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["action_taken"] == "auto_paused"
        assert body["upgraded"] is False

    def test_merges_ack_into_existing_action_details(self, client, auth):
        existing = {"auto_pause_reason": "low_reply_rate"}
        sess = self._ack_sess(action_taken="auto_paused", action_details=existing)
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.post(
                "/api/admin/lifecycle-incidents/1/acknowledge",
                headers=auth,
                json={"notes": "acknowledged"},
            )
        assert r.status_code == 200
        details = r.json()["action_details"]
        assert "auto_pause_reason" in details
        assert "acknowledgement" in details

    def test_404_when_incident_missing(self, client, auth):
        sess = _mock_session(first_row=None)
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.post(
                "/api/admin/lifecycle-incidents/9999/acknowledge",
                headers=auth,
                json={},
            )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/admin/lifecycle-incidents/{incident_id}/resolve
# ---------------------------------------------------------------------------

class TestResolveIncident:

    def _resolve_sess(self, *, breach_resolved=None):
        fetch_row = {
            "id": 1,
            "action_details": None,
            "breach_resolved": breach_resolved,
            "breach_started": datetime(2026, 5, 1, tzinfo=timezone.utc),
        }
        updated_row = {
            "id": 1,
            "breach_resolved": datetime(2026, 5, 2, tzinfo=timezone.utc),
            "duration_hours": 24,
            "action_taken": "resolved",
            "action_details": {"resolution": {"resolved_by": "admin-test"}},
        }
        sess = MagicMock()
        # First execute → fetch row; second execute → RETURNING row
        sess.execute.side_effect = [
            _make_exec_result(first_row=fetch_row),
            _make_exec_result(first_row=updated_row),
        ]
        return sess

    def test_resolves_open_incident(self, client, auth):
        sess = self._resolve_sess()
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.post(
                "/api/admin/lifecycle-incidents/1/resolve",
                headers=auth,
                json={"resolution_notes": "fixed by rollback", "resolved_by": "ops@heu.ai"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["action_taken"] == "resolved"
        assert body["duration_hours"] == 24

    def test_409_when_already_resolved(self, client, auth):
        sess = self._resolve_sess(
            breach_resolved=datetime(2026, 5, 2, tzinfo=timezone.utc)
        )
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.post(
                "/api/admin/lifecycle-incidents/1/resolve",
                headers=auth,
                json={},
            )
        assert r.status_code == 409

    def test_404_when_incident_missing(self, client, auth):
        sess = MagicMock()
        sess.execute.return_value.mappings.return_value.first.return_value = None
        with patch("src.api.lifecycle_incidents_router.get_db_context", _db_ctx(sess)):
            r = client.post(
                "/api/admin/lifecycle-incidents/9999/resolve",
                headers=auth,
                json={},
            )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_exec_result(*, first_row=None, all_rows=None):
    """Return a MagicMock that mimics sqlalchemy execute().mappings().*"""
    result = MagicMock()
    result.mappings.return_value.first.return_value = first_row
    result.mappings.return_value.all.return_value = all_rows or []
    return result
