"""
HTTP-level tests for the SMS personalization analytics endpoints.

Covers:
  GET /api/admin/message-outcomes
  GET /api/admin/message-outcomes/{message_id}
  GET /api/admin/sms-variant-performance
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
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
    sess = MagicMock()
    exec_result = sess.execute.return_value
    exec_result.mappings.return_value.all.return_value = all_rows or []
    exec_result.mappings.return_value.first.return_value = first_row
    return sess


def _outcome_row(**overrides):
    base = {
        "id": 10,
        "subscriber_id": 5,
        "lead_id": 100,
        "property_id": 200,
        "message_type": "marketing",
        "outcome": "sent",
        "sent_at": datetime(2026, 5, 15, tzinfo=timezone.utc),
        "replied_at": None,
        "trade_vertical": "fix_flip",
        "county_id": "hillsborough",
        "behavioral_segment": "high_intent",
        "revenue_signal_score": 72,
        "revenue_signal_score_band": "gold",
        "last_action_recency_band": "recent",
        "prompt_version": "v3",
        "has_context_snapshot": True,
        "_total": 1,
    }
    base.update(overrides)
    return base


def _outcome_detail_row(**overrides):
    base = {
        "id": 10,
        "subscriber_id": 5,
        "lead_id": 100,
        "property_id": 200,
        "message_type": "marketing",
        "outcome": "converted",
        "sent_at": datetime(2026, 5, 15, tzinfo=timezone.utc),
        "replied_at": datetime(2026, 5, 15, 1, 0, tzinfo=timezone.utc),
        "trade_vertical": "fix_flip",
        "county_id": "hillsborough",
        "behavioral_segment": "high_intent",
        "revenue_signal_score": 72,
        "revenue_signal_score_band": "gold",
        "last_action_recency_band": "recent",
        "prompt_version": "v3",
        "context_snapshot": {"score": 72, "vertical": "fix_flip"},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Auth — all endpoints require JWT
# ---------------------------------------------------------------------------

class TestAuthRequired:

    def test_list_requires_auth(self, client):
        r = client.get("/api/admin/message-outcomes")
        assert r.status_code in (401, 403)

    def test_detail_requires_auth(self, client):
        r = client.get("/api/admin/message-outcomes/1")
        assert r.status_code in (401, 403)

    def test_variant_performance_requires_auth(self, client):
        r = client.get("/api/admin/sms-variant-performance")
        assert r.status_code in (401, 403)


# ---------------------------------------------------------------------------
# GET /api/admin/message-outcomes
# ---------------------------------------------------------------------------

class TestListMessageOutcomes:

    def test_returns_200_and_paginated_shape(self, client, auth):
        row = _outcome_row()
        sess = _mock_session(all_rows=[row])
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/message-outcomes", headers=auth)
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert body["page"] == 1
        assert body["per_page"] == 50
        assert len(body["data"]) == 1
        assert "_total" not in body["data"][0]

    def test_empty_result(self, client, auth):
        sess = _mock_session(all_rows=[])
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/message-outcomes", headers=auth)
        assert r.status_code == 200
        assert r.json()["total"] == 0

    def test_context_snapshot_not_in_list_response(self, client, auth):
        """context_snapshot must never appear in the list payload — only has_context_snapshot."""
        row = _outcome_row()
        sess = _mock_session(all_rows=[row])
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/message-outcomes", headers=auth)
        assert r.status_code == 200
        first = r.json()["data"][0]
        assert "context_snapshot" not in first
        assert "has_context_snapshot" in first

    def test_inverted_date_range_rejected(self, client, auth):
        r = client.get(
            "/api/admin/message-outcomes"
            "?date_from=2026-05-10T00:00:00Z&date_to=2026-05-01T00:00:00Z",
            headers=auth,
        )
        assert r.status_code == 422

    def test_trade_vertical_filter_passed_as_param(self, client, auth):
        sess = _mock_session(all_rows=[])
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get(
                "/api/admin/message-outcomes?trade_vertical=wholesaler",
                headers=auth,
            )
        assert r.status_code == 200
        params = sess.execute.call_args[0][1]
        assert params.get("trade_vertical") == "wholesaler"

    def test_pagination_offset_calculated_correctly(self, client, auth):
        sess = _mock_session(all_rows=[])
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get(
                "/api/admin/message-outcomes?page=3&per_page=20",
                headers=auth,
            )
        assert r.status_code == 200
        params = sess.execute.call_args[0][1]
        assert params["offset"] == 40   # (3-1) * 20
        assert params["per_page"] == 20


# ---------------------------------------------------------------------------
# GET /api/admin/message-outcomes/{message_id}
# ---------------------------------------------------------------------------

class TestGetMessageOutcome:

    def test_returns_200_with_context_snapshot(self, client, auth):
        row = _outcome_detail_row()
        sess = _mock_session(first_row=row)
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/message-outcomes/10", headers=auth)
        assert r.status_code == 200
        body = r.json()
        assert body["id"] == 10
        assert body["context_snapshot"] == {"score": 72, "vertical": "fix_flip"}

    def test_context_snapshot_null_is_returned_as_null(self, client, auth):
        row = _outcome_detail_row(context_snapshot=None)
        sess = _mock_session(first_row=row)
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/message-outcomes/10", headers=auth)
        assert r.status_code == 200
        assert r.json()["context_snapshot"] is None

    def test_404_when_not_found(self, client, auth):
        sess = _mock_session(first_row=None)
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/message-outcomes/9999", headers=auth)
        assert r.status_code == 404
        assert "9999" in r.json()["detail"]


# ---------------------------------------------------------------------------
# GET /api/admin/sms-variant-performance
# ---------------------------------------------------------------------------

class TestSmsVariantPerformance:

    def _perf_row(self, group_key):
        return {
            group_key: "v3",
            "total_sent": 500,
            "total_replied": 60,
            "total_converted": 12,
            "reply_rate_pct": 12.00,
            "conversion_rate_pct": 2.40,
            "avg_revenue_signal_score": 68.5,
        }

    def test_default_group_by_prompt_version(self, client, auth):
        row = self._perf_row("prompt_version")
        sess = _mock_session(all_rows=[row])
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get("/api/admin/sms-variant-performance", headers=auth)
        assert r.status_code == 200
        body = r.json()
        assert body["group_by"] == "prompt_version"
        assert body["data"][0]["total_sent"] == 500
        assert body["data"][0]["reply_rate_pct"] == 12.00

    def test_valid_single_group_by_accepted(self, client, auth):
        for dim in [
            "variant_id", "template_id", "prompt_version", "trade_vertical",
            "county_id", "behavioral_segment", "revenue_signal_score_band",
            "last_action_recency_band",
        ]:
            sess = _mock_session(all_rows=[])
            with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
                r = client.get(
                    f"/api/admin/sms-variant-performance?group_by={dim}",
                    headers=auth,
                )
            assert r.status_code == 200, f"Expected 200 for group_by={dim}, got {r.status_code}"

    def test_valid_compound_group_by_accepted(self, client, auth):
        for compound in ["template_id,variant_id", "trade_vertical,behavioral_segment"]:
            sess = _mock_session(all_rows=[])
            with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
                r = client.get(
                    f"/api/admin/sms-variant-performance?group_by={compound}",
                    headers=auth,
                )
            assert r.status_code == 200, f"Expected 200 for compound group_by={compound}"

    def test_invalid_group_by_rejected_with_422(self, client, auth):
        r = client.get(
            "/api/admin/sms-variant-performance?group_by=injected_column; DROP TABLE",
            headers=auth,
        )
        assert r.status_code == 422

    def test_unknown_dimension_rejected(self, client, auth):
        r = client.get(
            "/api/admin/sms-variant-performance?group_by=not_a_real_column",
            headers=auth,
        )
        assert r.status_code == 422

    def test_inverted_date_range_rejected(self, client, auth):
        r = client.get(
            "/api/admin/sms-variant-performance"
            "?date_from=2026-05-10T00:00:00Z&date_to=2026-05-01T00:00:00Z",
            headers=auth,
        )
        assert r.status_code == 422

    def test_date_range_params_forwarded(self, client, auth):
        sess = _mock_session(all_rows=[])
        with patch("src.api.sms_analytics_router.get_db_context", _db_ctx(sess)):
            r = client.get(
                "/api/admin/sms-variant-performance"
                "?date_from=2026-05-01T00:00:00Z&date_to=2026-05-31T00:00:00Z",
                headers=auth,
            )
        assert r.status_code == 200
        params = sess.execute.call_args[0][1]
        assert "date_from" in params
        assert "date_to" in params
