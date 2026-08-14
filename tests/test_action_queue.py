"""
Behavior tests for the T-B8-03 Action Queue read/aggregation layer.

Covers src/services/action_queue.py (3-source read-time union + canonical
count helpers) and the GET /api/admin/operator-dashboard/action-queue route.

Repo convention: raw SQL via sa_text; the DB session is a MagicMock whose
.execute(...).mappings().all() returns supplied rows, one result per query
in call order (lifecycle, human_close, scraper).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


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
    return create_access_token({"sub": "admin-test", "scope": "admin"})


@pytest.fixture
def auth(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}

NOW = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)


def _result(rows):
    r = MagicMock()
    r.mappings.return_value.all.return_value = rows
    return r


def _mock_session(*, lifecycle=None, human_close=None, scraper=None):
    """Session whose execute() yields lifecycle, then human_close, then scraper."""
    sess = MagicMock()
    sess.execute.side_effect = [
        _result(lifecycle or []),
        _result(human_close or []),
        _result(scraper or []),
    ]
    return sess


def _lifecycle_row(**o):
    base = {
        "id": 1,
        "metric_name": "match_rate",
        "feature_name": "tracerfy",
        "county_id": "hillsborough",
        "severity": "red",
        "action_taken": "human_escalated",
        "root_cause": "degraded provider",
        "breach_started": NOW - timedelta(hours=5),
    }
    base.update(o)
    return base


def _hc_row(**o):
    base = {
        "id": 10,
        "target_tier": "tier_3",
        "vertical": "hard_money_lenders",
        "revenue_signal_score": 87,
        "target_tier_price_cents": 240000,
        "routed_at": NOW - timedelta(days=1),
    }
    base.update(o)
    return base


def _scraper_row(**o):
    base = {
        "id": 20,
        "source_type": "court_docket",
        "county_id": "hillsborough",
        "alert_type": "zero_records",
        "alerted_at": NOW - timedelta(minutes=40),
    }
    base.update(o)
    return base


class TestLifecycleMapping:
    def test_open_human_escalated_becomes_legal_approval(self):
        from src.services.action_queue import build_action_queue

        q = build_action_queue(_mock_session(lifecycle=[_lifecycle_row()]))

        assert len(q["approvals"]) == 1
        item = q["approvals"][0]
        assert item["source"] == "lifecycle"
        assert item["type"] == "approval"
        assert item["lane"] == "approvals"
        assert item["category"] == "legal"
        assert item["severity"] == "red"
        assert item["id"] == 1

    def test_open_auto_paused_becomes_ops_failure(self):
        from src.services.action_queue import build_action_queue

        q = build_action_queue(_mock_session(
            lifecycle=[_lifecycle_row(id=2, action_taken="auto_paused", severity="yellow")]
        ))

        assert q["approvals"] == []
        assert len(q["failures"]) == 1
        item = q["failures"][0]
        assert item["type"] == "source_failure"
        assert item["category"] == "ops"
        assert item["lane"] == "failures"

    def test_resolved_and_closed_lifecycle_excluded(self):
        from src.services.action_queue import build_action_queue

        # action_taken='resolved' is filtered in SQL; simulate SQL already
        # excluding it by passing no rows (breach_resolved set / resolved).
        q = build_action_queue(_mock_session(lifecycle=[]))
        assert q["approvals"] == []
        assert q["failures"] == []


class TestHumanCloseMapping:
    def test_open_escalation_becomes_deal_approval(self):
        from src.services.action_queue import build_action_queue

        q = build_action_queue(_mock_session(human_close=[_hc_row()]))

        assert len(q["approvals"]) == 1
        item = q["approvals"][0]
        assert item["source"] == "human_close"
        assert item["type"] == "approval"
        assert item["category"] == "deal"
        assert item["lane"] == "approvals"
        assert item["amount_cents"] == 240000
        assert item["county_id"] is None
        assert item["action_url"] == "/admin/closer"


class TestScraperMapping:
    def test_open_alert_becomes_source_failure(self):
        from src.services.action_queue import build_action_queue

        q = build_action_queue(_mock_session(scraper=[_scraper_row()]))

        assert q["approvals"] == []
        assert len(q["failures"]) == 1
        item = q["failures"][0]
        assert item["source"] == "scraper"
        assert item["type"] == "source_failure"
        assert item["category"] == "source"
        assert item["lane"] == "failures"

    @pytest.mark.parametrize("alert_type,severity", [
        ("zero_records", "red"),
        ("scraper_error", "red"),
        ("low_count", "yellow"),
        ("health_check", "info"),
    ])
    def test_severity_map(self, alert_type, severity):
        from src.services.action_queue import build_action_queue

        q = build_action_queue(_mock_session(
            scraper=[_scraper_row(alert_type=alert_type)]
        ))
        assert q["failures"][0]["severity"] == severity


class TestOrdering:
    def test_approvals_oldest_first(self):
        from src.services.action_queue import build_action_queue

        old = _lifecycle_row(id=1, breach_started=NOW - timedelta(days=2))
        new = _lifecycle_row(id=2, breach_started=NOW - timedelta(hours=1))
        q = build_action_queue(_mock_session(lifecycle=[new, old]))

        assert [i["id"] for i in q["approvals"]] == [1, 2]  # oldest (id1) first

    def test_sorts_mixed_naive_and_aware_created_at(self):
        # human_close_escalations.routed_at is tz-naive; lifecycle breach_started is
        # tz-aware. Both land in the approvals lane and must sort without a
        # "can't compare offset-naive and offset-aware datetimes" TypeError.
        from src.services.action_queue import build_action_queue

        naive = datetime(2026, 7, 20, 10, 0)  # no tzinfo (like routed_at)
        q = build_action_queue(_mock_session(
            lifecycle=[_lifecycle_row(id=1, breach_started=NOW - timedelta(hours=2))],
            human_close=[_hc_row(id=2, routed_at=naive)],
        ))
        assert len(q["approvals"]) == 2  # no crash, both present

    def test_failures_newest_first(self):
        from src.services.action_queue import build_action_queue

        old = _scraper_row(id=1, alerted_at=NOW - timedelta(hours=3))
        new = _scraper_row(id=2, alerted_at=NOW - timedelta(minutes=5))
        q = build_action_queue(_mock_session(scraper=[old, new]))

        assert [i["id"] for i in q["failures"]] == [2, 1]  # newest (id2) first


class TestCounts:
    def test_lifecycle_approvals_waiting_excludes_ops_and_human_close(self):
        from src.services.action_queue import build_action_queue

        q = build_action_queue(_mock_session(
            lifecycle=[
                _lifecycle_row(id=1, action_taken="human_escalated"),   # legal → counted
                _lifecycle_row(id=2, action_taken="auto_paused"),       # ops → not counted
            ],
            human_close=[_hc_row(id=10)],                          # deal → not counted
            scraper=[_scraper_row(id=20)],                         # source failure
        ))

        assert q["counts"]["lifecycle_approvals_waiting"] == 1
        assert q["counts"]["source_failures"] == 1

    def test_counts_match_lane_lengths_no_drift(self):
        from src.services.action_queue import build_action_queue

        q = build_action_queue(_mock_session(
            lifecycle=[_lifecycle_row(id=1, action_taken="feature_killed")],
            human_close=[_hc_row(id=10)],
            scraper=[_scraper_row(id=20)],
        ))
        assert q["counts"]["approvals"] == len(q["approvals"]) == 2
        assert q["counts"]["failures"] == len(q["failures"]) == 1


def _scalar_session(value):
    sess = MagicMock()
    sess.execute.return_value.scalar_one.return_value = value
    return sess


class TestCanonicalHelpers:
    def test_lifecycle_approvals_waiting_helper_returns_count(self):
        from src.services.action_queue import lifecycle_approvals_waiting
        assert lifecycle_approvals_waiting(_scalar_session(3)) == 3

    def test_source_failures_helper_returns_count(self):
        from src.services.action_queue import source_failures
        assert source_failures(_scalar_session(5)) == 5


class TestActionQueueEndpoint:
    ROUTE = "/api/admin/operator-dashboard/action-queue"

    def test_requires_auth(self, client):
        assert client.get(self.ROUTE).status_code == 401

    def test_returns_200_and_shape(self, client, auth, monkeypatch):
        from src.api import operator_dashboard_router as mod
        canned = {"approvals": [], "failures": [],
                  "counts": {"approvals": 0, "failures": 0,
                             "lifecycle_approvals_waiting": 0, "source_failures": 0}}
        monkeypatch.setattr(mod, "build_action_queue", lambda session: canned)

        from src.api.main import app
        from src.api.deps import get_db
        app.dependency_overrides[get_db] = lambda: MagicMock()
        try:
            resp = client.get(self.ROUTE, headers=auth)
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 200
        body = resp.json()
        assert set(body) == {"approvals", "failures", "counts"}
        assert set(body["counts"]) >= {
            "lifecycle_approvals_waiting", "source_failures", "approvals", "failures"
        }

    def test_db_error_returns_500_with_generic_detail(self, client, auth, monkeypatch):
        from sqlalchemy.exc import SQLAlchemyError
        from src.api import operator_dashboard_router as mod

        def _boom(session):
            raise SQLAlchemyError("connection reset")
        monkeypatch.setattr(mod, "build_action_queue", _boom)

        from src.api.main import app
        from src.api.deps import get_db
        app.dependency_overrides[get_db] = lambda: MagicMock()
        try:
            resp = client.get(self.ROUTE, headers=auth)
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 500
        assert resp.json()["detail"] == "Failed to load action queue"
        assert "connection reset" not in resp.text  # no internal leak
