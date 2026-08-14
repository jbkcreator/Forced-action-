"""Layer 3D — HTTP/RBAC tests for the Broker State Machine API.

Routes under test:
  POST /api/broker/lanes/{lane_id}/claim
  POST /api/broker/lanes/{lane_id}/transition
  GET  /api/broker/lanes/{lane_id}/transitions
  POST /api/admin/lanes/{lane_id}/reassign-broker

Strategy:
  - TestClient from FastAPI (no real server process).
  - dependency_overrides inject a mock broker identity and a mock DB session,
    so tests run without a live Postgres instance.
  - Service-layer functions are patched at the router-import site so each test
    controls the exact return value or exception.
  - Admin routes use a real JWT issued by the same create_access_token helper
    used in test_operator_crm_api.py.
"""
from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ── Stable test IDs ──────────────────────────────────────────────────────────
BROKER_ID = str(uuid.uuid4())
OTHER_BROKER_ID = str(uuid.uuid4())
LANE_ID = str(uuid.uuid4())


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def mock_session(client):
    """Install a MagicMock session as the get_db dependency for broker routes."""
    from src.api.main import app
    from src.api.deps import get_db

    sess = MagicMock()
    # Default: lane exists and is assigned to BROKER_ID
    _lane_row = SimpleNamespace(assigned_broker_id=uuid.UUID(BROKER_ID))
    sess.execute.return_value.fetchone.return_value = _lane_row

    def _override():
        yield sess

    app.dependency_overrides[get_db] = _override
    yield sess
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture
def broker_auth(mock_session):
    """Override get_current_broker to return a fixed broker dict."""
    from src.api.main import app
    from src.services.broker_auth import get_current_broker

    def _override():
        return {"broker_id": BROKER_ID, "email": "b@test.com", "name": "Test Broker"}

    app.dependency_overrides[get_current_broker] = _override
    yield {"X-Broker": "true"}   # header value doesn't matter — dependency is mocked
    app.dependency_overrides.pop(get_current_broker, None)


@pytest.fixture
def admin_token(monkeypatch):
    from pydantic import SecretStr
    from config.settings import settings
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    from src.api.admin_router import create_access_token
    return create_access_token({"sub": "ops@heu.ai", "scope": "admin"})


@pytest.fixture
def admin_auth(admin_token, mock_session):
    return {"Authorization": f"Bearer {admin_token}"}


# ── Route registration ────────────────────────────────────────────────────────

class TestRouteRegistration:
    def test_all_broker_routes_registered(self, client):
        from src.api.main import app
        paths = {r.path for r in app.routes if hasattr(r, "path")}
        expected = {
            "/api/broker/lanes/{lane_id}/claim",
            "/api/broker/lanes/{lane_id}/transition",
            "/api/broker/lanes/{lane_id}/transitions",
            "/api/admin/lanes/{lane_id}/reassign-broker",
        }
        missing = expected - paths
        assert not missing, f"Routes missing: {missing}"


# ── POST /api/broker/lanes/{lane_id}/claim ────────────────────────────────────

class TestClaimLane:
    def test_broker_can_claim_open_lane(self, client, broker_auth):
        lane_obj = {
            "lane_id": LANE_ID,
            "assigned_broker_id": BROKER_ID,
            "lane_type": "distressed-payoff",
            "current_stage": "initial_contact",
            "current_stage_display": "Initial Contact",
            "current_work_state": "assigned",
            "outcome": "open",
            "prospect": {"prospect_id": str(uuid.uuid4())},
        }
        with patch("src.api.broker_router.assign_broker", return_value=True), \
             patch("src.api.broker_router.fetch_lane", return_value=lane_obj):
            resp = client.post(f"/api/broker/lanes/{LANE_ID}/claim")
        assert resp.status_code == 200
        body = resp.json()
        assert body["lane_id"] == LANE_ID
        assert body["assigned_broker_id"] == BROKER_ID

    def test_claim_response_returns_assigned_work_state(self, client, broker_auth):
        lane_obj = {
            "lane_id": LANE_ID,
            "assigned_broker_id": BROKER_ID,
            "current_work_state": "assigned",
            "prospect": {},
        }
        with patch("src.api.broker_router.assign_broker", return_value=True), \
             patch("src.api.broker_router.fetch_lane", return_value=lane_obj):
            resp = client.post(f"/api/broker/lanes/{LANE_ID}/claim")
        assert resp.json()["current_work_state"] == "assigned"

    def test_second_broker_claim_returns_409(self, client, broker_auth):
        with patch("src.api.broker_router.assign_broker", return_value=False):
            resp = client.post(f"/api/broker/lanes/{LANE_ID}/claim")
        assert resp.status_code == 409

    def test_lane_not_found_returns_404(self, client, broker_auth):
        from src.services.broker_state_machine import LaneNotFound
        with patch("src.api.broker_router.assign_broker",
                   side_effect=LaneNotFound("not found")):
            resp = client.post(f"/api/broker/lanes/{LANE_ID}/claim")
        assert resp.status_code == 404

    def test_inactive_broker_returns_403(self, client, broker_auth):
        from src.services.broker_state_machine import BrokerInactive
        with patch("src.api.broker_router.assign_broker",
                   side_effect=BrokerInactive("inactive")):
            resp = client.post(f"/api/broker/lanes/{LANE_ID}/claim")
        assert resp.status_code == 403

    def test_unauthenticated_claim_returns_403(self, client, mock_session):
        resp = client.post(f"/api/broker/lanes/{LANE_ID}/claim")
        assert resp.status_code in (401, 403)


# ── POST /api/broker/lanes/{lane_id}/transition ───────────────────────────────

class TestTransitionRoute:
    def test_broker_can_transition_own_lane(self, client, broker_auth):
        tid = str(uuid.uuid4())
        with patch("src.api.broker_router.current_state", return_value="assigned"), \
             patch("src.api.broker_router.transition", return_value=tid):
            resp = client.post(
                f"/api/broker/lanes/{LANE_ID}/transition",
                json={"to_state": "working", "reason_code": "qualified"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["transition_id"] == tid
        assert body["to_state"] == "working"
        assert body["from_state"] == "assigned"

    def test_transition_response_includes_allowed_next_states(self, client, broker_auth):
        tid = str(uuid.uuid4())
        with patch("src.api.broker_router.current_state", return_value="assigned"), \
             patch("src.api.broker_router.transition", return_value=tid):
            resp = client.post(
                f"/api/broker/lanes/{LANE_ID}/transition",
                json={"to_state": "working", "reason_code": "qualified"},
            )
        body = resp.json()
        assert "allowed_next_states" in body
        assert isinstance(body["allowed_next_states"], list)
        assert "quoted" in body["allowed_next_states"]

    def test_ownership_violation_returns_403(self, client, broker_auth):
        from src.services.broker_state_machine import LaneOwnershipError
        with patch("src.api.broker_router.current_state", return_value="assigned"), \
             patch("src.api.broker_router.transition",
                   side_effect=LaneOwnershipError("not your lane")):
            resp = client.post(
                f"/api/broker/lanes/{LANE_ID}/transition",
                json={"to_state": "working", "reason_code": "qualified"},
            )
        assert resp.status_code == 403

    def test_illegal_transition_returns_409(self, client, broker_auth):
        from src.services.broker_state_machine import IllegalTransition
        with patch("src.api.broker_router.current_state", return_value="assigned"), \
             patch("src.api.broker_router.transition",
                   side_effect=IllegalTransition("not allowed")):
            resp = client.post(
                f"/api/broker/lanes/{LANE_ID}/transition",
                json={"to_state": "closed_won", "reason_code": "funded"},
            )
        assert resp.status_code == 409

    def test_closed_won_without_gross_amount_returns_422(self, client, broker_auth):
        from src.services.broker_state_machine import ClosedWonPayloadRequired
        with patch("src.api.broker_router.current_state", return_value="committed"), \
             patch("src.api.broker_router.transition",
                   side_effect=ClosedWonPayloadRequired("missing payload")):
            resp = client.post(
                f"/api/broker/lanes/{LANE_ID}/transition",
                json={"to_state": "closed_won", "reason_code": "funded"},
            )
        assert resp.status_code == 422

    def test_closed_won_without_split_config_returns_422(self, client, broker_auth):
        from src.services.broker_state_machine import ClosedWonPayloadRequired
        with patch("src.api.broker_router.current_state", return_value="committed"), \
             patch("src.api.broker_router.transition",
                   side_effect=ClosedWonPayloadRequired("missing split")):
            resp = client.post(
                f"/api/broker/lanes/{LANE_ID}/transition",
                json={
                    "to_state": "closed_won",
                    "reason_code": "funded",
                    "gross_amount_cents": 500000,
                },
            )
        assert resp.status_code == 422

    def test_closed_won_with_full_payload_succeeds(self, client, broker_auth):
        tid = str(uuid.uuid4())
        with patch("src.api.broker_router.current_state", return_value="committed"), \
             patch("src.api.broker_router.transition", return_value=tid):
            resp = client.post(
                f"/api/broker/lanes/{LANE_ID}/transition",
                json={
                    "to_state": "closed_won",
                    "reason_code": "funded",
                    "gross_amount_cents": 500000,
                    "split_config_id": "platform_50_broker_50",
                },
            )
        assert resp.status_code == 200
        assert resp.json()["to_state"] == "closed_won"


# ── GET /api/broker/lanes/{lane_id}/transitions ───────────────────────────────

class TestTransitionHistory:
    def test_returns_ordered_transition_rows(self, client, broker_auth, mock_session):
        items = [
            {"transition_id": str(uuid.uuid4()), "from_state": "unassigned",
             "to_state": "assigned", "reason_code": "qualified",
             "occurred_at": "2026-06-29T10:00:00+00:00",
             "lane_id": LANE_ID, "prospect_id": str(uuid.uuid4()),
             "broker_id": BROKER_ID, "actor": BROKER_ID},
            {"transition_id": str(uuid.uuid4()), "from_state": "assigned",
             "to_state": "working", "reason_code": "qualified",
             "occurred_at": "2026-06-29T10:05:00+00:00",
             "lane_id": LANE_ID, "prospect_id": str(uuid.uuid4()),
             "broker_id": BROKER_ID, "actor": BROKER_ID},
        ]
        with patch("src.api.broker_router.current_state", return_value="working"), \
             patch("src.api.broker_router.list_transitions", return_value=items):
            resp = client.get(f"/api/broker/lanes/{LANE_ID}/transitions")
        assert resp.status_code == 200
        body = resp.json()
        assert body["current_state"] == "working"
        assert len(body["transitions"]) == 2
        assert body["transitions"][0]["to_state"] == "assigned"
        assert body["transitions"][1]["to_state"] == "working"

    def test_broker_cannot_read_another_brokers_history(self, client, mock_session):
        """Broker B token is rejected because mock lane is assigned to BROKER_ID, not OTHER."""
        from src.api.main import app
        from src.services.broker_auth import get_current_broker

        def _other_broker():
            return {"broker_id": OTHER_BROKER_ID, "email": "other@test.com", "name": "Other"}

        app.dependency_overrides[get_current_broker] = _other_broker
        try:
            # mock_session returns lane assigned to BROKER_ID (not OTHER_BROKER_ID)
            _lane = SimpleNamespace(assigned_broker_id=uuid.UUID(BROKER_ID))
            mock_session.execute.return_value.fetchone.return_value = _lane
            resp = client.get(f"/api/broker/lanes/{LANE_ID}/transitions")
        finally:
            app.dependency_overrides.pop(get_current_broker, None)

        assert resp.status_code == 403


# ── POST /api/admin/lanes/{lane_id}/reassign-broker ──────────────────────────

class TestAdminReassign:
    def test_admin_can_reassign_lane(self, client, admin_auth):
        tid = str(uuid.uuid4())
        new_broker = str(uuid.uuid4())
        with patch(
            "src.api.admin_router._bsm_reassign" if False else
            "src.services.broker_state_machine.reassign_lane",
            return_value=tid,
        ):
            resp = client.post(
                f"/api/admin/lanes/{LANE_ID}/reassign-broker",
                json={"broker_id": new_broker},
                headers=admin_auth,
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["assigned_broker_id"] == new_broker
        assert body["transition_id"] == tid
        assert body["status"] == "reassigned"

    def test_admin_reassign_lane_not_found_returns_404(self, client, admin_auth):
        from src.services.broker_state_machine import LaneNotFound
        with patch(
            "src.services.broker_state_machine.reassign_lane",
            side_effect=LaneNotFound("no lane"),
        ):
            resp = client.post(
                f"/api/admin/lanes/{LANE_ID}/reassign-broker",
                json={"broker_id": str(uuid.uuid4())},
                headers=admin_auth,
            )
        assert resp.status_code == 404

    def test_admin_reassign_inactive_broker_returns_403(self, client, admin_auth):
        from src.services.broker_state_machine import BrokerInactive
        with patch(
            "src.services.broker_state_machine.reassign_lane",
            side_effect=BrokerInactive("inactive"),
        ):
            resp = client.post(
                f"/api/admin/lanes/{LANE_ID}/reassign-broker",
                json={"broker_id": str(uuid.uuid4())},
                headers=admin_auth,
            )
        assert resp.status_code == 403

    def test_unauthenticated_reassign_returns_403(self, client, mock_session):
        resp = client.post(
            f"/api/admin/lanes/{LANE_ID}/reassign-broker",
            json={"broker_id": str(uuid.uuid4())},
        )
        assert resp.status_code in (401, 403)
