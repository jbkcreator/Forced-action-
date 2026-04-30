"""
Stage 5 — FastAPI route smoke tests.

Verifies every new endpoint is registered, basic 4xx error handling works,
and request validation is wired up correctly. Real Stripe / GHL calls are
mocked at the service-method level so we don't hit live infra.

Run:
    pytest tests/test_stage5_api.py -v
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app)


# ============================================================================
# Route registration
# ============================================================================


class TestStage5Routes:
    def test_all_stage5_routes_present(self, client):
        from src.api.main import app
        paths = [r.path for r in app.routes if hasattr(r, "path")]
        expected = {
            "/api/premium/purchase",
            "/api/win-graphic/{deal_outcome_id}",
            "/api/proof-wall",
            "/api/annual/accept",
            "/api/upgrade",
            "/api/feed/{feed_uuid}/team-view",
            "/api/leaderboard",
        }
        missing = expected - set(paths)
        assert not missing, f"Stage 5 routes missing: {missing}"


# ============================================================================
# /api/premium/purchase
# ============================================================================


class TestPremiumPurchaseEndpoint:
    def test_invalid_sku_returns_422(self, client):
        resp = client.post("/api/premium/purchase", json={
            "feed_uuid": "x", "sku": "not_real", "payment_mode": "credits", "property_id": 1,
        })
        assert resp.status_code == 422

    def test_invalid_payment_mode_returns_422(self, client):
        resp = client.post("/api/premium/purchase", json={
            "feed_uuid": "x", "sku": "report", "payment_mode": "wire", "property_id": 1,
        })
        assert resp.status_code == 422

    def test_unknown_feed_uuid_returns_403(self, client):
        resp = client.post("/api/premium/purchase", json={
            "feed_uuid": "definitely-not-a-real-uuid",
            "sku": "report", "payment_mode": "credits", "property_id": 1,
        })
        assert resp.status_code == 403

    def test_missing_property_id_for_report_returns_400(self, client, fresh_db):
        from src.core.models import Subscriber
        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_pp_{uid}", tier="starter",
            vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"pp-{uid}",
        )
        fresh_db.add(sub)
        fresh_db.commit()
        resp = client.post("/api/premium/purchase", json={
            "feed_uuid": sub.event_feed_uuid,
            "sku": "report",
            "payment_mode": "credits",
            # property_id intentionally missing
        })
        assert resp.status_code == 400
        # Cleanup
        fresh_db.delete(sub)
        fresh_db.commit()


# ============================================================================
# /api/proof-wall + /api/win-graphic/{id}
# ============================================================================


class TestProofWallEndpoint:
    def test_proof_wall_returns_items_array(self, client):
        resp = client.get("/api/proof-wall")
        assert resp.status_code == 200
        body = resp.json()
        assert "items" in body
        assert isinstance(body["items"], list)

    def test_proof_wall_limit_clamped(self, client):
        resp = client.get("/api/proof-wall?limit=999")
        # FastAPI Query(le=200) → 422
        assert resp.status_code == 422


class TestWinGraphicEndpoint:
    def test_unknown_id_returns_404(self, client):
        # 99_999_999 is unlikely to exist in any test environment
        resp = client.get("/api/win-graphic/99999999")
        assert resp.status_code == 404


# ============================================================================
# /api/annual/accept
# ============================================================================


class TestAnnualAcceptEndpoint:
    def test_unknown_feed_uuid_returns_403(self, client):
        resp = client.post("/api/annual/accept", json={"feed_uuid": "no-such-uuid"})
        assert resp.status_code == 403

    def test_no_subscription_returns_400(self, client, fresh_db):
        from src.core.models import Subscriber
        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_aa_{uid}", tier="starter",
            vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"aa-{uid}",
            stripe_subscription_id=None,
        )
        fresh_db.add(sub)
        fresh_db.commit()
        resp = client.post("/api/annual/accept", json={"feed_uuid": sub.event_feed_uuid})
        assert resp.status_code == 400
        fresh_db.delete(sub)
        fresh_db.commit()


# ============================================================================
# /api/upgrade
# ============================================================================


class TestUpgradeEndpoint:
    def test_invalid_tier_returns_422(self, client):
        resp = client.post("/api/upgrade", json={"feed_uuid": "x", "tier": "fake_tier"})
        assert resp.status_code == 422

    def test_unknown_feed_uuid_returns_403(self, client):
        resp = client.post("/api/upgrade", json={"feed_uuid": "no-such", "tier": "autopilot_pro"})
        assert resp.status_code == 403


# ============================================================================
# /api/leaderboard
# ============================================================================


class TestLeaderboardEndpoint:
    def test_returns_payload_shape(self, client):
        resp = client.get("/api/leaderboard")
        assert resp.status_code == 200
        body = resp.json()
        assert "as_of" in body
        assert "leaderboards" in body
        assert isinstance(body["leaderboards"], list)

    def test_county_filter_accepted(self, client):
        resp = client.get("/api/leaderboard?county_id=hillsborough&vertical=roofing")
        assert resp.status_code == 200


# ============================================================================
# /api/feed/{feed_uuid}/team-view
# ============================================================================


class TestTeamViewEndpoint:
    def test_unknown_feed_returns_403(self, client):
        resp = client.get("/api/feed/no-such-uuid/team-view")
        assert resp.status_code == 403

    def test_no_team_returns_unlocked_false(self, client, fresh_db):
        from src.core.models import Subscriber
        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_tv_{uid}", tier="starter",
            vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"tv-{uid}",
        )
        fresh_db.add(sub)
        fresh_db.commit()
        resp = client.get(f"/api/feed/{sub.event_feed_uuid}/team-view")
        assert resp.status_code == 200
        body = resp.json()
        assert body["unlocked"] is False
        assert body["density"] == []
        fresh_db.delete(sub)
        fresh_db.commit()
