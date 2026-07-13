"""
Stripe switch pre-flight guard tests.

Verifies that `can_switch_subscription` blocks the right Subscriber.status
values and that the /api/annual/accept and /api/upgrade endpoints surface
a 409 with billing_status_blocked instead of falling through to a Stripe
502.

Run:
    pytest tests/test_stripe_switch_guards.py -v
"""
from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.core.models import Subscriber


@pytest.fixture
def client():
    from src.api.main import app
    return TestClient(app)


def _make_sub(db, status: str) -> Subscriber:
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_guard_{uid}",
        stripe_subscription_id=f"sub_guard_{uid}",
        tier="pro",
        vertical="roofing",
        county_id="hillsborough",
        event_feed_uuid=f"guard-{uid}",
        status=status,
        email=f"guard-{uid}@example.com",
    )
    db.add(sub)
    db.flush()
    db.commit()
    return sub


def _cleanup(db, sub):
    db.query(Subscriber).filter_by(id=sub.id).delete()
    db.commit()


# ── Unit tests on the helper ────────────────────────────────────────────────

class TestCanSwitchSubscription:
    def test_active_status_allowed(self):
        from src.services.stripe_service import can_switch_subscription
        sub = type("S", (), {"status": "active"})()
        ok, reason = can_switch_subscription(sub)
        assert ok is True
        assert reason is None

    def test_grace_blocked(self):
        from src.services.stripe_service import can_switch_subscription
        sub = type("S", (), {"status": "grace"})()
        ok, reason = can_switch_subscription(sub)
        assert ok is False
        assert reason == "grace"

    def test_churned_blocked(self):
        from src.services.stripe_service import can_switch_subscription
        sub = type("S", (), {"status": "churned"})()
        ok, reason = can_switch_subscription(sub)
        assert ok is False
        assert reason == "churned"

    def test_disputed_blocked(self):
        from src.services.stripe_service import can_switch_subscription
        sub = type("S", (), {"status": "disputed"})()
        ok, reason = can_switch_subscription(sub)
        assert ok is False
        assert reason == "disputed"

    def test_paused_blocked(self):
        from src.services.stripe_service import can_switch_subscription
        sub = type("S", (), {"status": "paused"})()
        ok, reason = can_switch_subscription(sub)
        assert ok is False
        assert reason == "paused"

    def test_cancelled_blocked(self):
        from src.services.stripe_service import can_switch_subscription
        sub = type("S", (), {"status": "cancelled"})()
        ok, reason = can_switch_subscription(sub)
        assert ok is False
        assert reason == "cancelled"

    def test_missing_subscriber_blocked(self):
        from src.services.stripe_service import can_switch_subscription
        ok, reason = can_switch_subscription(None)
        assert ok is False
        assert reason == "missing"


# ── Endpoint integration ────────────────────────────────────────────────────

class TestAnnualAcceptStatusGuard:
    def test_grace_subscriber_gets_409(self, fresh_db, client):
        sub = _make_sub(fresh_db, status="grace")
        try:
            resp = client.post("/api/annual/accept", json={"feed_uuid": sub.event_feed_uuid})
            assert resp.status_code == 409
            body = resp.json()["detail"]
            assert body["error"] == "billing_status_blocked"
            assert body["current_status"] == "grace"
            assert "billing_portal_url" in body
        finally:
            _cleanup(fresh_db, sub)

    def test_disputed_subscriber_gets_409(self, fresh_db, client):
        sub = _make_sub(fresh_db, status="disputed")
        try:
            resp = client.post("/api/annual/accept", json={"feed_uuid": sub.event_feed_uuid})
            assert resp.status_code == 409
            assert resp.json()["detail"]["current_status"] == "disputed"
        finally:
            _cleanup(fresh_db, sub)

    def test_active_subscriber_proceeds_to_stripe(self, fresh_db, client):
        sub = _make_sub(fresh_db, status="active")
        try:
            with patch("src.tasks.annual_push.switch_to_annual", return_value=True):
                resp = client.post("/api/annual/accept", json={"feed_uuid": sub.event_feed_uuid})
            assert resp.status_code == 200
            assert resp.json()["tier"] == "annual_lock"
        finally:
            _cleanup(fresh_db, sub)


class TestUpgradeStatusGuard:
    def test_grace_subscriber_gets_409(self, fresh_db, client):
        sub = _make_sub(fresh_db, status="grace")
        try:
            resp = client.post(
                "/api/upgrade",
                json={"feed_uuid": sub.event_feed_uuid, "tier": "autopilot_pro"},
            )
            assert resp.status_code == 409
            body = resp.json()["detail"]
            assert body["error"] == "billing_status_blocked"
            assert body["current_status"] == "grace"
        finally:
            _cleanup(fresh_db, sub)


class TestUpgradeSettlesOutgoingGuarantee:
    """A subscriber upgrading off a guaranteed tier (starter/pro/dominator)
    must not silently drop an already-closed guarantee cycle — /api/upgrade
    settles it before switching sub.tier away from the guaranteed value."""

    def test_guaranteed_tier_settlement_invoked_before_switch(self, fresh_db, client):
        sub = _make_sub(fresh_db, status="active")  # tier="pro" — guaranteed
        try:
            with (
                patch("src.services.stripe_service.switch_subscription_plan", return_value=True),
                patch("config.settings.AppSettings.active_stripe_price", return_value="price_test_123"),
                patch(
                    "src.tasks.guarantee_shortfall_sweep.evaluate_subscriber_guarantee",
                    return_value=None,
                ) as mock_evaluate,
            ):
                resp = client.post(
                    "/api/upgrade",
                    json={"feed_uuid": sub.event_feed_uuid, "tier": "autopilot_pro"},
                )
            assert resp.status_code == 200
            assert resp.json()["tier"] == "autopilot_pro"
            mock_evaluate.assert_called_once()
            called_sub = mock_evaluate.call_args.args[1]
            assert called_sub.id == sub.id
        finally:
            _cleanup(fresh_db, sub)

    def test_settles_every_backlogged_closed_period_before_switching(self, fresh_db, client):
        """A subscriber with several already-closed, never-evaluated cycles
        (sweep downtime, or guarantees just turned on) must have ALL of them
        settled before the tier flips — not just the oldest one."""
        sub = _make_sub(fresh_db, status="active")  # tier="pro" — guaranteed
        try:
            with (
                patch("src.services.stripe_service.switch_subscription_plan", return_value=True),
                patch("config.settings.AppSettings.active_stripe_price", return_value="price_test_123"),
                patch(
                    "src.tasks.guarantee_shortfall_sweep.evaluate_subscriber_guarantee",
                    side_effect=[
                        {"shortfall": True, "status": "issued"},
                        {"shortfall": False, "status": "met"},
                        {"shortfall": True, "status": "issued"},
                        None,  # no more closed cycles left to settle
                    ],
                ) as mock_evaluate,
            ):
                resp = client.post(
                    "/api/upgrade",
                    json={"feed_uuid": sub.event_feed_uuid, "tier": "autopilot_pro"},
                )
            assert resp.status_code == 200
            assert resp.json()["tier"] == "autopilot_pro"
            assert mock_evaluate.call_count == 4
        finally:
            _cleanup(fresh_db, sub)

    def test_settlement_loop_has_a_safety_cap(self, fresh_db, client):
        """evaluate_subscriber_guarantee() should never return non-None
        forever in practice, but the loop must not hang the request if it
        somehow did — and the upgrade must still complete."""
        sub = _make_sub(fresh_db, status="active")
        try:
            with (
                patch("src.services.stripe_service.switch_subscription_plan", return_value=True),
                patch("config.settings.AppSettings.active_stripe_price", return_value="price_test_123"),
                patch(
                    "src.tasks.guarantee_shortfall_sweep.evaluate_subscriber_guarantee",
                    return_value={"shortfall": False, "status": "met"},
                ) as mock_evaluate,
            ):
                resp = client.post(
                    "/api/upgrade",
                    json={"feed_uuid": sub.event_feed_uuid, "tier": "autopilot_pro"},
                )
            assert resp.status_code == 200
            assert mock_evaluate.call_count == 60
        finally:
            _cleanup(fresh_db, sub)

    def test_unguaranteed_tier_skips_settlement(self, fresh_db, client):
        """A subscriber already off a guaranteed tier (e.g. data_only)
        shouldn't trigger a settlement lookup on every upgrade call."""
        sub = _make_sub(fresh_db, status="active")
        sub.tier = "data_only"
        fresh_db.commit()
        try:
            with (
                patch("src.services.stripe_service.switch_subscription_plan", return_value=True),
                patch("config.settings.AppSettings.active_stripe_price", return_value="price_test_123"),
                patch(
                    "src.tasks.guarantee_shortfall_sweep.evaluate_subscriber_guarantee",
                ) as mock_evaluate,
            ):
                resp = client.post(
                    "/api/upgrade",
                    json={"feed_uuid": sub.event_feed_uuid, "tier": "partner"},
                )
            assert resp.status_code == 200
            mock_evaluate.assert_not_called()
        finally:
            _cleanup(fresh_db, sub)
