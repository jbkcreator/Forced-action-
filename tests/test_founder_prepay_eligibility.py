"""
B0-04 — Founder prepay portal eligibility endpoint.

Run:
    pytest tests/test_founder_prepay_eligibility.py -v
"""

import uuid
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app)


def _make_subscriber(fresh_db, **overrides):
    from src.core.models import Subscriber
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_fpe_{uid}",
        tier=overrides.pop("tier", "starter"),
        vertical="roofing",
        county_id="hillsborough",
        event_feed_uuid=f"fpe-{uid}",
        stripe_subscription_id=overrides.pop("stripe_subscription_id", "sub_fake"),
        founding_member=overrides.pop("founding_member", True),
        founding_price_id=overrides.pop("founding_price_id", "price_founding_starter"),
        rate_locked_at=overrides.pop("rate_locked_at", datetime.now(timezone.utc)),
        escalated_at=overrides.pop("escalated_at", None),
    )
    fresh_db.add(sub)
    fresh_db.commit()
    return sub


class TestFounderPrepayEligibility:
    def test_unknown_feed_uuid_returns_404(self, client):
        resp = client.get("/api/founders/prepay-eligibility", params={"feed_uuid": "no-such-uuid"})
        assert resp.status_code == 404

    def test_founding_member_within_window_is_eligible(self, client, fresh_db):
        sub = _make_subscriber(fresh_db)
        try:
            resp = client.get("/api/founders/prepay-eligibility", params={"feed_uuid": sub.event_feed_uuid})
            assert resp.status_code == 200
            body = resp.json()
            assert body["eligible"] is True
            assert body["has_active_subscription"] is True
        finally:
            fresh_db.delete(sub)
            fresh_db.commit()

    def test_escalated_founder_is_not_eligible(self, client, fresh_db):
        sub = _make_subscriber(fresh_db, escalated_at=datetime.now(timezone.utc))
        try:
            resp = client.get("/api/founders/prepay-eligibility", params={"feed_uuid": sub.event_feed_uuid})
            assert resp.status_code == 200
            assert resp.json()["eligible"] is False
        finally:
            fresh_db.delete(sub)
            fresh_db.commit()

    def test_already_annual_lock_is_not_eligible(self, client, fresh_db):
        sub = _make_subscriber(fresh_db, tier="annual_lock")
        try:
            resp = client.get("/api/founders/prepay-eligibility", params={"feed_uuid": sub.event_feed_uuid})
            assert resp.status_code == 200
            assert resp.json()["eligible"] is False
        finally:
            fresh_db.delete(sub)
            fresh_db.commit()

    def test_non_founder_is_not_eligible(self, client, fresh_db):
        sub = _make_subscriber(fresh_db, founding_member=False)
        try:
            resp = client.get("/api/founders/prepay-eligibility", params={"feed_uuid": sub.event_feed_uuid})
            assert resp.status_code == 200
            assert resp.json()["eligible"] is False
        finally:
            fresh_db.delete(sub)
            fresh_db.commit()

    def test_no_active_subscription_is_not_eligible(self, client, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_subscription_id=None)
        try:
            resp = client.get("/api/founders/prepay-eligibility", params={"feed_uuid": sub.event_feed_uuid})
            assert resp.status_code == 200
            body = resp.json()
            assert body["eligible"] is False
            assert body["has_active_subscription"] is False
        finally:
            fresh_db.delete(sub)
            fresh_db.commit()
