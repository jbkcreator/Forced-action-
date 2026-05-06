"""
Deal-capture Stage 5 hooks — graphic generation + annual-at-deal-win trigger.

Run:
    pytest tests/test_deal_capture_stage5.py -v
"""

import uuid
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.core.models import DealOutcome, Property, Subscriber


@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app)


def _mk_sub_and_prop(fresh_db, *, account_age_days: int = 30, founding: bool = False):
    """Create a subscriber + property for deal-capture e2e tests."""
    from datetime import datetime, timedelta, timezone
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_dc_{uid}",
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        event_feed_uuid=f"dc-{uid}",
        email=f"sub_{uid}@example.com",
        name=f"Test {uid}",
        founding_member=founding,
        status="active",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    sub.created_at = datetime.now(timezone.utc) - timedelta(days=account_age_days)

    prop = Property(
        parcel_id=f"P-DC-{uid}",
        address=f"42 Deal Capture #{uid}",
        city="Tampa", state="FL", zip="33601",
        county_id="hillsborough",
    )
    fresh_db.add(prop)
    fresh_db.flush()
    fresh_db.commit()
    return sub, prop


def _cleanup(fresh_db, sub, prop):
    fresh_db.execute(DealOutcome.__table__.delete().where(DealOutcome.subscriber_id == sub.id))
    fresh_db.delete(sub)
    fresh_db.delete(prop)
    fresh_db.commit()


class TestDealCaptureGraphic:
    def test_small_deal_no_graphic_no_annual(self, client, fresh_db):
        """Skip-bucket deals don't produce a graphic and don't fire annual."""
        sub, prop = _mk_sub_and_prop(fresh_db)
        with patch("src.tasks.annual_push._push_annual_offer") as mock_annual, \
             patch("src.services.win_graphic.generate") as mock_gen:
            resp = client.post("/api/deal-capture", json={
                "feed_uuid": sub.event_feed_uuid,
                "property_id": prop.id,
                "deal_size_bucket": "skip",
            })
        assert resp.status_code == 201
        body = resp.json()
        assert body["graphic_url"] is None
        assert body["annual_offered"] is False
        mock_gen.assert_not_called()
        mock_annual.assert_not_called()
        _cleanup(fresh_db, sub, prop)

    def test_medium_deal_makes_graphic_no_annual(self, client, fresh_db):
        sub, prop = _mk_sub_and_prop(fresh_db)
        from pathlib import Path
        fake_path = Path("data/win_graphics/dummy.png")
        with patch("src.tasks.annual_push._push_annual_offer") as mock_annual, \
             patch("src.services.win_graphic.generate", return_value=fake_path):
            resp = client.post("/api/deal-capture", json={
                "feed_uuid": sub.event_feed_uuid,
                "property_id": prop.id,
                "deal_size_bucket": "5_10k",
                "deal_amount": 7500,
            })
        assert resp.status_code == 201
        body = resp.json()
        assert body["graphic_url"] is not None       # graphic generated
        assert body["annual_offered"] is False       # below $10K threshold
        mock_annual.assert_not_called()
        _cleanup(fresh_db, sub, prop)

    def test_big_deal_fires_annual_and_graphic(self, client, fresh_db):
        """A $15K deal with bucket=10_25k fires both the graphic and annual push."""
        sub, prop = _mk_sub_and_prop(fresh_db)
        from pathlib import Path
        fake_path = Path("data/win_graphics/dummy.png")
        with patch("src.tasks.annual_push._push_annual_offer", return_value=True) as mock_annual, \
             patch("src.services.win_graphic.generate", return_value=fake_path):
            resp = client.post("/api/deal-capture", json={
                "feed_uuid": sub.event_feed_uuid,
                "property_id": prop.id,
                "deal_size_bucket": "10_25k",
                "deal_amount": 15000,
            })
        assert resp.status_code == 201
        body = resp.json()
        assert body["graphic_url"] is not None
        assert body["annual_offered"] is True
        # Trigger reason was deal_win_10k
        assert mock_annual.call_args[0][1] == "deal_win_10k"
        _cleanup(fresh_db, sub, prop)

    def test_25k_plus_bucket_fires_annual(self, client, fresh_db):
        sub, prop = _mk_sub_and_prop(fresh_db)
        from pathlib import Path
        fake_path = Path("data/win_graphics/dummy.png")
        with patch("src.tasks.annual_push._push_annual_offer", return_value=True) as mock_annual, \
             patch("src.services.win_graphic.generate", return_value=fake_path):
            resp = client.post("/api/deal-capture", json={
                "feed_uuid": sub.event_feed_uuid,
                "property_id": prop.id,
                "deal_size_bucket": "25k_plus",
                "deal_amount": None,
            })
        assert resp.status_code == 201
        assert resp.json()["annual_offered"] is True
        _cleanup(fresh_db, sub, prop)
