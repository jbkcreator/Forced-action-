"""CDE-11 — deal-capture tags subscriber-reported outcomes; ownerless outcomes
fire no subscriber-only side-effects."""
import uuid
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.core.models import CoraSuppression, DealOutcome, Property, Subscriber


@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app)


def _mk_sub_and_prop(fresh_db):
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_c11_{uid}",
        tier="starter", vertical="roofing", county_id="hillsborough",
        event_feed_uuid=f"c11-{uid}", email=f"c11_{uid}@example.com",
        name=f"C11 {uid}", status="active",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    prop = Property(
        parcel_id=f"P-C11-{uid}", address=f"11 CDE #{uid}",
        city="Tampa", state="FL", zip="33601", county_id="hillsborough",
    )
    fresh_db.add(prop)
    fresh_db.flush()
    fresh_db.commit()
    return sub, prop


def _cleanup(fresh_db, sub, prop):
    from sqlalchemy import text
    fresh_db.execute(DealOutcome.__table__.delete().where(DealOutcome.subscriber_id == sub.id))
    fresh_db.execute(CoraSuppression.__table__.delete().where(CoraSuppression.subscriber_id == sub.id))
    fresh_db.execute(text("DELETE FROM referral_prompt_funnel WHERE subscriber_id = :sid"), {"sid": sub.id})
    fresh_db.delete(sub)
    fresh_db.delete(prop)
    fresh_db.commit()


def test_deal_capture_tags_subscriber_reported(client, fresh_db):
    """A subscriber's one-tap outcome is tagged subscriber_reported / subscriber_tap."""
    sub, prop = _mk_sub_and_prop(fresh_db)
    with patch("src.services.win_graphic.generate", return_value=None):
        resp = client.post("/api/deal-capture", json={
            "feed_uuid": sub.event_feed_uuid,
            "property_id": prop.id,
            "deal_size_bucket": "5_10k",
            "deal_amount": 6000,
        })
    assert resp.status_code == 201
    deal_id = resp.json()["deal_id"]
    row = fresh_db.get(DealOutcome, deal_id)
    assert row.confidence_tier == "subscriber_reported"
    assert row.outcome_source == "subscriber_tap"
    _cleanup(fresh_db, sub, prop)


def test_ownerless_outcome_fires_no_subscriber_side_effects(fresh_db):
    """A public-record inferred outcome (no subscriber) triggers none of the four
    subscriber-only side-effects: win graphic, annual push, attribution, suppression."""
    from datetime import date

    from src.services.deal_outcome_effects import record_outcome_side_effects

    uid = uuid.uuid4().hex[:8]
    prop = Property(
        parcel_id=f"P-OWN-{uid}", address=f"0 Ownerless #{uid}",
        city="Tampa", state="FL", zip="33601", county_id="hillsborough",
    )
    fresh_db.add(prop)
    fresh_db.flush()
    outcome = DealOutcome(
        subscriber_id=None,
        property_id=prop.id,
        deal_size_bucket="25k_plus",
        deal_amount=50000,
        deal_date=date.today(),
        pipeline_stage="closed_won",
        confidence_tier="public_record_inferred",
        outcome_source="foreclosure_auction",
    )
    fresh_db.add(outcome)
    fresh_db.flush()

    with patch("src.services.cora_suppression.create_suppression") as mock_suppress, \
         patch("src.services.win_graphic.generate") as mock_graphic, \
         patch("src.tasks.annual_push._push_annual_offer") as mock_annual, \
         patch("src.services.attribution_service.record_conversion_attribution") as mock_attr:
        result = record_outcome_side_effects(outcome, None, fresh_db)

    mock_suppress.assert_not_called()
    mock_graphic.assert_not_called()
    mock_annual.assert_not_called()
    mock_attr.assert_not_called()
    assert result == {"graphic_url": None, "annual_offered": False}

    fresh_db.execute(DealOutcome.__table__.delete().where(DealOutcome.id == outcome.id))
    fresh_db.delete(prop)
    fresh_db.commit()
