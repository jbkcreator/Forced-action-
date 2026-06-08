from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.core.models import CoraSuppression, DealOutcome, DistressScore, Owner, Property, Subscriber


@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app)


def _mk_sub_and_prop(fresh_db):
    uid = datetime.now(timezone.utc).strftime("%H%M%S%f")
    sub = Subscriber(
        stripe_customer_id=f"cus_win_capture_{uid}",
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        event_feed_uuid=f"win-capture-{uid}",
        email=f"win_capture_{uid}@example.com",
        status="active",
    )
    prop = Property(
        parcel_id=f"WIN-CAPTURE-{uid}",
        address="42 Capture Way",
        city="Tampa",
        state="FL",
        zip="33601",
        county_id="hillsborough",
    )
    fresh_db.add_all([sub, prop])
    fresh_db.flush()
    fresh_db.commit()
    return sub, prop


def _cleanup(fresh_db, sub, prop):
    fresh_db.execute(DealOutcome.__table__.delete().where(DealOutcome.subscriber_id == sub.id))
    fresh_db.execute(CoraSuppression.__table__.delete().where(CoraSuppression.subscriber_id == sub.id))
    fresh_db.delete(sub)
    fresh_db.delete(prop)
    fresh_db.commit()


def test_record_win_autopsy_writes_learning_card(fresh_db):
    uid = datetime.now(timezone.utc).strftime("%H%M%S%f")
    sub = Subscriber(
        stripe_customer_id=f"cus_win_auto_{uid}",
        tier="starter",
        vertical="roofing",
        county_id="pinellas",
        event_feed_uuid=f"win-auto-{uid}",
        email=f"win_auto_{uid}@example.com",
        status="active",
    )
    prop = Property(
        parcel_id=f"WIN-AUTO-{uid}",
        address="123 Win Loop",
        city="St Petersburg",
        state="FL",
        zip="33701",
        county_id="pinellas",
    )
    fresh_db.add_all([sub, prop])
    fresh_db.flush()
    owner = Owner(
        property_id=prop.id,
        owner_name="Win Owner",
        owner_type="Individual",
        county_id="pinellas",
        phone_1="+17275550100",
        phone_metadata={"phone_1": {"type": "mobile", "score": 92, "reachable": True}},
        contact_info_confidence="high",
        contact_info_confidence_score=Decimal("0.920"),
    )
    score = DistressScore(
        property_id=prop.id,
        county_id="pinellas",
        lead_tier="Platinum",
        final_cds_score=Decimal("86.50"),
        distress_types={"storm_damage": True, "permit_history": 2},
        vertical_scores={"roofing": 9.1},
        urgency_level="High",
    )
    deal = DealOutcome(
        subscriber_id=sub.id,
        property_id=prop.id,
        deal_size_bucket="10_25k",
        deal_date=date.today(),
        days_to_close=12,
        pipeline_stage="closed_won",
        county_id="pinellas",
        trade_vertical="roofing",
    )
    fresh_db.add_all([owner, score, deal])
    fresh_db.flush()

    with patch("src.services.win_autopsy._prime_cache"):
        from src.services.win_autopsy import record_win_autopsy
        card = record_win_autopsy(deal.id, fresh_db)

    assert card is not None
    assert card.card_type == "win_autopsy"
    assert "reuse_success_patterns" == card.action_taken
    assert card.data_json["latest_win"]["county_id"] == "pinellas"
    assert "signal:storm_damage" in card.data_json["pattern_counts"]
    assert "contact_confidence:high" in card.data_json["pattern_counts"]


def test_deal_capture_calls_win_autopsy_for_real_win(client, fresh_db):
    sub, prop = _mk_sub_and_prop(fresh_db)
    from pathlib import Path
    fake_path = Path("data/win_graphics/dummy.png")
    with patch("src.tasks.annual_push._push_annual_offer") as mock_annual, \
         patch("src.services.win_graphic.generate", return_value=fake_path), \
         patch("src.services.win_autopsy.record_win_autopsy") as mock_autopsy:
        resp = client.post("/api/deal-capture", json={
            "feed_uuid": sub.event_feed_uuid,
            "property_id": prop.id,
            "deal_size_bucket": "5_10k",
            "deal_amount": 7500,
        })

    assert resp.status_code == 201
    mock_annual.assert_not_called()
    mock_autopsy.assert_called_once()
    _cleanup(fresh_db, sub, prop)
