"""
Lead-pack webhook reservation, segment-aware (Task #7 / ADR 0032, D5/D8).

`_on_lead_pack_payment` must apply the SAME insurance-distress filter used by
the checkout gate — otherwise the pre-payment gate and the post-payment
reservation could disagree, reserving leads the buyer never actually paid a
premium for (or short-packing a segment purchase that the gate wrongly let
through).
"""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from src.core.models import DistressScore, Incident, Owner, Property, Subscriber, LeadPackPurchase
from src.services import stripe_webhooks
from src.services.lead_exclusivity import get_exclusive_property_ids


def _table_exists(db):
    from sqlalchemy import text
    return db.execute(text("SELECT to_regclass('public.lead_exclusivity')")).scalar() is not None


@pytest.fixture
def db(fresh_db):
    if not _table_exists(fresh_db):
        pytest.skip("lead_exclusivity table not available")
    return fresh_db


def _mk_subscriber(db, uuid, vertical="wholesalers", county="hillsborough"):
    s = Subscriber(
        stripe_customer_id=f"cus_{uuid}", tier="pro", vertical=vertical,
        county_id=county, status="active", event_feed_uuid=uuid,
        email=f"{uuid}@example.com",
    )
    db.add(s)
    db.flush()
    return s


def _mk_property(db, parcel, zip_code, county, vertical_scores, has_incident=None, incident_days_ago=10):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county, address=f"{parcel} Test St")
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=80.0,
        vertical_scores=vertical_scores,
        score_date=datetime.now(timezone.utc).date(),
    ))
    db.add(Owner(property_id=p.id, phone_1="8135550100", contact_info_confidence="high"))
    if has_incident:
        db.add(Incident(
            property_id=p.id, incident_type=has_incident,
            incident_date=(datetime.now(timezone.utc) - timedelta(days=incident_days_ago)).date(),
        ))
    db.flush()
    return p.id


def _pi(pi_id, uuid, zip_code, vertical, segment, county):
    return {
        "id": pi_id,
        "metadata": {
            "product": "lead_pack", "feed_uuid": uuid, "zip_code": zip_code,
            "vertical": vertical, "county_id": county, "segment": segment,
        },
    }


def _get(db, pi_id):
    return db.execute(
        select(LeadPackPurchase).where(LeadPackPurchase.stripe_payment_intent_id == pi_id)
    ).scalar_one()


class TestSegmentWebhookReservation:
    def test_reserves_only_segment_qualifying_leads(self, db):
        _mk_subscriber(db, "wh-seg-ok", vertical="wholesalers")
        zip_code = "95301"
        county = "hillsborough"
        segment_ids = [
            _mk_property(db, f"WH-A{i}", zip_code, county, {"wholesalers": 50.0}, has_incident="storm_damage")
            for i in range(5)
        ]
        # Plenty of non-segment leads in the same ZIP that must NOT be selected.
        for i in range(5):
            _mk_property(db, f"WH-B{i}", zip_code, county, {"wholesalers": 50.0})

        stripe_webhooks._on_lead_pack_payment(
            _pi("pi_wh_ok", "wh-seg-ok", zip_code, "wholesalers", "insurance_distress", county), db
        )
        purchase = _get(db, "pi_wh_ok")

        assert purchase.status == "enriching"
        assert set(purchase.lead_ids) == set(segment_ids)

    def test_short_packs_when_segment_qualifying_leads_below_5(self, db):
        _mk_subscriber(db, "wh-seg-short", vertical="wholesalers")
        zip_code = "95302"
        county = "hillsborough"
        for i in range(4):
            _mk_property(db, f"WH-C{i}", zip_code, county, {"wholesalers": 50.0}, has_incident="flood_damage")
        # 5th qualifies for the PLAIN pack but not the segment (no damage incident).
        _mk_property(db, "WH-C4", zip_code, county, {"wholesalers": 50.0})

        from unittest.mock import patch
        with patch("stripe.Refund.create", return_value={"id": "re_wh_short"}):
            stripe_webhooks._on_lead_pack_payment(
                _pi("pi_wh_short", "wh-seg-short", zip_code, "wholesalers", "insurance_distress", county), db
            )
        purchase = _get(db, "pi_wh_short")

        assert purchase.status == "refunded"
        assert purchase.refund_reason == "short_pack_4_of_5"
        assert not purchase.lead_ids
