"""
Staging-level end-to-end test for the Lead Pack cross-trade exclusivity flow.

Drives the REAL webhook handler (_on_lead_pack_payment) against real Postgres,
with only the Stripe SDK mocked. Seeds qualified properties, delivers a pack,
and asserts: delivery, exclusivity rows, cross-trade feed gating, short-pack
refund, and the county-launch refund guard.

Self-skips when DATABASE_URL / the lead_exclusivity table are unavailable.
"""
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest
from sqlalchemy import text

from src.core.models import Property, DistressScore, Owner, Subscriber, LeadPackPurchase
from src.services.lead_exclusivity import get_exclusive_property_ids
from src.services import stripe_webhooks


SRC_COUNTY = None  # resolved from settings at runtime


def _table_exists(db) -> bool:
    return db.execute(text("SELECT to_regclass('public.lead_exclusivity')")).scalar() is not None


@pytest.fixture
def db(fresh_db):
    if not _table_exists(fresh_db):
        pytest.skip("lead_exclusivity table not migrated")
    global SRC_COUNTY
    from config.settings import get_settings
    SRC_COUNTY = get_settings().county_launch_source_county or "hillsborough"
    return fresh_db


def _mk_property(db, parcel, zip_code, county, score=80.0):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county, address=f"{parcel} Test St")
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=score,
        vertical_scores={"roofing": score, "restoration": score},
        score_date=datetime.now(timezone.utc).date(),
    ))
    db.add(Owner(property_id=p.id, phone_1="8135550100", contact_info_confidence="high"))
    db.flush()
    return p.id


def _mk_subscriber(db, uuid, vertical="roofing", county=None):
    s = Subscriber(
        stripe_customer_id=f"cus_{uuid}", tier="pro", vertical=vertical,
        county_id=county or SRC_COUNTY, status="active", event_feed_uuid=uuid,
        email=f"{uuid}@example.com",
    )
    db.add(s)
    db.flush()
    return s


def _pi(pi_id, uuid, zip_code, vertical="roofing", county=None):
    return {
        "id": pi_id,
        "metadata": {
            "product": "lead_pack", "feed_uuid": uuid, "zip_code": zip_code,
            "vertical": vertical, "county_id": county or SRC_COUNTY,
        },
    }


class TestLeadPackE2E:
    def test_full_delivery_and_cross_trade_gating(self, db):
        sub = _mk_subscriber(db, "e2e-deliver", vertical="roofing")
        zip_code = "95001"
        pids = [_mk_property(db, f"PARCEL-A{i}", zip_code, SRC_COUNTY, score=90 - i) for i in range(6)]

        stripe_webhooks._on_lead_pack_payment(_pi("pi_e2e_deliver", "e2e-deliver", zip_code), db)

        purchase = db.execute(
            select_leadpack("pi_e2e_deliver")
        ).scalar_one()
        assert purchase.status == "delivered"
        assert len(purchase.lead_ids) == 5
        assert set(purchase.lead_ids).issubset(set(pids))

        now = datetime.now(timezone.utc)
        # 5 exclusivity rows recorded
        assert get_exclusive_property_ids(db, SRC_COUNTY, now, zip_code=zip_code) == set(purchase.lead_ids)
        # Hidden from a DIFFERENT trade (restoration)
        assert get_exclusive_property_ids(db, SRC_COUNTY, now, zip_code=zip_code, exclude_trade="restoration") == set(purchase.lead_ids)
        # Buyer's own trade (roofing) still sees them
        assert get_exclusive_property_ids(db, SRC_COUNTY, now, zip_code=zip_code, exclude_trade="roofing") == set()

    def test_short_pack_triggers_full_refund(self, db):
        _mk_subscriber(db, "e2e-short", vertical="roofing")
        zip_code = "95002"
        # Only 4 qualified leads — below the 5 threshold.
        for i in range(4):
            _mk_property(db, f"PARCEL-B{i}", zip_code, SRC_COUNTY)

        with patch("stripe.Refund.create", return_value={"id": "re_e2e_short"}) as refund:
            stripe_webhooks._on_lead_pack_payment(_pi("pi_e2e_short", "e2e-short", zip_code), db)

        purchase = db.execute(select_leadpack("pi_e2e_short")).scalar_one()
        assert purchase.status == "refunded"
        assert purchase.refund_reason == "short_pack_4_of_5"
        assert purchase.stripe_refund_id == "re_e2e_short"
        assert not purchase.lead_ids
        refund.assert_called_once()
        # No exclusivity written for a refunded short pack
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, SRC_COUNTY, now, zip_code=zip_code) == set()

    def test_unlaunched_county_refunds(self, db):
        _mk_subscriber(db, "e2e-nolaunch", vertical="roofing", county="ghost_county")
        zip_code = "95003"
        for i in range(6):
            _mk_property(db, f"PARCEL-C{i}", zip_code, "ghost_county")

        with patch("stripe.Refund.create", return_value={"id": "re_e2e_ghost"}) as refund:
            stripe_webhooks._on_lead_pack_payment(
                _pi("pi_e2e_ghost", "e2e-nolaunch", zip_code, county="ghost_county"), db
            )

        purchase = db.execute(select_leadpack("pi_e2e_ghost")).scalar_one()
        assert purchase.status == "refunded"
        assert purchase.refund_reason == "county_not_launched"
        refund.assert_called_once()

    def test_concurrent_idempotency_second_pi_does_not_double_sell(self, db):
        """Two different buyers, same ZIP+trade: the second must get DIFFERENT
        (or fewer→refund) leads, never the same five."""
        _mk_subscriber(db, "e2e-buyer1", vertical="roofing")
        _mk_subscriber(db, "e2e-buyer2", vertical="restoration")
        zip_code = "95004"
        # Exactly 5 qualified — buyer 1 takes all, buyer 2 must short-pack refund.
        for i in range(5):
            _mk_property(db, f"PARCEL-D{i}", zip_code, SRC_COUNTY)

        stripe_webhooks._on_lead_pack_payment(_pi("pi_b1", "e2e-buyer1", zip_code, vertical="roofing"), db)
        p1 = db.execute(select_leadpack("pi_b1")).scalar_one()
        assert p1.status == "delivered" and len(p1.lead_ids) == 5

        with patch("stripe.Refund.create", return_value={"id": "re_b2"}):
            stripe_webhooks._on_lead_pack_payment(_pi("pi_b2", "e2e-buyer2", zip_code, vertical="restoration"), db)
        p2 = db.execute(select_leadpack("pi_b2")).scalar_one()
        # Cross-trade exclusivity locked all 5 → buyer 2 sees 0 available → refunded.
        assert p2.status == "refunded"
        assert not p2.lead_ids


def select_leadpack(pi_id):
    from sqlalchemy import select
    return select(LeadPackPurchase).where(LeadPackPurchase.stripe_payment_intent_id == pi_id)
