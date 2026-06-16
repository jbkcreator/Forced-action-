"""
Staging-level end-to-end test for the Lead Pack flow (ADR 0018).

Drives the REAL webhook handler (_on_lead_pack_payment) AND the REAL fulfillment
core (lead_pack_fulfillment_sweep.fulfill_purchase) against real Postgres, with
Stripe + Tracerfy mocked. Asserts the two-phase lifecycle:

  payment  → RESERVE (status='enriching', 5 leads locked at payment)
  sweep    → Hot-Enrichment + 100% Quality Floor → deliver | refund

Covers: reservation, cross-trade feed gating, full delivery + SentLead rows,
short-pack refund (webhook), unlaunched-county refund (webhook), no-double-sell,
and the Quality-Floor miss → refund + release path (sweep).

Self-skips when DATABASE_URL / the lead_exclusivity table are unavailable.
"""
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import select, text

from src.core.models import Property, DistressScore, Owner, Subscriber, LeadPackPurchase
from src.services.lead_exclusivity import get_exclusive_property_ids
from src.services import stripe_webhooks
from src.tasks.lead_pack_fulfillment_sweep import fulfill_purchase


SRC_COUNTY = None  # resolved from settings at runtime


def _table_exists(db) -> bool:
    return db.execute(text("SELECT to_regclass('public.lead_exclusivity')")).scalar() is not None


def _has_enriching_col(db) -> bool:
    return db.execute(text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name='lead_pack_purchases' AND column_name='tracerfy_queue_id'"
    )).scalar() is not None


@pytest.fixture
def db(fresh_db):
    if not _table_exists(fresh_db) or not _has_enriching_col(fresh_db):
        pytest.skip("lead pack hot-enrichment migration not applied")
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


def _get(db, pi_id):
    return db.execute(
        select(LeadPackPurchase).where(LeadPackPurchase.stripe_payment_intent_id == pi_id)
    ).scalar_one()


def _all_hit(lead_ids):
    return {pid: {"match_success": True, "mobile_phone": "8135550100",
                  "landline": None, "email": None, "mailing_address": None}
            for pid in lead_ids}


class TestLeadPackE2E:
    def test_reservation_locks_leads_at_payment(self, db):
        _mk_subscriber(db, "e2e-reserve", vertical="roofing")
        zip_code = "95001"
        pids = [_mk_property(db, f"PARCEL-A{i}", zip_code, SRC_COUNTY, score=90 - i) for i in range(6)]

        stripe_webhooks._on_lead_pack_payment(_pi("pi_reserve", "e2e-reserve", zip_code), db)

        purchase = _get(db, "pi_reserve")
        # Reserved, NOT yet delivered.
        assert purchase.status == "enriching"
        assert len(purchase.lead_ids) == 5
        assert set(purchase.lead_ids).issubset(set(pids))
        assert purchase.delivered_at is None

        now = datetime.now(timezone.utc)
        # 5 exclusivity rows already written at payment.
        assert get_exclusive_property_ids(db, SRC_COUNTY, now, zip_code=zip_code) == set(purchase.lead_ids)
        # Hidden from a DIFFERENT trade; the buyer's own trade still sees them.
        assert get_exclusive_property_ids(db, SRC_COUNTY, now, zip_code=zip_code, exclude_trade="restoration") == set(purchase.lead_ids)
        assert get_exclusive_property_ids(db, SRC_COUNTY, now, zip_code=zip_code, exclude_trade="roofing") == set()

    def test_sweep_delivers_when_floor_clears(self, db):
        _mk_subscriber(db, "e2e-deliver", vertical="roofing")
        zip_code = "95005"
        for i in range(6):
            _mk_property(db, f"PARCEL-E{i}", zip_code, SRC_COUNTY, score=90 - i)

        stripe_webhooks._on_lead_pack_payment(_pi("pi_deliver", "e2e-deliver", zip_code), db)
        purchase = _get(db, "pi_deliver")
        lead_ids = list(purchase.lead_ids)

        with patch("src.services.tracerfy_fallback.hot_enrich_properties", return_value=_all_hit(lead_ids)), \
             patch("src.services.email.send_email"):
            outcome = fulfill_purchase(db, purchase)

        assert outcome == "delivered"
        assert purchase.status == "delivered"
        assert purchase.delivered_at is not None

        # SentLead rows created with source='lead_pack'.
        sent = db.execute(text("""
            SELECT property_id FROM sent_leads
            WHERE subscriber_id = :sid AND source = 'lead_pack'
        """), {"sid": purchase.subscriber_id}).scalars().all()
        assert set(sent) == set(lead_ids)

        # Exclusivity kept after delivery.
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, SRC_COUNTY, now, zip_code=zip_code) == set(lead_ids)

    def test_quality_floor_miss_refunds_and_releases(self, db):
        _mk_subscriber(db, "e2e-floor", vertical="roofing")
        zip_code = "95006"
        for i in range(6):
            _mk_property(db, f"PARCEL-F{i}", zip_code, SRC_COUNTY, score=90 - i)

        stripe_webhooks._on_lead_pack_payment(_pi("pi_floor", "e2e-floor", zip_code), db)
        purchase = _get(db, "pi_floor")
        lead_ids = list(purchase.lead_ids)

        # One of the five comes back with no phone/email → floor fails.
        enriched = _all_hit(lead_ids)
        enriched[lead_ids[0]] = {"match_success": False, "mobile_phone": None,
                                 "landline": None, "email": None, "mailing_address": None}

        with patch("src.services.tracerfy_fallback.hot_enrich_properties", return_value=enriched), \
             patch("stripe.Refund.create", return_value={"id": "re_floor"}) as refund, \
             patch("src.services.email.send_email"):
            outcome = fulfill_purchase(db, purchase)

        assert outcome == "refunded"
        assert purchase.status == "refunded"
        assert purchase.refund_reason == "quality_floor_4_of_5"
        assert purchase.stripe_refund_id == "re_floor"
        refund.assert_called_once()

        # Reservation released — leads back on the market.
        now = datetime.now(timezone.utc)
        assert get_exclusive_property_ids(db, SRC_COUNTY, now, zip_code=zip_code) == set()

    def test_short_pack_triggers_full_refund(self, db):
        _mk_subscriber(db, "e2e-short", vertical="roofing")
        zip_code = "95002"
        for i in range(4):  # only 4 qualified — below the 5 threshold
            _mk_property(db, f"PARCEL-B{i}", zip_code, SRC_COUNTY)

        with patch("stripe.Refund.create", return_value={"id": "re_e2e_short"}) as refund:
            stripe_webhooks._on_lead_pack_payment(_pi("pi_e2e_short", "e2e-short", zip_code), db)

        purchase = _get(db, "pi_e2e_short")
        assert purchase.status == "refunded"
        assert purchase.refund_reason == "short_pack_4_of_5"
        assert purchase.stripe_refund_id == "re_e2e_short"
        assert not purchase.lead_ids
        refund.assert_called_once()
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

        purchase = _get(db, "pi_e2e_ghost")
        assert purchase.status == "refunded"
        assert purchase.refund_reason == "county_not_launched"
        refund.assert_called_once()

    def test_concurrent_second_buyer_cannot_double_sell(self, db):
        """Two buyers, same ZIP: buyer 1 reserves all 5; buyer 2 short-pack refunds."""
        _mk_subscriber(db, "e2e-buyer1", vertical="roofing")
        _mk_subscriber(db, "e2e-buyer2", vertical="restoration")
        zip_code = "95004"
        for i in range(5):  # exactly 5 — buyer 1 takes all
            _mk_property(db, f"PARCEL-D{i}", zip_code, SRC_COUNTY)

        stripe_webhooks._on_lead_pack_payment(_pi("pi_b1", "e2e-buyer1", zip_code, vertical="roofing"), db)
        p1 = _get(db, "pi_b1")
        assert p1.status == "enriching" and len(p1.lead_ids) == 5

        with patch("stripe.Refund.create", return_value={"id": "re_b2"}):
            stripe_webhooks._on_lead_pack_payment(_pi("pi_b2", "e2e-buyer2", zip_code, vertical="restoration"), db)
        p2 = _get(db, "pi_b2")
        # Cross-trade exclusivity locked all 5 at buyer-1 reservation → buyer 2 short-pack refund.
        assert p2.status == "refunded"
        assert not p2.lead_ids
