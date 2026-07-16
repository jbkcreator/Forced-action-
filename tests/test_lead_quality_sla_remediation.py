"""
B1-03 — Lead-Quality SLA auto-remediation.

Drives the REAL run_lead_quality_monitor() against real Postgres (fresh_db,
rolled back after each test), with Stripe mocked. Covers:

  A. Delivery (Block 1 storefront) sold before delivery    → auto-replace credit.
  B. SentLead lead_unlock_payment sold before delivery     → auto Stripe refund
     (one payment_intent = one lead, so a full-PI refund is correct).
  C. SentLead lead_pack sold before delivery               → NOT auto-refunded.
     A lead_pack's payment_intent is shared across up to 5 leads (see
     lead_pack_fulfillment_sweep.py), so a full-PI refund would over-refund
     the other, good leads — deliberately left on the alert-only path.
  D. SentLead free daily_email send sold before delivery   → no remediation (no payment to act on).
  E. Re-running the monitor never double-credits / double-refunds (idempotency).

Self-skips when DATABASE_URL / the lead_quality_snapshots migration are unavailable.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from src.core.models import (
    CustomerAccount, Deed, Delivery, DistressScore, Property, SentLead, Subscriber,
)
from src.tasks import lead_quality_monitor as lqm

COUNTY = "hillsborough"
SENT_30D_AGO = lambda: datetime.now(timezone.utc) - timedelta(days=30)


def _has_remediation_cols(db) -> bool:
    return db.execute(text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name='lead_quality_snapshots' AND column_name='delivery_id'"
    )).scalar() is not None


@pytest.fixture
def db(fresh_db):
    if not _has_remediation_cols(fresh_db):
        pytest.skip("B1-03 lead_quality_snapshots migration not applied")
    return fresh_db


def _mk_property_sold(db, parcel, sent_at):
    """A property that was Gold-scored at send time, then sold 5 days later."""
    p = Property(parcel_id=parcel, zip="33610", county_id=COUNTY, address=f"{parcel} Test St")
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=90.0, lead_tier="Gold",
        vertical_scores={"roofing": 90.0}, score_date=sent_at - timedelta(days=1),
    ))
    db.add(Deed(
        property_id=p.id, instrument_number=f"INST-{parcel}",
        record_date=(sent_at + timedelta(days=5)).date(),
    ))
    db.flush()
    return p.id


def _mk_subscriber(db, uuid):
    s = Subscriber(
        stripe_customer_id=f"cus_{uuid}", tier="pro", vertical="roofing",
        county_id=COUNTY, status="active", event_feed_uuid=uuid, email=f"{uuid}@example.com",
    )
    db.add(s)
    db.flush()
    return s


def _mk_account(db, subscriber_id):
    a = CustomerAccount(
        subscriber_id=subscriber_id, status="active", plan_tier=None,
        lead_entitlement={"gold": 5}, lead_credits={},
    )
    db.add(a)
    db.flush()
    return a


class TestLeadQualitySLARemediation:

    def test_delivery_sold_auto_credits(self, db, monkeypatch):
        """Case A: an entitlement-model Delivery that turns out sold → reject_delivery()
        fires automatically, marking it rejected and granting a +1 gold credit."""
        sent_at = SENT_30D_AGO()
        sub = _mk_subscriber(db, "b103-a")
        account = _mk_account(db, sub.id)
        prop_id = _mk_property_sold(db, "B103A", sent_at)

        delivery = Delivery(
            property_id=prop_id, account_id=account.account_id, grade="Gold",
            vertical="roofing", delivered_at=sent_at,
        )
        db.add(delivery)
        db.flush()

        result = lqm.run_lead_quality_monitor(county_id=COUNTY, dry_run=False, db=db)

        db.refresh(delivery)
        db.refresh(account)
        assert delivery.status == "rejected"
        assert delivery.rejection_reason == "sold_before_delivery"
        assert account.lead_credits.get("gold") == 1
        assert result["credits_issued"] >= 1
        assert result["sold"] >= 1

    def test_lead_unlock_sent_lead_sold_auto_refunds(self, db, monkeypatch):
        """Case B: a paid one-time SentLead (lead_unlock_payment) that turns out
        sold → an automatic full Stripe refund is issued (correct, since one
        payment_intent maps to exactly one lead for this product)."""
        mock_settings = MagicMock()
        mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test_123"
        monkeypatch.setattr(lqm, "settings", mock_settings)

        mock_refund = MagicMock(id="re_test_123")
        monkeypatch.setattr(lqm.stripe.Refund, "create", MagicMock(return_value=mock_refund))

        sent_at = SENT_30D_AGO()
        sub = _mk_subscriber(db, "b103-b")
        prop_id = _mk_property_sold(db, "B103B", sent_at)

        sl = SentLead(
            subscriber_id=sub.id, property_id=prop_id, sent_at=sent_at,
            source="lead_unlock_payment", stripe_payment_intent_id="pi_test_123",
            amount_cents=400,
        )
        db.add(sl)
        db.flush()

        result = lqm.run_lead_quality_monitor(county_id=COUNTY, dry_run=False, db=db)

        db.refresh(sl)
        lqm.stripe.Refund.create.assert_called_once_with(
            payment_intent="pi_test_123", idempotency_key="lqm-refund-pi_test_123",
        )
        assert sl.refunded_at is not None
        assert sl.refund_reason == "sold_before_delivery"
        assert sl.stripe_refund_id == "re_test_123"
        assert result["refunds_issued"] >= 1

    def test_lead_pack_sent_lead_sold_not_auto_refunded(self, db, monkeypatch):
        """Case C: a lead_pack SentLead that turns out sold is NOT auto-refunded.
        Its payment_intent is shared across up to 5 leads in the same purchase
        (no per-lead amount_cents is ever stored — see
        lead_pack_fulfillment_sweep.py), so a full-PI refund would over-refund
        the other, good leads. This stays on the alert-only path until a
        proper per-lead partial refund (sourced from platform_revenue_ledger)
        is built."""
        mock_refund_create = MagicMock()
        monkeypatch.setattr(lqm.stripe.Refund, "create", mock_refund_create)

        sent_at = SENT_30D_AGO()
        sub = _mk_subscriber(db, "b103-c2")
        prop_id = _mk_property_sold(db, "B103C2", sent_at)

        sl = SentLead(
            subscriber_id=sub.id, property_id=prop_id, sent_at=sent_at, source="lead_pack",
            stripe_payment_intent_id="pi_test_pack_123",
        )
        db.add(sl)
        db.flush()

        result = lqm.run_lead_quality_monitor(county_id=COUNTY, dry_run=False, db=db)

        db.refresh(sl)
        mock_refund_create.assert_not_called()
        assert sl.refunded_at is None
        assert result["refunds_issued"] == 0
        snap = db.execute(text(
            "SELECT remediation_action FROM lead_quality_snapshots "
            "WHERE property_id = :pid AND subscriber_id = :sid"
        ), {"pid": prop_id, "sid": sub.id}).scalar()
        assert snap == "not_applicable"

    def test_free_daily_email_sent_lead_not_remediated(self, db, monkeypatch):
        """Case D: a free-tier daily_email send has no payment to credit or refund —
        it's snapshotted (still feeds the FP-rate alert) but not remediated."""
        sent_at = SENT_30D_AGO()
        sub = _mk_subscriber(db, "b103-c")
        prop_id = _mk_property_sold(db, "B103C", sent_at)

        sl = SentLead(
            subscriber_id=sub.id, property_id=prop_id, sent_at=sent_at, source="daily_email",
        )
        db.add(sl)
        db.flush()

        result = lqm.run_lead_quality_monitor(county_id=COUNTY, dry_run=False, db=db)

        db.refresh(sl)
        assert sl.refunded_at is None
        assert result["sold"] >= 1
        # not_applicable isn't tracked in the counters dict — just assert no
        # credit/refund fired for this row.
        snap = db.execute(text(
            "SELECT remediation_action FROM lead_quality_snapshots "
            "WHERE property_id = :pid AND subscriber_id = :sid"
        ), {"pid": prop_id, "sid": sub.id}).scalar()
        assert snap == "not_applicable"

    def test_rerun_does_not_double_remediate(self, db, monkeypatch):
        """Case E: running the monitor twice on the same fixtures must not
        double-credit the account or double-refund the purchase."""
        sent_at = SENT_30D_AGO()
        sub = _mk_subscriber(db, "b103-d")
        account = _mk_account(db, sub.id)
        prop_id = _mk_property_sold(db, "B103D", sent_at)

        delivery = Delivery(
            property_id=prop_id, account_id=account.account_id, grade="Gold",
            vertical="roofing", delivered_at=sent_at,
        )
        db.add(delivery)
        db.flush()

        lqm.run_lead_quality_monitor(county_id=COUNTY, dry_run=False, db=db)
        lqm.run_lead_quality_monitor(county_id=COUNTY, dry_run=False, db=db)

        db.refresh(delivery)
        db.refresh(account)
        assert delivery.status == "rejected"
        assert account.lead_credits.get("gold") == 1  # not 2
