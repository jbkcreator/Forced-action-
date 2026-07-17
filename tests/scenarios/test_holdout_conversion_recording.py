"""
Scenario test — Task 4.1 holdout conversion recording.

wallet_push_v1 tracer bullet: wallet activation (stripe_webhooks._on_wallet_
subscription_invoice) must record the outcome against the frozen control
holdout test ("wallet_push_holdout") so holdout_verdict has real conversion
data for both arms — mirrors the existing bundle-pricing A/B conversion
recording at the same call site (stripe_webhooks.py ~line 2721).

fomo_v1: lead unlock (stripe_webhooks._on_lead_unlock_payment) is the
conversion event, recorded against "fomo_holdout".

retention_v1: "any paid action" is hooked at revenue_ledger.record_revenue —
the one chokepoint every purchase-confirmation path already funnels
through — recorded against "retention_holdout".

lock_close_v1's conversion (stripe_webhooks._on_checkout_completed) is
wired but NOT covered here: that handler pulls in Stripe API retrieval
calls, revenue_engine, wallet_engine, GHL, Meta CAPI, and the referral
engine — a full mock scaffold for it is a separate, disproportionate piece
of work, and writing one un-runnable (DB currently unreachable) risks false
confidence. Deferred explicitly, not silently skipped.

Marker: scenario_platform (real Postgres via fresh_db).
"""

import uuid

import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import select

pytestmark = pytest.mark.scenario_platform


def _seed_holdout_assignment(fresh_db, subscriber_id: int, arm: str, test_name: str = "wallet_push_holdout"):
    from src.services.ab_engine import get_or_create_test
    from src.core.models import AbAssignment

    test = get_or_create_test(
        test_name=test_name, segment="all",
        variant_a={"path": "control"}, variant_b={"path": "variant"},
        traffic_pct=90, db=fresh_db,
    )
    fresh_db.add(AbAssignment(test_id=test.id, subscriber_id=subscriber_id, variant=arm))
    fresh_db.flush()
    return test


class TestWalletActivationRecordsHoldoutOutcome:
    def test_wallet_activation_records_converted_for_control_arm(self, fresh_db):
        from src.core.models import Subscriber, AbAssignment

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_hcr_{uid}", tier="starter", vertical="roofing",
            county_id="hillsborough", event_feed_uuid=f"hcr-uuid-{uid}", status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        holdout_test = _seed_holdout_assignment(fresh_db, sub.id, "control")

        invoice = {
            "id": f"in_{uid}",
            "payment_intent": f"pi_{uid}",
            "subscription": f"sub_hcr_{uid}",
            "metadata": {"subscriber_id": str(sub.id), "tier": "starter_wallet"},
        }

        with patch(
            "src.services.stripe_webhooks._extract_wallet_sub_metadata",
            return_value={
                "subscriber_id": str(sub.id), "tier": "starter_wallet",
                "subscription_id": f"sub_hcr_{uid}",
            },
        ), patch("src.services.wallet_engine.enroll"), \
           patch("src.services.stripe_webhooks.send_sms", create=True), \
           patch("src.services.segmentation_engine.reclassify_safe"):
            from src.services.stripe_webhooks import _on_wallet_subscription_invoice
            _on_wallet_subscription_invoice(invoice, fresh_db)

        recorded = fresh_db.execute(
            select(AbAssignment).where(
                AbAssignment.test_id == holdout_test.id,
                AbAssignment.subscriber_id == sub.id,
            )
        ).scalar_one_or_none()
        assert recorded is not None
        assert recorded.outcome == "converted"

    def test_wallet_activation_records_converted_for_variant_arm(self, fresh_db):
        """Both arms must be recorded, not just control — holdout_verdict's
        z-test needs conversion data for n_var as much as n_ctrl."""
        from src.core.models import Subscriber, AbAssignment

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_hcr_{uid}", tier="starter", vertical="roofing",
            county_id="hillsborough", event_feed_uuid=f"hcr-uuid-{uid}", status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        holdout_test = _seed_holdout_assignment(fresh_db, sub.id, "variant")

        invoice = {
            "id": f"in_{uid}",
            "payment_intent": f"pi_{uid}",
            "subscription": f"sub_hcr_{uid}",
            "metadata": {"subscriber_id": str(sub.id), "tier": "starter_wallet"},
        }

        with patch(
            "src.services.stripe_webhooks._extract_wallet_sub_metadata",
            return_value={
                "subscriber_id": str(sub.id), "tier": "starter_wallet",
                "subscription_id": f"sub_hcr_{uid}",
            },
        ), patch("src.services.wallet_engine.enroll"), \
           patch("src.services.stripe_webhooks.send_sms", create=True), \
           patch("src.services.segmentation_engine.reclassify_safe"):
            from src.services.stripe_webhooks import _on_wallet_subscription_invoice
            _on_wallet_subscription_invoice(invoice, fresh_db)

        recorded = fresh_db.execute(
            select(AbAssignment).where(
                AbAssignment.test_id == holdout_test.id,
                AbAssignment.subscriber_id == sub.id,
            )
        ).scalar_one_or_none()
        assert recorded is not None
        assert recorded.outcome == "converted"


class TestLeadUnlockRecordsHoldoutOutcome:
    """fomo_v1 tracer: lead unlock (stripe_webhooks._on_lead_unlock_payment)
    is the conversion event for the fomo sequence — mirrors the wallet_push
    wiring above but against the "fomo_holdout" test."""

    def test_lead_unlock_records_converted_for_control_arm(self, fresh_db):
        from src.core.models import Subscriber, Property, AbAssignment

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_lu_{uid}", tier="starter", vertical="roofing",
            county_id="hillsborough", event_feed_uuid=f"lu-uuid-{uid}", status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        prop = Property(
            parcel_id=f"lu-parcel-{uid}", address=f"123 Lu St {uid}", county_id="hillsborough",
        )
        fresh_db.add(prop)
        fresh_db.flush()

        holdout_test = _seed_holdout_assignment(fresh_db, sub.id, "control", test_name="fomo_holdout")

        pi = MagicMock()
        pi.id = f"pi_lu_{uid}"
        pi.customer = f"cus_lu_{uid}"
        pi.metadata = {"property_id": str(prop.id), "product": "lead_unlock"}
        pi.amount_received = 400

        with patch("src.services.stripe_webhooks._send_lead_unlock_email"):
            from src.services.stripe_webhooks import _on_lead_unlock_payment
            _on_lead_unlock_payment(pi, fresh_db)

        recorded = fresh_db.execute(
            select(AbAssignment).where(
                AbAssignment.test_id == holdout_test.id,
                AbAssignment.subscriber_id == sub.id,
            )
        ).scalar_one_or_none()
        assert recorded is not None
        assert recorded.outcome == "converted"

    def test_lead_unlock_records_converted_for_variant_arm(self, fresh_db):
        from src.core.models import Subscriber, Property, AbAssignment

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_lu_{uid}", tier="starter", vertical="roofing",
            county_id="hillsborough", event_feed_uuid=f"lu-uuid-{uid}", status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        prop = Property(
            parcel_id=f"lu-parcel-{uid}", address=f"123 Lu St {uid}", county_id="hillsborough",
        )
        fresh_db.add(prop)
        fresh_db.flush()

        holdout_test = _seed_holdout_assignment(fresh_db, sub.id, "variant", test_name="fomo_holdout")

        pi = MagicMock()
        pi.id = f"pi_lu_{uid}"
        pi.customer = f"cus_lu_{uid}"
        pi.metadata = {"property_id": str(prop.id), "product": "lead_unlock"}
        pi.amount_received = 400

        with patch("src.services.stripe_webhooks._send_lead_unlock_email"):
            from src.services.stripe_webhooks import _on_lead_unlock_payment
            _on_lead_unlock_payment(pi, fresh_db)

        recorded = fresh_db.execute(
            select(AbAssignment).where(
                AbAssignment.test_id == holdout_test.id,
                AbAssignment.subscriber_id == sub.id,
            )
        ).scalar_one_or_none()
        assert recorded is not None
        assert recorded.outcome == "converted"


class TestRecordRevenueRecordsHoldoutOutcome:
    """retention_v1 tracer: retention_v1's conversion is "any paid action" —
    hooked at revenue_ledger.record_revenue, the one chokepoint every
    purchase-confirmation path already funnels through, rather than each
    individual webhook handler. Records against "retention_holdout"."""

    def test_any_revenue_event_records_converted_and_stamps_outcome_at(self, fresh_db):
        from src.core.models import Subscriber, AbAssignment
        from src.services.revenue_ledger import record_revenue, stripe_payment_intent_ledger_id

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_rr_{uid}", tier="starter", vertical="roofing",
            county_id="hillsborough", event_feed_uuid=f"rr-uuid-{uid}", status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        holdout_test = _seed_holdout_assignment(fresh_db, sub.id, "variant", test_name="retention_holdout")

        record_revenue(
            fresh_db, subscriber_id=sub.id, product_type="lead_unlock",
            amount_cents=400, source_table="stripe_payment_intent",
            source_id=stripe_payment_intent_ledger_id(f"pi_rr_{uid}"),
        )

        recorded = fresh_db.execute(
            select(AbAssignment).where(
                AbAssignment.test_id == holdout_test.id,
                AbAssignment.subscriber_id == sub.id,
            )
        ).scalar_one_or_none()
        assert recorded is not None
        assert recorded.outcome == "converted"
        assert recorded.outcome_at is not None

    def test_unassigned_subscriber_is_a_safe_noop(self, fresh_db):
        """A subscriber with no retention_holdout assignment must not error
        or create a spurious row — record_outcome's existing no-op guard."""
        from src.core.models import Subscriber
        from src.services.revenue_ledger import record_revenue, stripe_payment_intent_ledger_id

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_rr_{uid}", tier="starter", vertical="roofing",
            county_id="hillsborough", event_feed_uuid=f"rr-uuid-{uid}", status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        # No _seed_holdout_assignment call — subscriber has no arm assigned.
        record_revenue(
            fresh_db, subscriber_id=sub.id, product_type="lead_unlock",
            amount_cents=400, source_table="stripe_payment_intent",
            source_id=stripe_payment_intent_ledger_id(f"pi_rr_{uid}"),
        )  # must not raise
