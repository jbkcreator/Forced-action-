"""
Scenario test — Task 4.1 holdout conversion recording.

wallet_push_v1 tracer bullet: wallet activation (stripe_webhooks._on_wallet_
subscription_invoice) must record the outcome against the frozen control
holdout test ("wallet_push_holdout") so holdout_verdict has real conversion
data for both arms — mirrors the existing bundle-pricing A/B conversion
recording at the same call site (stripe_webhooks.py ~line 2721).

Marker: scenario_platform (real Postgres via fresh_db).
"""

import uuid

import pytest
from unittest.mock import patch
from sqlalchemy import select

pytestmark = pytest.mark.scenario_platform


def _seed_holdout_assignment(fresh_db, subscriber_id: int, arm: str):
    from src.services.ab_engine import get_or_create_test
    from src.core.models import AbAssignment

    test = get_or_create_test(
        test_name="wallet_push_holdout", segment="all",
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
