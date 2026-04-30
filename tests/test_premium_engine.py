"""
Premium credits — Stage 5 — unit + integration tests.

Run:
    pytest tests/test_premium_engine.py -v
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from src.core.models import PremiumPurchase, Subscriber


# ============================================================================
# Unit tests — mock DB, no real schema
# ============================================================================


class TestPremiumEngineUnit:
    def test_get_sku_config_known(self):
        from src.services.premium_engine import get_sku_config
        cfg = get_sku_config("report")
        assert cfg["credits_cost"] == 3
        assert cfg["retail_price_cents"] == 700

    def test_get_sku_config_unknown_raises(self):
        from src.services.premium_engine import get_sku_config
        with pytest.raises(ValueError):
            get_sku_config("not_a_real_sku")

    def test_record_credit_purchase_persists_row(self):
        from src.services.premium_engine import record_credit_purchase
        db = MagicMock()
        purchase = record_credit_purchase(subscriber_id=1, sku="brief", db=db, property_id=42)
        # MagicMock-backed db: assert add() was called with the new row
        added = db.add.call_args[0][0]
        assert added.sku == "brief"
        assert added.paid_via == "credits"
        assert added.credits_spent == 5
        assert added.property_id == 42

    def test_record_card_purchase_uses_retail_amount_when_none_provided(self):
        from src.services.premium_engine import record_card_purchase
        db = MagicMock()
        purchase = record_card_purchase(
            subscriber_id=1, sku="report", stripe_payment_intent_id="pi_x", db=db, property_id=42,
        )
        added = db.add.call_args[0][0]
        assert added.amount_cents == 700  # retail price for report

    def test_fulfill_idempotent_on_delivered(self):
        from src.services.premium_engine import fulfill
        db = MagicMock()
        delivered = MagicMock(status="delivered")
        db.get.return_value = delivered
        result = fulfill(99, db)
        # Should return without re-running fulfillment
        assert result is delivered

    def test_fulfill_byol_requires_target_address(self):
        from src.services.premium_engine import fulfill
        db = MagicMock()
        purchase = PremiumPurchase(
            id=1, subscriber_id=1, sku="byol", paid_via="credits",
            target_address=None, status="pending",
        )
        db.get.return_value = purchase
        with pytest.raises(ValueError):
            fulfill(1, db)

    def test_fulfill_report_requires_property_id(self):
        from src.services.premium_engine import fulfill
        db = MagicMock()
        purchase = PremiumPurchase(
            id=1, subscriber_id=1, sku="report", paid_via="credits",
            property_id=None, status="pending",
        )
        db.get.return_value = purchase
        with pytest.raises(ValueError):
            fulfill(1, db)

    def test_fulfill_byol_returns_queued_ref(self):
        from src.services.premium_engine import fulfill
        db = MagicMock()
        purchase = PremiumPurchase(
            id=7, subscriber_id=1, sku="byol", paid_via="credits",
            target_address="123 Main St, Tampa FL", status="pending",
        )
        db.get.return_value = purchase
        out = fulfill(7, db)
        assert out.status == "delivered"
        assert "byol:queued" in (out.output_ref or "")


# ============================================================================
# Integration tests — real schema (Postgres)
# ============================================================================


class TestPremiumEngineIntegration:
    def test_credit_path_round_trip(self, fresh_db):
        from src.services.premium_engine import record_credit_purchase, fulfill
        from src.core.models import Property

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_premium_{uid}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"premium-uuid-{uid}",
        )
        fresh_db.add(sub)
        fresh_db.flush()
        prop = Property(
            parcel_id=f"P-{uid}",
            address=f"100 Premium Way #{uid}",
            city="Tampa", state="FL", zip="33601",
            county_id="hillsborough",
        )
        fresh_db.add(prop)
        fresh_db.flush()

        purchase = record_credit_purchase(
            subscriber_id=sub.id, sku="report", db=fresh_db, property_id=prop.id,
        )
        assert purchase.id is not None
        assert purchase.paid_via == "credits"
        assert purchase.credits_spent == 3

        delivered = fulfill(purchase.id, fresh_db)
        assert delivered.status == "delivered"
        assert delivered.output_ref == f"report:{prop.id}"

    def test_card_path_persists_with_pi_id(self, fresh_db):
        from src.services.premium_engine import record_card_purchase
        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_premium_card_{uid}",
            tier="starter", vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"premium-card-{uid}",
        )
        fresh_db.add(sub)
        fresh_db.flush()
        purchase = record_card_purchase(
            subscriber_id=sub.id, sku="transfer",
            stripe_payment_intent_id=f"pi_premium_{uid}",
            db=fresh_db, property_id=None,
        )
        assert purchase.paid_via == "card"
        assert purchase.amount_cents == 6500     # transfer retail price
        assert purchase.stripe_payment_intent_id == f"pi_premium_{uid}"
