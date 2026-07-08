"""
Hot lead unlock ($150 standard / $99 reduced) — checkout + fulfillment.

Covers the revenue-path bug where Checkout Session metadata never reached the
PaymentIntent, so payment_intent.succeeded routed to the bare card-save branch
and a paid unlock produced no SentLead delivery record.

  - create_hot_lead_unlock_link passes payment_intent_data.metadata
  - _on_payment_intent_succeeded routes product=hot_lead_unlock to fulfillment
  - fulfillment writes a SentLead row with the paid amount
  - checkout.session.completed short-circuits hot_lead_unlock sessions
  - /api/hot-lead-unlock endpoint is enabled by default

Self-skips DB-backed tests when DATABASE_URL is not configured.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from src.core.models import DistressScore, Property, SentLead, Subscriber
from src.services import stripe_webhooks


# ── create_hot_lead_unlock_link — metadata must reach the PaymentIntent ─────

class TestCreateHotLeadUnlockLink:
    def _create(self, reduced=False):
        from src.services import stripe_service

        fake_settings = MagicMock()
        fake_settings.active_stripe_price.return_value = "price_hot_150"
        fake_settings.stripe_price_lead_pack = "price_pack_99"
        fake_settings.app_base_url = "https://app.test"
        fake_session = MagicMock(id="cs_test_123", url="https://stripe.test/cs_test_123")
        with patch.object(stripe_service, "_init_stripe", return_value=True), \
             patch.object(stripe_service, "settings", fake_settings), \
             patch.object(stripe_service.stripe.checkout.Session, "create",
                          return_value=fake_session) as create:
            result = stripe_service.create_hot_lead_unlock_link(
                subscriber_stripe_customer_id="cus_test",
                lead_id="4242",
                reduced=reduced,
            )
        return result, create.call_args.kwargs

    def test_payment_intent_metadata_is_set(self):
        result, kwargs = self._create()
        pi_meta = kwargs["payment_intent_data"]["metadata"]
        assert pi_meta["product"] == "hot_lead_unlock"
        assert pi_meta["property_id"] == "4242"
        assert pi_meta["reduced_rate"] == "False"
        assert result["url"] == "https://stripe.test/cs_test_123"

    def test_session_metadata_kept_for_abandonment_handler(self):
        _, kwargs = self._create()
        assert kwargs["metadata"]["product"] == "hot_lead_unlock"
        assert kwargs["metadata"]["lead_id"] == "4242"

    def test_reduced_rate_uses_lead_pack_price(self):
        _, kwargs = self._create(reduced=True)
        assert kwargs["line_items"] == [{"price": "price_pack_99", "quantity": 1}]
        assert kwargs["payment_intent_data"]["metadata"]["reduced_rate"] == "True"


# ── payment_intent.succeeded routing + fulfillment ───────────────────────────

def _seed_unlockable_lead(db):
    cust = f"cus_{uuid.uuid4().hex[:8]}"
    sub = Subscriber(
        stripe_customer_id=cust, tier="pro", vertical="roofing",
        county_id="hillsborough", status="active",
        event_feed_uuid=f"hlu-{uuid.uuid4().hex[:8]}",
        email=f"{uuid.uuid4().hex[:8]}@example.com",
    )
    db.add(sub)
    prop = Property(parcel_id=f"HLU-{uuid.uuid4().hex[:6]}", zip="33602",
                    county_id="hillsborough", address="1 Hot Lead St")
    db.add(prop)
    db.flush()
    db.add(DistressScore(
        property_id=prop.id, qualified=True, final_cds_score=95.0,
        vertical_scores={"roofing": 95.0},
        score_date=datetime.now(timezone.utc).date(),
    ))
    db.flush()
    return sub, prop, cust


def _hot_lead_pi(pi_id, cust, property_id, amount=15000, reduced=False):
    return {
        "id": pi_id,
        "customer": cust,
        "amount": amount,
        "amount_received": amount,
        "currency": "usd",
        "metadata": {
            "product": "hot_lead_unlock",
            "property_id": str(property_id),
            "reduced_rate": str(reduced),
        },
    }


def _quiet_unlock_side_effects():
    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(patch("src.services.auto_mode.enqueue_action"))
    stack.enter_context(patch("src.services.stripe_webhooks._send_lead_unlock_email"))
    stack.enter_context(patch("src.services.email.send_welcome_email"))
    stack.enter_context(patch("src.services.segmentation_engine.reclassify_safe"))
    stack.enter_context(patch.object(stripe_webhooks, "_fire_capi_for_pi"))
    return stack


class TestHotLeadUnlockFulfillment:
    def test_router_creates_delivery_record(self, fresh_db):
        sub, prop, cust = _seed_unlockable_lead(fresh_db)
        pi_id = f"pi_{uuid.uuid4().hex[:8]}"
        with _quiet_unlock_side_effects():
            stripe_webhooks._on_payment_intent_succeeded(
                _hot_lead_pi(pi_id, cust, prop.id), fresh_db,
            )
        sent = fresh_db.execute(
            select(SentLead).where(
                SentLead.subscriber_id == sub.id,
                SentLead.property_id == prop.id,
            )
        ).scalar_one()
        assert sent.stripe_payment_intent_id == pi_id
        assert sent.amount_cents == 15000

    def test_reduced_rate_records_99(self, fresh_db):
        sub, prop, cust = _seed_unlockable_lead(fresh_db)
        pi_id = f"pi_{uuid.uuid4().hex[:8]}"
        with _quiet_unlock_side_effects():
            stripe_webhooks._on_payment_intent_succeeded(
                _hot_lead_pi(pi_id, cust, prop.id, amount=9900, reduced=True), fresh_db,
            )
        sent = fresh_db.execute(
            select(SentLead).where(SentLead.subscriber_id == sub.id)
        ).scalar_one()
        assert sent.amount_cents == 9900

    def test_duplicate_webhook_is_idempotent(self, fresh_db):
        sub, prop, cust = _seed_unlockable_lead(fresh_db)
        pi_id = f"pi_{uuid.uuid4().hex[:8]}"
        pi = _hot_lead_pi(pi_id, cust, prop.id)
        with _quiet_unlock_side_effects():
            stripe_webhooks._on_payment_intent_succeeded(pi, fresh_db)
            stripe_webhooks._on_payment_intent_succeeded(pi, fresh_db)
        rows = fresh_db.execute(
            select(SentLead).where(
                SentLead.subscriber_id == sub.id,
                SentLead.property_id == prop.id,
            )
        ).scalars().all()
        assert len(rows) == 1

    def test_repeat_payment_with_new_pi_still_records_revenue(self, fresh_db):
        """A property already unlocked once (e.g. the cheap $2.50-$7
        lead_unlock) and later unlocked again via a separate, distinctly
        priced payment (e.g. the $150 hot lead unlock) must record BOTH
        charges to the revenue ledger — sent_leads has a hard
        UniqueConstraint(subscriber_id, property_id), so the second charge
        must not be silently dropped just because it reuses the first
        charge's SentLead row."""
        from src.core.models import PlatformRevenueLedger

        sub, prop, cust = _seed_unlockable_lead(fresh_db)
        first_pi = f"pi_{uuid.uuid4().hex[:8]}"
        second_pi = f"pi_{uuid.uuid4().hex[:8]}"

        with _quiet_unlock_side_effects():
            stripe_webhooks._on_lead_unlock_payment(
                _hot_lead_pi(first_pi, cust, prop.id, amount=500), fresh_db,
            )
            stripe_webhooks._on_lead_unlock_payment(
                _hot_lead_pi(second_pi, cust, prop.id, amount=15000), fresh_db,
            )

        # Only one SentLead row can exist (subscriber_id, property_id is
        # unique) — but it must reflect the latest payment, not the first.
        sent = fresh_db.execute(
            select(SentLead).where(
                SentLead.subscriber_id == sub.id,
                SentLead.property_id == prop.id,
            )
        ).scalar_one()
        assert sent.stripe_payment_intent_id == second_pi
        assert sent.amount_cents == 15000

        ledger_rows = fresh_db.execute(
            select(PlatformRevenueLedger).where(
                PlatformRevenueLedger.subscriber_id == sub.id,
                PlatformRevenueLedger.property_id == prop.id,
            )
        ).scalars().all()
        assert {r.amount_cents for r in ledger_rows} == {500, 15000}

        # A retried webhook for the SAME payment_intent must not double-record.
        with _quiet_unlock_side_effects():
            stripe_webhooks._on_lead_unlock_payment(
                _hot_lead_pi(second_pi, cust, prop.id, amount=15000), fresh_db,
            )
        ledger_rows_after_retry = fresh_db.execute(
            select(PlatformRevenueLedger).where(
                PlatformRevenueLedger.subscriber_id == sub.id,
                PlatformRevenueLedger.property_id == prop.id,
            )
        ).scalars().all()
        assert len(ledger_rows_after_retry) == 2


# ── checkout.session.completed must ignore hot_lead_unlock sessions ─────────

class TestCheckoutCompletedShortCircuit:
    def test_hot_lead_session_is_skipped(self):
        session = {
            "metadata": {"product": "hot_lead_unlock", "lead_id": "4242",
                         "reduced_rate": "False"},
            "customer": "cus_x",
        }
        db = MagicMock()
        stripe_webhooks._on_checkout_completed(session, db)
        db.execute.assert_not_called()


# ── endpoint enablement ──────────────────────────────────────────────────────

class TestEndpointEnabled:
    def test_hot_lead_unlock_enabled_by_default(self):
        from config.settings import AppSettings
        assert AppSettings.model_fields["hot_lead_unlock_enabled"].default is True
