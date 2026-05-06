"""
Stripe webhook replay tests — proves every handler is idempotent against
duplicate event delivery.

Stripe sends the same event id more than once for reliability (multiple
listeners, server retries on transient errors). Every handler must be
provably no-op on the second delivery; this suite asserts that for each
event type we handle.

We stub `stripe.Webhook.construct_event` so the test doesn't need a real
signing secret, and call `handle_webhook` twice with the same event payload.
The assertions confirm: (a) the second call returns "Already processed",
(b) no second row is written for handlers that mutate state, (c) wallet
balances and counters don't double-increment.

Run:
    pytest tests/test_stripe_replay.py -v
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from src.core.models import (
    PremiumPurchase,
    StripeWebhookEvent,
    Subscriber,
    WalletBalance,
    WalletTransaction,
)
from src.services.stripe_webhooks import handle_webhook


# ── Helpers ──────────────────────────────────────────────────────────────────


def _seed_subscriber(db, **overrides) -> Subscriber:
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=overrides.get("stripe_customer_id", f"cus_replay_{uid}"),
        tier=overrides.get("tier", "starter"),
        vertical=overrides.get("vertical", "roofing"),
        county_id=overrides.get("county_id", "hillsborough"),
        event_feed_uuid=overrides.get("event_feed_uuid", f"replay-{uid}"),
        has_saved_card=overrides.get("has_saved_card", False),
    )
    db.add(sub)
    db.flush()
    return sub


def _stub_construct_event(event: dict):
    """Patch stripe.Webhook.construct_event to return our payload directly."""
    return patch(
        "src.services.stripe_webhooks.stripe.Webhook.construct_event",
        return_value=event,
    )


def _stub_init_stripe():
    """Make _init_stripe() return True without needing a real secret."""
    return patch("src.services.stripe_webhooks._init_stripe", return_value=True)


def _stub_settings_secret():
    """Provide a fake webhook secret so the helper doesn't bail.

    `active_stripe_webhook_secret` is a computed property on AppSettings;
    patching it requires PropertyMock. Easier to patch the underlying field
    that the property reads — pydantic-settings allows runtime attribute
    assignment for those.
    """
    from unittest.mock import MagicMock, PropertyMock
    from config.settings import AppSettings
    fake_secret = MagicMock()
    fake_secret.get_secret_value.return_value = "whsec_replay_test"
    return patch.object(
        AppSettings,
        "active_stripe_webhook_secret",
        new_callable=PropertyMock,
        return_value=fake_secret,
    )


def _post(event: dict, db):
    """Invoke handle_webhook with a stubbed event."""
    raw = json.dumps(event).encode("utf-8")
    with _stub_init_stripe(), _stub_settings_secret(), _stub_construct_event(event):
        ok, msg = handle_webhook(raw, sig_header="t=stub,v1=stub", db=db)
    return ok, msg


# ── Tests ────────────────────────────────────────────────────────────────────


class TestReplayIdempotency:
    def test_event_id_unique_constraint_blocks_second_processing(self, fresh_db):
        """Smoke: post any event twice; second call returns 'Already processed'."""
        event = {
            "id": f"evt_replay_{uuid.uuid4().hex[:8]}",
            "type": "invoice.payment_succeeded",
            "created": int(datetime.now(timezone.utc).timestamp()),
            "data": {"object": {"id": "in_test", "subscription": None, "customer": "cus_x"}},
        }
        # First call: handler may legitimately error on missing data — that's OK.
        # We're asserting only that the StripeWebhookEvent row gets written.
        _post(event, fresh_db)
        fresh_db.commit()
        rows1 = fresh_db.query(StripeWebhookEvent).filter_by(event_id=event["id"]).count()
        assert rows1 == 1

        ok, msg = _post(event, fresh_db)
        assert ok is True
        assert msg == "Already processed"
        rows2 = fresh_db.query(StripeWebhookEvent).filter_by(event_id=event["id"]).count()
        assert rows2 == 1

    def test_premium_payment_intent_replay_creates_one_purchase(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        fresh_db.commit()

        pi_id = f"pi_replay_{uuid.uuid4().hex[:8]}"
        event = {
            "id": f"evt_premium_{uuid.uuid4().hex[:8]}",
            "type": "payment_intent.succeeded",
            "created": int(datetime.now(timezone.utc).timestamp()),
            "data": {
                "object": {
                    "id": pi_id,
                    "amount": 700,
                    "amount_received": 700,
                    "metadata": {
                        "product": "premium",
                        "sku": "report",
                        "subscriber_id": str(sub.id),
                        "property_id": "",
                        "target_address": "",
                    },
                }
            },
        }
        # Need property_id for report fulfillment to succeed; for this test we
        # accept that fulfillment fails (logged not raised) and assert only
        # that the purchase row is created exactly once.
        _post(event, fresh_db)
        fresh_db.commit()
        # Replay
        _post(event, fresh_db)
        fresh_db.commit()

        rows = fresh_db.query(PremiumPurchase).filter_by(stripe_payment_intent_id=pi_id).count()
        assert rows == 1

    def test_wallet_topup_replay_credits_once(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        # Pre-create the wallet so credit() doesn't race the first run
        wallet = WalletBalance(
            subscriber_id=sub.id, wallet_tier="starter_wallet",
            credits_remaining=0, credits_used_total=0, auto_reload_enabled=False,
        )
        fresh_db.add(wallet)
        fresh_db.commit()

        pi_id = f"pi_topup_{uuid.uuid4().hex[:8]}"
        event = {
            "id": f"evt_topup_{uuid.uuid4().hex[:8]}",
            "type": "payment_intent.succeeded",
            "created": int(datetime.now(timezone.utc).timestamp()),
            "data": {
                "object": {
                    "id": pi_id,
                    "amount": 5000,
                    "metadata": {
                        "product": "wallet_topup",
                        "subscriber_id": str(sub.id),
                        "amount_cents": "5000",
                        "credits": "22",
                    },
                }
            },
        }
        _post(event, fresh_db)
        fresh_db.commit()
        _post(event, fresh_db)
        fresh_db.commit()

        # Verify wallet credited exactly once
        fresh_db.refresh(wallet)
        assert wallet.credits_remaining == 22, f"expected 22 credits, got {wallet.credits_remaining}"
        # Exactly one matching transaction
        txns = fresh_db.query(WalletTransaction).filter_by(stripe_charge_id=pi_id).count()
        assert txns == 1

    def test_charge_refunded_replay_flips_status_once(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        pi_id = f"pi_refund_{uuid.uuid4().hex[:8]}"
        purchase = PremiumPurchase(
            subscriber_id=sub.id, sku="report", paid_via="card",
            amount_cents=700, stripe_payment_intent_id=pi_id, status="delivered",
        )
        fresh_db.add(purchase)
        fresh_db.commit()

        charge_event = {
            "id": f"evt_refund_{uuid.uuid4().hex[:8]}",
            "type": "charge.refunded",
            "created": int(datetime.now(timezone.utc).timestamp()),
            "data": {
                "object": {
                    "id": f"ch_{uuid.uuid4().hex[:8]}",
                    "payment_intent": pi_id,
                    "amount": 700,
                    "amount_refunded": 700,
                    "refunds": {"data": [{"reason": "requested_by_customer"}]},
                }
            },
        }

        # Founder alert is fire-and-forget over Twilio; stub it to avoid
        # any actual SMS attempt during tests.
        with patch("src.services.stripe_webhooks._send_founder_alert"):
            _post(charge_event, fresh_db)
            fresh_db.commit()
            _post(charge_event, fresh_db)
            fresh_db.commit()

        fresh_db.refresh(purchase)
        assert purchase.status == "refunded"
        assert purchase.refund_amount_cents == 700
        # No clawback for card-paid purchases — wallet untouched
        wallet_count = fresh_db.query(WalletTransaction).filter_by(subscriber_id=sub.id).count()
        assert wallet_count == 0

    def test_dispute_created_replay_increments_counter_once(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        pi_id = f"pi_dispute_{uuid.uuid4().hex[:8]}"
        purchase = PremiumPurchase(
            subscriber_id=sub.id, sku="transfer", paid_via="card",
            amount_cents=6500, stripe_payment_intent_id=pi_id, status="delivered",
        )
        fresh_db.add(purchase)
        fresh_db.commit()

        event = {
            "id": f"evt_dispute_{uuid.uuid4().hex[:8]}",
            "type": "charge.dispute.created",
            "created": int(datetime.now(timezone.utc).timestamp()),
            "data": {
                "object": {
                    "id": f"dp_{uuid.uuid4().hex[:8]}",
                    "charge": {"id": f"ch_{uuid.uuid4().hex[:8]}", "payment_intent": pi_id},
                    "reason": "fraudulent",
                    "amount": 6500,
                }
            },
        }

        with patch("src.services.stripe_webhooks._send_founder_alert"):
            _post(event, fresh_db)
            fresh_db.commit()
            _post(event, fresh_db)
            fresh_db.commit()

        fresh_db.refresh(sub)
        fresh_db.refresh(purchase)
        # Counter incremented exactly once (second event blocked at idempotency guard)
        assert sub.disputed_count == 1
        assert purchase.status == "disputed"
        assert purchase.dispute_reason == "fraudulent"

    def test_two_disputes_in_90d_flips_subscriber_status(self, fresh_db):
        """Soft-block: second distinct dispute inside 90 days → status=disputed."""
        sub = _seed_subscriber(fresh_db)

        for i in range(2):
            pi_id = f"pi_d_{i}_{uuid.uuid4().hex[:8]}"
            purchase = PremiumPurchase(
                subscriber_id=sub.id, sku="transfer", paid_via="card",
                amount_cents=6500, stripe_payment_intent_id=pi_id, status="delivered",
            )
            fresh_db.add(purchase)
            fresh_db.commit()

            event = {
                "id": f"evt_d_{i}_{uuid.uuid4().hex[:8]}",
                "type": "charge.dispute.created",
                "created": int(datetime.now(timezone.utc).timestamp()),
                "data": {
                    "object": {
                        "id": f"dp_{i}_{uuid.uuid4().hex[:8]}",
                        "charge": {"id": f"ch_{i}", "payment_intent": pi_id},
                        "reason": "fraudulent",
                        "amount": 6500,
                    }
                },
            }
            with patch("src.services.stripe_webhooks._send_founder_alert"):
                _post(event, fresh_db)
                fresh_db.commit()

        fresh_db.refresh(sub)
        assert sub.disputed_count == 2
        assert sub.status == "disputed"
