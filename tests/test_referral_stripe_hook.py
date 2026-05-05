"""
Phase A.1 (2026-05-04): Stripe checkout.session.completed must confirm any
pending ReferralEvent for the referee and credit the referrer 20 credits.

Before this fix, `confirm_purchase` and `reward_referrer` were only ever
called from tests — production checkout never flipped pending → confirmed,
so `_check_team_unlock` could never fire and no referral ever paid out.

Run:
    pytest tests/test_referral_stripe_hook.py -v
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import patch, PropertyMock, MagicMock

import pytest

from src.core.models import (
    ReferralEvent,
    Subscriber,
    WalletBalance,
    WalletTransaction,
)


def _stub_init_stripe():
    return patch("src.services.stripe_webhooks._init_stripe", return_value=True)


def _stub_settings_secret():
    from config.settings import AppSettings
    fake_secret = MagicMock()
    fake_secret.get_secret_value.return_value = "whsec_referral_test"
    return patch.object(
        AppSettings,
        "active_stripe_webhook_secret",
        new_callable=PropertyMock,
        return_value=fake_secret,
    )


def _stub_construct_event(event):
    return patch(
        "src.services.stripe_webhooks.stripe.Webhook.construct_event",
        return_value=event,
    )


def _post(event, db):
    """Invoke handle_webhook with all the side-effecty stubs in place."""
    from src.services.stripe_webhooks import handle_webhook
    raw = json.dumps(event).encode("utf-8")
    # Stub out heavy I/O the test isn't exercising
    with _stub_init_stripe(), _stub_settings_secret(), _stub_construct_event(event), \
         patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
         patch("src.services.stripe_webhooks._send_welcome_email"), \
         patch("src.services.stripe_webhooks._send_first_leads_email"):
        return handle_webhook(raw, sig_header="t=stub,v1=stub", db=db)


def _seed_referrer(db) -> Subscriber:
    """Create a referrer + their wallet (so we can credit the reward)."""
    uid = uuid.uuid4().hex[:8]
    referrer = Subscriber(
        stripe_customer_id=f"cus_referrer_{uid}",
        tier="pro",
        vertical="roofing",
        county_id="hillsborough",
        event_feed_uuid=f"referrer-{uid}",
        referral_code=f"REF{uid.upper()}",
        status="active",
    )
    db.add(referrer)
    db.flush()
    db.add(WalletBalance(
        subscriber_id=referrer.id,
        wallet_tier="starter_wallet",
        credits_remaining=0,
        credits_used_total=0,
    ))
    db.flush()
    return referrer


def _make_checkout_event(*, customer_id, sub_id, email, event_id=None, tier="pro", vertical="roofing"):
    return {
        "id": event_id or f"evt_chk_{uuid.uuid4().hex[:8]}",
        "type": "checkout.session.completed",
        "created": int(datetime.now(timezone.utc).timestamp()),
        "data": {
            "object": {
                "id": f"cs_{uuid.uuid4().hex[:8]}",
                "customer": customer_id,
                "subscription": sub_id,
                "payment_status": "paid",
                "metadata": {
                    "tier": tier,
                    "vertical": vertical,
                    "county_id": "hillsborough",
                    "zip_codes": "33602",
                    "is_founding": "False",
                },
                "customer_details": {"email": email, "name": "Referee Person"},
            }
        },
    }


class TestReferralConfirmOnCheckout:
    def test_pending_referral_flips_to_rewarded_and_credits_referrer(self, fresh_db):
        referrer = _seed_referrer(fresh_db)

        # Pre-create the referee's row + a pending ReferralEvent so the
        # checkout.session.completed handler finds an existing record to merge
        # onto. (The handler also has a path that creates a brand-new row,
        # but the referral flow is the same.)
        uid = uuid.uuid4().hex[:8]
        referee = Subscriber(
            stripe_customer_id=f"cus_referee_{uid}",
            tier="pro",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"referee-{uid}",
            email=f"referee-{uid}@example.com",
            status="active",
        )
        fresh_db.add(referee)
        fresh_db.flush()
        pending = ReferralEvent(
            referrer_subscriber_id=referrer.id,
            referee_subscriber_id=referee.id,
            referral_code=referrer.referral_code,
            status="pending",
            reward_type="credits",
            reward_value="20",
        )
        fresh_db.add(pending)
        fresh_db.commit()

        event = _make_checkout_event(
            customer_id=referee.stripe_customer_id,
            sub_id=f"sub_{uid}",
            email=referee.email,
        )
        _post(event, fresh_db)
        fresh_db.commit()

        fresh_db.refresh(pending)
        assert pending.status == "rewarded", \
            f"expected rewarded, got {pending.status}"
        assert pending.confirmed_at is not None

        # Referrer wallet should now have +20 credits
        wallet = fresh_db.query(WalletBalance).filter_by(
            subscriber_id=referrer.id
        ).one()
        assert wallet.credits_remaining == 20, \
            f"expected 20 credits, got {wallet.credits_remaining}"

        # And one credit-type WalletTransaction logged the reward
        txns = fresh_db.query(WalletTransaction).filter_by(
            subscriber_id=referrer.id
        ).all()
        assert any(t.txn_type == "credit" and t.amount == 20 for t in txns), \
            f"expected a +20 credit transaction, got {[(t.txn_type, t.amount) for t in txns]}"

    def test_replay_does_not_double_credit_referrer(self, fresh_db):
        """A replayed checkout.session.completed must not pay the referrer twice.

        Idempotency comes from two layers:
          1. StripeWebhookEvent unique constraint on event_id (handles duplicate
             event delivery).
          2. confirm_purchase only matches status='pending'; the second call
             finds nothing and is a no-op.
        """
        referrer = _seed_referrer(fresh_db)
        uid = uuid.uuid4().hex[:8]
        referee = Subscriber(
            stripe_customer_id=f"cus_replay_{uid}",
            tier="pro", vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"replay-{uid}",
            email=f"replay-{uid}@example.com",
            status="active",
        )
        fresh_db.add(referee)
        fresh_db.flush()
        pending = ReferralEvent(
            referrer_subscriber_id=referrer.id,
            referee_subscriber_id=referee.id,
            referral_code=referrer.referral_code,
            status="pending",
            reward_type="credits",
            reward_value="20",
        )
        fresh_db.add(pending)
        fresh_db.commit()

        event = _make_checkout_event(
            customer_id=referee.stripe_customer_id,
            sub_id=f"sub_{uid}",
            email=referee.email,
            event_id=f"evt_replay_{uid}",
        )
        _post(event, fresh_db)
        fresh_db.commit()
        # Replay
        _post(event, fresh_db)
        fresh_db.commit()

        wallet = fresh_db.query(WalletBalance).filter_by(
            subscriber_id=referrer.id
        ).one()
        # Still only +20, never +40
        assert wallet.credits_remaining == 20, \
            f"replay double-credited: balance={wallet.credits_remaining}"

    def test_no_pending_referral_does_not_credit(self, fresh_db):
        """A subscriber checking out with no referral event → no wallet
        credit, no error."""
        referrer = _seed_referrer(fresh_db)
        uid = uuid.uuid4().hex[:8]
        referee_email = f"plain-{uid}@example.com"

        event = _make_checkout_event(
            customer_id=f"cus_plain_{uid}",
            sub_id=f"sub_{uid}",
            email=referee_email,
        )
        _post(event, fresh_db)
        fresh_db.commit()

        # Referrer wallet untouched
        wallet = fresh_db.query(WalletBalance).filter_by(
            subscriber_id=referrer.id
        ).one()
        assert wallet.credits_remaining == 0
