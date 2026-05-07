"""
Stripe Payment Recovery handler tests.

Covers:
  - invoice.payment_failed  → _on_payment_failed()
  - customer.subscription.deleted → _on_subscription_deleted()

Mock pattern mirrors tests/test_stripe_replay.py: seed real DB rows,
stub Stripe verification, patch external side-effects (GHL, email).

Run:
    pytest tests/test_stripe_payment_recovery.py -v
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest

from src.core.models import (
    Subscriber,
    StripeWebhookEvent,
    ZipTerritory,
)
from src.services.stripe_webhooks import handle_webhook


# ── Helpers (same pattern as test_stripe_replay.py) ─────────────────────────


def _seed_subscriber(db, **overrides) -> Subscriber:
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=overrides.get("stripe_customer_id", f"cus_rec_{uid}"),
        tier=overrides.get("tier", "starter"),
        vertical=overrides.get("vertical", "roofing"),
        county_id=overrides.get("county_id", "hillsborough"),
        event_feed_uuid=overrides.get("event_feed_uuid", f"rec-{uid}"),
        has_saved_card=overrides.get("has_saved_card", True),
        email=overrides.get("email", f"sub_{uid}@example.com"),
        name=overrides.get("name", "Test User"),
        founding_member=overrides.get("founding_member", False),
        status=overrides.get("status", "active"),
    )
    db.add(sub)
    db.flush()
    return sub


def _seed_zip(db, subscriber_id: int, zip_code: str = None, status: str = "locked") -> ZipTerritory:
    zip_code = zip_code or f"T{uuid.uuid4().hex[:4]}"
    t = ZipTerritory(
        zip_code=zip_code,
        vertical="roofing",
        county_id="hillsborough",
        subscriber_id=subscriber_id,
        status=status,
    )
    db.add(t)
    db.flush()
    return t


def _stub_construct_event(event: dict):
    return patch(
        "src.services.stripe_webhooks.stripe.Webhook.construct_event",
        return_value=event,
    )


def _stub_init_stripe():
    return patch("src.services.stripe_webhooks._init_stripe", return_value=True)


def _stub_settings_secret():
    from config.settings import AppSettings
    fake_secret = MagicMock()
    fake_secret.get_secret_value.return_value = "whsec_test"
    return patch.object(
        AppSettings,
        "active_stripe_webhook_secret",
        new_callable=PropertyMock,
        return_value=fake_secret,
    )


def _post(event: dict, db):
    raw = json.dumps(event).encode("utf-8")
    with _stub_init_stripe(), _stub_settings_secret(), _stub_construct_event(event):
        ok, msg = handle_webhook(raw, sig_header="t=stub,v1=stub", db=db)
    return ok, msg


def _payment_failed_event(customer_id: str) -> dict:
    return {
        "id": f"evt_pf_{uuid.uuid4().hex[:8]}",
        "type": "invoice.payment_failed",
        "created": int(datetime.now(timezone.utc).timestamp()),
        "data": {
            "object": {
                "id": f"in_{uuid.uuid4().hex[:8]}",
                "customer": customer_id,
                "subscription": f"sub_{uuid.uuid4().hex[:8]}",
                "amount_due": 29700,
                "attempt_count": 1,
            }
        },
    }


def _subscription_deleted_event(customer_id: str, subscription_id: str = None) -> dict:
    return {
        "id": f"evt_del_{uuid.uuid4().hex[:8]}",
        "type": "customer.subscription.deleted",
        "created": int(datetime.now(timezone.utc).timestamp()),
        "data": {
            "object": {
                "id": subscription_id or f"sub_{uuid.uuid4().hex[:8]}",
                "customer": customer_id,
                "status": "canceled",
            }
        },
    }


# ── invoice.payment_failed ────────────────────────────────────────────────────


class TestPaymentFailedHandler:
    def test_ghl_called_with_payment_failed_tag(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        fresh_db.commit()
        event = _payment_failed_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl") as mock_ghl, \
             patch("src.services.email.send_email"):
            ok, _ = _post(event, fresh_db)

        assert ok is True
        mock_ghl.assert_called_once()
        assert mock_ghl.call_args.kwargs.get("tags") == ["payment_failed"]

    def test_subscriber_status_unchanged_on_payment_failure(self, fresh_db):
        sub = _seed_subscriber(fresh_db, status="active")
        fresh_db.commit()
        event = _payment_failed_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email"):
            _post(event, fresh_db)
            fresh_db.commit()

        fresh_db.refresh(sub)
        assert sub.status == "active"

    def test_payment_failed_email_sent(self, fresh_db):
        sub = _seed_subscriber(fresh_db, email="customer@example.com")
        fresh_db.commit()
        event = _payment_failed_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email") as mock_email:
            _post(event, fresh_db)

        mock_email.assert_called_once()
        subject = mock_email.call_args.kwargs.get("subject", "")
        assert "payment" in subject.lower() or "action" in subject.lower()

    def test_payment_failed_idempotency(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        fresh_db.commit()
        event = _payment_failed_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email") as mock_email:
            _post(event, fresh_db)
            fresh_db.commit()
            ok, msg = _post(event, fresh_db)

        assert ok is True
        assert msg == "Already processed"
        assert mock_email.call_count == 1

        rows = fresh_db.query(StripeWebhookEvent).filter_by(event_id=event["id"]).count()
        assert rows == 1

    def test_payment_failed_unknown_customer_is_graceful(self, fresh_db):
        event = _payment_failed_event("cus_does_not_exist")

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email"):
            ok, _ = _post(event, fresh_db)

        assert ok is True


# ── customer.subscription.deleted ────────────────────────────────────────────


class TestSubscriptionDeletedHandler:
    def test_status_set_to_grace_on_deletion(self, fresh_db):
        sub = _seed_subscriber(fresh_db, status="active")
        fresh_db.commit()
        event = _subscription_deleted_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email"):
            ok, _ = _post(event, fresh_db)
            fresh_db.commit()

        assert ok is True
        fresh_db.refresh(sub)
        assert sub.status == "grace"

    def test_grace_expires_at_set_on_deletion(self, fresh_db):
        sub = _seed_subscriber(fresh_db, status="active")
        fresh_db.commit()
        event = _subscription_deleted_event(sub.stripe_customer_id)

        before = datetime.now(timezone.utc)
        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email"):
            _post(event, fresh_db)
            fresh_db.commit()
        after = datetime.now(timezone.utc)

        fresh_db.refresh(sub)
        assert sub.grace_expires_at is not None
        expires = sub.grace_expires_at
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        # Grace window should be ≈168hr from now (within a few seconds of execution)
        assert timedelta(hours=167) < (expires - before) < timedelta(hours=169)

    def test_zip_territories_enter_grace(self, fresh_db):
        sub = _seed_subscriber(fresh_db, status="active")
        t1 = _seed_zip(fresh_db, sub.id)
        t2 = _seed_zip(fresh_db, sub.id)
        fresh_db.commit()
        event = _subscription_deleted_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email"):
            _post(event, fresh_db)
            fresh_db.commit()

        fresh_db.refresh(t1)
        fresh_db.refresh(t2)
        assert t1.status == "grace"
        assert t2.status == "grace"
        assert t1.grace_expires_at is not None
        assert t2.grace_expires_at is not None

    def test_ghl_stage_7_pushed_on_deletion(self, fresh_db):
        sub = _seed_subscriber(fresh_db, status="active")
        fresh_db.commit()
        event = _subscription_deleted_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl") as mock_ghl, \
             patch("src.services.email.send_email"):
            _post(event, fresh_db)

        mock_ghl.assert_called_once()
        assert mock_ghl.call_args.kwargs.get("stage") == 7

    def test_churned_regular_tag_on_deletion(self, fresh_db):
        sub = _seed_subscriber(fresh_db, founding_member=False)
        fresh_db.commit()
        event = _subscription_deleted_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl") as mock_ghl, \
             patch("src.services.email.send_email"):
            _post(event, fresh_db)

        assert "churned_regular" in mock_ghl.call_args.kwargs.get("tags", [])

    def test_churned_founding_tag_for_founding_member(self, fresh_db):
        sub = _seed_subscriber(fresh_db, founding_member=True)
        fresh_db.commit()
        event = _subscription_deleted_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl") as mock_ghl, \
             patch("src.services.email.send_email"):
            _post(event, fresh_db)

        assert "churned_founding" in mock_ghl.call_args.kwargs.get("tags", [])

    def test_cancellation_email_sent_on_deletion(self, fresh_db):
        sub = _seed_subscriber(fresh_db, email="sub@example.com")
        fresh_db.commit()
        event = _subscription_deleted_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email") as mock_email:
            _post(event, fresh_db)

        mock_email.assert_called_once()
        subject = mock_email.call_args.kwargs.get("subject", "")
        assert "cancel" in subject.lower() or "subscription" in subject.lower()

    def test_subscription_deleted_idempotency(self, fresh_db):
        sub = _seed_subscriber(fresh_db, status="active")
        fresh_db.commit()
        event = _subscription_deleted_event(sub.stripe_customer_id)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email") as mock_email:
            _post(event, fresh_db)
            fresh_db.commit()
            ok, msg = _post(event, fresh_db)

        assert ok is True
        assert msg == "Already processed"
        assert mock_email.call_count == 1

        rows = fresh_db.query(StripeWebhookEvent).filter_by(event_id=event["id"]).count()
        assert rows == 1

    def test_deletion_unknown_customer_is_graceful(self, fresh_db):
        event = _subscription_deleted_event("cus_does_not_exist")

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.email.send_email"):
            ok, _ = _post(event, fresh_db)

        assert ok is True
