"""
Webhook hook tests for the non-buyer nurture sequence.

checkout.session.expired (non-hot_lead_unlock) -> captures a checkout_abandon
candidate. checkout.session.completed (first paid conversion) -> mark_converted
+ remove_lead, matched by email, idempotent on replay.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import patch, PropertyMock, MagicMock

import pytest

from src.core.models import NonBuyerNurtureSequence


def _stub_init_stripe():
    return patch("src.services.stripe_webhooks._init_stripe", return_value=True)


def _stub_settings_secret():
    from config.settings import AppSettings
    fake_secret = MagicMock()
    fake_secret.get_secret_value.return_value = "whsec_nurture_test"
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
    from src.services.stripe_webhooks import handle_webhook
    raw = json.dumps(event).encode("utf-8")
    with _stub_init_stripe(), _stub_settings_secret(), _stub_construct_event(event), \
         patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
         patch("src.services.email.send_welcome_email"), \
         patch("src.services.stripe_webhooks._send_first_leads_email"):
        return handle_webhook(raw, sig_header="t=stub,v1=stub", db=db)


def _make_expired_event(*, customer_id, email, product=None, event_id=None):
    meta = {"tier": "pro", "vertical": "roofing", "county_id": "hillsborough"}
    if product:
        meta["product"] = product
    return {
        "id": event_id or f"evt_exp_{uuid.uuid4().hex[:8]}",
        "type": "checkout.session.expired",
        "created": int(datetime.now(timezone.utc).timestamp()),
        "data": {
            "object": {
                "id": f"cs_{uuid.uuid4().hex[:8]}",
                "customer": customer_id,
                "metadata": meta,
                "customer_details": {"email": email},
            }
        },
    }


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
                "customer_details": {"email": email, "name": "Nurture Test"},
            }
        },
    }


class TestCheckoutExpiredCapturesNurtureCandidate:
    def test_abandoned_subscription_checkout_records_candidate(self, fresh_db):
        uid = uuid.uuid4().hex[:8]
        email = f"abandon-{uid}@example.com"
        event = _make_expired_event(customer_id=f"cus_{uid}", email=email)

        _post(event, fresh_db)
        fresh_db.commit()

        row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email=email).one()
        assert row.source == "checkout_abandon"
        assert row.status == "eligible"

    def test_hot_lead_unlock_expiry_does_not_record_candidate(self, fresh_db):
        uid = uuid.uuid4().hex[:8]
        email = f"hotlead-{uid}@example.com"
        event = _make_expired_event(customer_id=f"cus_{uid}", email=email, product="hot_lead_unlock")

        _post(event, fresh_db)
        fresh_db.commit()

        row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email=email).one_or_none()
        assert row is None


class TestFirstPaidConversionMarksNurtureConverted:
    def test_conversion_removes_lead_and_marks_converted(self, fresh_db):
        uid = uuid.uuid4().hex[:8]
        email = f"convert-{uid}@example.com"
        now = datetime.now(timezone.utc)
        fresh_db.add(NonBuyerNurtureSequence(
            email=email, source="free_signup", captured_at=now,
            status="enrolled", instantly_campaign_id="camp_1",
            instantly_lead_id="lead_abc", enrolled_at=now,
        ))
        fresh_db.commit()

        event = _make_checkout_event(customer_id=f"cus_{uid}", sub_id=f"sub_{uid}", email=email)

        with patch("src.services.non_buyer_nurture.instantly.remove_lead", return_value=True) as mock_remove:
            _post(event, fresh_db)
        fresh_db.commit()

        mock_remove.assert_called_once_with("lead_abc")
        row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email=email).one()
        assert row.status == "converted"
        assert row.removal_reason == "paid_conversion"

    def test_conversion_replay_is_idempotent(self, fresh_db):
        uid = uuid.uuid4().hex[:8]
        email = f"replay-{uid}@example.com"
        now = datetime.now(timezone.utc)
        fresh_db.add(NonBuyerNurtureSequence(
            email=email, source="free_signup", captured_at=now,
            status="enrolled", instantly_campaign_id="camp_1",
            instantly_lead_id="lead_xyz", enrolled_at=now,
        ))
        fresh_db.commit()

        event = _make_checkout_event(customer_id=f"cus_{uid}", sub_id=f"sub_{uid}", email=email)

        with patch("src.services.non_buyer_nurture.instantly.remove_lead", return_value=True) as mock_remove:
            _post(event, fresh_db)
            fresh_db.commit()
            # Same event_id — Stripe's own idempotency dedupe short-circuits the replay.
            _post(event, fresh_db)
            fresh_db.commit()

        mock_remove.assert_called_once_with("lead_xyz")
