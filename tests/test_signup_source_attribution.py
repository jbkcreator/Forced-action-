"""
Block 4 (Attribution) — Phase 1 bug-fix verification.

Covers the two `signup_source` defects found while auditing the anon→checkout→
signup attribution path, fixed in stripe_webhooks.py:
  1. A genuinely-new paid subscriber (no prior /api/free-signup) now records
     signup_source='landing_page' when Stripe metadata carries utm_source/
     campaign_id, instead of silently falling back to 'direct'.
  2. _stamp_campaign_fields upgrades a still-unattributed subscriber's
     signup_source to 'landing_page' on backfill (not NULL-only, since the
     column has a NOT NULL 'direct' default) — but never clobbers a real
     first-touch source (e.g. 'referral').
  3. The dbpr_email campaign-attribution branch writes signup_source (not the
     nonexistent Subscriber.acquisition_source, which silently discarded the
     write) and also respects first-touch.

Self-skips when DATABASE_URL is not configured (see tests/test_stripe_meta_capi.py,
whose stub/fixture pattern this file mirrors).
"""
from __future__ import annotations

import json
import uuid
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from unittest.mock import patch

from sqlalchemy import select

from src.core.models import Subscriber
from src.services import meta_capi_service
from src.services.stripe_webhooks import handle_webhook


def _stub_construct_event(event):
    return patch("src.services.stripe_webhooks.stripe.Webhook.construct_event", return_value=event)


def _stub_init_stripe():
    return patch("src.services.stripe_webhooks._init_stripe", return_value=True)


def _stub_settings_secret():
    from unittest.mock import MagicMock, PropertyMock
    from config.settings import AppSettings
    fake = MagicMock()
    fake.get_secret_value.return_value = "whsec_signup_source_test"
    return patch.object(AppSettings, "active_stripe_webhook_secret",
                        new_callable=PropertyMock, return_value=fake)


def _post(event, db):
    raw = json.dumps(event).encode("utf-8")
    with _stub_init_stripe(), _stub_settings_secret(), _stub_construct_event(event):
        return handle_webhook(raw, sig_header="t=stub,v1=stub", db=db)


@contextmanager
def _quiet_subscription_side_effects():
    with ExitStack() as stack:
        stack.enter_context(patch("src.services.stripe_webhooks.push_subscriber_to_ghl"))
        stack.enter_context(patch("src.services.email.send_welcome_email"))
        stack.enter_context(patch("src.services.stripe_webhooks._send_first_leads_email"))
        stack.enter_context(patch("src.services.segmentation_engine.reclassify_safe"))
        stack.enter_context(patch.object(meta_capi_service, "fire_purchase_event",
                                          return_value={"status": "sent"}))
        yield


def _unique_phone():
    return "+1813555" + f"{uuid.uuid4().int % 10000:04d}"


def _checkout_event(*, cs_id, customer, email, meta_extra=None, amount_cents=9900):
    meta = {
        "tier": "starter",
        "vertical": "roofing",
        "county_id": "hillsborough",
        "is_founding": "False",
        "zip_codes": "33602",
    }
    meta.update(meta_extra or {})
    return {
        "id": f"evt_{uuid.uuid4().hex[:8]}",
        "type": "checkout.session.completed",
        "created": int(datetime.now(timezone.utc).timestamp()),
        "data": {"object": {
            "id": cs_id,
            "customer": customer,
            "subscription": f"sub_{uuid.uuid4().hex[:8]}",
            "payment_status": "paid",
            "amount_total": amount_cents,
            "currency": "usd",
            "customer_details": {"email": email, "name": "Test Buyer", "phone": _unique_phone()},
            "metadata": meta,
        }},
    }


class TestNewPaidSubscriberSignupSource:
    """Bug B: a paid-first buyer (no /api/free-signup row) must not be
    silently recorded as signup_source='direct' when campaign metadata is
    present on the Stripe session."""

    def test_utm_source_present_yields_landing_page(self, fresh_db):
        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        email = f"{uuid.uuid4().hex[:8]}@example.com"
        event = _checkout_event(
            cs_id=cs_id, customer=cust, email=email,
            meta_extra={"utm_source": "facebook", "utm_campaign": "tampa_investors"},
        )
        with _quiet_subscription_side_effects():
            ok, _msg = _post(event, fresh_db)
            fresh_db.commit()
        assert ok is True

        sub = fresh_db.execute(
            select(Subscriber).where(Subscriber.stripe_customer_id == cust)
        ).scalar_one()
        assert sub.signup_source == "landing_page"
        assert sub.utm_source == "facebook"

    def test_campaign_id_present_yields_landing_page(self, fresh_db):
        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        email = f"{uuid.uuid4().hex[:8]}@example.com"
        event = _checkout_event(
            cs_id=cs_id, customer=cust, email=email,
            meta_extra={"campaign_id": "fa_campaign_007"},
        )
        with _quiet_subscription_side_effects():
            _post(event, fresh_db)
            fresh_db.commit()

        sub = fresh_db.execute(
            select(Subscriber).where(Subscriber.stripe_customer_id == cust)
        ).scalar_one()
        assert sub.signup_source == "landing_page"

    def test_no_attribution_metadata_yields_direct(self, fresh_db):
        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        email = f"{uuid.uuid4().hex[:8]}@example.com"
        event = _checkout_event(cs_id=cs_id, customer=cust, email=email)
        with _quiet_subscription_side_effects():
            _post(event, fresh_db)
            fresh_db.commit()

        sub = fresh_db.execute(
            select(Subscriber).where(Subscriber.stripe_customer_id == cust)
        ).scalar_one()
        assert sub.signup_source == "direct"


class TestStampCampaignFieldsFirstTouch:
    """_stamp_campaign_fields must upgrade an unattributed subscriber's
    signup_source, but never clobber a real first-touch source already
    captured via /api/free-signup (e.g. 'referral')."""

    def test_upgrades_free_signup_row_still_default(self, fresh_db):
        # Pre-provisioned free row (as /api/free-signup would create), source
        # left at the default 'direct' — simulates a buyer whose free-signup
        # call didn't carry utm (e.g. it failed / was skipped) but whose
        # later paid checkout did.
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        email = f"{uuid.uuid4().hex[:8]}@example.com"
        free_sub = Subscriber(
            stripe_customer_id=f"cus_free_{uuid.uuid4().hex[:8]}", tier="free", vertical="roofing",
            county_id="hillsborough", status="active",
            event_feed_uuid=str(uuid.uuid4()), email=email,
        )
        fresh_db.add(free_sub)
        fresh_db.commit()
        assert free_sub.signup_source == "direct"

        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        event = _checkout_event(
            cs_id=cs_id, customer=cust, email=email,
            meta_extra={"utm_source": "google", "utm_campaign": "q3_push"},
        )
        with _quiet_subscription_side_effects():
            _post(event, fresh_db)
            fresh_db.commit()

        fresh_db.refresh(free_sub)
        assert free_sub.signup_source == "landing_page"
        assert free_sub.utm_source == "google"

    def test_does_not_clobber_real_first_touch_source(self, fresh_db):
        # A subscriber who already has a genuine first-touch source (referral)
        # must keep it even though the paid checkout carries utm metadata.
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        email = f"{uuid.uuid4().hex[:8]}@example.com"
        referred_sub = Subscriber(
            stripe_customer_id=f"cus_free_{uuid.uuid4().hex[:8]}", tier="free", vertical="roofing",
            county_id="hillsborough", status="active",
            event_feed_uuid=str(uuid.uuid4()), email=email,
            signup_source="referral",
        )
        fresh_db.add(referred_sub)
        fresh_db.commit()

        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        event = _checkout_event(
            cs_id=cs_id, customer=cust, email=email,
            meta_extra={"utm_source": "facebook"},
        )
        with _quiet_subscription_side_effects():
            _post(event, fresh_db)
            fresh_db.commit()

        fresh_db.refresh(referred_sub)
        assert referred_sub.signup_source == "referral"


class TestDbprEmailSignupSourceFix:
    """The campaign-attribution (B6) branch previously wrote to the
    nonexistent Subscriber.acquisition_source, silently discarding the
    dbpr_email origin. It must now persist onto signup_source, and still
    honor first-touch."""

    def test_attributed_conversion_sets_signup_source_dbpr_email(self, fresh_db):
        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        email = f"{uuid.uuid4().hex[:8]}@example.com"
        event = _checkout_event(
            cs_id=cs_id, customer=cust, email=email,
            meta_extra={"campaign_attribution_token": "fake.token.value"},
        )
        with _quiet_subscription_side_effects(), \
             patch("src.services.campaign_attribution.decode_attribution_token",
                   return_value=4242), \
             patch("src.services.campaign_attribution.record_conversion",
                   return_value=True):
            ok, _msg = _post(event, fresh_db)
            fresh_db.commit()
        assert ok is True

        sub = fresh_db.execute(
            select(Subscriber).where(Subscriber.stripe_customer_id == cust)
        ).scalar_one()
        assert sub.signup_source == "dbpr_email"
        # The old broken code path set a transient, non-persisted
        # `acquisition_source` attribute — Subscriber has no such column.
        assert not hasattr(Subscriber, "acquisition_source")

    def test_attributed_conversion_does_not_clobber_first_touch(self, fresh_db):
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        email = f"{uuid.uuid4().hex[:8]}@example.com"
        referred_sub = Subscriber(
            stripe_customer_id=f"cus_free_{uuid.uuid4().hex[:8]}", tier="free", vertical="roofing",
            county_id="hillsborough", status="active",
            event_feed_uuid=str(uuid.uuid4()), email=email,
            signup_source="referral",
        )
        fresh_db.add(referred_sub)
        fresh_db.commit()

        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        event = _checkout_event(
            cs_id=cs_id, customer=cust, email=email,
            meta_extra={"campaign_attribution_token": "fake.token.value"},
        )
        with _quiet_subscription_side_effects(), \
             patch("src.services.campaign_attribution.decode_attribution_token",
                   return_value=4242), \
             patch("src.services.campaign_attribution.record_conversion",
                   return_value=True):
            _post(event, fresh_db)
            fresh_db.commit()

        fresh_db.refresh(referred_sub)
        assert referred_sub.signup_source == "referral"
