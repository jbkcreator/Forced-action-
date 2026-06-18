"""
Webhook → Meta CAPI integration tests (S2).

Drives the REAL Stripe webhook handlers against Postgres (fresh_db) with Stripe
and all external side-effects stubbed, and asserts:
  - subscription purchase fires CAPI once, after activation, event_id=sub_<cs>
  - subscription revenue is written to conversion_attribution_events
  - utm/campaign fields are stamped onto the subscriber from Stripe metadata
  - a payment_failed (churned) checkout does NOT fire CAPI
  - a duplicate webhook replay fires CAPI only once
  - lead-pack reservation fires CAPI once, event_id=leadpack_<pi>
  - lead-pack refund branches (unlaunched county, short pack) do NOT fire CAPI

Self-skips when DATABASE_URL is not configured.
"""
from __future__ import annotations

import json
import uuid
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select, text

from src.core.models import LeadPackPurchase, Property, DistressScore, Owner, Subscriber
from src.services import meta_capi_service, stripe_webhooks
from src.services.stripe_webhooks import handle_webhook


# ── Stripe webhook stubs (mirrors tests/test_stripe_replay.py) ───────────────

def _stub_construct_event(event):
    return patch("src.services.stripe_webhooks.stripe.Webhook.construct_event", return_value=event)


def _stub_init_stripe():
    return patch("src.services.stripe_webhooks._init_stripe", return_value=True)


def _stub_settings_secret():
    from unittest.mock import MagicMock, PropertyMock
    from config.settings import AppSettings
    fake = MagicMock()
    fake.get_secret_value.return_value = "whsec_meta_test"
    return patch.object(AppSettings, "active_stripe_webhook_secret",
                        new_callable=PropertyMock, return_value=fake)


def _post(event, db):
    raw = json.dumps(event).encode("utf-8")
    with _stub_init_stripe(), _stub_settings_secret(), _stub_construct_event(event):
        return handle_webhook(raw, sig_header="t=stub,v1=stub", db=db)


@contextmanager
def _quiet_subscription_side_effects():
    """Stub the external side-effects of _on_checkout_completed so the test
    isolates the CAPI + attribution behaviour."""
    with ExitStack() as stack:
        stack.enter_context(patch("src.services.stripe_webhooks.push_subscriber_to_ghl"))
        stack.enter_context(patch("src.services.email.send_welcome_email"))
        stack.enter_context(patch("src.services.stripe_webhooks._send_first_leads_email"))
        stack.enter_context(patch("src.services.segmentation_engine.reclassify_safe"))
        yield


def _unique_phone():
    return "+1813555" + f"{uuid.uuid4().int % 10000:04d}"


def _checkout_event(*, cs_id, customer, email, amount_cents=9900, payment_status="paid",
                    zip_codes="33602", with_attribution=True):
    meta = {
        "tier": "starter",
        "vertical": "roofing",
        "county_id": "hillsborough",
        "is_founding": "False",
        "zip_codes": zip_codes,
    }
    if with_attribution:
        meta.update({
            "utm_source": "meta",
            "utm_medium": "paid_social",
            "utm_campaign": "fa_test_campaign",
            "campaign_id": "fa_test_001",
            "fbclid": "test_fbclid",
            "buyer_ip": "203.0.113.7",
            "buyer_user_agent": "Mozilla/5.0 (test)",
        })
    return {
        "id": f"evt_{uuid.uuid4().hex[:8]}",
        "type": "checkout.session.completed",
        "created": int(datetime.now(timezone.utc).timestamp()),
        "data": {"object": {
            "id": cs_id,
            "customer": customer,
            "subscription": f"sub_{uuid.uuid4().hex[:8]}",
            "payment_status": payment_status,
            "amount_total": amount_cents,
            "currency": "usd",
            "customer_details": {"email": email, "name": "Test Buyer", "phone": _unique_phone()},
            "metadata": meta,
        }},
    }


# ── Subscription ─────────────────────────────────────────────────────────────

class TestSubscriptionCapi:
    def test_success_fires_capi_once_and_records_revenue(self, fresh_db):
        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        email = f"{uuid.uuid4().hex[:8]}@example.com"
        event = _checkout_event(cs_id=cs_id, customer=cust, email=email, amount_cents=9900)

        with _quiet_subscription_side_effects(), \
             patch.object(meta_capi_service, "fire_purchase_event",
                          return_value={"status": "sent"}) as fire:
            ok, _msg = _post(event, fresh_db)
            fresh_db.commit()

        assert ok is True
        fire.assert_called_once()
        assert fire.call_args.kwargs["event_id"] == f"sub_{cs_id}"
        assert fire.call_args.kwargs["source"] == "subscription"
        assert fire.call_args.kwargs["amount"] == 99.0

        sub = fresh_db.execute(
            select(Subscriber).where(Subscriber.stripe_customer_id == cust)
        ).scalar_one()
        # utm/campaign stamped from metadata
        assert sub.utm_campaign == "fa_test_campaign"
        assert sub.campaign_id == "fa_test_001"

        # revenue recorded in the attribution ledger
        rev = fresh_db.execute(text("""
            SELECT revenue_amount FROM conversion_attribution_events
            WHERE source_table = 'checkout_sessions' AND source_event_id = :cs
        """), {"cs": cs_id}).scalar()
        assert rev is not None
        assert float(rev) == 99.0

    def test_payment_failed_does_not_fire_capi(self, fresh_db):
        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        event = _checkout_event(
            cs_id=cs_id, customer=cust, email=f"{uuid.uuid4().hex[:8]}@example.com",
            payment_status="unpaid",
        )
        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch.object(meta_capi_service, "fire_purchase_event") as fire:
            _post(event, fresh_db)
            fresh_db.commit()
        fire.assert_not_called()

    def test_replay_fires_capi_once(self, fresh_db):
        cs_id = f"cs_{uuid.uuid4().hex[:8]}"
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        email = f"{uuid.uuid4().hex[:8]}@example.com"
        event = _checkout_event(cs_id=cs_id, customer=cust, email=email)

        with _quiet_subscription_side_effects(), \
             patch.object(meta_capi_service, "fire_purchase_event",
                          return_value={"status": "sent"}) as fire:
            _post(event, fresh_db)
            fresh_db.commit()
            ok2, msg2 = _post(event, fresh_db)   # same event id → dedupe
            fresh_db.commit()

        assert ok2 is True
        assert msg2 == "Already processed"
        fire.assert_called_once()


# ── Lead pack ────────────────────────────────────────────────────────────────

def _lp_table_ready(db) -> bool:
    if db.execute(text("SELECT to_regclass('public.lead_exclusivity')")).scalar() is None:
        return False
    return db.execute(text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name='lead_pack_purchases' AND column_name='tracerfy_queue_id'"
    )).scalar() is not None


@pytest.fixture
def lp_db(fresh_db):
    if not _lp_table_ready(fresh_db):
        pytest.skip("lead pack hot-enrichment migration not applied")
    return fresh_db


def _src_county():
    from config.settings import get_settings
    return get_settings().county_launch_source_county or "hillsborough"


def _mk_subscriber(db, feed_uuid, county):
    s = Subscriber(
        stripe_customer_id=f"cus_{feed_uuid}", tier="pro", vertical="roofing",
        county_id=county, status="active", event_feed_uuid=feed_uuid,
        email=f"{feed_uuid}@example.com",
    )
    db.add(s)
    db.flush()
    return s


def _mk_property(db, parcel, zip_code, county, score=85.0):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county, address=f"{parcel} Test St")
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=score,
        vertical_scores={"roofing": score, "restoration": score},
        score_date=datetime.now(timezone.utc).date(),
    ))
    db.add(Owner(property_id=p.id, phone_1="8135550100", contact_info_confidence="high"))
    db.flush()
    return p.id


def _pi(pi_id, feed_uuid, zip_code, county, amount=9900):
    return {
        "id": pi_id,
        "amount": amount,
        "amount_received": amount,
        "currency": "usd",
        "metadata": {
            "product": "lead_pack", "feed_uuid": feed_uuid, "zip_code": zip_code,
            "vertical": "roofing", "county_id": county,
            "utm_campaign": "fa_test_campaign", "campaign_id": "fa_test_001",
            "fbclid": "test_fbclid", "buyer_ip": "203.0.113.9",
            "buyer_user_agent": "Mozilla/5.0 (test)",
        },
    }


class TestLeadPackCapi:
    def test_reservation_fires_capi(self, lp_db):
        county = _src_county()
        feed = f"capi-reserve-{uuid.uuid4().hex[:6]}"
        zip_code = "33611"
        _mk_subscriber(lp_db, feed, county)
        for i in range(6):
            _mk_property(lp_db, f"CAPI-R{i}-{uuid.uuid4().hex[:4]}", zip_code, county, score=90 - i)
        pi_id = f"pi_{uuid.uuid4().hex[:8]}"

        with patch.object(meta_capi_service, "fire_purchase_event",
                          return_value={"status": "sent"}) as fire:
            stripe_webhooks._on_lead_pack_payment(_pi(pi_id, feed, zip_code, county), lp_db)

        purchase = lp_db.execute(
            select(LeadPackPurchase).where(LeadPackPurchase.stripe_payment_intent_id == pi_id)
        ).scalar_one()
        assert purchase.status == "enriching"
        fire.assert_called_once()
        assert fire.call_args.kwargs["event_id"] == f"leadpack_{pi_id}"
        assert fire.call_args.kwargs["source"] == "lead_pack"

    def test_unlaunched_county_refund_does_not_fire(self, lp_db):
        feed = f"capi-nolaunch-{uuid.uuid4().hex[:6]}"
        zip_code = "33611"
        _mk_subscriber(lp_db, feed, _src_county())
        pi_id = f"pi_{uuid.uuid4().hex[:8]}"

        with patch.object(meta_capi_service, "fire_purchase_event") as fire, \
             patch("src.services.stripe_webhooks.stripe.Refund.create", return_value={"id": "re_x"}):
            stripe_webhooks._on_lead_pack_payment(
                _pi(pi_id, feed, zip_code, "nowhere_county"), lp_db
            )
        fire.assert_not_called()

    def test_short_pack_refund_does_not_fire(self, lp_db):
        county = _src_county()
        feed = f"capi-short-{uuid.uuid4().hex[:6]}"
        zip_code = "33699"  # no properties seeded → fewer than 5 qualified leads
        _mk_subscriber(lp_db, feed, county)
        pi_id = f"pi_{uuid.uuid4().hex[:8]}"

        with patch.object(meta_capi_service, "fire_purchase_event") as fire, \
             patch("src.services.stripe_webhooks.stripe.Refund.create", return_value={"id": "re_y"}):
            stripe_webhooks._on_lead_pack_payment(_pi(pi_id, feed, zip_code, county), lp_db)
        fire.assert_not_called()


# ── _fire_capi_for_pi helper (DB-free) ───────────────────────────────────────

class TestFireCapiForPi:
    def test_maps_pi_to_capi_call(self):
        sub = SimpleNamespace(id=1, email="b@example.com", phone=None,
                              utm_campaign=None, campaign_id=None)
        pi = {
            "id": "pi_x", "amount": 9900, "amount_received": 9900, "currency": "usd",
            "metadata": {"buyer_ip": "203.0.113.1", "buyer_user_agent": "UA/9",
                         "fbclid": "fbx", "utm_campaign": "fa_test_campaign",
                         "campaign_id": "fa_test_001"},
        }
        with patch.object(meta_capi_service, "fire_purchase_event",
                          return_value={"status": "sent"}) as fire:
            stripe_webhooks._fire_capi_for_pi(pi, sub, "bundle", "bundle_pi_x", MagicMock())
        fire.assert_called_once()
        kw = fire.call_args.kwargs
        assert kw["event_id"] == "bundle_pi_x"
        assert kw["source"] == "bundle"
        assert kw["amount"] == 99.0
        ctx = kw["request_context"]
        assert ctx["buyer_ip"] == "203.0.113.1"
        assert ctx["utm_campaign"] == "fa_test_campaign"
        assert ctx["campaign_id"] == "fa_test_001"
        assert ctx["fbclid"] == "fbx"


# ── lead_unlock / bundle / premium PI flows ──────────────────────────────────

def _seed_sub(db, customer):
    s = Subscriber(
        stripe_customer_id=customer, tier="pro", vertical="roofing",
        county_id="hillsborough", status="active",
        event_feed_uuid=f"pi-{uuid.uuid4().hex[:8]}",
        email=f"{uuid.uuid4().hex[:8]}@example.com",
    )
    db.add(s)
    db.flush()
    return s


def _pi_generic(pi_id, amount, meta):
    return {"id": pi_id, "amount": amount, "amount_received": amount, "currency": "usd", **meta}


class TestPaymentIntentProductsCapi:
    def test_lead_unlock_fires(self, fresh_db):
        cust = f"cus_{uuid.uuid4().hex[:8]}"
        _seed_sub(fresh_db, cust)
        prop = Property(parcel_id=f"LU-{uuid.uuid4().hex[:6]}", zip="33602",
                        county_id="hillsborough", address="1 Test St")
        fresh_db.add(prop)
        fresh_db.flush()
        # A real DistressScore exercises the score path in the handler (the
        # reclassify metadata previously did int(score) on the ORM object).
        fresh_db.add(DistressScore(
            property_id=prop.id, qualified=True, final_cds_score=82.0,
            vertical_scores={"roofing": 82.0}, score_date=datetime.now(timezone.utc).date(),
        ))
        fresh_db.flush()
        pi_id = f"pi_{uuid.uuid4().hex[:8]}"
        pi = _pi_generic(pi_id, 400, {"customer": cust, "metadata": {
            "product": "lead_unlock", "property_id": str(prop.id),
            "utm_campaign": "fa_test_campaign", "campaign_id": "fa_test_001",
            "buyer_ip": "203.0.113.5", "buyer_user_agent": "UA",
        }})
        with patch.object(meta_capi_service, "fire_purchase_event",
                          return_value={"status": "sent"}) as fire, \
             patch("src.services.auto_mode.enqueue_action"), \
             patch("src.services.stripe_webhooks._send_lead_unlock_email"), \
             patch("src.services.email.send_welcome_email"), \
             patch("src.services.segmentation_engine.reclassify_safe"):
            stripe_webhooks._on_lead_unlock_payment(pi, fresh_db)
        fire.assert_called_once()
        assert fire.call_args.kwargs["event_id"] == f"unlock_{pi_id}"
        assert fire.call_args.kwargs["source"] == "lead_unlock"
        assert fire.call_args.kwargs["amount"] == 4.0

    def test_bundle_fires(self, fresh_db):
        sub = _seed_sub(fresh_db, f"cus_{uuid.uuid4().hex[:8]}")
        pi_id = f"pi_{uuid.uuid4().hex[:8]}"
        pi = _pi_generic(pi_id, 9900, {"metadata": {
            "product": "bundle", "bundle_type": "weekend", "subscriber_id": str(sub.id),
            "zip_code": "33602", "vertical": "roofing",
            "utm_campaign": "fa_test_campaign", "campaign_id": "fa_test_001",
        }})
        with patch.object(meta_capi_service, "fire_purchase_event",
                          return_value={"status": "sent"}) as fire, \
             patch("src.services.bundle_engine.deliver"):
            stripe_webhooks._on_bundle_payment(pi, fresh_db)
        fire.assert_called_once()
        assert fire.call_args.kwargs["event_id"] == f"bundle_{pi_id}"
        assert fire.call_args.kwargs["source"] == "bundle"

    def test_premium_fires(self, fresh_db):
        sub = _seed_sub(fresh_db, f"cus_{uuid.uuid4().hex[:8]}")
        pi_id = f"pi_{uuid.uuid4().hex[:8]}"
        pi = _pi_generic(pi_id, 4900, {"metadata": {
            "product": "premium", "sku": "report", "subscriber_id": str(sub.id),
            "property_id": "", "utm_campaign": "fa_test_campaign", "campaign_id": "fa_test_001",
        }})
        with patch.object(meta_capi_service, "fire_purchase_event",
                          return_value={"status": "sent"}) as fire, \
             patch("src.services.premium_engine.record_card_purchase",
                   return_value=SimpleNamespace(id=1, status="delivered")), \
             patch("src.services.premium_engine.fulfill"):
            stripe_webhooks._on_premium_payment(pi, fresh_db)
        fire.assert_called_once()
        assert fire.call_args.kwargs["event_id"] == f"premium_{pi_id}"
        assert fire.call_args.kwargs["source"] == "premium"
