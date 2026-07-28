from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import select
from unittest.mock import patch
from unittest.mock import MagicMock


def _ensure_memory_tables(db):
    from src.core.models import UnifiedSubscriberMemory, SubscriberMemorySummary

    bind = db.get_bind()
    UnifiedSubscriberMemory.__table__.create(bind, checkfirst=True)
    SubscriberMemorySummary.__table__.create(bind, checkfirst=True)


def _make_subscriber(db):
    from src.core.models import Subscriber

    run_id = uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_mem_{run_id}",
        stripe_subscription_id=f"sub_mem_{run_id}",
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        status="active",
        event_feed_uuid=f"feed-{run_id}",
        email=f"memory_{run_id}@example.com",
    )
    db.add(sub)
    db.flush()
    return sub


def test_append_memory_event_creates_timeline_row_and_summary(fresh_db):
    _ensure_memory_tables(fresh_db)
    sub = _make_subscriber(fresh_db)

    from src.core.models import UnifiedSubscriberMemory, SubscriberMemorySummary
    from src.services.subscriber_memory import append_memory_event

    occurred_at = datetime(2026, 6, 25, 14, 1, tzinfo=timezone.utc)

    created = append_memory_event(
        fresh_db,
        subscriber_id=sub.id,
        stream_source="STRIPE",
        event_type="checkout_completed",
        source_event_id="evt_checkout_1",
        source_event_name="checkout.session.completed",
        occurred_at=occurred_at,
        status="completed",
        summary="Subscriber completed checkout for starter plan",
        channel="stripe",
        actor={"type": "system", "id": "stripe"},
        raw={"stripe_customer_id": sub.stripe_customer_id},
    )

    assert created is True

    row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.subscriber_id == str(sub.id),
            UnifiedSubscriberMemory.event_type == "checkout_completed",
        )
    ).scalar_one()

    assert row.stream_source == "STRIPE"
    assert row.event_payload["source_event_id"] == "evt_checkout_1"
    assert row.event_payload["source_event_name"] == "checkout.session.completed"
    assert row.event_payload["occurred_at"] == occurred_at.isoformat()
    assert row.event_payload["status"] == "completed"
    assert row.event_payload["summary"] == "Subscriber completed checkout for starter plan"
    assert row.event_payload["channel"] == "stripe"
    assert row.event_payload["actor"] == {"type": "system", "id": "stripe"}
    assert row.event_payload["raw"] == {"stripe_customer_id": sub.stripe_customer_id}

    summary_row = fresh_db.get(SubscriberMemorySummary, sub.id)
    assert summary_row is not None
    assert summary_row.subscriber_id == sub.id
    assert summary_row.last_event_type == "checkout_completed"
    assert summary_row.last_stripe_event_type == "checkout_completed"
    assert summary_row.latest_checkout_state == "completed"
    assert summary_row.last_event_at == occurred_at


def test_append_memory_event_ignores_duplicate_source_event(fresh_db):
    _ensure_memory_tables(fresh_db)
    sub = _make_subscriber(fresh_db)

    from src.core.models import UnifiedSubscriberMemory
    from src.services.subscriber_memory import append_memory_event

    occurred_at = datetime(2026, 6, 25, 14, 5, tzinfo=timezone.utc)
    kwargs = dict(
        subscriber_id=sub.id,
        stream_source="STRIPE",
        event_type="checkout_completed",
        source_event_id="evt_checkout_dupe",
        source_event_name="checkout.session.completed",
        occurred_at=occurred_at,
        status="completed",
        summary="Subscriber completed checkout for starter plan",
        channel="stripe",
        actor={"type": "system", "id": "stripe"},
        raw={"stripe_customer_id": sub.stripe_customer_id},
    )

    assert append_memory_event(fresh_db, **kwargs) is True
    assert append_memory_event(fresh_db, **kwargs) is False

    rows = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "STRIPE",
            UnifiedSubscriberMemory.event_type == "checkout_completed",
            UnifiedSubscriberMemory.event_payload["source_event_id"].astext == "evt_checkout_dupe",
        )
    ).scalars().all()

    assert len(rows) == 1


def test_checkout_completed_projects_memory_event(fresh_db):
    _ensure_memory_tables(fresh_db)

    from src.core.models import SubscriberMemorySummary, UnifiedSubscriberMemory
    from src.services.stripe_webhooks import _on_checkout_completed

    session = {
        "id": "cs_test_memory_checkout",
        "customer": "cus_memory_checkout",
        "subscription": "sub_memory_checkout",
        "payment_status": "paid",
        "amount_total": 9700,
        "metadata": {
            "tier": "starter",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "zip_codes": "",
            "is_founding": "False",
        },
        "customer_details": {
            "email": "memory_checkout@example.com",
            "name": "Memory Checkout",
            "phone": "+18135550101",
        },
    }

    with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
         patch("src.services.stripe_webhooks.send_welcome_email", create=True), \
         patch("src.services.stripe_webhooks._send_first_leads_email", create=True):
        _on_checkout_completed(session, fresh_db)

    row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "STRIPE",
            UnifiedSubscriberMemory.event_type == "checkout_completed",
            UnifiedSubscriberMemory.event_payload["source_event_id"].astext == "cs_test_memory_checkout",
        )
    ).scalar_one()

    assert row.event_payload["source_event_name"] == "checkout.session.completed"
    assert row.event_payload["status"] == "completed"
    assert row.event_payload["channel"] == "stripe"
    assert row.event_payload["raw"]["stripe_customer_id"] == "cus_memory_checkout"

    subscriber_id = int(row.subscriber_id)
    summary_row = fresh_db.get(SubscriberMemorySummary, subscriber_id)
    assert summary_row is not None
    assert summary_row.last_event_type == "checkout_completed"
    assert summary_row.latest_checkout_state == "completed"


def test_payment_failed_projects_memory_event(fresh_db):
    _ensure_memory_tables(fresh_db)

    from src.core.models import SubscriberMemorySummary, UnifiedSubscriberMemory
    from src.services.stripe_webhooks import _on_payment_failed

    sub = _make_subscriber(fresh_db)
    sub.email = "memory_failed@example.com"
    fresh_db.flush()

    invoice = {
        "id": "in_memory_failed_1",
        "customer": sub.stripe_customer_id,
    }

    with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
         patch("src.services.email.send_email"):
        _on_payment_failed(invoice, fresh_db)

    row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "STRIPE",
            UnifiedSubscriberMemory.event_type == "payment_failed",
            UnifiedSubscriberMemory.event_payload["source_event_id"].astext == "in_memory_failed_1",
        )
    ).scalar_one()

    assert row.event_payload["source_event_name"] == "invoice.payment_failed"
    assert row.event_payload["status"] == "failed"
    assert row.event_payload["channel"] == "stripe"
    assert row.event_payload["raw"]["stripe_customer_id"] == sub.stripe_customer_id

    summary_row = fresh_db.get(SubscriberMemorySummary, sub.id)
    assert summary_row is not None
    assert summary_row.last_event_type == "payment_failed"
    assert summary_row.last_stripe_event_type == "payment_failed"
    assert summary_row.latest_payment_state == "failed"


def test_subscription_canceled_projects_memory_event(fresh_db):
    _ensure_memory_tables(fresh_db)

    from src.core.models import SubscriberMemorySummary, UnifiedSubscriberMemory
    from src.services.stripe_webhooks import _on_subscription_deleted

    sub = _make_subscriber(fresh_db)
    sub.email = "memory_canceled@example.com"
    fresh_db.flush()

    subscription = {
        "id": "sub_memory_canceled_1",
        "customer": sub.stripe_customer_id,
    }

    with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
         patch("src.services.email.send_email"):
        _on_subscription_deleted(subscription, fresh_db)

    row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "STRIPE",
            UnifiedSubscriberMemory.event_type == "subscription_canceled",
            UnifiedSubscriberMemory.event_payload["source_event_id"].astext == "sub_memory_canceled_1",
        )
    ).scalar_one()

    assert row.event_payload["source_event_name"] == "customer.subscription.deleted"
    assert row.event_payload["status"] == "canceled"
    assert row.event_payload["channel"] == "stripe"
    assert row.event_payload["raw"]["stripe_customer_id"] == sub.stripe_customer_id

    summary_row = fresh_db.get(SubscriberMemorySummary, sub.id)
    assert summary_row is not None
    assert summary_row.last_event_type == "subscription_canceled"
    assert summary_row.last_stripe_event_type == "subscription_canceled"
    assert summary_row.latest_payment_state == "canceled"


def test_payment_succeeded_projects_subscription_activated_memory_event(fresh_db):
    _ensure_memory_tables(fresh_db)

    from src.core.models import SubscriberMemorySummary, UnifiedSubscriberMemory
    from src.services.stripe_webhooks import _on_payment_succeeded

    sub = _make_subscriber(fresh_db)
    sub.payment_failed_at = datetime(2026, 6, 25, 12, 0, tzinfo=timezone.utc)
    fresh_db.flush()

    invoice = {
        "id": "in_memory_succeeded_1",
        "customer": sub.stripe_customer_id,
        "billing_reason": "subscription_cycle",
    }

    with patch("src.services.email.send_email"):
        _on_payment_succeeded(invoice, fresh_db)

    row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "STRIPE",
            UnifiedSubscriberMemory.event_type == "subscription_activated",
            UnifiedSubscriberMemory.event_payload["source_event_id"].astext == "in_memory_succeeded_1",
        )
    ).scalar_one()

    assert row.event_payload["source_event_name"] == "invoice.payment_succeeded"
    assert row.event_payload["status"] == "active"
    assert row.event_payload["channel"] == "stripe"
    assert row.event_payload["raw"]["stripe_customer_id"] == sub.stripe_customer_id

    summary_row = fresh_db.get(SubscriberMemorySummary, sub.id)
    assert summary_row is not None
    assert summary_row.last_event_type == "subscription_activated"
    assert summary_row.last_stripe_event_type == "subscription_activated"
    assert summary_row.latest_payment_state == "active"


def test_handle_inbound_stop_projects_sms_opt_out_memory_event(fresh_db):
    _ensure_memory_tables(fresh_db)

    from src.core.models import SmsOptIn, SubscriberMemorySummary, UnifiedSubscriberMemory
    from src.services.sms_compliance import handle_inbound

    sub = _make_subscriber(fresh_db)
    sub.phone = "+18135550102"
    fresh_db.add(
        SmsOptIn(
            phone=sub.phone,
            subscriber_id=sub.id,
            source="widget",
            opt_in_message="test",
            opted_in_at=datetime.now(timezone.utc),
        )
    )
    fresh_db.flush()

    reply = handle_inbound(sub.phone, "STOP", fresh_db)

    assert reply is not None
    row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "SMS",
            UnifiedSubscriberMemory.event_type == "sms_opt_out",
            UnifiedSubscriberMemory.subscriber_id == str(sub.id),
        )
    ).scalar_one()

    assert row.event_payload["status"] == "opted_out"
    assert row.event_payload["channel"] == "sms"
    assert row.event_payload["raw"]["keyword"] == "STOP"

    summary_row = fresh_db.get(SubscriberMemorySummary, sub.id)
    assert summary_row is not None
    assert summary_row.last_event_type == "sms_opt_out"
    assert summary_row.latest_sms_state == "opted_out"
    assert summary_row.sms_opted_out is True


def test_handle_inbound_non_stop_projects_sms_replied_memory_event(fresh_db):
    _ensure_memory_tables(fresh_db)

    from src.core.models import SmsOptIn, SubscriberMemorySummary, UnifiedSubscriberMemory
    from src.services.sms_compliance import handle_inbound

    sub = _make_subscriber(fresh_db)
    sub.phone = "+18135550103"
    fresh_db.add(
        SmsOptIn(
            phone=sub.phone,
            subscriber_id=sub.id,
            source="widget",
            opt_in_message="test",
            opted_in_at=datetime.now(timezone.utc),
        )
    )
    fresh_db.flush()

    reply = handle_inbound(sub.phone, "Tell me more", fresh_db)

    assert reply is None
    row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "SMS",
            UnifiedSubscriberMemory.event_type == "sms_replied",
            UnifiedSubscriberMemory.subscriber_id == str(sub.id),
        )
    ).scalar_one()

    assert row.event_payload["status"] == "replied"
    assert row.event_payload["channel"] == "sms"
    assert row.event_payload["raw"]["body"] == "Tell me more"

    summary_row = fresh_db.get(SubscriberMemorySummary, sub.id)
    assert summary_row is not None
    assert summary_row.last_event_type == "sms_replied"
    assert summary_row.latest_sms_state == "replied"
    assert summary_row.last_sms_reply_at is not None


def test_push_subscriber_to_ghl_projects_crm_events(fresh_db):
    _ensure_memory_tables(fresh_db)
    sub = _make_subscriber(fresh_db)
    sub.name = "GHL Memory"
    fresh_db.flush()

    from src.core.models import SubscriberMemorySummary, UnifiedSubscriberMemory
    from src.services.ghl_webhook import push_subscriber_to_ghl

    contact_resp = MagicMock()
    contact_resp.status_code = 200
    contact_resp.ok = True
    contact_resp.json.return_value = {"contact": {"id": "ghl_contact_1"}}

    opp_resp = MagicMock()
    opp_resp.status_code = 200
    opp_resp.ok = True
    opp_resp.json.return_value = {}
    opp_resp.raise_for_status.return_value = None

    with patch("src.services.ghl_webhook._is_configured", return_value=True), \
         patch("src.services.ghl_webhook._ghl_request", side_effect=[contact_resp, opp_resp]), \
         patch("src.services.ghl_webhook._find_opportunity_for_contact", return_value=None):
        assert push_subscriber_to_ghl(sub, stage=5, tags=["new_paid"], db=fresh_db) is True

    created_row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "GHL",
            UnifiedSubscriberMemory.event_type == "crm_contact_created",
            UnifiedSubscriberMemory.subscriber_id == str(sub.id),
        )
    ).scalar_one()
    assert created_row.event_payload["status"] == "created"

    stage_row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "GHL",
            UnifiedSubscriberMemory.event_type == "crm_stage_changed",
            UnifiedSubscriberMemory.subscriber_id == str(sub.id),
        )
    ).scalar_one()
    assert stage_row.event_payload["status"] == "5"
    assert stage_row.event_payload["external_contact_id"] == "ghl_contact_1"

    summary_row = fresh_db.get(SubscriberMemorySummary, sub.id)
    assert summary_row is not None
    assert summary_row.latest_crm_stage == "5"


def test_onboard_inbound_caller_projects_voice_signup_captured(fresh_db):
    _ensure_memory_tables(fresh_db)

    from src.core.models import SubscriberMemorySummary, UnifiedSubscriberMemory
    from src.services.signup_engine import onboard_inbound_caller

    fake_first_leads = MagicMock(sent=True, lead_count=3)

    with patch("src.services.signup_engine.can_send", return_value=False), \
         patch("src.services.first_leads.deliver_first_leads", return_value=fake_first_leads):
        result = onboard_inbound_caller(
            phone="+18135550104",
            source="synthflow_inbound",
            db=fresh_db,
            zip_code="33601",
            vertical="roofing",
            call_id="call_memory_1",
            name="Voice Signup",
        )

    sub_id = result["subscriber_id"]
    row = fresh_db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == "SYNTHFLOW",
            UnifiedSubscriberMemory.event_type == "voice_signup_captured",
            UnifiedSubscriberMemory.subscriber_id == str(sub_id),
        )
    ).scalar_one()

    assert row.event_payload["status"] == "captured"
    assert row.event_payload["call_id"] == "call_memory_1"
    assert row.event_payload["raw"]["zip_code"] == "33601"
    assert row.event_payload["raw"]["vertical"] == "roofing"

    summary_row = fresh_db.get(SubscriberMemorySummary, sub_id)
    assert summary_row is not None
    assert summary_row.last_voice_event_type == "voice_signup_captured"


# ──────────────────────────────────────────────────────────────────────────────
# Lifecycle read path — get_subscriber_memory
# ──────────────────────────────────────────────────────────────────────────────

def _append(db, sub_id, *, source, etype, eid, occurred_at, status, summary):
    from src.services.subscriber_memory import append_memory_event

    return append_memory_event(
        db,
        subscriber_id=sub_id,
        stream_source=source,
        event_type=etype,
        source_event_id=eid,
        source_event_name=f"{source.lower()}.{etype}",
        occurred_at=occurred_at,
        status=status,
        summary=summary,
        channel=source.lower(),
        actor={"type": "system", "id": source.lower()},
    )


def test_get_subscriber_memory_returns_timeline_newest_first_and_summary(fresh_db):
    _ensure_memory_tables(fresh_db)
    sub = _make_subscriber(fresh_db)

    from src.services.subscriber_memory import get_subscriber_memory

    t1 = datetime(2026, 6, 20, 10, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 6, 25, 10, 0, tzinfo=timezone.utc)
    _append(fresh_db, sub.id, source="STRIPE", etype="checkout_completed",
            eid="e1", occurred_at=t1, status="completed", summary="checkout done")
    _append(fresh_db, sub.id, source="SMS", etype="sms_replied",
            eid="e2", occurred_at=t2, status="replied", summary="they replied")

    mem = get_subscriber_memory(fresh_db, sub.id)

    timeline = mem["timeline"]
    assert len(timeline) == 2
    # newest first (insertion order → created_at)
    assert timeline[0]["event_type"] == "sms_replied"
    assert timeline[0]["stream_source"] == "SMS"
    assert timeline[0]["event_payload"]["summary"] == "they replied"
    assert timeline[1]["event_type"] == "checkout_completed"

    summary = mem["summary"]
    assert summary["last_event_type"] == "sms_replied"
    assert summary["latest_checkout_state"] == "completed"
    assert summary["latest_sms_state"] == "replied"


def test_get_subscriber_memory_empty_when_no_events(fresh_db):
    _ensure_memory_tables(fresh_db)
    sub = _make_subscriber(fresh_db)

    from src.services.subscriber_memory import get_subscriber_memory

    mem = get_subscriber_memory(fresh_db, sub.id)
    assert mem["timeline"] == []
    assert mem["summary"] == {}


def test_get_subscriber_memory_respects_limit(fresh_db):
    _ensure_memory_tables(fresh_db)
    sub = _make_subscriber(fresh_db)

    from src.services.subscriber_memory import get_subscriber_memory

    base = datetime(2026, 6, 25, 10, 0, tzinfo=timezone.utc)
    for i in range(3):
        _append(fresh_db, sub.id, source="STRIPE", etype=f"evt_{i}",
                eid=f"e{i}", occurred_at=base + timedelta(minutes=i),
                status="ok", summary=f"s{i}")

    mem = get_subscriber_memory(fresh_db, sub.id, limit=2)
    assert len(mem["timeline"]) == 2
    # newest two (evt_2, evt_1)
    assert mem["timeline"][0]["event_type"] == "evt_2"
    assert mem["timeline"][1]["event_type"] == "evt_1"
