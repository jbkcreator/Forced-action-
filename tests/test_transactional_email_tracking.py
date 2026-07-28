from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text

from src.core.models import EmailOptOut, MessageOutcome, SmsOptOut, Subscriber
from src.services.subscriber_auth import send_magic_link_email
from src.services.transactional_email_tracking import record_mandrill_event


def _make_subscriber(db, *, email: str, phone: str = "+18135550199") -> Subscriber:
    sub = Subscriber(
        stripe_customer_id=f"cus_txn_{email.split('@')[0]}",
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        status="active",
        email=email,
        phone=phone,
        event_feed_uuid=f"feed-{email.split('@')[0]}",
    )
    db.add(sub)
    db.flush()
    return sub


def _ensure_message_outcome_email_tracking_columns(db) -> None:
    db.execute(text("ALTER TABLE message_outcomes ADD COLUMN IF NOT EXISTS recipient_email VARCHAR(255)"))
    db.execute(text("ALTER TABLE message_outcomes ADD COLUMN IF NOT EXISTS provider_message_id VARCHAR(100)"))
    db.execute(text("ALTER TABLE message_outcomes ADD COLUMN IF NOT EXISTS failure_reason VARCHAR(255)"))
    db.flush()


def test_send_magic_link_email_logs_message_outcome(fresh_db, monkeypatch):
    _ensure_message_outcome_email_tracking_columns(fresh_db)
    sub = _make_subscriber(fresh_db, email="magic-track@example.com")
    monkeypatch.setattr("src.services.subscriber_auth.send_email", lambda **kwargs: True)

    send_magic_link_email(
        sub.email,
        sub.name,
        "raw-token",
        db=fresh_db,
        subscriber_id=sub.id,
    )

    outcome = fresh_db.execute(
        select(MessageOutcome).where(MessageOutcome.recipient_email == sub.email)
    ).scalar_one()
    assert outcome.subscriber_id == sub.id
    assert outcome.template_id == "magic_link_email"
    assert outcome.send_status == "sent"


def test_hard_bounce_suppresses_contact(fresh_db):
    _ensure_message_outcome_email_tracking_columns(fresh_db)
    sub = _make_subscriber(fresh_db, email="bounce-hard@example.com", phone="+18135550200")
    fresh_db.add(
        MessageOutcome(
            subscriber_id=sub.id,
            message_type="email",
            template_id="welcome_email",
            channel="mandrill",
            recipient_email=sub.email,
            sent_at=datetime.now(timezone.utc),
            send_status="sent",
        )
    )
    fresh_db.flush()

    record_mandrill_event(
        fresh_db,
        {
            "event": "hard_bounce",
            "msg": {"email": sub.email, "_id": "mdr_hard_1"},
        },
    )

    email_opt_out = fresh_db.execute(
        select(EmailOptOut).where(EmailOptOut.email == sub.email)
    ).scalar_one()
    sms_opt_out = fresh_db.execute(
        select(SmsOptOut).where(SmsOptOut.phone == sub.phone)
    ).scalar_one()
    assert email_opt_out.source == "mandrill_hard_bounce"
    assert sms_opt_out.source == "mandrill_hard_bounce"


def test_third_soft_bounce_within_seven_days_suppresses_contact(fresh_db):
    _ensure_message_outcome_email_tracking_columns(fresh_db)
    sub = _make_subscriber(fresh_db, email="bounce-soft@example.com", phone="+18135550201")
    now = datetime.now(timezone.utc)
    for offset_days in (1, 3):
        fresh_db.add(
            MessageOutcome(
                subscriber_id=sub.id,
                message_type="email",
                template_id="welcome_email",
                channel="mandrill",
                recipient_email=sub.email,
                sent_at=now - timedelta(days=offset_days),
                send_status="sent",
                failure_reason="soft_bounce",
            )
        )
    fresh_db.add(
        MessageOutcome(
            subscriber_id=sub.id,
            message_type="email",
            template_id="welcome_email",
            channel="mandrill",
            recipient_email=sub.email,
            sent_at=now,
            send_status="sent",
        )
    )
    fresh_db.flush()

    record_mandrill_event(
        fresh_db,
        {
            "event": "soft_bounce",
            "msg": {"email": sub.email, "_id": "mdr_soft_3"},
        },
    )

    email_opt_out = fresh_db.execute(
        select(EmailOptOut).where(EmailOptOut.email == sub.email)
    ).scalar_one()
    assert email_opt_out.source == "mandrill_soft_bounce_threshold"
