from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text

from src.core.models import EmailOptOut, MessageOutcome, SmsOptOut, Subscriber
from src.services.subscriber_auth import (
    send_magic_link_email,
    send_subscriber_password_reset_email,
)
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


def _capture_send_email(monkeypatch, target: str):
    """Patch send_email in `target` module to capture kwargs and return True."""
    captured = {}

    def _fake(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(f"{target}.send_email", _fake)
    return captured


def test_send_magic_link_email_passes_tracking_to_send_email(fresh_db, monkeypatch):
    _ensure_message_outcome_email_tracking_columns(fresh_db)
    sub = _make_subscriber(fresh_db, email="magic-track@example.com")
    captured = _capture_send_email(monkeypatch, "src.services.subscriber_auth")

    result = send_magic_link_email(
        sub.email, sub.name, "raw-token", db=fresh_db, subscriber_id=sub.id,
    )

    assert result is True
    assert captured["db"] is fresh_db
    assert captured["tracking"]["template_id"] == "magic_link_email"
    assert captured["tracking"]["subscriber_id"] == sub.id


def test_send_password_reset_email_passes_tracking_to_send_email(fresh_db, monkeypatch):
    _ensure_message_outcome_email_tracking_columns(fresh_db)
    sub = _make_subscriber(fresh_db, email="reset-track@example.com", phone="+18135550220")
    captured = _capture_send_email(monkeypatch, "src.services.subscriber_auth")

    result = send_subscriber_password_reset_email(
        sub.email, sub.name, "raw-token", db=fresh_db, subscriber_id=sub.id,
    )

    assert result is True
    assert captured["db"] is fresh_db
    assert captured["tracking"]["template_id"] == "password_reset_email"
    assert captured["tracking"]["subscriber_id"] == sub.id


def test_send_email_emits_metadata_header_with_precreated_outcome(fresh_db, monkeypatch):
    """The correlation path: send_email creates the outcome pre-send and stamps
    its id onto the message as X-MC-Metadata."""
    from config.settings import get_settings
    import src.services.email as email_mod

    _ensure_message_outcome_email_tracking_columns(fresh_db)
    sub = _make_subscriber(fresh_db, email="meta-header@example.com")

    s = get_settings()
    monkeypatch.setattr(s, "smtp_host", "smtp.test", raising=False)
    monkeypatch.setattr(s, "smtp_user", "u", raising=False)

    class _Secret:
        def get_secret_value(self):
            return "pw"

    monkeypatch.setattr(s, "smtp_pass", _Secret(), raising=False)
    monkeypatch.setattr(email_mod, "get_settings", lambda: s)
    monkeypatch.setattr("src.services.email_suppression.is_email_suppressed", lambda db, to: False)

    sent_payloads = {}

    class _FakeSMTP:
        def __init__(self, *a, **k): ...
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def starttls(self): ...
        def login(self, *a): ...
        def sendmail(self, frm, to, body): sent_payloads["body"] = body

    monkeypatch.setattr(email_mod.smtplib, "SMTP", _FakeSMTP)

    ok = email_mod.send_email(
        to=sub.email, subject="s", body_text="b",
        tracking={"subscriber_id": sub.id, "template_id": "welcome_email", "channel": "mandrill"},
        db=fresh_db,
    )
    assert ok is True

    outcome = fresh_db.execute(
        select(MessageOutcome).where(MessageOutcome.recipient_email == sub.email)
    ).scalar_one()
    assert outcome.send_status == "sent"
    assert "X-MC-Metadata" in sent_payloads["body"]
    assert str(outcome.id) in sent_payloads["body"]


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


def _add_outcome(db, sub, *, provider_id=None, failure=None, sent_at=None):
    from datetime import datetime as _dt, timezone as _tz
    o = MessageOutcome(
        subscriber_id=sub.id,
        message_type="email",
        template_id="welcome_email",
        channel="mandrill",
        recipient_email=sub.email,
        provider_message_id=provider_id,
        failure_reason=failure,
        sent_at=sent_at or _dt.now(_tz.utc),
        send_status="sent",
    )
    db.add(o)
    db.flush()
    return o


def test_metadata_targets_exact_send_not_latest(fresh_db):
    """Two sends to one recipient; a soft-bounce with metadata must mark ITS
    own row, never the most recently logged one (the old latest-email bug)."""
    _ensure_message_outcome_email_tracking_columns(fresh_db)
    sub = _make_subscriber(fresh_db, email="attr-a@example.com", phone="+18135550210")
    first = _add_outcome(fresh_db, sub, sent_at=datetime.now(timezone.utc) - timedelta(hours=2))
    latest = _add_outcome(fresh_db, sub, sent_at=datetime.now(timezone.utc))

    # Out-of-order: the FIRST send bounces after the second was already logged.
    record_mandrill_event(
        fresh_db,
        {"event": "soft_bounce",
         "msg": {"email": sub.email, "_id": "mdr_a1",
                 "metadata": {"message_outcome_id": first.id}}},
    )

    fresh_db.refresh(first)
    fresh_db.refresh(latest)
    assert first.failure_reason == "soft_bounce"
    assert latest.failure_reason is None  # NOT overwritten


def test_provider_message_id_fallback_resolves_later_event(fresh_db):
    """A second event without metadata resolves by the provider _id stored on
    the first (metadata-matched) event."""
    _ensure_message_outcome_email_tracking_columns(fresh_db)
    sub = _make_subscriber(fresh_db, email="attr-b@example.com", phone="+18135550211")
    outcome = _add_outcome(fresh_db, sub)

    # 1st event carries metadata → stamps provider_message_id on the row.
    record_mandrill_event(
        fresh_db,
        {"event": "open",
         "msg": {"email": sub.email, "_id": "mdr_b1",
                 "metadata": {"message_outcome_id": outcome.id}}},
    )
    # 2nd event has NO metadata but the same provider _id → resolves by it.
    record_mandrill_event(
        fresh_db,
        {"event": "click", "msg": {"email": sub.email, "_id": "mdr_b1"}},
    )

    fresh_db.refresh(outcome)
    assert outcome.opened_at is not None
    assert outcome.clicked_at is not None


def test_unattributable_soft_bounce_does_not_touch_latest(fresh_db):
    """A soft-bounce with no metadata and no matching provider id must NOT fall
    back to the latest email row for this recipient."""
    _ensure_message_outcome_email_tracking_columns(fresh_db)
    sub = _make_subscriber(fresh_db, email="attr-c@example.com", phone="+18135550212")
    latest = _add_outcome(fresh_db, sub)

    record_mandrill_event(
        fresh_db,
        {"event": "soft_bounce", "msg": {"email": sub.email, "_id": "mdr_unknown"}},
    )

    fresh_db.refresh(latest)
    assert latest.failure_reason is None
    assert fresh_db.execute(
        select(EmailOptOut).where(EmailOptOut.email == sub.email)
    ).scalar_one_or_none() is None
