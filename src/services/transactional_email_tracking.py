from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select, text

from src.core.models import MessageOutcome
from src.services.email_suppression import suppress_contact
from src.services.email import send_alert

logger = logging.getLogger(__name__)

# E1: a hard bounce suppresses immediately, but a soft bounce is usually
# transient (full mailbox, temporary defer) — only suppress once this many
# land inside the trailing 7-day window.
SOFT_BOUNCE_SUPPRESS_THRESHOLD = 3


def _email_tracking_columns_ready(db) -> bool:
    required = {"recipient_email", "provider_message_id", "failure_reason"}
    rows = db.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'message_outcomes'
              AND column_name IN ('recipient_email', 'provider_message_id', 'failure_reason')
            """
        )
    ).fetchall()
    return {row[0] for row in rows} == required


def log_transactional_email_send(
    db,
    *,
    recipient_email: str,
    subscriber_id: Optional[int],
    template_id: str,
    context_snapshot: Optional[dict] = None,
) -> None:
    if not _email_tracking_columns_ready(db):
        logger.warning("message_outcomes email-tracking columns missing - skipping transactional email log")
        return
    outcome = MessageOutcome(
        subscriber_id=subscriber_id,
        message_type="email",
        template_id=template_id,
        channel="mandrill",
        recipient_email=recipient_email.strip().lower(),
        sent_at=datetime.now(timezone.utc),
        send_status="sent",
        context_snapshot=context_snapshot,
    )
    db.add(outcome)
    db.flush()


def _latest_outcome_for_email(db, email: str) -> Optional[MessageOutcome]:
    if not _email_tracking_columns_ready(db):
        return None
    return db.execute(
        select(MessageOutcome)
        .where(
            MessageOutcome.message_type == "email",
            MessageOutcome.recipient_email == email.strip().lower(),
            MessageOutcome.channel == "mandrill",
        )
        .order_by(MessageOutcome.sent_at.desc(), MessageOutcome.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def _outcome_by_provider_id(db, provider_message_id: str) -> Optional[MessageOutcome]:
    if not _email_tracking_columns_ready(db):
        return None
    return db.execute(
        select(MessageOutcome)
        .where(
            MessageOutcome.message_type == "email",
            MessageOutcome.provider_message_id == provider_message_id,
        )
        .order_by(MessageOutcome.sent_at.desc(), MessageOutcome.id.desc())
        .limit(1)
    ).scalar_one_or_none()


# Events for which a last-resort "latest email for this recipient" match is
# acceptable — engagement only. Failure events (bounces, complaints) must NEVER
# fall back to latest: an out-of-order soft-bounce callback would then overwrite
# whichever row was logged most recently, so several bounces collapse onto one
# row and the 3-bounce suppression threshold is never reached.
_LATEST_FALLBACK_EVENTS = {"open", "click"}


def record_mandrill_event(db, event: dict) -> None:
    event_type = event.get("event") or ""
    msg = event.get("msg") or {}
    email = (msg.get("email") or "").strip().lower()
    provider_message_id = msg.get("_id")
    if not email:
        return
    columns_ready = _email_tracking_columns_ready(db)

    outcome = None
    metadata = msg.get("metadata") or {}
    outcome_id = metadata.get("message_outcome_id")
    # 1. Exact match by the metadata id we stamped at send time.
    if columns_ready and outcome_id:
        try:
            outcome = db.get(MessageOutcome, int(outcome_id))
        except Exception:
            outcome = None
    # 2. Fallback to the provider's own message id (stored by an earlier event
    #    for this same send) — handles events that arrive without metadata.
    if columns_ready and outcome is None and provider_message_id:
        outcome = _outcome_by_provider_id(db, provider_message_id)
    # 3. Engagement-only last resort. Never for failure metrics (see above).
    if columns_ready and outcome is None and event_type in _LATEST_FALLBACK_EVENTS:
        outcome = _latest_outcome_for_email(db, email)
    if outcome is not None:
        outcome.provider_message_id = provider_message_id or outcome.provider_message_id

    if event_type == "open" and outcome is not None and outcome.opened_at is None:
        outcome.opened_at = datetime.now(timezone.utc)
    elif event_type == "click" and outcome is not None and outcome.clicked_at is None:
        outcome.clicked_at = datetime.now(timezone.utc)
    elif event_type in {"hard_bounce", "soft_bounce", "reject"}:
        if outcome is not None:
            outcome.failure_reason = event_type
        if event_type in {"hard_bounce", "reject"}:
            suppress_contact(db, email=email, source=f"mandrill_{event_type}")
        elif columns_ready:
            # Count soft bounces in the trailing 7 days, INCLUDING this one.
            # `outcome.failure_reason` was just set above, and the query below
            # autoflushes that pending write, so the current row is already
            # counted — excluding it here and adding 1 keeps the total correct
            # regardless of whether autoflush is on (a previous version double
            # counted it and suppressed at 2 bounces instead of 3).
            cutoff = datetime.now(timezone.utc) - timedelta(days=7)
            current_id = getattr(outcome, "id", None)
            prior_filters = [
                MessageOutcome.message_type == "email",
                MessageOutcome.recipient_email == email,
                MessageOutcome.failure_reason == "soft_bounce",
                MessageOutcome.sent_at >= cutoff,
            ]
            if current_id is not None:
                prior_filters.append(MessageOutcome.id != current_id)
            prior_soft_bounces = db.execute(
                select(func.count()).select_from(MessageOutcome).where(*prior_filters)
            ).scalar_one()
            if prior_soft_bounces + 1 >= SOFT_BOUNCE_SUPPRESS_THRESHOLD:
                suppress_contact(db, email=email, source="mandrill_soft_bounce_threshold")
    elif event_type == "spam":
        if outcome is not None:
            outcome.failure_reason = "spam"
        suppress_contact(db, email=email, source="mandrill_spam_complaint")
        send_alert(
            subject="[FA] Paying subscriber email complaint",
            body=f"Mandrill spam complaint for {email}. Follow up manually.",
        )
    elif event_type == "unsub":
        if outcome is not None:
            outcome.failure_reason = "unsub"
        suppress_contact(db, email=email, source="mandrill_unsub")

    db.flush()
