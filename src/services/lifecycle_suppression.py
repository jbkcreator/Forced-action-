"""
Subscriber-level suppression for Lifecycle-led outbound messages.

This module intentionally does not handle TCPA STOP compliance. STOP remains in
sms_compliance; these helpers pause Lifecycle follow-up after human engagement or
deal outcomes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import LifecycleSuppression, MessageOutcome, SmsOptIn

logger = logging.getLogger(__name__)

PENDING_LIFECYCLE_STATUSES = ("pending_review", "approved")


def has_active_suppression(db: Session, subscriber_id: int) -> bool:
    """Return True when Lifecycle outbound touches are paused for this subscriber."""
    if subscriber_id is None:
        return False
    row = db.execute(
        select(LifecycleSuppression.id).where(
            LifecycleSuppression.subscriber_id == subscriber_id,
            LifecycleSuppression.is_active.is_(True),
        ).limit(1)
    ).first()
    return row is not None


def cancel_pending_messages(
    db: Session,
    subscriber_id: int,
    *,
    reason: str,
    now: Optional[datetime] = None,
) -> int:
    """Cancel queued Lifecycle MessageOutcome rows for a subscriber."""
    now = now or datetime.now(timezone.utc)
    rows = db.execute(
        select(MessageOutcome).where(
            MessageOutcome.subscriber_id == subscriber_id,
            MessageOutcome.send_status.in_(PENDING_LIFECYCLE_STATUSES),
            MessageOutcome.cancelled_at.is_(None),
        )
    ).scalars().all()

    for outcome in rows:
        outcome.send_status = "cancelled"
        outcome.cancelled_at = now
        outcome.cancel_reason = reason

    if rows:
        logger.info(
            "Lifecycle suppression cancelled %d pending messages for subscriber=%s reason=%s",
            len(rows),
            subscriber_id,
            reason,
        )
    return len(rows)


def create_suppression(
    db: Session,
    *,
    subscriber_id: int,
    reason: str,
    source: str,
    source_id: Optional[object] = None,
    notes: str,
    created_by: Optional[str] = None,
    cancel_reason: str,
    now: Optional[datetime] = None,
) -> LifecycleSuppression:
    """Create an active suppression row and cancel queued Lifecycle messages."""
    now = now or datetime.now(timezone.utc)
    suppression = LifecycleSuppression(
        subscriber_id=subscriber_id,
        reason=reason,
        source=source,
        source_id=str(source_id) if source_id is not None else None,
        paused_at=now,
        is_active=True,
        created_by=created_by,
        notes=notes,
    )
    db.add(suppression)
    cancel_pending_messages(db, subscriber_id, reason=cancel_reason, now=now)
    db.flush()
    logger.info(
        "Lifecycle suppression created subscriber=%s reason=%s source=%s source_id=%s",
        subscriber_id,
        reason,
        source,
        source_id,
    )
    return suppression


def record_generic_sms_reply(
    db: Session,
    *,
    phone: str,
    source_id: Optional[str],
    now: Optional[datetime] = None,
) -> Optional[LifecycleSuppression]:
    """
    Mark the latest Lifecycle SMS as replied and pause future Lifecycle touches.

    Returns None when the phone cannot be tied to a subscriber.
    """
    now = now or datetime.now(timezone.utc)
    opt_in = db.execute(
        select(SmsOptIn).where(SmsOptIn.phone == phone).order_by(SmsOptIn.opted_in_at.desc()).limit(1)
    ).scalar_one_or_none()
    if opt_in is None or opt_in.subscriber_id is None:
        logger.info("Generic SMS reply had no subscriber mapping phone=%s source_id=%s", phone, source_id)
        return None

    outcome = db.execute(
        select(MessageOutcome).where(
            MessageOutcome.subscriber_id == opt_in.subscriber_id,
            MessageOutcome.message_type == "sms",
            MessageOutcome.replied_at.is_(None),
        ).order_by(MessageOutcome.sent_at.desc().nullslast(), MessageOutcome.id.desc()).limit(1)
    ).scalar_one_or_none()
    if outcome is not None:
        outcome.replied_at = now

    return create_suppression(
        db,
        subscriber_id=opt_in.subscriber_id,
        reason="human_replied",
        source="inbound_sms",
        source_id=source_id,
        notes="Auto-pause triggered by human reply",
        cancel_reason="human_replied_auto_pause",
        now=now,
    )
