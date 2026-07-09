"""
Non-buyer nurture sequence — core service.

Owns eligibility, enrollment, conversion suppression, and Instantly status
sync for the multi-touch email nurture sequence covering email-captured
non-purchasers (free-signup subscribers, abandoned-checkout leads, waitlist
entrants). Email-keyed; sweep/sync/webhook callers stay thin.

See NON_BUYER_NURTURE_PRD.md and FREE_TO_PAID_UPGRADE_SEQUENCE_PLAN.md (v3).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select

from src.core.models import NonBuyerNurtureSequence, Subscriber, WaitlistEntry

logger = logging.getLogger(__name__)

CAPTURED_WINDOW_DAYS = 90
MIN_AGE_HOURS = 24
# ponytail: fixed daily cap for now, ramp manually with Instantly domain warm-up.
DAILY_CAP = 25


def record_checkout_abandon_candidate(db, email: str, subscriber_id: Optional[int] = None) -> None:
    """
    Idempotently record an abandoned-checkout email as an eligible nurture
    candidate. checkout_abandon leads have no other source table, so the
    capture point (webhook) writes the row directly.
    """
    existing = db.execute(
        select(NonBuyerNurtureSequence).where(NonBuyerNurtureSequence.email == email)
    ).scalar_one_or_none()
    if existing:
        return
    db.add(NonBuyerNurtureSequence(
        email=email,
        subscriber_id=subscriber_id,
        source="checkout_abandon",
        captured_at=datetime.now(timezone.utc),
        status="eligible",
    ))


def find_candidates(db, limit: int = DAILY_CAP) -> list[dict]:
    """
    Union free-signup + waitlist (live source tables) + already-recorded
    checkout_abandon/retry rows (status='eligible' in the nurture table
    itself), aged between MIN_AGE_HOURS and CAPTURED_WINDOW_DAYS, newest-first,
    deduped by email, capped at limit.

    Emails that have moved past 'eligible' (enrolled/converted/unsubscribed/
    bounced/removed) are permanently excluded — the once-per-email rule.
    """
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=CAPTURED_WINDOW_DAYS)
    window_end = now - timedelta(hours=MIN_AGE_HOURS)

    terminal_emails = {
        row[0]
        for row in db.execute(
            select(NonBuyerNurtureSequence.email).where(
                NonBuyerNurtureSequence.status != "eligible"
            )
        ).all()
    }

    candidates: list[dict] = []
    seen_emails: set[str] = set()

    eligible_rows = db.execute(
        select(
            NonBuyerNurtureSequence.email,
            NonBuyerNurtureSequence.subscriber_id,
            NonBuyerNurtureSequence.source,
            NonBuyerNurtureSequence.captured_at,
        ).where(
            NonBuyerNurtureSequence.status == "eligible",
            NonBuyerNurtureSequence.captured_at >= window_start,
            NonBuyerNurtureSequence.captured_at <= window_end,
        )
    ).all()
    for email, sub_id, source, captured_at in eligible_rows:
        seen_emails.add(email)
        candidates.append({
            "email": email,
            "subscriber_id": sub_id,
            "source": source,
            "captured_at": captured_at,
        })

    free_subs = db.execute(
        select(Subscriber.id, Subscriber.email, Subscriber.created_at).where(
            Subscriber.tier == "free",
            Subscriber.email.isnot(None),
            Subscriber.created_at >= window_start,
            Subscriber.created_at <= window_end,
        )
    ).all()
    for sub_id, email, created_at in free_subs:
        if email in terminal_emails or email in seen_emails:
            continue
        seen_emails.add(email)
        candidates.append({
            "email": email,
            "subscriber_id": sub_id,
            "source": "free_signup",
            "captured_at": created_at,
        })

    waitlist_rows = db.execute(
        select(WaitlistEntry.email, WaitlistEntry.created_at).where(
            WaitlistEntry.created_at >= window_start,
            WaitlistEntry.created_at <= window_end,
        )
    ).all()
    for email, created_at in waitlist_rows:
        if email in terminal_emails or email in seen_emails:
            continue
        seen_emails.add(email)
        candidates.append({
            "email": email,
            "subscriber_id": None,
            "source": "waitlist",
            "captured_at": created_at,
        })

    candidates.sort(key=lambda c: _as_utc(c["captured_at"]), reverse=True)
    return candidates[:limit]


def _as_utc(dt: datetime) -> datetime:
    """Subscriber.created_at is a naive TIMESTAMP column; treat naive as UTC."""
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
