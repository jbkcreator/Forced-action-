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


def find_candidates(db, limit: int = DAILY_CAP) -> list[dict]:
    """
    Union free-signup + waitlist emails not yet in the nurture table, aged
    between MIN_AGE_HOURS and CAPTURED_WINDOW_DAYS, newest-first, capped at limit.
    """
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=CAPTURED_WINDOW_DAYS)
    window_end = now - timedelta(hours=MIN_AGE_HOURS)

    existing_emails = {
        row[0]
        for row in db.execute(select(NonBuyerNurtureSequence.email)).all()
    }

    candidates: list[dict] = []

    free_subs = db.execute(
        select(Subscriber.id, Subscriber.email, Subscriber.created_at).where(
            Subscriber.tier == "free",
            Subscriber.email.isnot(None),
            Subscriber.created_at >= window_start,
            Subscriber.created_at <= window_end,
        )
    ).all()
    for sub_id, email, created_at in free_subs:
        if email in existing_emails:
            continue
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
    seen_waitlist_emails = set()
    for email, created_at in waitlist_rows:
        if email in existing_emails or email in seen_waitlist_emails:
            continue
        seen_waitlist_emails.add(email)
        candidates.append({
            "email": email,
            "subscriber_id": None,
            "source": "waitlist",
            "captured_at": created_at,
        })

    candidates.sort(key=lambda c: c["captured_at"], reverse=True)
    return candidates[:limit]
