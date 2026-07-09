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
from src.services import instantly_service as instantly

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


def enroll(db, candidates: list[dict], campaign_id: str) -> dict:
    """
    Batch-add candidates to the shared nurture campaign in one Instantly call.
    On success, all candidates are written as 'enrolled'. On failure, they're
    inserted/left as 'eligible' so the next sweep retries — never fake enrollment.
    """
    if not candidates:
        return {"enrolled": 0, "retried": 0}

    now = datetime.now(timezone.utc)
    leads = [{"email": c["email"]} for c in candidates]
    result = instantly.add_leads(campaign_id, leads)
    succeeded = result is not None

    existing_rows = {
        row.email: row
        for row in db.execute(
            select(NonBuyerNurtureSequence).where(
                NonBuyerNurtureSequence.email.in_([c["email"] for c in candidates])
            )
        ).scalars().all()
    }

    for c in candidates:
        row = existing_rows.get(c["email"])
        if row is None:
            row = NonBuyerNurtureSequence(
                email=c["email"],
                subscriber_id=c["subscriber_id"],
                source=c["source"],
                captured_at=c["captured_at"],
                status="eligible",
            )
            db.add(row)

        if succeeded:
            row.status = "enrolled"
            row.instantly_campaign_id = campaign_id
            row.eligible_at = row.eligible_at or now
            row.enrolled_at = now

    return {"enrolled": len(candidates) if succeeded else 0, "retried": 0 if succeeded else len(candidates)}


def mark_converted(db, email: str) -> None:
    """
    First paid conversion, matched by email. Removes the Instantly lead if
    known, marks the row 'converted' (terminal — DB suppression wins over
    remote cleanup). No-op if no row exists (never enrolled) or already
    converted (idempotent on webhook replay).
    """
    row = db.execute(
        select(NonBuyerNurtureSequence).where(NonBuyerNurtureSequence.email == email)
    ).scalar_one_or_none()
    if row is None or row.status == "converted":
        return

    if row.instantly_lead_id:
        instantly.remove_lead(row.instantly_lead_id)

    now = datetime.now(timezone.utc)
    row.status = "converted"
    row.removal_reason = "paid_conversion"
    row.converted_at = now
    row.removed_at = now
