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

from sqlalchemy import select, text

from src.core.models import NonBuyerNurtureSequence
from src.services import instantly_service as instantly

logger = logging.getLogger(__name__)

CAPTURED_WINDOW_DAYS = 90
MIN_AGE_HOURS = 24


def _daily_cap() -> int:
    """Enrollment ceiling per sweep. Ramps with Instantly domain warm-up via config."""
    from config.settings import get_settings
    return get_settings().non_buyer_nurture_daily_cap


# Single-query candidate selection: unions the three sources, dedupes by email
# (newest capture wins), and excludes any email that is already past 'eligible'
# in the nurture table (once-per-email) OR belongs to a paying subscriber
# (no "please buy" mail to someone who already bought — plan §3.2, User Story 5).
# subscribers.created_at is a naive TIMESTAMP; cast to UTC timestamptz so the
# UNION type-matches the timestamptz columns.
_CANDIDATES_SQL = text("""
WITH raw AS (
    SELECT s.email                              AS email,
           s.id                                 AS subscriber_id,
           'free_signup'                        AS source,
           (s.created_at AT TIME ZONE 'UTC')    AS captured_at
    FROM subscribers s
    WHERE s.tier = 'free'
      AND s.email IS NOT NULL
      AND (s.created_at AT TIME ZONE 'UTC') BETWEEN :window_start AND :window_end
    UNION ALL
    SELECT w.email, NULL::int, 'waitlist', w.created_at
    FROM waitlist_entries w
    WHERE w.created_at BETWEEN :window_start AND :window_end
    UNION ALL
    SELECT n.email, n.subscriber_id, n.source, n.captured_at
    FROM non_buyer_nurture_sequences n
    WHERE n.status = 'eligible'
      AND n.captured_at BETWEEN :window_start AND :window_end
),
deduped AS (
    SELECT DISTINCT ON (lower(email)) email, subscriber_id, source, captured_at
    FROM raw
    ORDER BY lower(email), captured_at DESC
)
SELECT d.email, d.subscriber_id, d.source, d.captured_at
FROM deduped d
WHERE NOT EXISTS (
        SELECT 1 FROM non_buyer_nurture_sequences t
        WHERE lower(t.email) = lower(d.email) AND t.status <> 'eligible'
      )
  AND NOT EXISTS (
        SELECT 1 FROM subscribers p
        WHERE lower(p.email) = lower(d.email) AND p.tier <> 'free'
      )
ORDER BY d.captured_at DESC
LIMIT :limit
""")


def record_checkout_abandon_candidate(db, email: str, subscriber_id: Optional[int] = None) -> None:
    """
    Idempotently record an abandoned-checkout email as an eligible nurture
    candidate. checkout_abandon leads have no other source table, so the
    capture point (webhook) writes the row directly.
    """
    db.execute(
        text("""
            INSERT INTO non_buyer_nurture_sequences (email, subscriber_id, source, captured_at, status)
            VALUES (:email, :subscriber_id, :source, NOW(), 'eligible')
            ON CONFLICT (email) DO NOTHING
        """),
        {"email": email, "subscriber_id": subscriber_id, "source": "checkout_abandon"},
    )


def find_candidates(db, limit: Optional[int] = None) -> list[dict]:
    """
    Eligible non-buyer candidates from the three sources (free-signup, waitlist,
    already-recorded checkout-abandon/retry rows), aged between MIN_AGE_HOURS and
    CAPTURED_WINDOW_DAYS, deduped by email (newest capture wins), newest-first,
    capped. Excludes once-per-email terminal rows AND any email with a paid
    subscriber. All filtering/sorting/paging happens in SQL.
    """
    now = datetime.now(timezone.utc)
    rows = db.execute(_CANDIDATES_SQL, {
        "window_start": now - timedelta(days=CAPTURED_WINDOW_DAYS),
        "window_end": now - timedelta(hours=MIN_AGE_HOURS),
        "limit": limit if limit is not None else _daily_cap(),
    }).all()
    return [
        {"email": r.email, "subscriber_id": r.subscriber_id, "source": r.source, "captured_at": r.captured_at}
        for r in rows
    ]


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
    try:
        result = instantly.add_leads(campaign_id, leads)
    except Exception:
        logger.error(
            "[NonBuyerNurture] Instantly add_leads failed for campaign %s (%d leads) — "
            "leaving rows eligible for retry", campaign_id, len(leads), exc_info=True,
        )
        result = None

    # Success = Instantly acknowledged the leads (created new OR recognised
    # existing). A dict with both counts zero means every lead was rejected
    # (e.g. invalid address) — treat as failure so rows stay eligible and the
    # next sweep retries, instead of silently marking them enrolled forever.
    created = (result or {}).get("leads_created", 0) or 0
    skipped = (result or {}).get("leads_skipped", 0) or 0
    succeeded = result is not None and (created + skipped) > 0

    # ponytail: batch-level success only — Instantly's add response gives counts,
    # not per-email results, so a partially-rejected batch still marks all rows
    # enrolled. The sync task (list_leads) is the per-email reconciler. Warn so
    # partial acceptance is visible in logs.
    if succeeded and created < len(leads):
        logger.warning(
            "[NonBuyerNurture] partial enroll on campaign %s: %d leads sent, "
            "created=%d skipped=%d", campaign_id, len(leads), created, skipped,
        )

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
        try:
            instantly.remove_lead(row.instantly_lead_id)
        except Exception:
            logger.warning(
                "[NonBuyerNurture] Instantly remove_lead failed for lead %s — "
                "marking converted anyway (DB suppression wins)", row.instantly_lead_id,
                exc_info=True,
            )

    now = datetime.now(timezone.utc)
    row.status = "converted"
    row.removal_reason = "paid_conversion"
    row.converted_at = now
    row.removed_at = now


def reconcile_conversions(db) -> int:
    """
    Repair backstop (plan §3.3): mark any still-enrolled row whose email now
    belongs to a paying subscriber as converted, in case the paid-conversion
    webhook was missed. Returns the number reconciled. The webhook is primary;
    this catches the gaps on the daily sweep.
    """
    emails = [
        r.email
        for r in db.execute(text("""
            SELECT n.email
            FROM non_buyer_nurture_sequences n
            WHERE n.status = 'enrolled'
              AND EXISTS (
                    SELECT 1 FROM subscribers p
                    WHERE lower(p.email) = lower(n.email) AND p.tier <> 'free'
                  )
        """)).all()
    ]
    for email in emails:
        mark_converted(db, email)
    if emails:
        logger.info("[NonBuyerNurture] reconcile_conversions: %d enrolled row(s) marked converted", len(emails))
    return len(emails)


_TERMINAL_STATUSES = {"converted", "unsubscribed", "bounced", "removed"}
_TERMINAL_MAP = {"unsubscribed": "unsubscribe", "bounced": "bounce"}


def apply_instantly_status(db, email: str, mapped_status: str, instantly_lead_id: Optional[str] = None) -> None:
    """
    Sync hook: apply an Instantly-mapped lead status (see
    instantly_service.map_lead_status) to the nurture row. unsubscribed/
    bounced are terminal (block re-enrollment via the once-per-email row).
    Never downgrades an already-terminal row. Also backfills instantly_lead_id.
    """
    row = db.execute(
        select(NonBuyerNurtureSequence).where(NonBuyerNurtureSequence.email == email)
    ).scalar_one_or_none()
    if row is None:
        return

    if instantly_lead_id and not row.instantly_lead_id:
        row.instantly_lead_id = instantly_lead_id

    if row.status in _TERMINAL_STATUSES:
        return

    reason = _TERMINAL_MAP.get(mapped_status)
    if reason:
        row.status = mapped_status
        row.removal_reason = reason
        row.removed_at = datetime.now(timezone.utc)
        # This nurture campaign is standalone (no EmailCampaign row), so the
        # normal campaign-sync suppression path never runs for it. Write the
        # global opt-out here so an unsubscribe/bounce is honoured across every
        # email/SMS channel (ADR 0028 block-all), not just this sequence.
        from src.services.email_suppression import suppress_contact
        suppress_contact(db, email=email, source="instantly_nurture_sync")
