"""
Abandoned-checkout recovery — core service (Task 7).

Owns capture, nurture suppression, touch cadence, and terminal transitions for
the fast "finish your purchase" recovery sequence. Two capture paths feed it,
both captured at their real source (never inferred from a free signup):
`pre_payment` — a subscription checkout session was created at /api/checkout
but not completed; and `session_expired` — a Stripe `checkout.session.expired`
webhook. pre_payment fires the earlier nudge; session_expired is the backstop
(dedup keeps them from double-starting the same email).

While a recovery row is `active`, the sibling non_buyer_nurture row is held at
`in_recovery` so the slower nurture drip never contacts the same person at the
same time. Recovery failure releases the hold (nurture row → `eligible`); a
paid conversion closes recovery as `recovered`.

Callers own the transaction — nothing here commits.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select

from src.core.models import CheckoutRecovery, NonBuyerNurtureSequence
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

# Sources that participate in the non-buyer nurture dedup. lead_pack recovery
# targets *existing paying subscribers* (a lead pack requires an active sub),
# who are not nurture candidates — so it neither suppresses nor is gated by
# the nurture drip.
_NURTURE_SOURCES = {"session_expired", "pre_payment"}
# Nurture states that block starting recovery: 'enrolled' = the drip is
# already contacting them (no double-contact); the terminal states =
# converted or opted-out (must not contact at all).
_NURTURE_BLOCKING = {"enrolled", "converted", "unsubscribed", "bounced", "removed"}

# Cadence: first "you left this behind" touch shortly after drop-off, one
# follow-up a day later, then hand off to nurture. Tunable.
FIRST_TOUCH_DELAY = timedelta(hours=1)
SECOND_TOUCH_DELAY = timedelta(hours=24)
MAX_TOUCHES = 2


def next_action(touches_sent: int, started_at: datetime, last_touch_at: Optional[datetime], now: datetime) -> Optional[str]:
    """Pure cadence decision for one active row. Returns 'send' (a touch is
    due), 'fail' (all touches sent and the final grace window elapsed with no
    conversion), or None (nothing due yet)."""
    if touches_sent == 0:
        return "send" if now >= started_at + FIRST_TOUCH_DELAY else None
    anchor = last_touch_at or started_at
    if touches_sent < MAX_TOUCHES:
        return "send" if now >= anchor + SECOND_TOUCH_DELAY else None
    # All touches sent — fail once the post-final grace window elapses.
    return "fail" if now >= anchor + SECOND_TOUCH_DELAY else None


def build_resume_url(base_url: str, resume_context: Optional[dict]) -> str:
    """A link back to where the buyer dropped off. Lead-pack abandoners are
    existing subscribers, so they resume on their own dashboard (which is where
    the lead-pack purchase lives); subscription abandoners resume on the
    pricing funnel with county/vertical pre-selected (the expired Stripe
    session can't be reused)."""
    ctx = resume_context or {}
    base = base_url.rstrip("/")
    from urllib.parse import urlencode

    if ctx.get("kind") == "lead_pack" and ctx.get("feed_uuid"):
        params = {k: ctx[k] for k in ("lead_pack_zip",) if ctx.get(k)}
        qs = f"?{urlencode(params)}" if params else ""
        return f"{base}/dashboard/{ctx['feed_uuid']}{qs}"

    params = {k: ctx[k] for k in ("county_id", "vertical") if ctx.get(k)}
    qs = f"?{urlencode(params)}" if params else ""
    return f"{base}/{qs}#pricing"


def record_touch(db, row: "CheckoutRecovery", now: Optional[datetime] = None) -> None:
    """Advance a row after a touch is sent: bump the counter and stamp
    first/last touch timestamps."""
    now = now or datetime.now(timezone.utc)
    row.touches_sent = (row.touches_sent or 0) + 1
    if row.first_touch_at is None:
        row.first_touch_at = now
    row.last_touch_at = now


def _suppress_nurture(db, email: str, subscriber_id: Optional[int]) -> None:
    """Hold the email out of the non-buyer nurture drip while recovery runs.
    Upserts the nurture row to 'in_recovery' (non-'eligible' → find_candidates
    skips it). Never downgrades an already-terminal nurture row (converted/
    unsubscribed/bounced/removed stay put — those win over recovery)."""
    row = db.execute(
        select(NonBuyerNurtureSequence).where(NonBuyerNurtureSequence.email == email)
    ).scalar_one_or_none()
    now = datetime.now(timezone.utc)
    if row is None:
        db.add(NonBuyerNurtureSequence(
            email=email,
            subscriber_id=subscriber_id,
            source="checkout_abandon",
            captured_at=now,
            status="in_recovery",
        ))
        return
    if row.status in ("eligible",):
        row.status = "in_recovery"


def start_recovery(
    db,
    email: str,
    *,
    source: str,
    subscriber_id: Optional[int] = None,
    phone: Optional[str] = None,
    resume_context: Optional[dict] = None,
) -> Optional[CheckoutRecovery]:
    """Idempotently begin recovery for an abandoned checkout. Returns the row,
    or None if the email is already past recovery (recovered/failed) — a
    closed sequence is never reopened for the same email (once per email)."""
    email = (email or "").strip().lower()
    if not email:
        return None
    phone = normalize_phone(phone) if phone else None

    existing = db.execute(
        select(CheckoutRecovery).where(CheckoutRecovery.email == email)
    ).scalar_one_or_none()
    if existing is not None:
        # Already active → no-op replay; already closed → don't reopen.
        return existing if existing.status == "active" else None

    participates_in_nurture = source in _NURTURE_SOURCES
    if participates_in_nurture:
        nurture = db.execute(
            select(NonBuyerNurtureSequence).where(NonBuyerNurtureSequence.email == email)
        ).scalar_one_or_none()
        if nurture is not None and nurture.status in _NURTURE_BLOCKING:
            # enrolled → already being contacted by the drip; terminal →
            # converted or opted-out. Either way, don't start a second contact.
            logger.info(
                "[CheckoutRecovery] skip email=%s source=%s (nurture status=%s)",
                email, source, nurture.status,
            )
            return None

    row = CheckoutRecovery(
        email=email,
        subscriber_id=subscriber_id,
        phone=phone,
        source=source,
        status="active",
        touches_sent=0,
        resume_context=resume_context,
    )
    db.add(row)
    if participates_in_nurture:
        _suppress_nurture(db, email, subscriber_id)
    logger.info("[CheckoutRecovery] started email=%s source=%s", email, source)
    _alert_founder(email, source, phone)
    return row


def _alert_founder(email: str, source: str, phone: Optional[str]) -> None:
    """B1-04: ping the founder on every new abandonment (subscription or lead
    pack) so they can personally follow up. Fires once per email (this is only
    reached on the new-row path, never the no-op replay) and unconditionally —
    unlike the customer-facing touches this isn't gated by
    checkout_recovery_enabled, since it sends nothing to the buyer."""
    from src.services.stripe_webhooks import _send_founder_alert
    message = f"ABANDONED CHECKOUT: {source} email={email}"
    if phone:
        message += f" phone={phone}"
    _send_founder_alert(message)


def _close(db, email: str, status: str) -> Optional[CheckoutRecovery]:
    row = db.execute(
        select(CheckoutRecovery).where(CheckoutRecovery.email == email)
    ).scalar_one_or_none()
    if row is None or row.status != "active":
        return row
    row.status = status
    row.closed_at = datetime.now(timezone.utc)
    return row


def mark_recovered(db, email: str) -> None:
    """Buyer completed payment — close recovery. Idempotent; no-op if there's
    no active row. The nurture side is closed separately by
    non_buyer_nurture.mark_converted on the same payment webhook."""
    email = (email or "").strip().lower()
    if not email:
        return
    row = _close(db, email, "recovered")
    if row is not None and row.status == "recovered":
        logger.info("[CheckoutRecovery] recovered email=%s", email)


def mark_failed(db, email: str) -> None:
    """Recovery exhausted (all touches sent, no conversion). Close the row and
    release the email back to the nurture drip (nurture row → 'eligible', only
    if it's still held at 'in_recovery' — never overrides a terminal state)."""
    email = (email or "").strip().lower()
    if not email:
        return
    row = _close(db, email, "failed")
    if row is None or row.status != "failed":
        return
    nurture = db.execute(
        select(NonBuyerNurtureSequence).where(NonBuyerNurtureSequence.email == email)
    ).scalar_one_or_none()
    if nurture is not None and nurture.status == "in_recovery":
        nurture.status = "eligible"
        nurture.eligible_at = datetime.now(timezone.utc)
    logger.info("[CheckoutRecovery] failed→nurture email=%s", email)


