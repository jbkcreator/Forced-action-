"""
Stage 12 — Post-signup bankruptcy-alert invite.

When a new property subscriber (free or paid) signs up, the signup path calls
schedule_invite(), which writes a `scheduled` row to message_outcomes due at
T + BANKRUPTCY_INVITE_DELAY_MINUTES. The invite-sweep cron later calls
send_due_invites(), which mints a fresh Stripe checkout session and emails the
link.

Why mint the session at SEND time (not schedule time): a Stripe Checkout
Session URL expires (~24h). Creating it when the email is actually sent keeps
the link valid for the recipient.

Dedup / idempotency:
  - schedule_invite is a no-op if a bankruptcy_alert_invite row already exists
    for the subscriber (INSERT ... WHERE NOT EXISTS).
  - send_due_invites flips send_status 'scheduled' → 'sent' atomically per row;
    a crash mid-sweep just re-processes the still-'scheduled' rows next pass.

All DB I/O via sa_text.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.bankruptcy_alert_config import (
    INVITE_GIVE_UP_HOURS,
    INVITE_SKIP_STATUSES,
    INVITE_SUBJECT,
    INVITE_SWEEP_BATCH,
    INVITE_TEMPLATE_ID,
    PRICE_MONTHLY_CENTS,
)
from config.settings import get_settings

logger = logging.getLogger(__name__)


# ── Scheduling (called from signup paths) ─────────────────────────────────────

def schedule_invite(db: Session, subscriber_id: int) -> bool:
    """Schedule a bankruptcy-alert invite for a freshly-created subscriber.

    Best-effort and idempotent: writes a 'scheduled' message_outcomes row due at
    T + delay. Returns True if a new row was created, False if one already
    existed. Never raises — signup must not break if scheduling hiccups.
    """
    try:
        delay_min = get_settings().bankruptcy_invite_delay_minutes
        due = datetime.now(timezone.utc) + timedelta(minutes=delay_min)
        # NOTE: message_outcomes has NOT-NULL boolean columns whose defaults are
        # Python-side only (no server_default) — requires_review and the three
        # conversion_within_* flags. A raw INSERT must set them explicitly or the
        # NULLs violate NOT NULL.
        result = db.execute(sa_text("""
            INSERT INTO message_outcomes
                (subscriber_id, message_type, template_id, channel, send_status,
                 requires_review, conversion_within_4h, conversion_within_24h,
                 conversion_within_48h, scheduled_send_at, sent_at, created_at)
            SELECT :sid, 'email', :tpl, 'ses', 'scheduled',
                   false, false, false, false, :due, NOW(), NOW()
            WHERE NOT EXISTS (
                SELECT 1 FROM message_outcomes
                WHERE subscriber_id = :sid AND template_id = :tpl
            )
            RETURNING id
        """), {"sid": subscriber_id, "tpl": INVITE_TEMPLATE_ID, "due": due}).first()
        created = result is not None
        if created:
            logger.info("[bk-invite] scheduled invite sub=%s due=%s", subscriber_id, due.isoformat())
        return created
    except Exception:
        logger.warning("[bk-invite] failed to schedule invite for sub=%s", subscriber_id, exc_info=True)
        return False


# ── Email body ────────────────────────────────────────────────────────────────

def _email_body(name: Optional[str], link: str) -> tuple[str, str]:
    greeting = f"Hi {name}," if name else "Hi,"
    price = f"${PRICE_MONTHLY_CENTS // 100}/mo"
    text_body = (
        f"{greeting}\n\n"
        f"Thanks for signing up. We also run Daily Bankruptcy Filing Alerts — the moment "
        f"a Chapter 7, 11, or 13 bankruptcy is filed in your market, you get an email "
        f"(and optional SMS) with the case number, chapter, filer, and filing date. "
        f"Built for attorneys, investors, and lenders who need to move first.\n\n"
        f"{price}. Start here:\n{link}\n\n"
        f"You'll only be charged if you complete checkout. Reply with any questions.\n\n"
        f"— Forced Action"
    )
    html_body = (
        f"<p>{greeting}</p>"
        f"<p>Thanks for signing up. We also run <strong>Daily Bankruptcy Filing Alerts</strong> — "
        f"the moment a Chapter 7, 11, or 13 bankruptcy is filed in your market, you get an email "
        f"(and optional SMS) with the case number, chapter, filer, and filing date. "
        f"Built for attorneys, investors, and lenders who need to move first.</p>"
        f"<p style='font-size:16px;'><strong>{price}</strong></p>"
        f"<p><a href='{link}' style='display:inline-block;padding:10px 18px;background:#1a1a2e;"
        f"color:#fff;text-decoration:none;border-radius:6px;'>Start your subscription</a></p>"
        f"<p style='color:#888;font-size:12px;'>You'll only be charged if you complete checkout. "
        f"Reply with any questions. — Forced Action</p>"
    )
    return text_body, html_body


# ── Sweep (called from cron) ──────────────────────────────────────────────────

@dataclass
class SweepResult:
    due: int = 0
    sent: int = 0
    failed: int = 0
    skipped: int = 0
    gave_up: int = 0


def _due_invites(db: Session, batch: int) -> list:
    """Scheduled invites past their send time, whose subscriber is still active
    and hasn't already converted to a bankruptcy subscription."""
    return db.execute(sa_text("""
        SELECT m.id AS outcome_id, m.scheduled_send_at, s.id AS sub_id,
               s.email, s.name, s.status
        FROM message_outcomes m
        JOIN subscribers s ON s.id = m.subscriber_id
        WHERE m.template_id = :tpl
          AND m.send_status = 'scheduled'
          AND m.scheduled_send_at <= NOW()
        ORDER BY m.scheduled_send_at
        LIMIT :batch
    """), {"tpl": INVITE_TEMPLATE_ID, "batch": batch}).fetchall()


def _mark(db: Session, outcome_id: int, status: str, reason: Optional[str] = None) -> None:
    db.execute(sa_text("""
        UPDATE message_outcomes
        SET send_status = :status,
            sent_at = NOW(),
            review_reason = COALESCE(:reason, review_reason)
        WHERE id = :id
    """), {"status": status, "reason": reason, "id": outcome_id})


def _already_subscribed(db: Session, email: str) -> bool:
    row = db.execute(sa_text("""
        SELECT 1 FROM bankruptcy_alert_subscriptions
        WHERE LOWER(email) = LOWER(:e) AND status IN ('trialing', 'active') LIMIT 1
    """), {"e": email}).first()
    return row is not None


def _is_stale(scheduled_send_at: datetime) -> bool:
    if scheduled_send_at is None:
        return False
    ts = scheduled_send_at
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts) > timedelta(hours=INVITE_GIVE_UP_HOURS)


def send_due_invites(db: Session, *, batch: int = INVITE_SWEEP_BATCH) -> SweepResult:
    """Send all due bankruptcy-alert invites. Mints a fresh Stripe session per
    invite at send time so the embedded link is valid. Idempotent."""
    from src.services.bankruptcy_alert.subscription import create_checkout
    from src.services.email import send_email

    result = SweepResult()
    settings = get_settings()
    base = settings.app_base_url.rstrip("/")

    rows = _due_invites(db, batch)
    result.due = len(rows)

    for r in rows:
        # Skip dead accounts or already-converted subscribers.
        if r.status in INVITE_SKIP_STATUSES:
            _mark(db, r.outcome_id, "cancelled", reason=f"subscriber status={r.status}")
            result.skipped += 1
            db.flush()
            continue
        if not r.email:
            _mark(db, r.outcome_id, "cancelled", reason="no email on subscriber")
            result.skipped += 1
            db.flush()
            continue
        if _already_subscribed(db, r.email):
            _mark(db, r.outcome_id, "cancelled", reason="already has active bankruptcy subscription")
            result.skipped += 1
            db.flush()
            continue

        try:
            session = create_checkout(
                success_url=f"{base}/bankruptcy-alerts/success?session_id={{CHECKOUT_SESSION_ID}}",
                cancel_url=f"{base}/bankruptcy-alerts",
                customer_email=r.email,
            )
            text_body, html_body = _email_body(r.name, session["url"])
            ok = send_email(r.email, INVITE_SUBJECT, text_body, body_html=html_body)
            if ok:
                _mark(db, r.outcome_id, "sent")
                result.sent += 1
            else:
                raise RuntimeError("send_email returned False")
        except Exception as exc:  # noqa: BLE001
            if _is_stale(r.scheduled_send_at):
                _mark(db, r.outcome_id, "failed", reason=str(exc)[:255])
                result.gave_up += 1
                logger.warning("[bk-invite] gave up on sub=%s after %dh: %s",
                               r.sub_id, INVITE_GIVE_UP_HOURS, exc)
            else:
                # Leave 'scheduled' → retried next sweep.
                result.failed += 1
                logger.warning("[bk-invite] send failed sub=%s (will retry): %s", r.sub_id, exc)
        db.flush()

    logger.info(
        "[bk-invite] sweep: due=%d sent=%d failed=%d skipped=%d gave_up=%d",
        result.due, result.sent, result.failed, result.skipped, result.gave_up,
    )
    return result
