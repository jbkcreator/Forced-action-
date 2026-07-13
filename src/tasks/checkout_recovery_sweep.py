"""
Abandoned-checkout recovery sweep (Task 7).

Cadence only: walks active checkout_recovery rows and, per
checkout_recovery.next_action, either sends the next recovery touch (email +
optional SMS) or fails the row (handing the contact back to non-buyer
nurture). Capture is NOT done here — rows are created at their real source:
the pre_payment path at /api/checkout session creation, and the
session_expired path by the checkout.session.expired webhook.

Touch sends and fail-transitions only happen when
settings.checkout_recovery_enabled is true; otherwise the sweep logs what it
would send/fail instead of acting, so it can be scheduled before messaging is
switched on.

    python -m src.tasks.checkout_recovery_sweep
    python -m src.tasks.checkout_recovery_sweep --dry-run
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


def _subject(touch_number: int, source: str) -> str:
    if source == "lead_pack":
        return (
            "Your Forced Action lead pack is waiting"
            if touch_number == 1
            else "Still want those leads? Your pack isn't purchased yet"
        )
    return (
        "You're one step from your Forced Action territory"
        if touch_number == 1
        else "Still want your ZIP? It's not locked yet"
    )


def _email_body(resume_url: str, touch_number: int, source: str, unsubscribe_url: str) -> str:
    if source == "lead_pack":
        lead = (
            "You started buying a lead pack but didn't finish — those leads are "
            "still available."
            if touch_number == 1
            else "A quick nudge: the lead pack you started is still available, "
            "but leads get claimed fast."
        )
    else:
        lead = (
            "You started claiming your territory but didn't finish — your ZIP is "
            "still open for now."
            if touch_number == 1
            else "A quick nudge: the ZIP you were about to lock is still available, "
            "but founding spots are limited."
        )
    return (
        f"{lead}\n\n"
        f"Pick up where you left off: {resume_url}\n\n"
        f"— Forced Action\n\n"
        f"Don't want these reminders? Unsubscribe: {unsubscribe_url}"
    )


def run_sweep(dry_run: bool = False) -> dict:
    from config.settings import get_settings
    from sqlalchemy import select
    from src.core.database import get_db_context
    from src.core.models import CheckoutRecovery
    from src.services import checkout_recovery

    settings = get_settings()
    sends_on = settings.checkout_recovery_enabled and not dry_run
    now = datetime.now(timezone.utc)
    sent = failed = skipped = undelivered = 0

    with get_db_context() as db:
        # Claim active rows with FOR UPDATE SKIP LOCKED so overlapping cron runs
        # never send the same touch twice: a second worker skips the rows this
        # one holds and picks up the rest. Locks are held until the commit
        # below — bounded by the small live-recovery window (rows close on
        # recover/fail), so a per-row lease/outbox would be over-built here.
        # ponytail: single locked scan, no batching; add a LIMIT + lease only if
        # the active window ever grows enough that lock-hold-during-send bites.
        rows = db.execute(
            select(CheckoutRecovery)
            .where(CheckoutRecovery.status == "active")
            .with_for_update(skip_locked=True)
        ).scalars().all()

        for row in rows:
            action = checkout_recovery.next_action(
                row.touches_sent, row.started_at, row.last_touch_at, now
            )
            if action is None:
                skipped += 1
                continue

            if action == "fail":
                if not sends_on:
                    logger.info("[recovery-sweep] would FAIL email=%s (flag off/dry-run)", row.email)
                    skipped += 1
                    continue
                checkout_recovery.mark_failed(db, row.email)
                failed += 1
                continue

            # action == "send"
            touch_number = (row.touches_sent or 0) + 1
            resume_url = checkout_recovery.build_resume_url(settings.app_base_url, row.resume_context)
            if not sends_on:
                logger.info(
                    "[recovery-sweep] would SEND touch#%d email=%s url=%s (flag off/dry-run)",
                    touch_number, row.email, resume_url,
                )
                skipped += 1
                continue

            # Only advance the touch count when a channel actually delivered.
            if _send_touch(row, touch_number, resume_url, settings.app_base_url):
                checkout_recovery.record_touch(db, row, now)
                sent += 1
                logger.debug("[recovery-sweep] sent touch#%d email=%s source=%s", touch_number, row.email, row.source)
            elif _is_email_opted_out(db, row.email):
                # Opted out (unsubscribed) — email will never deliver, so close
                # the row instead of retrying it every sweep forever.
                checkout_recovery.mark_failed(db, row.email)
                failed += 1
                logger.info("[recovery-sweep] email opted out — closing recovery email=%s", row.email)
            else:
                # Transient failure (provider down/misconfigured) — leave due.
                undelivered += 1
                logger.warning(
                    "[recovery-sweep] touch#%d not delivered (all channels failed) email=%s — leaving due for retry",
                    touch_number, row.email,
                )

        if sends_on:
            db.commit()

    result = {"sent": sent, "failed": failed, "skipped": skipped, "undelivered": undelivered, "sends_enabled": sends_on}
    logger.info("[recovery-sweep] %s", result)
    return result


def _is_email_opted_out(db, email: str) -> bool:
    """True if the email is on the global opt-out list — a send will never
    deliver, so recovery should close the row rather than retry it."""
    try:
        from src.services.email_suppression import is_email_suppressed
        return bool(is_email_suppressed(db, email))
    except Exception:
        logger.warning("[recovery-sweep] opt-out check failed email=%s", email, exc_info=True)
        return False


def _send_touch(row, touch_number: int, resume_url: str, base_url: str) -> bool:
    """Email always; SMS best-effort when a phone is on file (the compliant
    sender enforces opt-in/opt-out).

    Returns True only if a channel ACTUALLY accepted the message. send_email /
    send_sms return False for non-exceptional declines (SMTP unconfigured,
    email opted-out, SMS lacking consent/subscriber_id, provider disabled) — we
    honour that return, not just the absence of an exception, so a declined send
    never counts as a delivered touch and the row stays due for retry."""
    from src.services.email import send_email
    from src.services.email_unsubscribe import mint_unsubscribe_token

    source = getattr(row, "source", "session_expired")

    # Recovery mail is promotional → must carry a one-click unsubscribe (body +
    # List-Unsubscribe header). Opting out writes the global opt-out, which the
    # send_email suppression gate then honours on the next touch.
    unsubscribe_url = f"{base_url.rstrip('/')}/api/email/unsubscribe?token={mint_unsubscribe_token(row.email)}"

    email_ok = False
    try:
        email_ok = bool(send_email(
            to=row.email,
            subject=_subject(touch_number, source),
            body_text=_email_body(resume_url, touch_number, source, unsubscribe_url),
            list_unsubscribe_url=unsubscribe_url,
        ))
    except Exception:
        logger.warning("[recovery-sweep] email send failed email=%s", row.email, exc_info=True)

    sms_ok = False
    if row.phone:
        sms_body = (
            f"Your Forced Action lead pack is still available — finish here: {resume_url}"
            if source == "lead_pack"
            else f"Your Forced Action ZIP is still open — finish here: {resume_url}"
        )
        try:
            from src.services.sms_compliance import send_sms
            from src.core.database import get_db_context
            with get_db_context() as sms_db:
                # subscriber_id is required — the compliance sender rejects
                # marketing SMS without it (returns False, not an exception).
                sms_ok = bool(send_sms(
                    row.phone, sms_body, sms_db,
                    message_type="marketing", subscriber_id=row.subscriber_id,
                ))
        except Exception:
            logger.warning("[recovery-sweep] sms send failed email=%s", row.email, exc_info=True)

    return email_ok or sms_ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run_sweep(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
