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


def _email_body(resume_url: str, touch_number: int, source: str) -> str:
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
        f"— Forced Action"
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
            # A total failure leaves the row due so the next sweep retries.
            if _send_touch(row, touch_number, resume_url):
                checkout_recovery.record_touch(db, row, now)
                sent += 1
                logger.debug("[recovery-sweep] sent touch#%d email=%s source=%s", touch_number, row.email, row.source)
            else:
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


def _send_touch(row, touch_number: int, resume_url: str) -> bool:
    """Email always; SMS best-effort when a phone is on file (the compliant
    sender enforces opt-in/opt-out, so no consent check is duplicated here).

    Returns True if at least one channel accepted the message. A False return
    means nothing was delivered (provider down, bad creds, timeout) — the caller
    must NOT advance the touch count, so the row stays due and the next sweep
    retries instead of silently burning a recovery attempt."""
    from src.services.email import send_email

    source = getattr(row, "source", "session_expired")
    email_ok = False
    try:
        send_email(
            to=row.email,
            subject=_subject(touch_number, source),
            body_text=_email_body(resume_url, touch_number, source),
        )
        email_ok = True
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
                send_sms(row.phone, sms_body, sms_db, message_type="marketing")
            sms_ok = True
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
