"""
Abandoned-checkout recovery sweep (Task 7).

Two jobs, one run:
  1. Capture — finds aged free-tier signups that never reached a Stripe
     session (checkout_recovery.find_pre_payment_candidates) and starts
     recovery for them. Runs unconditionally; this is the only place the
     pre_payment path gets picked up (the session_expired path is captured
     directly by the checkout.session.expired webhook, not here).
  2. Cadence — walks active checkout_recovery rows and, per
     checkout_recovery.next_action, either sends the next recovery touch
     (email + optional SMS) or fails the row (handing the contact back to
     non-buyer nurture).

Touch sends and fail-transitions only happen when
settings.checkout_recovery_enabled is true; otherwise the sweep still
captures and ages rows but logs what it would send/fail instead of acting, so
it can be scheduled before messaging is switched on.

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


def _subject(touch_number: int) -> str:
    return (
        "You're one step from your Forced Action territory"
        if touch_number == 1
        else "Still want your ZIP? It's not locked yet"
    )


def _email_body(resume_url: str, touch_number: int) -> str:
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
    sent = failed = skipped = 0
    captured = 0

    with get_db_context() as db:
        # Capture always runs, independent of the send flag — this is the only
        # place the pre_payment path (no Stripe session ever created, so no
        # checkout.session.expired webhook) gets picked up.
        for candidate in checkout_recovery.find_pre_payment_candidates(db, now=now):
            row = checkout_recovery.start_recovery(
                db,
                email=candidate["email"],
                source="pre_payment",
                subscriber_id=candidate["subscriber_id"],
                phone=candidate["phone"],
            )
            if row is not None:
                captured += 1
        if captured:
            db.commit()

        rows = db.execute(
            select(CheckoutRecovery).where(CheckoutRecovery.status == "active")
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

            _send_touch(row, touch_number, resume_url)
            checkout_recovery.record_touch(db, row, now)
            sent += 1

        if sends_on:
            db.commit()

    result = {"captured": captured, "sent": sent, "failed": failed, "skipped": skipped, "sends_enabled": sends_on}
    logger.info("[recovery-sweep] %s", result)
    return result


def _send_touch(row, touch_number: int, resume_url: str) -> None:
    """Email always; SMS best-effort when a phone is on file (the compliant
    sender enforces opt-in/opt-out, so no consent check is duplicated here)."""
    from src.services.email import send_email

    try:
        send_email(
            to=row.email,
            subject=_subject(touch_number),
            body_text=_email_body(resume_url, touch_number),
        )
    except Exception:
        logger.warning("[recovery-sweep] email send failed email=%s", row.email, exc_info=True)

    if row.phone:
        try:
            from src.services.sms_compliance import send_sms
            from src.core.database import get_db_context
            with get_db_context() as sms_db:
                send_sms(
                    row.phone,
                    f"Your Forced Action ZIP is still open — finish here: {resume_url}",
                    sms_db,
                    message_type="marketing",
                )
        except Exception:
            logger.warning("[recovery-sweep] sms send failed email=%s", row.email, exc_info=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run_sweep(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
