"""
Pause resume reminder task — Phase B.

Sends a reminder SMS 7 days before a subscriber's pause is set to auto-resume.
Cron: 0 11 * * * (11 AM UTC daily)

Usage:
    python -m src.tasks.pause_resume_reminder [--dry-run]
"""
import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from src.core.database import get_db_context
from src.core.models import Subscriber

logger = logging.getLogger(__name__)

_REMINDER_DAYS_BEFORE = 7


def run(dry_run: bool = False) -> dict:
    results = {"checked": 0, "reminders_sent": 0, "errors": 0}
    now = datetime.now(timezone.utc)
    window_start = now + timedelta(days=_REMINDER_DAYS_BEFORE - 1)
    window_end = now + timedelta(days=_REMINDER_DAYS_BEFORE + 1)

    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(
                Subscriber.status == "paused",
                Subscriber.pause_resume_at >= window_start,
                Subscriber.pause_resume_at <= window_end,
            )
        ).scalars().all()

        for sub in subs:
            results["checked"] += 1
            try:
                if not dry_run:
                    _send_reminder(sub)
                    results["reminders_sent"] += 1
                    logger.info(
                        "pause_reminder: sent to sub=%d resume_at=%s",
                        sub.id, sub.pause_resume_at.isoformat(),
                    )
            except Exception as exc:
                logger.error("pause_reminder: error sub=%d: %s", sub.id, exc)
                results["errors"] += 1

    logger.info(
        "[PauseReminder] checked=%d sent=%d errors=%d dry_run=%s",
        results["checked"], results["reminders_sent"], results["errors"], dry_run,
    )
    return results


def _send_reminder(sub: Subscriber) -> None:
    from src.services.twilio_client import send_sms
    from config.settings import settings

    resume_date = sub.pause_resume_at.strftime("%B %d")
    feed_url = f"{settings.app_base_url}/dashboard?uuid={sub.event_feed_uuid}"
    msg = (
        f"Your Forced Action subscription resumes {resume_date}. "
        f"Leads will start flowing again automatically. "
        f"Questions? {feed_url}"
    )[:160]
    send_sms(to=sub.phone if hasattr(sub, "phone") and sub.phone else None, body=msg, subscriber=sub)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
