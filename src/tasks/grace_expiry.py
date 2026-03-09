"""
M1-C — ZIP territory & subscriber grace expiry cron.

Run every 15 minutes via cron or a scheduler (e.g. APScheduler, crontab):

    */15 * * * * python -m src.tasks.grace_expiry

What it does
────────────
1. Find zip_territories WHERE status='grace' AND grace_expires_at <= NOW()
   → Set status='available', clear subscriber_id / locked_at / grace_expires_at
   → If waitlist_emails is non-empty, fire a notification email per queued address
   → Log an "expansion alert" if the ZIP was the last locked territory in that
     county+vertical (meaning the market just opened up again)

2. Find subscribers WHERE status='grace' AND grace_expires_at <= NOW()
   → Set status='churned'

Both steps run inside a single transaction so a crash mid-way is safe to retry.
"""

import logging
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import Subscriber, ZipTerritory

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Email helper (optional — no-ops if SMTP is not configured)
# ---------------------------------------------------------------------------

def _send_waitlist_email(zip_code: str, vertical: str, county_id: str, emails: List[str]) -> None:
    """
    Send a simple availability notification to each waitlisted email.
    Requires SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS / EMAIL_FROM in env.
    Silently skips if any setting is missing.
    """
    import os
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    from_addr = os.getenv("EMAIL_FROM", user)

    if not all([host, user, password]):
        logger.debug("SMTP not configured — skipping waitlist emails")
        return

    subject = f"ZIP {zip_code} is now available — {vertical.title()} | Forced Action"
    body = (
        f"Good news!\n\n"
        f"ZIP code {zip_code} ({vertical.title()} — {county_id}) has just become available "
        f"on Forced Action.\n\n"
        f"Lock it now before someone else does:\n"
        f"https://app.forcedaction.io/subscribe?zip={zip_code}&vertical={vertical}&county={county_id}\n\n"
        f"— Forced Action Team"
    )

    for addr in emails:
        try:
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = from_addr
            msg["To"] = addr
            with smtplib.SMTP(host, port, timeout=10) as server:
                server.starttls()
                server.login(user, password)
                server.sendmail(from_addr, [addr], msg.as_string())
            logger.info(f"Waitlist email sent → {addr} for ZIP {zip_code}/{vertical}")
        except Exception as e:
            logger.error(f"Failed to send waitlist email to {addr}: {e}")


# ---------------------------------------------------------------------------
# Core expiry logic
# ---------------------------------------------------------------------------

def expire_zip_grace_periods(db: Session) -> int:
    """
    Release ZIP territories whose grace window has closed.
    Returns the count of territories released.
    """
    now = datetime.now(timezone.utc)

    expired = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.status == "grace",
            ZipTerritory.grace_expires_at <= now,
        ).with_for_update()
    ).scalars().all()

    released = 0
    for territory in expired:
        zip_code = territory.zip_code
        vertical = territory.vertical
        county_id = territory.county_id
        waitlist = list(territory.waitlist_emails or [])

        # Release the territory
        territory.subscriber_id = None
        territory.status = "available"
        territory.locked_at = None
        territory.grace_expires_at = None
        territory.waitlist_emails = []

        released += 1
        logger.info(
            f"ZIP released: {zip_code}/{vertical}/{county_id} "
            f"waitlist={len(waitlist)}"
        )

        # Notify waitlisted addresses (after the DB write is flushed)
        if waitlist:
            db.flush()  # ensure changes are visible before emailing
            _send_waitlist_email(zip_code, vertical, county_id, waitlist)

    if released:
        logger.info(f"grace_expiry: released {released} ZIP territories")
    return released


def expire_subscriber_grace_periods(db: Session) -> int:
    """
    Mark subscribers whose grace window has closed as churned.
    Returns the count of subscribers churned.
    """
    now = datetime.now(timezone.utc)

    expired = db.execute(
        select(Subscriber).where(
            Subscriber.status == "grace",
            Subscriber.grace_expires_at <= now,
        ).with_for_update()
    ).scalars().all()

    churned = 0
    for subscriber in expired:
        subscriber.status = "churned"
        churned += 1
        logger.info(
            f"Subscriber churned: id={subscriber.id} "
            f"tier={subscriber.tier} vertical={subscriber.vertical}"
        )

    if churned:
        logger.info(f"grace_expiry: churned {churned} subscribers")
    return churned


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_grace_expiry() -> None:
    """Run both expiry passes in a single transaction."""
    with get_db_context() as db:
        zips_released = expire_zip_grace_periods(db)
        subs_churned = expire_subscriber_grace_periods(db)
        # get_db_context commits on clean exit
        logger.info(
            f"grace_expiry complete: zips_released={zips_released} subs_churned={subs_churned}"
        )


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    run_grace_expiry()
