"""
M1-C — ZIP territory & subscriber grace expiry cron.

Run every 15 minutes via cron or a scheduler (e.g. APScheduler, crontab):

    */15 * * * * python -m src.tasks.grace_expiry

What it does
────────────
1. Find zip_territories WHERE status='grace' AND grace_expires_at <= NOW()
   → Set status='available', clear subscriber_id / locked_at / grace_expires_at
   → If waitlist_emails is non-empty, fire a notification email per queued address
     (legacy array — superseded by waitlist_entries, kept for rows still on it)
   → Fire the sold-out waitlist notification for every released territory, so
     WaitlistEntry(waitlist_type='sold_out') rows are told the ZIP just freed up
   → Log an "expansion alert" if the ZIP was the last locked territory in that
     county+vertical (meaning the market just opened up again)

2. Find subscribers WHERE status='grace' AND grace_expires_at <= NOW()
   → Set status='churned'

Both steps run inside a single transaction so a crash mid-way is safe to retry.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import Subscriber, ZipTerritory
from src.services.email import send_email
from src.services.email_shell import ACCENT, paragraph, render_email_shell
from src.tasks.sold_out_reactivation import reactivate_for_zip

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Email helper (optional — no-ops if SMTP is not configured)
# ---------------------------------------------------------------------------

def _send_waitlist_email(zip_code: str, vertical: str, county_id: str, emails: List[str]) -> None:
    """
    Send a simple availability notification to each waitlisted email.
    Requires SMTP settings in AppSettings (SMTP_HOST / SMTP_USER / SMTP_PASS).
    Silently skips if any setting is missing.
    """
    from config.settings import get_settings
    settings = get_settings()
    app_base_url = settings.app_base_url
    deep_link = f"{app_base_url}/?zip={zip_code}&vertical={vertical}&county={county_id}"
    subject = f"ZIP {zip_code} is now available — {vertical.title()} | Forced Action"
    body = (
        f"Good news!\n\n"
        f"ZIP code {zip_code} ({vertical.title()} — {county_id}) has just become available "
        f"on Forced Action.\n\n"
        f"Lock it now before someone else does:\n"
        f"{deep_link}\n\n"
        f"— Forced Action Team"
    )

    inner_html = (
        paragraph(
            f"Great news &mdash; ZIP code <strong>{zip_code}</strong> in the "
            f"<strong>{vertical.title()}</strong> vertical for "
            f"<strong>{county_id}</strong> has just been released and is open "
            f"for a new subscriber."
        )
        + paragraph(
            "First come, first served. Territories are exclusive &mdash; only one "
            "subscriber per ZIP per vertical. Lock it now to make sure no one else "
            "grabs it."
        )
        + paragraph(
            "Questions? Reply to this email or reach us at "
            "<a href=\"mailto:support@forcedactionleads.com\" "
            f"style=\"color:{ACCENT};text-decoration:none;\">support@forcedactionleads.com</a>.",
            muted=True,
        )
    )
    body_html = render_email_shell(
        headline=f"ZIP {zip_code} is available",
        subhead="A territory you wanted just opened up",
        inner_html=inner_html,
        cta_text=f"Lock ZIP {zip_code} now",
        cta_url=deep_link,
        preheader=f"ZIP {zip_code} ({vertical.title()}) just opened up.",
    )

    for addr in emails:
        send_email(to=addr, subject=subject, body_text=body, body_html=body_html)


# ---------------------------------------------------------------------------
# Core expiry logic
# ---------------------------------------------------------------------------

def expire_zip_grace_periods(db: Session, released_out: Optional[list] = None) -> int:
    """
    Release ZIP territories whose grace window has closed.
    Returns the count of territories released.

    When `released_out` is given, each released territory is appended to it as a
    (zip_code, vertical, county_id) tuple so the caller can fire the sold-out
    waitlist notification *after* this transaction commits — see run_grace_expiry.
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
        if released_out is not None:
            released_out.append((zip_code, vertical, county_id))
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
    from src.services.referral_engine import revoke_team_for_subscriber
    for subscriber in expired:
        subscriber.status = "churned"
        subscriber.churned_at = now
        churned += 1
        logger.info(
            f"Subscriber churned: id={subscriber.id} "
            f"tier={subscriber.tier} vertical={subscriber.vertical}"
        )
        revoke_team_for_subscriber(subscriber.id, "churn", db)

    if churned:
        logger.info(f"grace_expiry: churned {churned} subscribers")
    return churned


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_grace_expiry() -> None:
    """Run both expiry passes in a single transaction, then notify waitlists.

    The sold-out waitlist notification runs *after* the transaction commits:
    reactivate_for_zip opens its own session, so firing it inside this one would
    have it contend with the FOR UPDATE locks still held on the released rows —
    and would announce a ZIP as free before that release was durable.
    """
    released: list[tuple[str, str, str]] = []

    with get_db_context() as db:
        zips_released = expire_zip_grace_periods(db, released_out=released)
        subs_churned = expire_subscriber_grace_periods(db)
        # get_db_context commits on clean exit
        logger.info(
            f"grace_expiry complete: zips_released={zips_released} subs_churned={subs_churned}"
        )

    notified = 0
    for zip_code, vertical, county_id in released:
        try:
            result = reactivate_for_zip(zip_code, vertical, county_id)
            notified += int(result.get("fired", 0) or 0)
        except Exception:
            logger.error(
                "grace_expiry: sold-out waitlist notify failed for %s/%s/%s",
                zip_code, vertical, county_id, exc_info=True,
            )

    if released:
        logger.info(
            f"grace_expiry: sold-out waitlist notified={notified} "
            f"across {len(released)} released territories"
        )


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    run_grace_expiry()
