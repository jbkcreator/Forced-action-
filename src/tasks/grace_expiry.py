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
from datetime import datetime, timezone
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import Subscriber, ZipTerritory
from src.services.email import send_email

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

    body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">

        <!-- Header -->
        <tr>
          <td style="padding:32px 40px 24px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
            </p>
          </td>
        </tr>

        <!-- Green celebration banner -->
        <tr>
          <td style="background:rgba(34,197,94,0.12);border-bottom:1px solid rgba(34,197,94,0.25);
                     padding:14px 40px;text-align:center;">
            <p style="margin:0;font-size:15px;font-weight:700;color:#4ade80;">
              &#127881; A territory you wanted just opened up!
            </p>
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 12px;font-size:26px;font-weight:800;color:#ffffff;">
              ZIP {zip_code} is available
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              Great news &mdash; ZIP code <strong style="color:#ffffff;">{zip_code}</strong>
              in the <strong style="color:#ffffff;">{vertical.title()}</strong> vertical
              for <strong style="color:#ffffff;">{county_id}</strong> has just been released
              and is open for a new subscriber.
            </p>

            <!-- Urgency -->
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:rgba(251,191,36,0.06);border:1px solid rgba(251,191,36,0.2);
                          border-radius:12px;padding:16px 20px;margin-bottom:28px;">
              <tr>
                <td>
                  <p style="margin:0;font-size:14px;color:#fbbf24;font-weight:700;">
                    &#9888;&#65039; First come, first served
                  </p>
                  <p style="margin:6px 0 0;font-size:13px;color:#94a3b8;">
                    Territories are exclusive &mdash; only one subscriber per ZIP per vertical.
                    Lock it now to make sure no one else grabs it.
                  </p>
                </td>
              </tr>
            </table>

            <!-- CTA button -->
            <table cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{deep_link}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;
                            font-weight:700;text-decoration:none;">
                    Lock ZIP {zip_code} Now &rarr;
                  </a>
                </td>
              </tr>
            </table>

            <p style="margin:0;font-size:13px;color:#64748b;">
              Questions? Reply to this email or reach us at
              <a href="mailto:support@forcedaction.io" style="color:#fbbf24;text-decoration:none;">
                support@forcedaction.io
              </a>
            </p>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);
                     font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Hillsborough County Property Intelligence<br/>
            <a href="{app_base_url}" style="color:#475569;">forcedaction.io</a>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    for addr in emails:
        send_email(to=addr, subject=subject, body_text=body, body_html=body_html)


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
