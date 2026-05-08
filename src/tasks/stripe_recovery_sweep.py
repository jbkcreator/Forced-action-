"""
Stripe payment-failure recovery sweep.

Day 1: soft reminder (~24h after failure).
Day 3: urgency message + missed Gold-lead count (~72h after failure).
Day 5: handled by proactive_save.py (existing, no change needed here).

Cron: 0 16 * * * (daily 16:00 UTC, after proactive_save 15:00).

Flags reset automatically on invoice.payment_succeeded.
"""
import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from src.core.database import get_db_context
from src.core.models import Subscriber

logger = logging.getLogger(__name__)

DAY1_MIN = timedelta(hours=20)
DAY1_MAX = timedelta(hours=28)
DAY3_MIN = timedelta(days=2, hours=20)
DAY3_MAX = timedelta(days=3, hours=4)


def run(dry_run: bool = False) -> dict:
    now = datetime.now(timezone.utc)
    sent = {"day1": 0, "day3": 0, "skipped": 0, "errors": 0}

    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(Subscriber.payment_failed_at.is_not(None))
        ).scalars().all()

        for sub in subs:
            try:
                elapsed = now - sub.payment_failed_at
                if DAY1_MIN <= elapsed <= DAY1_MAX and not sub.recovery_day1_sent:
                    if not dry_run:
                        _send_day1(sub)
                        sub.recovery_day1_sent = True
                    sent["day1"] += 1
                elif DAY3_MIN <= elapsed <= DAY3_MAX and not sub.recovery_day3_sent:
                    if not dry_run:
                        _send_day3(sub, db)
                        sub.recovery_day3_sent = True
                    sent["day3"] += 1
                else:
                    sent["skipped"] += 1
            except Exception as exc:
                logger.error("stripe_recovery_sweep error sub=%s: %s", sub.id, exc)
                sent["errors"] += 1

        if not dry_run:
            db.commit()

    logger.info("[StripeRecoverySweep] %s dry_run=%s", sent, dry_run)
    return sent


def _send_day1(sub: Subscriber) -> None:
    from config.settings import get_settings
    from src.services.email import send_email

    settings = get_settings()
    name = sub.name or "there"
    feed_url = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        if sub.event_feed_uuid else settings.app_base_url
    )
    subject = "Heads up — your card didn't go through"
    body_text = (
        f"Hi {name},\n\n"
        f"We weren't able to process your Forced Action payment. "
        f"Update your card to keep your territories locked:\n\n"
        f"{feed_url}\n\n"
        f"Questions? support@forcedaction.io\n\n— Forced Action Team"
    )
    body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:40px;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <h2 style="color:#f1f5f9;">Heads up, {name}</h2>
  <p>We weren't able to process your Forced Action payment.</p>
  <p>Update your payment method to keep your ZIP territories active:</p>
  <p><a href="{feed_url}" style="color:#38bdf8;">Update billing →</a></p>
  <p style="color:#94a3b8;font-size:12px;">Questions? support@forcedaction.io</p>
</body>
</html>"""
    send_email(
        to_address=sub.email,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
    )
    logger.info("stripe_recovery day1 sent sub=%s", sub.id)


def _send_day3(sub: Subscriber, db) -> None:
    from config.settings import get_settings
    from src.services.email import send_email
    from src.agents.tools.read_tools import get_lead_pool

    settings = get_settings()
    name = sub.name or "there"
    feed_url = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        if sub.event_feed_uuid else settings.app_base_url
    )

    # Find missed Gold leads in subscriber's territory since payment failed
    gold_leads = []
    try:
        from src.core.models import ZipTerritory
        locked_zips = db.execute(
            select(ZipTerritory.zip_code).where(
                ZipTerritory.subscriber_id == sub.id,
                ZipTerritory.status.in_(["locked", "grace"]),
            )
        ).scalars().all()
        for zip_code in locked_zips[:3]:
            leads = get_lead_pool(zip_code=zip_code, vertical=sub.vertical, min_score=70, limit=10)
            gold = [l for l in leads if (l.get("tier") or "").lower() == "gold"]
            gold_leads.extend(gold[:2])
    except Exception as exc:
        logger.warning("stripe_recovery day3 lead fetch failed sub=%s: %s", sub.id, exc)

    gold_count = len(gold_leads)
    lead_lines = "\n".join(
        f"  • {l.get('address', 'Undisclosed address')} ({l.get('zip', '')})"
        for l in gold_leads[:3]
    ) or "  • Leads available in your territory"

    subject = f"{gold_count or 'New'} Gold leads in your ZIP you can't see"
    body_text = (
        f"Hi {name},\n\n"
        f"Your payment is still past due and {gold_count} new Gold leads have appeared "
        f"in your locked territory that you're missing:\n\n"
        f"{lead_lines}\n\n"
        f"Fix your billing now to regain access:\n{feed_url}\n\n"
        f"— Forced Action Team"
    )
    body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:40px;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <h2 style="color:#fbbf24;">You're missing {gold_count} Gold leads, {name}</h2>
  <p>Your payment is past due. New leads in your territory:</p>
  <pre style="background:#1e293b;padding:12px;border-radius:8px;">{lead_lines}</pre>
  <p><a href="{feed_url}" style="color:#38bdf8;font-weight:bold;">Fix billing to unlock access →</a></p>
  <p style="color:#94a3b8;font-size:12px;">Questions? support@forcedaction.io</p>
</body>
</html>"""
    send_email(
        to_address=sub.email,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
    )
    logger.info("stripe_recovery day3 sent sub=%s gold_count=%d", sub.id, gold_count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
