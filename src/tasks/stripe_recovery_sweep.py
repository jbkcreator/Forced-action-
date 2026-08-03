"""
Stripe payment-failure recovery sweep.

Day 1: soft reminder (~24h after failure).
Day 3: urgency message + missed Gold-lead count (~72h after failure).
Day 5: downgrade/save offer — pivot away from payment retry, offer Data-Only plan.

Cron: 0 16 * * * (daily 16:00 UTC, after proactive_save 15:00).

Flags reset automatically on invoice.payment_succeeded.
"""
import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from config.revenue_ladder import DATA_ONLY_TIER
from src.core.database import get_db_context
from src.core.models import Subscriber
from src.services.claude_router import call_claude_with_usage
from src.utils.prompt_loader import get_prompt

logger = logging.getLogger(__name__)

DAY1_MIN = timedelta(hours=20)
DAY1_MAX = timedelta(hours=28)
DAY3_MIN = timedelta(days=2, hours=20)
DAY3_MAX = timedelta(days=3, hours=4)
DAY5_MIN = timedelta(days=4, hours=20)
DAY5_MAX = timedelta(days=5, hours=4)


def run(dry_run: bool = False) -> dict:
    now = datetime.now(timezone.utc)
    sent = {"day1": 0, "day3": 0, "day5": 0, "skipped": 0, "errors": 0}

    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(Subscriber.payment_failed_at.is_not(None))
        ).scalars().all()

        for sub in subs:
            try:
                elapsed = now - sub.payment_failed_at.replace(tzinfo=timezone.utc)
                if DAY1_MIN <= elapsed <= DAY1_MAX and not sub.recovery_day1_sent:
                    if not dry_run:
                        _send_day1(sub, db)
                        sub.recovery_day1_sent = True
                    sent["day1"] += 1
                elif DAY3_MIN <= elapsed <= DAY3_MAX and not sub.recovery_day3_sent:
                    if not dry_run:
                        _send_day3(sub, db)
                        sub.recovery_day3_sent = True
                    sent["day3"] += 1
                elif DAY5_MIN <= elapsed <= DAY5_MAX and not sub.recovery_day5_sent:
                    if not dry_run:
                        _send_day5(sub, db)
                        sub.recovery_day5_sent = True
                    sent["day5"] += 1
                else:
                    sent["skipped"] += 1
            except Exception as exc:
                logger.error("stripe_recovery_sweep error sub=%s: %s", sub.id, exc)
                sent["errors"] += 1

        if not dry_run:
            db.commit()

    logger.info("[StripeRecoverySweep] %s dry_run=%s", sent, dry_run)
    return sent


def _parse_email(text: str) -> tuple[str, str]:
    lines = text.strip().splitlines()
    subject = next((l.replace("SUBJECT:", "").strip() for l in lines if l.startswith("SUBJECT:")), "")
    body_start = next((i for i, l in enumerate(lines) if l.startswith("BODY:")), None)
    body = "\n".join(lines[body_start + 1:]).strip() if body_start is not None else ""
    if not subject or len(subject) > 60 or not body:
        return "", ""
    return subject, body


def _send_day1(sub: Subscriber, db) -> None:
    from config.settings import get_settings
    from src.services.email import send_email

    settings = get_settings()
    name = sub.name or "there"
    feed_url = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        if sub.event_feed_uuid else settings.app_base_url
    )

    subject = ""
    body_text = ""
    try:
        system_prompt = get_prompt("emails/stripe_recovery.yaml", "day1.system")
        user_prompt = get_prompt(
            "emails/stripe_recovery.yaml", "day1.user",
            name=name, feed_url=feed_url,
        )
        result = call_claude_with_usage(
            task_type="email_copy",
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
            max_tokens=600,
            subscriber_id=sub.id,
            db=db,
        )
        subject, body_text = _parse_email(result["text"])
    except Exception as exc:
        logger.warning("stripe_recovery day1 Lifecycle composition failed sub=%s, using fallback: %s", sub.id, exc)

    if not subject or not body_text:
        subject = "Heads up — your card didn't go through"
        body_text = (
            f"Hi {name},\n\n"
            f"We weren't able to process your Forced Action payment. "
            f"Update your card to keep your territories locked:\n\n"
            f"{feed_url}\n\n"
            f"Questions? support@forcedactionleads.com\n\n— Forced Action Team"
        )

    send_email(to=sub.email, subject=subject, body_text=body_text)
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
    lead_list = "\n".join(
        f"  - {l.get('address', 'Undisclosed address')} ({l.get('zip', '')})"
        for l in gold_leads[:3]
    ) or "  - Leads available in your territory"

    subject = ""
    body_text = ""
    try:
        system_prompt = get_prompt("emails/stripe_recovery.yaml", "day3.system")
        user_prompt = get_prompt(
            "emails/stripe_recovery.yaml", "day3.user",
            name=name, gold_count=gold_count,
            lead_list=lead_list, feed_url=feed_url,
        )
        result = call_claude_with_usage(
            task_type="email_copy",
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
            max_tokens=900,
            subscriber_id=sub.id,
            db=db,
        )
        subject, body_text = _parse_email(result["text"])
    except Exception as exc:
        logger.warning("stripe_recovery day3 Lifecycle composition failed sub=%s, using fallback: %s", sub.id, exc)

    if not subject or not body_text:
        subject = f"{gold_count or 'New'} Gold leads in your ZIP you can't see"
        body_text = (
            f"Hi {name},\n\n"
            f"Your payment is still past due and {gold_count} new Gold leads have appeared "
            f"in your locked territory that you're missing:\n\n"
            f"{lead_list}\n\n"
            f"Fix your billing now to regain access:\n{feed_url}\n\n"
            f"— Forced Action Team"
        )

    send_email(to=sub.email, subject=subject, body_text=body_text)
    logger.info("stripe_recovery day3 sent sub=%s gold_count=%d", sub.id, gold_count)


def _send_day5(sub: Subscriber, db) -> None:
    from config.settings import get_settings
    from src.services.email import send_email

    settings = get_settings()
    name = sub.name or "there"
    price = DATA_ONLY_TIER["price_cents"] // 100
    feed_url = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        if sub.event_feed_uuid else settings.app_base_url
    )

    subject = ""
    body_text = ""
    try:
        system_prompt = get_prompt("emails/stripe_recovery.yaml", "day5_downgrade.system")
        user_prompt = get_prompt(
            "emails/stripe_recovery.yaml", "day5_downgrade.user",
            name=name, price=price, feed_url=feed_url,
        )
        result = call_claude_with_usage(
            task_type="email_copy",
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
            max_tokens=800,
            subscriber_id=sub.id,
            db=db,
        )
        subject, body_text = _parse_email(result["text"])
    except Exception as exc:
        logger.warning("stripe_recovery day5 Lifecycle composition failed sub=%s, using fallback: %s", sub.id, exc)

    if not subject or not body_text:
        subject = f"Stay on Forced Action for ${price}/mo — Data-Only plan"
        body_text = (
            f"Hi {name},\n\n"
            f"Your payment has been past due for 5 days. We'd rather keep you than lose you.\n\n"
            f"Our Data-Only plan lets you keep your territory and full property data feed "
            f"at just ${price}/mo — no enrichment fees, cancel anytime.\n\n"
            f"Switch now:\n{feed_url}\n\n"
            f"Questions? support@forcedactionleads.com\n\n— Forced Action Team"
        )

    send_email(to=sub.email, subject=subject, body_text=body_text)
    logger.info("stripe_recovery day5 sent sub=%s", sub.id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
