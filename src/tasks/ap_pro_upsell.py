"""
AutoPilot Pro upsell — Stage 5.

Daily cron that finds AP Lite subscribers who:
  * have been on Lite for >= AP_PRO_UPSELL.min_days_on_lite (default 30)
  * have a 30-day close rate (DealOutcome / SentLead) >= close_rate_threshold (0.15)
  * have NOT received an `ap_pro_upsell` MessageOutcome in the last 14 days

For each qualifier, send the email upsell + tag GHL contact `ap_pro_upsell`
so the GHL workflow handles 5-touch follow-up. Subscriber.tier flip happens
when the subscriber accepts via `POST /api/upgrade` (which calls
`switch_subscription_plan`).

Run via `python -m src.tasks.ap_pro_upsell [--dry-run]`.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.revenue_ladder import AP_PRO_UPSELL
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import DealOutcome, MessageOutcome, SentLead, Subscriber

logger = logging.getLogger(__name__)


def _close_rate(subscriber_id: int, db: Session, window_days: int = 30) -> float:
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    sent = db.execute(
        select(func.count()).select_from(SentLead).where(
            SentLead.subscriber_id == subscriber_id,
            SentLead.sent_at >= cutoff,
        )
    ).scalar() or 0
    if sent == 0:
        return 0.0
    deals = db.execute(
        select(func.count()).select_from(DealOutcome).where(
            DealOutcome.subscriber_id == subscriber_id,
            DealOutcome.deal_size_bucket != "skip",
            DealOutcome.created_at >= cutoff,
        )
    ).scalar() or 0
    return float(deals) / float(sent)


def _was_offered_recently(subscriber_id: int, db: Session) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(days=AP_PRO_UPSELL["offer_cooldown_days"])
    found = db.execute(
        select(MessageOutcome.id).where(
            MessageOutcome.subscriber_id == subscriber_id,
            MessageOutcome.template_id == "ap_pro_upsell",
            MessageOutcome.sent_at >= cutoff,
        ).limit(1)
    ).scalar_one_or_none()
    return found is not None


def _send_offer(sub: Subscriber, close_rate: float, db: Session) -> bool:
    if not sub.email:
        return False
    feed_url = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        if sub.event_feed_uuid else settings.app_base_url
    )
    upgrade_url = f"{settings.app_base_url}/api/upgrade?feed_uuid={sub.event_feed_uuid}&tier=autopilot_pro" \
        if sub.event_feed_uuid else feed_url

    rate_pct = round(close_rate * 100)
    try:
        from src.services.email import send_email
        send_email(
            to=sub.email,
            subject="You earned AutoPilot Pro - 5-touch + appointment setting",
            body_text=(
                f"Hi {sub.name or 'there'},\n\n"
                f"You're closing at {rate_pct}% on AutoPilot Lite. That's the threshold "
                f"where 5-touch sequences + appointment setting start paying back.\n\n"
                f"AutoPilot Pro adds:\n"
                f"  - 5-touch outbound sequences per lead\n"
                f"  - Premium routing (Immediate-tier leads first)\n"
                f"  - Appointment setting handled by Cora\n\n"
                f"Upgrade with one tap:  {upgrade_url}\n"
                f"Or open your dashboard: {feed_url}\n\n"
                f"- Forced Action Team"
            ),
        )
    except Exception as exc:
        logger.error("[APProUpsell] email send failed for sub=%d: %s", sub.id, exc)
        return False

    # Tag the GHL contact so the workflow handles follow-up
    try:
        if sub.ghl_contact_id:
            from src.services.synthflow_service import _apply_tags_to_contact
            _apply_tags_to_contact(sub.ghl_contact_id, ["ap_pro_upsell"])
    except Exception as exc:
        logger.warning("[APProUpsell] GHL tag failed for sub=%d: %s", sub.id, exc)

    db.add(MessageOutcome(
        subscriber_id=sub.id,
        message_type="email",
        template_id="ap_pro_upsell",
        channel="ses",
        sent_at=datetime.now(timezone.utc),
    ))
    db.flush()
    return True


def run(dry_run: bool = False) -> dict:
    stats = {"checked": 0, "qualified": 0, "offered": 0, "skipped_recent": 0, "errors": 0}
    cutoff = datetime.now(timezone.utc) - timedelta(days=AP_PRO_UPSELL["min_days_on_lite"])

    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(
                Subscriber.tier == "autopilot_lite",
                Subscriber.status == "active",
                Subscriber.created_at <= cutoff,
            )
        ).scalars().all()

        for sub in subs:
            stats["checked"] += 1
            try:
                if _was_offered_recently(sub.id, db):
                    stats["skipped_recent"] += 1
                    continue
                rate = _close_rate(sub.id, db)
                if rate < AP_PRO_UPSELL["close_rate_threshold"]:
                    continue
                stats["qualified"] += 1
                if dry_run:
                    logger.info("[APProUpsell] DRY-RUN would offer sub=%d rate=%.2f", sub.id, rate)
                    continue
                if _send_offer(sub, rate, db):
                    stats["offered"] += 1
            except Exception as exc:
                logger.error("[APProUpsell] sub=%d error: %s", sub.id, exc)
                stats["errors"] += 1

    logger.info("[APProUpsell] %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
