"""
Revenue Pulse — Items 9 + 18.

Daily + weekly founder SMS with platform health snapshot.

Daily (7:30 AM UTC): lead count, wallet actives, top deal, top alert, kill switch
Weekly (Monday 9 AM UTC): revenue est, new/churned subs, kill switch, top learning

Cron lines (add to scripts/cron/crontab.txt):
  30 7 * * *    cd /opt/forced-action && python -m src.tasks.revenue_pulse --daily
  0 9 * * 1     cd /opt/forced-action && python -m src.tasks.revenue_pulse --weekly
"""
import logging
import sys
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.revenue_pulse import (
    DAILY_PULSE_TEMPLATE,
    KILL_SWITCH_LEVELS,
    MAX_DAILY_SMS_CHARS,
    WEEKLY_PULSE_TEMPLATE,
)
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import (
    DealOutcome,
    DistressScore,
    LearningCard,
    Subscriber,
    UserSegment,
    WalletBalance,
)

logger = logging.getLogger(__name__)


def run_daily_pulse(dry_run: bool = False) -> dict:
    """Compose and optionally send the daily founder SMS."""
    with get_db_context() as db:
        msg = _compose_daily(db)

    logger.info("[RevenuePulse] Daily message: %r", msg)

    if not dry_run:
        sent = _send_sms(msg)
        return {"sent": sent, "message": msg}
    return {"sent": False, "dry_run": dry_run, "message": msg}


def run_weekly_pulse(dry_run: bool = False) -> dict:
    """Compose and optionally send the weekly founder SMS."""
    with get_db_context() as db:
        msg = _compose_weekly(db)

    logger.info("[RevenuePulse] Weekly message: %r", msg)

    if not dry_run:
        sent = _send_sms(msg)
        return {"sent": sent, "message": msg}
    return {"sent": False, "dry_run": dry_run, "message": msg}


def _compose_daily(db: Session) -> str:
    today = date.today()

    lead_count = db.execute(
        select(func.count(DistressScore.id)).where(
            DistressScore.score_date >= datetime.combine(today, datetime.min.time()),
            DistressScore.qualified == True,  # noqa: E712
        )
    ).scalar_one_or_none() or 0

    wallet_active = db.execute(
        select(func.count(WalletBalance.id)).where(
            WalletBalance.credits_remaining > 0
        )
    ).scalar_one_or_none() or 0

    top_deal = db.execute(
        select(DealOutcome)
        .where(
            DealOutcome.deal_date == today,
            DealOutcome.deal_size_bucket != "skip",
        )
        .order_by(DealOutcome.deal_amount.desc().nullslast())
        .limit(1)
    ).scalar_one_or_none()
    top_deal_str = (
        f"${int(top_deal.deal_amount):,}" if top_deal and top_deal.deal_amount
        else "no deals"
    )

    card = db.execute(
        select(LearningCard).order_by(LearningCard.card_date.desc()).limit(1)
    ).scalar_one_or_none()
    alert_str = (card.summary_text[:55] + "…") if card and len(card.summary_text) > 55 else (card.summary_text if card else "no alerts")

    kill = _kill_switch_status(db)

    return DAILY_PULSE_TEMPLATE.format(
        date=today.strftime("%m/%d").lstrip("0").replace("/0", "/") if hasattr(today, "strftime") else str(today),
        lead_count=lead_count,
        wallet_active=wallet_active,
        top_deal=top_deal_str,
        alert=alert_str,
        kill_switch=kill["status"],
    )


def _compose_weekly(db: Session) -> str:
    now = datetime.now(timezone.utc)
    week_start = now - timedelta(days=now.weekday() + 7)

    new_subs = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.created_at >= week_start,
            Subscriber.status == "active",
        )
    ).scalar_one_or_none() or 0

    churned = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.updated_at >= week_start,
            Subscriber.status.in_(["churned", "cancelled"]),
        )
    ).scalar_one_or_none() or 0

    active_count = db.execute(
        select(func.count(Subscriber.id)).where(Subscriber.status == "active")
    ).scalar_one_or_none() or 0
    est_revenue = active_count * 800

    kill = _kill_switch_status(db)

    card = db.execute(
        select(LearningCard).order_by(LearningCard.card_date.desc()).limit(1)
    ).scalar_one_or_none()
    learning_str = card.summary_text[:75] if card else "no card"

    return WEEKLY_PULSE_TEMPLATE.format(
        week=now.strftime("%W"),
        revenue=f"{est_revenue:,}",
        new_subs=new_subs,
        churned=churned,
        kill_switch=kill["status"],
        kill_label=kill["label"],
        learning=learning_str,
    )


def _kill_switch_status(db: Session) -> dict:
    avg_score = db.execute(
        select(func.avg(UserSegment.revenue_signal_score))
    ).scalar_one_or_none() or 0

    total = db.execute(
        select(func.count(Subscriber.id))
        .where(Subscriber.status.in_(["active", "churned", "cancelled"]))
    ).scalar_one_or_none() or 1

    churned = db.execute(
        select(func.count(Subscriber.id))
        .where(Subscriber.status.in_(["churned", "cancelled"]))
    ).scalar_one_or_none() or 0

    churn_pct = (churned / total) * 100

    for level in KILL_SWITCH_LEVELS:
        if float(avg_score) >= level["min_avg_revenue_score"] and churn_pct <= level["max_churn_rate_pct"]:
            return {"status": level["status"], "label": level["label"]}
    return {"status": "RED", "label": "investigate"}


def _send_sms(message: str) -> bool:
    """
    Send pulse SMS to the founder's phone.

    Previously bypassed the compliance gate by calling Twilio directly.
    Now routes through src.services.sms_compliance.send_sms so the founder
    SMS respects opt-out + TCPA quiet hours like every other outbound
    message. If the founder ever does opt out, that intent is honoured.

    Returns True if Twilio accepted the message, False otherwise
    (no phone configured, compliance suppression, or send error).
    """
    phone = settings.founder_phone
    if not phone:
        logger.warning("[RevenuePulse] FOUNDER_PHONE not set — SMS skipped")
        return False
    from src.core.database import get_db_context
    from src.services.sms_compliance import send_sms
    try:
        with get_db_context() as db:
            sent = send_sms(
                to=phone,
                body=message[:MAX_DAILY_SMS_CHARS],
                db=db,
                task_type="revenue_pulse",
                campaign="revenue_pulse",
            )
        if sent:
            logger.info("[RevenuePulse] SMS sent to founder")
        else:
            logger.info("[RevenuePulse] SMS suppressed by compliance gate (opt-out / quiet hours / not configured)")
        return bool(sent)
    except Exception as exc:
        logger.error("[RevenuePulse] SMS failed: %s", exc)
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if "--daily" in sys.argv:
        result = run_daily_pulse(dry_run="--dry-run" in sys.argv)
    elif "--weekly" in sys.argv:
        result = run_weekly_pulse(dry_run="--dry-run" in sys.argv)
    else:
        print("Usage: python -m src.tasks.revenue_pulse [--daily|--weekly] [--dry-run]")
        sys.exit(1)
    print(result)
