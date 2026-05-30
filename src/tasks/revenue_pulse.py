"""
Revenue Pulse — Items 9 + 18.

Daily + weekly founder SMS with platform health snapshot.

Daily (7:30 AM UTC): lead count, wallet actives, top deal, top alert, kill switch
Weekly (Monday 9 AM UTC): revenue est, new/churned subs, kill switch, top learning

Cron lines (add to scripts/cron/crontab.txt):
  30 7 * * *    cd /opt/forced-action && python -m src.tasks.revenue_pulse --daily
  0 9 * * 1     cd /opt/forced-action && python -m src.tasks.revenue_pulse --weekly
"""
from langchain_core.messages import ChatMessage
import logging
import sys
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select, text as sa_text
from sqlalchemy.orm import Session

from config.revenue_pulse import (
    DAILY_PULSE_TEMPLATE,
    KILL_SWITCH_LEVELS,
    MAX_DAILY_SMS_CHARS,
    VENDOR_COST_LINE_MAX_CHARS,
    WEEKLY_PULSE_TEMPLATE,
)
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import (
    ChatMessage,
    ChatSession,
    DealOutcome,
    DistressScore,
    LearningCard,
    Subscriber,
    UserSegment,
    WalletBalance,
)
from src.services.vendor_cost_report import build_vendor_cost_summary, format_sms_cost_summary

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

    # fa034: the "alert" slot in the daily pulse prefers an unresolved
    # Cora incident over a learning-card snippet. If no incidents are open,
    # fall back to the latest learning card as before.
    alert_str = _format_cora_incident_alert(db)
    if alert_str is None:
        card = db.execute(
            select(LearningCard).order_by(LearningCard.card_date.desc()).limit(1)
        ).scalar_one_or_none()
        alert_str = (
            (card.summary_text[:55] + "…") if card and len(card.summary_text) > 55
            else (card.summary_text if card else "no alerts")
        )

    kill = _kill_switch_status(db)
    chat = _chat_metrics_today(db)

    # Vendor cost summary line (Phase 3)
    vendor_cost_line = ""
    try:
        vc_summary = build_vendor_cost_summary(db)
        cost_text = format_sms_cost_summary(vc_summary)
        if cost_text:
            vendor_cost_line = cost_text[:VENDOR_COST_LINE_MAX_CHARS] + "\n"
    except Exception as exc:
        logger.warning("[RevenuePulse] Vendor cost summary failed: %s", exc)
        
    msg = DAILY_PULSE_TEMPLATE.format(
        date=today.strftime("%m/%d").lstrip("0").replace("/0", "/") if hasattr(today, "strftime") else str(today),
        lead_count=lead_count,
        wallet_active=wallet_active,
        top_deal=top_deal_str,
        alert=alert_str,
        vendor_cost=vendor_cost_line,
        kill_switch=kill["status"],
    )
    if chat["sessions"] > 0:
        msg += (
            f"\nChat: {chat['sessions']}sess "
            f"{chat['intent_detected']}int "
            f"{chat['payment_triggered']}paid"
        )
    return msg


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

    body = WEEKLY_PULSE_TEMPLATE.format(
        week=now.strftime("%W"),
        revenue=f"{est_revenue:,}",
        new_subs=new_subs,
        churned=churned,
        kill_switch=kill["status"],
        kill_label=kill["label"],
        learning=learning_str,
    )

    # fa034: append a one-line Cora incidents summary if there's room.
    incidents_line = _format_cora_incidents_weekly_summary(db)
    if incidents_line:
        candidate = f"{body}\nIncidents 7d: {incidents_line}"
        if len(candidate) <= MAX_DAILY_SMS_CHARS:
            body = candidate

    # fa036: append a one-line Cora autonomy summary from the latest
    # autonomy_summary learning card (written by cora_autonomy_report at
    # Monday 08:45 UTC, 15 min before this task at 09:00). Defensively
    # truncated if it would blow the SMS budget — same pattern as the
    # incidents line.
    autonomy_line = _format_cora_autonomy_weekly_summary(db)
    if autonomy_line:
        candidate = f"{body}\n{autonomy_line}"
        if len(candidate) <= MAX_DAILY_SMS_CHARS:
            body = candidate

    # fa037: append the kill-switch scorecard alarm line (written by
    # kill_switch_scorecard at Monday 08:50 UTC, 5 min before this task).
    scorecard_line = _format_kill_switch_scorecard_line(db)
    if scorecard_line:
        candidate = f"{body}\n{scorecard_line}"
        if len(candidate) <= MAX_DAILY_SMS_CHARS:
            body = candidate
    return body


# ── fa034 helpers — pure raw SQL, no ORM ────────────────────────────────────

def _format_cora_incident_alert(db: Session) -> str | None:
    """Return a 140-char alert string built from the latest unresolved
    cora_incident, or None if no incident is open.

    Prioritizes severity=red over yellow, then most recent breach_started.
    """
    row = db.execute(sa_text("""
        SELECT metric_name, severity, observed_value, threshold_value,
               action_taken, breach_started, county_id
        FROM cora_incident
        WHERE breach_resolved IS NULL
        ORDER BY (severity = 'red') DESC, breach_started DESC
        LIMIT 1
    """)).first()
    if row is None:
        return None
    # Format compactly: "[RED] first_payment_rate 18 (thr 20) — fallback_enabled"
    sev = (row.severity or "?").upper()
    metric = row.metric_name or "?"
    obs = row.observed_value
    thr = row.threshold_value
    act = row.action_taken or "no_op"
    pieces = [f"[{sev}] {metric}"]
    if obs is not None and thr is not None:
        pieces.append(f"{obs} (thr {thr})")
    if act and act != "no_op":
        pieces.append(act)
    text = " — ".join(pieces)
    return text[:140]


def _format_cora_autonomy_weekly_summary(db: Session) -> str | None:
    """Return a one-line summary built from the latest autonomy_summary
    learning card (written by src/tasks/cora_autonomy_report.py), or None
    if no card exists yet.

    Honest about missing data: null metrics render as 'n/a', not '0' —
    so a fresh deploy with no classified decisions shows the gap clearly
    instead of faking a feel-good 0%.

    Example output:
        "Cora autonomy: 72% autonomous, 3% overridden, 4 adopted, +2 net playbooks"
        "Cora autonomy: n/a autonomous, n/a overridden, 0 adopted, +0 net playbooks"
    """
    row = db.execute(sa_text("""
        SELECT data_json FROM learning_cards
        WHERE card_type = 'autonomy_summary'
        ORDER BY card_date DESC
        LIMIT 1
    """)).first()
    if row is None or not row.data_json:
        return None
    d = row.data_json

    def pct(v):
        return f"{v}%" if v is not None else "n/a"

    net = d.get("net_new_playbooks", 0)
    return (
        f"Cora autonomy: {pct(d.get('autonomous_pct'))} autonomous, "
        f"{pct(d.get('overridden_pct'))} overridden, "
        f"{d.get('recommended_adoptions', 0)} adopted, "
        f"{net:+d} net playbooks"
    )


def _format_cora_incidents_weekly_summary(db: Session) -> str | None:
    """Return a one-line counts summary or None if no incident activity
    in the last 7 days.

    Example output: "2 red / 4 yellow open, 3 resolved, 1 kill-pending"
    """
    row = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (WHERE severity='red'    AND breach_resolved IS NULL) AS red_open,
            COUNT(*) FILTER (WHERE severity='yellow' AND breach_resolved IS NULL) AS yellow_open,
            COUNT(*) FILTER (WHERE breach_resolved >= NOW() - INTERVAL '7 days') AS resolved_7d,
            COUNT(*) FILTER (WHERE action_taken='feature_killed'
                            AND created_at >= NOW() - INTERVAL '7 days')        AS kill_pending_7d
        FROM cora_incident
    """)).first()
    if row is None:
        return None
    total = (row.red_open or 0) + (row.yellow_open or 0) + (row.resolved_7d or 0) + (row.kill_pending_7d or 0)
    if total == 0:
        return None
    return (
        f"{row.red_open or 0} red / {row.yellow_open or 0} yellow open, "
        f"{row.resolved_7d or 0} resolved, {row.kill_pending_7d or 0} kill-pending"
    )


def _format_kill_switch_scorecard_line(db: Session) -> str | None:
    """Return the compact KS alarm line from the latest kill_switch_scorecard.

    Returns None if no card exists or every county is all-green.
    Format: "KS: hills 🔴1 🟡2 (lock_conv red 5/7d→kill rec) | pinellas 🟢 all"
    """
    row = db.execute(sa_text("""
        SELECT data_json FROM learning_cards
        WHERE card_type = 'kill_switch_scorecard'
        ORDER BY card_date DESC
        LIMIT 1
    """)).first()
    if row is None or not row.data_json:
        return None

    data = row.data_json
    counties = data.get("counties", {})
    if not counties:
        return None

    parts = []
    any_problem = False

    for county_id, cdata in counties.items():
        summary = cdata.get("summary", {})
        red_n = summary.get("red", 0)
        yellow_n = summary.get("yellow", 0)

        if red_n == 0 and yellow_n == 0:
            parts.append(f"{county_id[:6]} 🟢 all")
            continue

        any_problem = True
        counts_str = ""
        if red_n:
            counts_str += f"🔴{red_n}"
        if yellow_n:
            counts_str += f" 🟡{yellow_n}"
        counts_str = counts_str.strip()

        features = cdata.get("features", [])
        worst = _pick_worst_feature_for_line(features)
        worst_str = ""
        if worst:
            streak = worst.get("red_streak")
            streak_str = f"{streak}/7d" if streak is not None else "n/a"
            kill_flag = "→kill rec" if worst.get("kill_rec_pending") else ""
            suffix = f" {kill_flag}".rstrip()
            worst_str = f" ({worst['metric']} red {streak_str}{suffix})"

        parts.append(f"{county_id[:6]} {counts_str}{worst_str}")

    if not any_problem:
        return None

    return "KS: " + " | ".join(parts)


def _pick_worst_feature_for_line(features: list) -> dict | None:
    """Red beats yellow; within same color, longest streak wins."""
    reds = [f for f in features if f.get("current_color") == "red"]
    yellows = [f for f in features if f.get("current_color") == "yellow"]
    candidates = reds or yellows
    if not candidates:
        return None
    return max(candidates, key=lambda f: (f.get("red_streak") or 0))


def _chat_metrics_today(db: Session) -> dict:
    """Return Concierge Chat funnel metrics for today."""
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        sessions = db.execute(
            select(func.count(ChatSession.id)).where(
                ChatSession.created_at >= today_start
            )
        ).scalar_one_or_none() or 0

        intent_detected = db.execute(
            select(func.count(ChatMessage.id)).where(
                ChatMessage.created_at >= today_start,
                ChatMessage.role == "assistant",
                ChatMessage.intent_label.notin_(["none", "pricing_question", "coverage_question", "support_question", "comparison_question"]),
                ChatMessage.intent_confidence >= 0.85,
            )
        ).scalar_one_or_none() or 0

        payment_triggered = db.execute(
            select(func.count(ChatMessage.id)).where(
                ChatMessage.created_at >= today_start,
                ChatMessage.payment_trigger_json.isnot(None),
            )
        ).scalar_one_or_none() or 0

        return {
            "sessions": sessions,
            "intent_detected": intent_detected,
            "payment_triggered": payment_triggered,
        }
    except Exception as exc:
        logger.warning("[RevenuePulse] chat metrics failed: %s", exc)
        return {"sessions": 0, "intent_detected": 0, "payment_triggered": 0}


def _chat_metrics_today(db: Session) -> dict:
    """Return Concierge Chat funnel metrics for today."""
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        sessions = db.execute(
            select(func.count(ChatSession.id)).where(
                ChatSession.created_at >= today_start
            )
        ).scalar_one_or_none() or 0

        assistant_replies = db.execute(
            select(func.count(ChatMessage.id)).where(
                ChatMessage.created_at >= today_start,
                ChatMessage.role == "assistant",
            )
        ).scalar_one_or_none() or 0

        return {
            "sessions": sessions,
            "intent_detected": 0,
            "payment_triggered": 0,
            "assistant_replies": assistant_replies,
        }
    except Exception as exc:
        logger.warning("[RevenuePulse] chat metrics failed: %s", exc)
        return {"sessions": 0, "intent_detected": 0, "payment_triggered": 0}


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
                message_type="transactional",
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
