"""
Human Close SLA Sweep — daily alert for unclaimed escalations past 24 hours.

Finds open escalations with no closer_assigned that were routed between 24h and
48h ago. The 24–48h window means each escalation triggers exactly one re-alert.
Posts a Slack message per breach so unclaimed leads don't silently age out.

Runs daily at 14:00 UTC — 1 hour after the 13:00 weekday sweep, so any lead
escalated at 13:00 the previous day (now 25h old) is caught on the first tick.
"""

import logging
from datetime import datetime, timedelta, timezone

import requests
from sqlalchemy import select, text as sa_text

from config.settings import get_settings
from src.core.database import get_db_context
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def run() -> None:
    settings = get_settings()
    webhook = settings.slack_human_close_webhook
    if not webhook:
        logger.info("[sla_sweep] no slack_human_close_webhook configured, skipping")
        return

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=48)
    window_end = now - timedelta(hours=24)

    with get_db_context() as db:
        rows = db.execute(sa_text("""
            SELECT
                hce.id,
                hce.subscriber_id,
                hce.revenue_signal_score,
                hce.target_tier,
                hce.vertical,
                hce.routed_at,
                s.name AS subscriber_name
            FROM human_close_escalations hce
            LEFT JOIN subscribers s ON s.id = hce.subscriber_id
            WHERE hce.closer_assigned IS NULL
              AND hce.outcome IS NULL
              AND hce.routed_at >= :window_start
              AND hce.routed_at < :window_end
            ORDER BY hce.revenue_signal_score DESC
        """), {"window_start": window_start, "window_end": window_end}).fetchall()

    if not rows:
        logger.info("[sla_sweep] no SLA breaches found")
        return

    logger.warning("[sla_sweep] %d unclaimed escalation(s) past 24h SLA", len(rows))

    for row in rows:
        hours_old = int((now - row.routed_at.replace(tzinfo=timezone.utc)).total_seconds() / 3600)
        name = row.subscriber_name or f"Sub #{row.subscriber_id}"
        tier = (row.target_tier or "unknown").replace("_", " ").title()
        vertical = (row.vertical or "unknown").replace("_", " ").title()

        payload = {
            "text": f":warning: *SLA BREACH — {name} unclaimed for {hours_old}h*",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f":warning: *SLA BREACH* — escalation #{row.id} has had no closer assigned for *{hours_old} hours*.\n"
                            f"*{name}* · RSS {row.revenue_signal_score}/100 · {tier} · {vertical}\n"
                            f"_Routed at {row.routed_at.strftime('%Y-%m-%d %H:%M')} UTC — action required._"
                        ),
                    },
                }
            ],
        }
        try:
            resp = requests.post(webhook, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("[sla_sweep] alerted escalation_id=%d subscriber=%s", row.id, name)
        except Exception:
            logger.error("[sla_sweep] failed to post Slack for escalation_id=%d", row.id, exc_info=True)


if __name__ == "__main__":
    run()
