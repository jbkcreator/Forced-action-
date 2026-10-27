"""R-1 hold-queue Slack summary: posts at 8:00 AM ET.

Cron (two lines for DST):
  0 12 * * * $PROJECT/scripts/cron/run.sh src.tasks.lending_routing_hold_summary  # UTC-4 (EDT)
  0 13 * * * $PROJECT/scripts/cron/run.sh src.tasks.lending_routing_hold_summary  # UTC-5 (EST)

The task checks the current ET hour and exits silently if it is not 8:xx,
so both cron lines are safe year-round (only one will pass the check each day).

Reports:
  - Files with status 'held' (no lender matched).
  - Files with status 'exhausted' (all signed lenders declined).
"""
import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import text
from zoneinfo import ZoneInfo

from config.lending_routing import HOLD_SUMMARY_HOUR_ET, ROUTING_TZ

logger = logging.getLogger(__name__)


def _is_summary_hour(now_et: datetime) -> bool:
    return now_et.hour == HOLD_SUMMARY_HOUR_ET


def run_hold_summary() -> None:
    now_et = datetime.now(timezone.utc).astimezone(ROUTING_TZ)
    if not _is_summary_hour(now_et):
        logger.debug("lending_routing_hold_summary: not 8 AM ET (%s) — exiting", now_et.strftime("%H:%M %Z"))
        return

    from config.settings import get_settings
    from src.lending.db import lending_session

    settings = get_settings()
    token = settings.lending_cora_slack_bot_token
    channel = settings.lending_cora_channel
    if not token or not channel:
        logger.warning("lending_routing_hold_summary: Slack not configured — skipping")
        return

    from slack_sdk import WebClient

    client = WebClient(token=token.get_secret_value())

    with lending_session() as db:
        rows = db.execute(
            text(
                "SELECT fr.status, fr.hold_reason, COUNT(*) AS cnt"
                "  FROM lending.file_routing fr"
                " WHERE fr.status IN ('held', 'exhausted')"
                " GROUP BY fr.status, fr.hold_reason"
                " ORDER BY fr.status, cnt DESC"
            )
        ).mappings().all()

        total_held = sum(r["cnt"] for r in rows if r["status"] == "held")
        total_exhausted = sum(r["cnt"] for r in rows if r["status"] == "exhausted")

    if total_held == 0 and total_exhausted == 0:
        text_body = ":white_check_mark: *Hold queue*: empty — all files routed."
    else:
        lines = [":clipboard: *Hold queue daily summary* (8 AM ET)"]
        if total_held:
            lines.append(f"  • *Held (no lender matched)*: {total_held} file(s)")
        if total_exhausted:
            lines.append(f"  • *Exhausted (all lenders declined)*: {total_exhausted} file(s)")
        lines.append("  See them with `/lenders`, then Action: Show the hold queue.")
        text_body = "\n".join(lines)

    try:
        client.chat_postMessage(channel=channel, text=text_body)
        logger.info(
            "lending_routing_hold_summary: posted held=%d exhausted=%d", total_held, total_exhausted
        )
    except Exception:
        logger.exception("lending_routing_hold_summary: Slack post failed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_hold_summary()
    except Exception:
        logger.exception("lending_routing_hold_summary failed")
        sys.exit(1)
