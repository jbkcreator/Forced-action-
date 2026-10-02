"""Post the open booking-confirmation calls to Slack at 9:00am ET (WP-GL-10).

``lending.confirmation_tasks`` is the list of calls a caller must make to confirm a booked call. Nothing
else surfaces it, so this posts, once a morning, every task that is due today or overdue (up to
``LOOKBACK_DAYS``) for a call that has not started yet, grouped by assignee. A task drops off by itself
once its call starts. Cron fires at 13:00 and 14:00 UTC; only the one that is 9:xx ET acts.

Shows first name and the last four digits of the phone only, never the full number.
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import text

from config.lending_reminders import TIMEZONE
from config.settings import get_settings
from src.lending.db import lending_session

logger = logging.getLogger(__name__)

POST_HOUR_ET = 9
LOOKBACK_DAYS = 7

_OPEN_TASKS = text("""
    SELECT t.booking_ref, t.assignee, t.due_date, m.first_name, m.contact_phone, m.slot_start_utc
      FROM lending.confirmation_tasks t
      JOIN lending.booking_messages m ON m.booking_ref = t.booking_ref AND m.kind = 'confirmation'
     WHERE t.completed_at IS NULL
       AND t.due_date <= :today AND t.due_date >= :floor
       AND m.slot_start_utc > :now
     ORDER BY t.assignee, m.slot_start_utc
""")


def open_tasks(db, today: date, now: datetime) -> list[Mapping[str, Any]]:
    return list(db.execute(_OPEN_TASKS, {"today": today, "floor": today - timedelta(days=LOOKBACK_DAYS),
                                         "now": now}).mappings())


def _line(row: Mapping[str, Any], today: date) -> str:
    who = row["first_name"] or "Unknown contact"
    tail = f" (…{row['contact_phone'][-4:]})" if row["contact_phone"] else ""
    slot = row["slot_start_utc"].astimezone(TIMEZONE)
    overdue = f" — OVERDUE since {row['due_date']:%a %b} {row['due_date'].day}" if row["due_date"] < today else ""
    return (f"• {who}{tail}: call is {slot:%a %b} {slot.day} at {slot.hour % 12 or 12}:{slot:%M} "
            f"{'am' if slot.hour < 12 else 'pm'} ET{overdue}")


def format_slack(rows: Sequence[Mapping[str, Any]], today: date) -> Optional[str]:
    """The Slack message, or None when nothing is due."""
    if not rows:
        return None
    by_assignee: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        by_assignee[row["assignee"]].append(_line(row, today))
    sections = [f"*{assignee}*\n" + "\n".join(lines) for assignee, lines in by_assignee.items()]
    return f"*Confirmation calls due ({len(rows)})*\n\n" + "\n\n".join(sections)


def main(argv=None, *, now: Optional[datetime] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="post regardless of the ET hour")
    args = parser.parse_args(argv)
    now = now or datetime.now(timezone.utc)
    now_et = now.astimezone(TIMEZONE)
    if now_et.hour != POST_HOUR_ET and not args.force:
        return 0
    settings = get_settings()
    channel = settings.lending_dial_tasks_channel
    if not (channel and settings.lending_slack_bot_token):
        logger.error("[confirmation-tasks] not posted: LENDING_DIAL_TASKS_CHANNEL or the Slack token is not configured")
        return 1
    try:
        with lending_session() as db:
            message = format_slack(open_tasks(db, now_et.date(), now), now_et.date())
        if message is None:
            logger.info("[confirmation-tasks] no confirmation calls due")
            return 0
        from slack_sdk import WebClient
        WebClient(token=settings.lending_slack_bot_token.get_secret_value()).chat_postMessage(channel=channel, text=message)
    except Exception as exc:  # class only: rows carry phone digits
        logger.error("[confirmation-tasks] post failed (%s)", type(exc).__name__)
        return 1
    logger.info("[confirmation-tasks] posted for %s", now_et.date())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
