"""Post the after-hours voicemail callers to call first, at 08:55 ET (split T-06, Runbook §5.3 Layer 2).

A caller who left an after-hours voicemail is pinned (``src.lending.after_hours_intake``). This posts, once a morning
before the 09:00 shift, every contact still pinned: not called back yet and within its 24-hour pin. Goes to Cora's
channel (``LENDING_CORA_CHANNEL``) with Cora's bot token, where the reply drafts wait for Approve. Cron fires at 12:55
and 13:55 UTC; only the one that is 8:xx ET acts.

Shows the last four digits of the phone only, never the full number.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

from config.lending_after_hours import MORNING_POST_HOUR_ET
from config.lending_reminders import TIMEZONE
from config.settings import get_settings
from src.lending.after_hours_intake import active_pins
from src.lending.db import lending_session

logger = logging.getLogger(__name__)


def _line(row: Mapping[str, Any]) -> str:
    left = row["pinned_at"].astimezone(TIMEZONE)
    return (f"• contact:{row['id']} (…{row['phone'][-4:]}): voicemail {left:%a} "
            f"{left.hour % 12 or 12}:{left:%M} {'am' if left.hour < 12 else 'pm'} ET")


def format_slack(rows: Sequence[Mapping[str, Any]]) -> Optional[str]:
    """The Slack message, or None when nobody is pinned."""
    if not rows:
        return None
    return (f"*Call these first ({len(rows)}): after-hours voicemails*\n" + "\n".join(_line(row) for row in rows)
            + "\nTheir reply texts are waiting for Approve in this channel.")


def main(argv=None, *, now: Optional[datetime] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="post regardless of the ET hour")
    args = parser.parse_args(argv)
    now = now or datetime.now(timezone.utc)
    if now.astimezone(TIMEZONE).hour != MORNING_POST_HOUR_ET and not args.force:
        return 0
    settings = get_settings()
    channel, token = settings.lending_cora_channel, settings.lending_cora_slack_bot_token
    if not (channel and token):
        logger.error("[pinned-callbacks] not posted: LENDING_CORA_CHANNEL or LENDING_CORA_SLACK_BOT_TOKEN is not set")
        return 1
    try:
        with lending_session() as db:
            message = format_slack(active_pins(db, now))
        if message is None:
            logger.info("[pinned-callbacks] no pinned callers")
            return 0
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(channel=channel, text=message)
    except Exception as exc:  # class only: rows carry phone digits
        logger.error("[pinned-callbacks] post failed (%s)", type(exc).__name__)
        return 1
    logger.info("[pinned-callbacks] posted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
