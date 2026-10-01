"""Post the lending scoreboard to Slack at 7pm ET. Cron fires at 23:00 and 00:00 UTC; only the one that is 19:xx ET acts."""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Mapping, Optional
from zoneinfo import ZoneInfo

from config.lending_compliance import DEFAULT_TZ
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.dialer_port import get_http
from src.lending.scoreboard import build_scoreboard, format_slack

logger = logging.getLogger(__name__)


def campaign_names() -> Mapping[str, str]:
    http = get_http()
    if http is None:
        return {}
    try:
        rows = http("GET", "/campaigns", json=None)
        items = rows.get("items", []) if isinstance(rows, Mapping) else rows
        return {str(r["id"]): str(r["name"]) for r in items if "id" in r and "name" in r}
    except Exception:
        logger.warning("[lending] could not read campaign names; scoreboard uses ids", exc_info=True)
        return {}


def main(argv=None, *, now: Optional[datetime] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="post regardless of the ET hour")
    args = parser.parse_args(argv)
    now_et = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo(DEFAULT_TZ))
    if now_et.hour != 19 and not args.force:
        return 0
    s = get_settings()
    channel = s.lending_dial_tasks_channel
    if not (channel and s.lending_slack_bot_token):
        logger.error("[lending] scoreboard not posted: Slack channel or token is not configured")
        return 1
    with lending_session() as db:
        message = format_slack(build_scoreboard(db, now_et.date(), campaign_names()), now_et.date())
    from slack_sdk import WebClient
    WebClient(token=s.lending_slack_bot_token.get_secret_value()).chat_postMessage(channel=channel, text=message)
    logger.info("[lending] scoreboard posted for %s", now_et.date())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
