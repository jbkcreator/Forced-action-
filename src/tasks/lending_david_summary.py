"""Post David's end-of-day activity to Slack (LENDING_DAILY_CHANNEL), with the leads he flagged for Josh.

Posted at 7:xx ET with the closing scoreboard. Cron fires at 23:25 and 00:25 UTC; only the one that is 19:xx ET acts,
so daylight saving needs no change. Off until LENDING_DAVID_SUMMARY_ENABLED=true and LENDING_DAVID_GHL_USER_ID is set.

    python -m src.tasks.lending_david_summary [--force] [--dry-run]

``--dry-run`` logs the message instead of posting it.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from config.lending_compliance import DEFAULT_TZ
from config.lending_dispositions import DAVID_SUMMARY_HOUR_ET
from config.settings import get_settings
from src.lending.david_activity import build_activity, format_slack, ghl_transport
from src.lending.db import lending_session
from src.lending.ghl_account import lending_ghl_account
from src.lending.lead_arrival_alert import ghl_contact_url
from src.tasks.lending_daily_scoreboard import _post_with_retry

logger = logging.getLogger(__name__)


def main(argv=None, *, now: Optional[datetime] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="post regardless of the ET hour")
    parser.add_argument("--dry-run", action="store_true", help="log the message, do not post it")
    args = parser.parse_args(argv)
    now_et = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo(DEFAULT_TZ))
    if now_et.hour != DAVID_SUMMARY_HOUR_ET and not args.force:
        return 0
    s = get_settings()
    if not s.lending_david_summary_enabled:
        logger.info("[lending-david] summary off (LENDING_DAVID_SUMMARY_ENABLED is not true)")
        return 0
    user_id = s.lending_david_ghl_user_id.strip()
    account = lending_ghl_account()
    if not user_id or account is None:
        logger.error("[lending-david] summary not built: LENDING_DAVID_GHL_USER_ID or the lending GHL account is not set")
        return 1
    if not args.dry_run and not (s.lending_daily_channel and s.lending_slack_bot_token):
        logger.error("[lending-david] summary not posted: Slack channel or token is not configured")
        return 1
    from src.services.ghl_webhook import _GHL_BASE

    day = now_et.date()
    get, post, headers = ghl_transport(account.api_key)
    try:
        with lending_session() as db:
            activity = build_activity(db, day, user_id=user_id, flagged_tag=s.lending_flagged_tag.strip(), get=get,
                                      post=post, base=_GHL_BASE, headers=headers, location_id=account.location_id)
        message = format_slack(activity, lambda cid: ghl_contact_url(s.lending_ghl_app_url, account.location_id, cid))
        if args.dry_run:
            logger.info("[lending-david] dry run for %s:\n%s", day, message)
            return 0
        from slack_sdk import WebClient
        _post_with_retry(WebClient(token=s.lending_slack_bot_token.get_secret_value()), s.lending_daily_channel,
                         message, [{"type": "section", "text": {"type": "mrkdwn", "text": message}}])
    except Exception:
        logger.exception("[lending-david] summary post failed for %s", day)
        return 1
    logger.info("[lending-david] summary posted for %s", day)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
