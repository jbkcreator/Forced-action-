"""R1 1.6: David's 6 AM dialer queue check, posted to LENDING_DAVID_CHANNEL (#isa-daily).

Counts the contacts David can actually call today, or names every dialer pool that ran dry. A short queue also goes
to the ops channel. Nothing is padded and nothing is pushed to BatchDialer.

Cron fires at 10:20 and 11:20 UTC, Mon-Fri; only the one that is 6:xx ET acts, so daylight saving needs no change. Off
until LENDING_DIALER_QUEUE_CHECK_ENABLED=true.

    python -m src.tasks.lending_dialer_queue_check [--force] [--dry-run]

``--force`` skips the hour and weekday checks. ``--dry-run`` reads and logs the post instead of sending it, and works
with the switch off.

Delivery is tracked per destination for the ET day (``src/lending/dialer/delivery_state.py``). A run that fails
returns 1; the cron retry (run.sh, +15 and +30 min) then sends only the message that is still missing, with the text
first composed, even after 7:00 ET. A finished day is never posted again. ``--force`` and ``--dry-run`` neither read
nor write that state.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from config.lending_compliance import DEFAULT_TZ
from config.lending_dialer_queue import QUEUE_CHECK_HOUR_ET, WORKDAYS
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.dialer import delivery_state
from src.lending.dialer.queue_manager import read_queue, render_queue_post
from src.lending.ops_alerts import post_ops_notice
from src.lending.slack_channels import BOT_TOKEN, report_unset
from src.tasks.lending_daily_scoreboard import _post_with_retry

logger = logging.getLogger(__name__)
STATE = "queue_check"


def _slack_poster(token: str, channel: str) -> Callable[[str], bool]:
    from slack_sdk import WebClient

    client = WebClient(token=token)

    def post(message: str) -> bool:
        _post_with_retry(client, channel, message, [{"type": "section", "text": {"type": "mrkdwn", "text": message}}])
        return True

    return post


def main(argv=None, *, now: Optional[datetime] = None, post: Optional[Callable[[str], bool]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="run regardless of the ET hour and weekday")
    parser.add_argument("--dry-run", action="store_true", help="log the post, do not send it")
    args = parser.parse_args(argv)
    now = now or datetime.now(timezone.utc)
    now_et = now.astimezone(ZoneInfo(DEFAULT_TZ))
    stateful = not (args.force or args.dry_run)
    state = delivery_state.load(STATE, now_et.date()) if stateful else None
    if state is not None and state.get("done"):
        return 0
    due = now_et.hour == QUEUE_CHECK_HOUR_ET and now_et.weekday() in WORKDAYS
    if not args.force and not due and state is None:  # a retry of today's unfinished run may act after 6:xx
        return 0
    s = get_settings()
    if not s.lending_dialer_queue_check_enabled and not args.dry_run:
        logger.info("[queue-check] off (LENDING_DIALER_QUEUE_CHECK_ENABLED is not true)")
        return 0
    channel = s.lending_david_channel.strip()
    if post is None and not args.dry_run:
        if not (channel and s.lending_slack_bot_token):
            report_unset("David's dialer queue check", ("LENDING_DAVID_CHANNEL", BOT_TOKEN))
            return 1
        post = _slack_poster(s.lending_slack_bot_token.get_secret_value(), channel)
    state = state or {}

    def remember() -> None:
        if stateful:
            delivery_state.save(STATE, now_et.date(), state)

    remember()  # marks today's run as started, so a retry after 6:xx still acts
    if "message" not in state:
        try:
            with lending_session() as db:
                report = read_queue(db, now, lead_days=s.lending_morning_list_lead_days,
                                    autoload_enabled=s.lending_pr_dialer_autoload_enabled)
        except Exception as exc:  # class only: SQL errors embed bound parameters
            logger.error("[queue-check] could not read the queue (%s)", type(exc).__name__)
            if not args.dry_run:  # a dry run sends nothing
                post_ops_notice(f":rotating_light: David's dialer queue check could not run ({type(exc).__name__}). "
                                "No count was posted; see the lending logs.")
            return 1
        message = render_queue_post(report)
        if args.dry_run:
            logger.info("[queue-check] dry run for %s:\n%s", report.day, message)
            return 0
        state.update(message=message, david=False, ops=not report.short)  # a short queue also goes to ops
        remember()
        logger.info("[queue-check] %s: %d eligible", report.day, report.eligible_total)
    if not state["david"]:
        try:
            post(state["message"])
            state["david"] = True
        except Exception:
            logger.exception("[queue-check] post to David's channel failed for %s", now_et.date())
    if not state["ops"]:
        state["ops"] = post_ops_notice(state["message"])
        if not state["ops"]:
            logger.error("[queue-check] ops notice failed for %s", now_et.date())
    state["done"] = state["david"] and state["ops"]
    remember()
    return 0 if state["done"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
