"""Post the lending scoreboard to Slack.

Closing board: 7:20pm ET (the dialer stops at 7:15pm). Cron fires at 23:20 and 00:20 UTC; only the one that is 19:xx ET acts.
Hourly board (``--hourly``): the day so far, at 10:00 through 19:00 ET (the 7:20pm closing board follows the 7:00pm one). Cron fires
every UTC hour; only the hours in HOURLY_POST_HOURS_ET act, so daylight saving needs no second line. An hourly board
with no dials yet (a weekend, a day off) is not posted.

P2.5: the closing board always posts, also on a day with no calls ("No calls logged today"), and carries the pipeline:
active deals, deals per GHL stage and the deals stuck more than 3 days (scoreboard_pipeline). Just before the post its
numbers go to Drive NDL Ledger Exports/{ET date}/scoreboard.csv (scoreboard_export; LENDING_SCOREBOARD_EXPORT_ENABLED,
default off). The brief says 7:00pm ET; the board stays at 7:20pm (after the dialer stops at 7:15pm) until Josh
answers (PR Client blockers)."""
from __future__ import annotations

import argparse
import logging
import time
from dataclasses import replace
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Callable, Mapping, Optional
from zoneinfo import ZoneInfo

from config.lending_compliance import DEFAULT_TZ
from config.lending_dispositions import HOURLY_POST_HOURS_ET
from config.settings import get_settings
from src.lending.slack_channels import BOT_TOKEN, report_unset
from src.lending.db import lending_session
from src.lending.dialer_port import get_http
from src.lending.scoreboard import ScoreboardData, build_scoreboard, format_blocks, format_slack
from src.lending.scoreboard_pipeline import build_pipeline

if TYPE_CHECKING:  # a type only: the scoreboard must not depend on the ledger export's imports
    from src.lending.ledger_export import Uploader

logger = logging.getLogger(__name__)

SLACK_ATTEMPTS = 3
SLACK_RETRY_SECONDS = 20


def campaign_names() -> Mapping[str, str]:
    try:
        http = get_http()
        if http is None:
            return {}
        rows = http("GET", "/campaigns", json=None)
        items = rows.get("items", []) if isinstance(rows, Mapping) else rows
        return {str(r["id"]): str(r["name"]) for r in items if "id" in r and "name" in r}
    except Exception:
        logger.warning("[lending] could not read campaign names; scoreboard uses ids", exc_info=True)
        return {}


def _post_with_retry(client, channel: str, message: str, blocks: list) -> None:
    for attempt in range(1, SLACK_ATTEMPTS + 1):
        try:
            client.chat_postMessage(channel=channel, text=message, blocks=blocks)
            return
        except Exception:
            if attempt == SLACK_ATTEMPTS:
                raise
            logger.warning("[lending] scoreboard Slack post failed (attempt %d/%d), retrying", attempt, SLACK_ATTEMPTS)
            time.sleep(SLACK_RETRY_SECONDS)


def main(argv=None, *, now: Optional[datetime] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="post regardless of the ET hour")
    parser.add_argument("--hourly", action="store_true", help="post the day-so-far board; acts only at the hourly ET hours")
    args = parser.parse_args(argv)
    now_et = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo(DEFAULT_TZ))
    due = now_et.hour in HOURLY_POST_HOURS_ET if args.hourly else now_et.hour == 19
    if not due and not args.force:
        return 0
    as_of = now_et if args.hourly else None
    s = get_settings()
    channel = s.lending_daily_channel
    if not (channel and s.lending_slack_bot_token):
        report_unset("scoreboard", ("LENDING_DAILY_CHANNEL", BOT_TOKEN))
        return 1
    day = now_et.date()
    try:
        with lending_session() as db:
            data = build_scoreboard(db, day, campaign_names())
            if not args.hourly:  # P2.5: the closing board carries the pipeline, also on a day with no dials
                data = replace(data, pipeline=build_pipeline(db, now_et))
        if args.hourly and not args.force and not data.total.dials:
            logger.info("[lending] hourly scoreboard skipped for %s: no dials yet", day)
            return 0
        if not args.hourly:  # before the post: a Slack outage must not lose the day's numbers (the file is replaced on a retry)
            export_to_drive(data, day)
        from slack_sdk import WebClient
        _post_with_retry(WebClient(token=s.lending_slack_bot_token.get_secret_value()), channel,
                         format_slack(data, day, as_of=as_of), format_blocks(data, day, as_of=as_of))
    except Exception:
        logger.exception("[lending] scoreboard post failed for %s", day)
        return 1
    logger.info("[lending] scoreboard posted for %s (%d dials)", day, data.total.dials)
    return 0


def export_to_drive(data: ScoreboardData, day: date, *, upload: Optional["Uploader"] = None,
                    notify: Optional[Callable[[str], bool]] = None) -> bool:
    """P2.5: the closing board's numbers to Drive, just before the post (LENDING_SCOREBOARD_EXPORT_ENABLED, default off).
    Never raises and never fails the run: a failed export must not stop the post, and a non-zero exit after a posted
    board would make run.sh post it again. A failure is logged and posted as an ops notice (``ops_alerts``). Returns
    whether the file was written."""
    from src.lending.ops_alerts import post_ops_notice
    from src.lending.scoreboard_export import export

    s = get_settings()
    if not s.lending_scoreboard_export_enabled:
        logger.info("[lending] scoreboard export off (LENDING_SCOREBOARD_EXPORT_ENABLED)")
        return False
    notify = notify or post_ops_notice
    if not s.lending_ledger_export_folder_id:
        logger.error("[lending] scoreboard export on but LENDING_LEDGER_EXPORT_FOLDER_ID is not set: %s not exported", day)
        notify(f":warning: Scoreboard numbers for {day.isoformat()} were not exported to Drive: "
               "LENDING_LEDGER_EXPORT_FOLDER_ID is not set.")
        return False
    try:
        if upload is None:
            from src.lending.drive_upload import DriveUploader
            upload = DriveUploader()
        name = export(data, day, folder_id=s.lending_ledger_export_folder_id, upload=upload)
    except Exception as exc:  # class only: Drive errors can embed ids
        logger.error("[lending] scoreboard export for %s failed (%s)", day, type(exc).__name__)
        notify(f":warning: Scoreboard numbers for {day.isoformat()} were not exported to Drive "
               f"({type(exc).__name__}). The board in #lending-daily is not affected.")
        return False
    logger.info("[lending] scoreboard numbers for %s exported to Drive (%s)", day, name)
    return True


if __name__ == "__main__":
    raise SystemExit(main())
