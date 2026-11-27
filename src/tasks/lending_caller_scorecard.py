"""R1 1.6: David's 2:45 PM caller scorecard, posted to LENDING_DAVID_CHANNEL (#isa-daily).

The main post has his day's numbers and top calls. When a connected call has no recording and no notes, an "Add notes"
thread under it opens a Slack form (src/lending/dialer/call_notes.py).

Cron fires at 18:45 and 19:45 UTC, Mon-Fri; the one that is 14:xx ET acts, so daylight saving needs no change. A later
call the same day only finishes an unfinished run. Off until LENDING_CALLER_SCORECARD_ENABLED=true.

    python -m src.tasks.lending_caller_scorecard [--force] [--dry-run]

``--force`` skips the hour and weekday checks. ``--dry-run`` builds and logs the post instead of sending it, and works
with the switch off.

Delivery is tracked for the ET day (``src/lending/dialer/delivery_state.py``): the main post's Slack timestamp is
saved as soon as it is sent, and the rendered main post and notes thread are saved before either is sent. A run that
fails returns 1, and the cron retry (run.sh, +15 and +30 min, so after 3 PM) resumes it from the saved payload without
reading the database again: the main post is sent only if it was not, and the notes thread goes under the saved one.
A finished day is never posted again. Overlapping runs wait on a per-day lock. ``--force`` and ``--dry-run`` neither
read nor write that state.
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from config.lending_compliance import DEFAULT_TZ
from config.lending_dialer_queue import SCORECARD_HOUR_ET, WORKDAYS
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.dialer import delivery_state
from src.lending.dialer.scorecard import build_scorecard, render_notes_thread, render_scorecard
from src.lending.ops_alerts import post_ops_notice
from src.lending.slack_channels import BOT_TOKEN, report_unset
from src.tasks.lending_daily_scoreboard import SLACK_ATTEMPTS, SLACK_RETRY_SECONDS

logger = logging.getLogger(__name__)
POST = "David's 2:45 PM scorecard"
STATE = "caller_scorecard"


def _send(client: Any, **kwargs: Any) -> Any:
    for attempt in range(1, SLACK_ATTEMPTS + 1):
        try:
            return client.chat_postMessage(**kwargs)
        except Exception:
            if attempt == SLACK_ATTEMPTS:
                raise
            logger.warning("[scorecard] Slack post failed (attempt %d/%d), retrying", attempt, SLACK_ATTEMPTS)
            time.sleep(SLACK_RETRY_SECONDS)


def main(argv=None, *, now: Optional[datetime] = None, client: Any = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="run regardless of the ET hour and weekday")
    parser.add_argument("--dry-run", action="store_true", help="log the post, do not send it")
    args = parser.parse_args(argv)
    now = now or datetime.now(timezone.utc)
    stateful = not (args.force or args.dry_run)
    day = now.astimezone(ZoneInfo(DEFAULT_TZ)).date()
    with delivery_state.locked(STATE, day) if stateful else nullcontext():
        return _run(args, now, client, stateful)


def _run(args: argparse.Namespace, now: datetime, client: Any, stateful: bool) -> int:
    now_et = now.astimezone(ZoneInfo(DEFAULT_TZ))
    state = delivery_state.load(STATE, now_et.date()) if stateful else None
    if state is not None and state.get("done"):
        return 0
    due = now_et.hour == SCORECARD_HOUR_ET and now_et.weekday() in WORKDAYS
    if not args.force and not due and state is None:  # a retry of today's unfinished run may act after 14:xx
        return 0
    s = get_settings()
    if not s.lending_caller_scorecard_enabled and not args.dry_run:
        logger.info("[scorecard] off (LENDING_CALLER_SCORECARD_ENABLED is not true)")
        return 0
    channel, agent_id = s.lending_david_channel.strip(), s.lending_david_bd_agent_id.strip()
    if not args.dry_run and not (channel and agent_id and (client or s.lending_slack_bot_token)):
        report_unset(POST, ("LENDING_DAVID_CHANNEL", "LENDING_DAVID_BD_AGENT_ID", BOT_TOKEN))
        return 1
    if args.dry_run and not agent_id:
        logger.error("[scorecard] LENDING_DAVID_BD_AGENT_ID is not set")
        return 1
    state = state or {}

    def remember() -> None:
        if stateful:
            delivery_state.save(STATE, now_et.date(), state)

    remember()  # marks today's run as started, so a retry after 14:xx still acts
    if "notes" not in state:  # a retry reuses the saved payload, so a failed read cannot drop the notes thread
        try:
            with lending_session() as db:
                card = build_scorecard(db, now_et.date(), agent_id=agent_id, as_of=now)
        except Exception as exc:  # class only: SQL errors embed bound parameters
            logger.error("[scorecard] could not build the scorecard (%s)", type(exc).__name__)
            if not args.dry_run:  # a dry run sends nothing
                post_ops_notice(f":rotating_light: David's 2:45 PM scorecard could not be built "
                                f"({type(exc).__name__}). Nothing was posted; see the lending logs.")
            return 1
        blocks, notes = render_scorecard(card), render_notes_thread(card)
        if args.dry_run:
            logger.info("[scorecard] dry run for %s:\n%s\nnotes thread: %d lines", card.day,
                        json.dumps(blocks, ensure_ascii=False), len(notes))
            return 0
        state.update(blocks=blocks, notes=notes)
        remember()
        logger.info("[scorecard] built for %s: %d connected calls, %d need notes", card.day, len(card.calls),
                    len(notes))
    if client is None:
        from slack_sdk import WebClient
        client = WebClient(token=s.lending_slack_bot_token.get_secret_value())
    try:
        if not state.get("parent_ts"):
            state["parent_ts"] = _send(client, channel=channel, text=f"David's scorecard for {now_et.date()}",
                                       blocks=state["blocks"])["ts"]
            remember()
        if state["notes"]:
            _send(client, channel=channel, thread_ts=state["parent_ts"], text="Calls that need notes",
                  blocks=state["notes"])
    except Exception:
        logger.exception("[scorecard] post failed for %s", now_et.date())
        return 1
    state["done"] = True
    remember()
    logger.info("[scorecard] posted for %s", now_et.date())
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
