"""08:55 ET on business days: draft the after-hours voicemail replies and post who to call first.

A caller who left an after-hours voicemail is pinned (``src.lending.after_hours_intake``). Once a business-day
morning, before the 09:00 shift, this takes every contact still pinned (not called back yet, within its pin), queues
a reply draft for Approve for each one with text consent (Cora's sweep posts the card within a minute), and posts the
list to Cora's channel (``LENDING_CORA_CHANNEL``, Cora's bot token). A caller without text consent is listed as
call-only. Cron fires at 12:55 and 13:55 UTC Monday-Friday; only the one that is 8:xx ET acts.

Drafts are idempotent per voicemail, so a rerun never drafts twice. The Slack post is not: ``--force`` reposts the
list (use it by hand when the morning post failed; the drafts are already queued).

Shows the last four digits of the phone only, never the full number.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

from config.lending_after_hours import BUSINESS_DAYS, MORNING_POST_HOUR_ET
from config.lending_reminders import TIMEZONE
from config.settings import AppSettings, get_settings
from packages.agent_core.pending_actions import PendingActionQueue
from packages.agent_core.redaction import slack_escape
from packages.agent_core.store import AgentStore
from src.lending.after_hours_intake import MorningReply, draft_morning_replies, model_drafter, pins_to_call
from src.lending.db import lending_session
from src.lending.models import LENDING_SCHEMA

logger = logging.getLogger(__name__)


def _line(reply: MorningReply) -> str:
    event = reply.pin.event
    left = event.received_at.astimezone(TIMEZONE)
    status = "reply draft waiting for Approve" if reply.action_id else "no text consent, call only"
    name = slack_escape(event.first_name) if event.first_name else "Caller"  # caller-supplied: no live mentions
    return (f"• {name} ({reply.pin.lead_ref}, …{event.phone[-4:]}): voicemail {left:%a} "
            f"{left.hour % 12 or 12}:{left:%M} {'am' if left.hour < 12 else 'pm'} ET — {status}")


def format_slack(replies: Sequence[MorningReply]) -> Optional[str]:
    """The Slack message, or None when nobody is pinned."""
    if not replies:
        return None
    return f"*Call these first ({len(replies)}): after-hours voicemails*\n" + "\n".join(map(_line, replies))


def is_post_time(now: datetime) -> bool:
    """8:xx ET on a business day (the cron's two UTC lines cover EDT and EST)."""
    local = now.astimezone(TIMEZONE)
    return local.hour == MORNING_POST_HOUR_ET and local.weekday() in BUSINESS_DAYS


def _drafter(settings: AppSettings):
    import anthropic  # loaded only when there is something to draft

    client = anthropic.Anthropic(api_key=settings.anthropic_api_key.get_secret_value(), timeout=30.0, max_retries=1)
    return model_drafter(client.messages, settings.lending_cora_model)


def main(argv=None, *, now: Optional[datetime] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="run now (manual repost); drafts are never duplicated")
    args = parser.parse_args(argv)
    now = now or datetime.now(timezone.utc)
    if not (args.force or is_post_time(now)):
        return 0
    settings = get_settings()
    channel, token = settings.lending_cora_channel, settings.lending_cora_slack_bot_token
    if not (channel and token):
        logger.error("[pinned-callbacks] not run: LENDING_CORA_CHANNEL or LENDING_CORA_SLACK_BOT_TOKEN is not set")
        return 1
    try:
        with lending_session() as db:
            pins = pins_to_call(db, now)
            if not pins:
                logger.info("[pinned-callbacks] no pinned callers")
                return 0
            queue = PendingActionQueue(AgentStore(db.get_bind(), LENDING_SCHEMA),
                                       default_ttl=timedelta(hours=settings.lending_cora_pending_action_ttl_hours))
            message = format_slack(draft_morning_replies(db, pins, queue, _drafter(settings)))
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(channel=channel, text=message)
    except Exception as exc:  # class only: rows carry phone digits
        logger.error("[pinned-callbacks] run failed (%s); drafts already queued are kept, repost with --force",
                     type(exc).__name__)
        return 1
    logger.info("[pinned-callbacks] posted %s pinned caller(s)", len(pins))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
