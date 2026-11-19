"""08:55 ET on business days: draft the after-hours voicemail replies and post who to call first.

The list also carries the new LendingFlow and website leads that came in outside working hours since the previous
business morning's list and are still not reached: uncalled-lead alarms are not sent outside working hours (LENDING_ALARM_HOURS_START / _END, default 09:00-19:15 ET Mon-Fri)
(client list Nov 2, C1), so this list is where those leads surface (``uncalled_alarm_worker.after_hours_leads``).

A caller who left an after-hours voicemail is pinned (``src.lending.after_hours_intake``). Once a business-day
morning, before the 09:00 shift, this takes every contact still pinned (not called back yet, within its pin), queues
a reply draft for Approve for each one with text consent (Cora's sweep posts the card within a minute), and posts the
list to Cora's channel (``LENDING_CORA_CHANNEL``, Cora's bot token). A caller without text consent is listed as
call-only. Cron fires at 12:55 and 13:55 UTC Monday-Friday; only the one that is 8:xx ET acts.

Drafts are idempotent per voicemail, so a rerun never drafts twice. The Slack post is not: ``--force`` reposts the
list (use it by hand when the morning post failed; the drafts are already queued).

Shows each caller's full name with their contact ref, and the last four digits of the phone only, never the full
number.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence

from config.lending_after_hours import BUSINESS_DAYS, MORNING_POST_HOUR_ET
from config.lending_reminders import TIMEZONE
from config.settings import AppSettings, get_settings
from packages.agent_core.pending_actions import PendingActionQueue
from packages.agent_core.store import AgentStore
from src.lending.after_hours_intake import MorningReply, draft_morning_replies, model_drafter, pins_to_call
from src.lending.db import lending_session
from src.lending.slack_channels import CORA_BOT_TOKEN, report_unset
from src.lending.models import LENDING_SCHEMA
from src.lending.slack_names import name_and_ref, names_by_phone, readable_ref
from src.lending.slack_test_filter import hidden_phones
from src.lending.uncalled_alarm_worker import after_hours_leads, lead_who

logger = logging.getLogger(__name__)


def _line(reply: MorningReply, names: Mapping[str, str]) -> str:
    event = reply.pin.event
    left = event.received_at.astimezone(TIMEZONE)
    status = "reply draft waiting for Approve" if reply.action_id else "no text consent, call only"
    # Full name from the lead record when known, else the name the caller gave; escaped (caller-supplied).
    who = name_and_ref(names.get(event.phone) or event.first_name, readable_ref(reply.pin.lead_ref))
    return (f"• {who} (…{event.phone[-4:]}): voicemail {left:%a} "
            f"{left.hour % 12 or 12}:{left:%M} {'am' if left.hour < 12 else 'pm'} ET — {status}")


def _lead_line(lead: Mapping[str, Any]) -> str:
    came = lead["arrived_at"].astimezone(TIMEZONE)
    source = "website" if lead["from_web"] else "LendingFlow"
    return (f"• {lead_who(lead)} (…{lead['phone'][-4:]}): {source} lead, came in {came:%a} "
            f"{came.hour % 12 or 12}:{came:%M} {'am' if came.hour < 12 else 'pm'} ET, no alarm sent (after hours)")


def format_slack(replies: Sequence[MorningReply], names: Optional[Mapping[str, str]] = None,
                 leads: Sequence[Mapping[str, Any]] = ()) -> Optional[str]:
    """The Slack message, or None when nobody is pinned and no lead came in after hours. ``names`` maps a phone to
    the caller's full name."""
    blocks = []
    if replies:
        blocks.append(f"*Call these first ({len(replies)}): after-hours voicemails*\n"
                      + "\n".join(_line(reply, names or {}) for reply in replies))
    if leads:
        blocks.append(f"*Call these first ({len(leads)}): new leads that came in after hours*\n"
                      + "\n".join(_lead_line(lead) for lead in leads))
    return "\n\n".join(blocks) or None


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
        report_unset("pinned callbacks", ("LENDING_CORA_CHANNEL", CORA_BOT_TOKEN))
        return 1
    try:
        with lending_session() as db:
            pins = pins_to_call(db, now)
            leads = after_hours_leads(db, now)
            test = hidden_phones(db, [pin.event.phone for pin in pins] + [lead["phone"] for lead in leads])
            pins = [pin for pin in pins if pin.event.phone not in test]  # no post and no draft card for test callers
            leads = [lead for lead in leads if lead["phone"] not in test]
            if not (pins or leads):
                logger.info("[pinned-callbacks] no pinned callers and no after-hours leads")
                return 0
            replies = []
            if pins:
                queue = PendingActionQueue(AgentStore(db.get_bind(), LENDING_SCHEMA),
                                           default_ttl=timedelta(hours=settings.lending_cora_pending_action_ttl_hours))
                replies = draft_morning_replies(db, pins, queue, _drafter(settings))
            message = format_slack(replies, names_by_phone(db, [pin.event.phone for pin in pins]), leads)
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(channel=channel, text=message)
    except Exception as exc:  # class only: rows carry phone digits
        logger.error("[pinned-callbacks] run failed (%s); drafts already queued are kept, repost with --force",
                     type(exc).__name__)
        return 1
    logger.info("[pinned-callbacks] posted %s pinned caller(s), %s after-hours lead(s)", len(pins), len(leads))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
