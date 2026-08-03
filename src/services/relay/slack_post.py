"""
Push a pending relay_approval_queue row to Slack for Josh's decision
(RELAY-v2.2 sub-task R1, build spec §1.1.13 — "Decision Packets render as
interactive approve/reject buttons").

Mirrors the existing county-launch approval pattern
(src/api/admin_router.py:slack_county_launch_interact / _update_slack_message)
— same WebClient usage, same "JSON string in the button value" convention.
"""
from __future__ import annotations

import json
import logging

from config.settings import get_settings
from src.services.relay import queue
from src.services.relay.queue import QueueItem
from src.utils.venture_config import get_venture_config

logger = logging.getLogger(__name__)


def _summary_text(item: QueueItem) -> str:
    subject = item.payload.get("subject") if isinstance(item.payload, dict) else None
    preview = subject or str(item.payload)[:120]
    return (
        f"*Relay approval needed* (#{item.id})\n"
        f"Channel: `{item.channel}`  ·  To: `{item.recipient}`\n"
        f"{preview}"
    )


def post_for_approval(item: QueueItem) -> None:
    """Post an interactive Approve/Reject Slack message for a pending item.

    The channel comes from the item's venture (CLONE-v2.2 / CL3) so each
    venture's approvals land in its own channel — one shared channel would
    make it impossible to tell whose prospect an approve button belongs to.
    The bot token stays fleet-wide (one Slack app, per RELAY-v2.2 R1).

    No-ops (logs and returns) if Slack isn't configured — this keeps --seed
    usable in local/dev environments without a live Slack app.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = get_venture_config(item.venture_key).relay_slack_channel
    if not token or not channel:
        logger.info(
            "[Relay] Slack not configured for venture %s (no slack channel or "
            "bot token) — item %d stays pending without a posted message",
            item.venture_key, item.id,
        )
        return

    approve_value = json.dumps({"item_id": item.id, "action": "approve"})
    reject_value = json.dumps({"item_id": item.id, "action": "reject"})

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        response = client.chat_postMessage(
            channel=channel,
            text=_summary_text(item),
            blocks=[
                {"type": "section", "text": {"type": "mrkdwn", "text": _summary_text(item)}},
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Approve"},
                            "style": "primary",
                            "action_id": "approve",
                            "value": approve_value,
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Reject"},
                            "style": "danger",
                            "action_id": "reject",
                            "value": reject_value,
                        },
                    ],
                },
            ],
        )
        queue.set_slack_message_ts(item.id, response["ts"])
    except Exception as exc:
        logger.error("[Relay] Slack post failed for item %d: %s", item.id, exc, exc_info=True)
