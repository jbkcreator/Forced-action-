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
from typing import TYPE_CHECKING

from config.settings import get_settings
from config.venture_template import DEFAULT_VENTURE_KEY
from src.services.relay import queue
from src.services.relay.queue import QueueItem
from src.utils.venture_config import get_venture_config

if TYPE_CHECKING:
    from src.services.relay.engine import BatchResult

logger = logging.getLogger(__name__)


def _summary_text(item: QueueItem) -> str:
    subject = item.payload.get("subject") if isinstance(item.payload, dict) else None
    preview = subject or str(item.payload)[:120]
    return (
        f"*Relay approval needed* (#{item.id})\n"
        f"Channel: `{item.channel}`  ·  To: `{item.recipient}`\n"
        f"{preview}"
    )


_FA_MAX_VENTURE = "fa_max_lending"

_LANE_CHANNEL_ATTR = {
    "MONEY": "fa_max_slack_channel_money",
    "EXCEPTIONS": "fa_max_slack_channel_exceptions",
    "RELATIONSHIPS": "fa_max_slack_channel_relationships",
}


def _resolve_channel(item: QueueItem, settings) -> str:
    """Return the Slack channel for this item.

    FA Max items with a lane route to their lane-specific channel from
    settings. All other items (and FA Max items with no lane) fall back to
    the venture's relay_slack_channel.
    """
    if item.venture_key == _FA_MAX_VENTURE and item.lane:
        attr = _LANE_CHANNEL_ATTR.get(item.lane)
        if attr:
            lane_channel = getattr(settings, attr, "")
            if lane_channel:
                return lane_channel
    return get_venture_config(item.venture_key).relay_slack_channel


def post_for_approval(item: QueueItem) -> None:
    """Post an interactive Approve/Reject Slack message for a pending item.

    For FA Max items, routes to the lane-specific channel (MONEY /
    EXCEPTIONS / RELATIONSHIPS) from settings. Other ventures use the
    venture's relay_slack_channel as before (CLONE-v2.2 / CL3).

    No-ops (logs and returns) if Slack isn't configured — this keeps --seed
    usable in local/dev environments without a live Slack app.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = _resolve_channel(item, settings)
    if not token or not channel:
        logger.info(
            "[Relay] Slack not configured for venture %s lane %s (no slack channel or "
            "bot token) — item %d stays pending without a posted message",
            item.venture_key, item.lane, item.id,
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


def post_completion_receipt(
    batch_id: str,
    result: "BatchResult",
    venture_key: str = DEFAULT_VENTURE_KEY,
) -> None:
    """Post a send-completion receipt to the venture's relay Slack channel.

    Called by the cron sweep after execute_batch() returns so Josh can see
    in #agent-daily that an approved batch actually went out (build spec §1.1
    Phase 1 DoD: "first receipted sends").

    No-ops if Slack isn't configured — mirrors post_for_approval's behaviour.
    Never raises: a failed receipt must not mark the batch as failed.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = get_venture_config(venture_key).relay_slack_channel
    if not token or not channel:
        logger.info(
            "[Relay] Slack not configured for venture %s — skipping completion receipt for %s",
            venture_key, batch_id,
        )
        return

    if result.halted:
        icon = "🛑"
        suffix = " — halted by kill switch, remaining items queued for next sweep"
    elif result.failed:
        icon = "⚠️"
        suffix = " — check logs"
    else:
        icon = "✅"
        suffix = ""

    text = (
        f"{icon} Relay `{batch_id}` · "
        f"{result.sent} sent · {result.failed} failed · {result.deferred} deferred"
        f"{suffix}"
    )

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        client.chat_postMessage(channel=channel, text=text)
        logger.info("[Relay] completion receipt posted for %s", batch_id)
    except Exception as exc:
        logger.error(
            "[Relay] completion receipt post failed for %s: %s",
            batch_id, exc, exc_info=True,
        )
