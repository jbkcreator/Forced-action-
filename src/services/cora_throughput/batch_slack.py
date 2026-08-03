"""
Push a pending cora_draft_batches batch to Slack for Josh's one-tap decision.

Mirrors src.services.relay.slack_post's WebClient/token/channel usage and
"JSON string in the button value" convention, generalized from one item to
a batch of N — one primary "Approve Batch" action plus one small
per-item "Exception-reject" button, so Josh can approve everything but one
without leaving the batch in limbo.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)


def _draft_summary_line(draft: Dict[str, Any]) -> str:
    subject = (draft.get("subject") or "")[:80]
    return (
        f"• `{draft['draft_id'][:8]}` — *{draft.get('cell_id', '')}* "
        f"({draft.get('recommended_channel', '')}) — {draft.get('opportunity_thread_id', '')} — {subject}"
    )


def post_batch_for_approval(
    batch_id: str, drafts: List[Dict[str, Any]], extra_blocks: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:
    """
    Posts one Slack message summarizing every draft in the batch, with one
    "Approve Batch" primary button and one small "Reject #N" button per
    draft for the single-item exception case.

    extra_blocks (the Daily Revenue Power Block digest — power_block.py)
    is appended into this same message rather than posted separately, per
    the THROUGH-v2.2 plan: "not a separate system or a PDF pipeline."

    No-ops (logs and returns None) if Slack isn't configured for THROUGH —
    mirrors relay.slack_post.post_for_approval's local/dev-without-Slack
    behavior. Returns the posted message's ts, or None if not posted.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.cora_throughput_slack_channel
    if not token or not channel:
        logger.info(
            "[Through] Slack not configured (cora_throughput_slack_channel/slack_bot_token "
            "unset) — batch %s stays pending without a posted message", batch_id,
        )
        return None

    header_text = f"*Cora batch review* — {len(drafts)} draft(s) ready (`{batch_id[:8]}`)"
    blocks: List[Dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": header_text}},
    ]
    for draft in drafts:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": _draft_summary_line(draft)}})
    if extra_blocks:
        blocks.append({"type": "divider"})
        blocks.extend(extra_blocks)

    actions_elements: List[Dict[str, Any]] = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Approve Batch"},
            "style": "primary",
            "action_id": "approve_all",
            "value": json.dumps({"batch_id": batch_id, "action": "approve_all"}),
        },
    ]
    for draft in drafts:
        actions_elements.append({
            "type": "button",
            "text": {"type": "plain_text", "text": f"Reject {draft['draft_id'][:8]}"},
            "style": "danger",
            "action_id": f"reject_{draft['draft_id']}",
            "value": json.dumps({"batch_id": batch_id, "draft_id": draft["draft_id"], "action": "reject_item"}),
        })
    blocks.append({"type": "actions", "elements": actions_elements})

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        response = client.chat_postMessage(channel=channel, text=header_text, blocks=blocks)
        return response["ts"]
    except Exception as exc:
        logger.error("[Through] Slack post failed for batch %s: %s", batch_id, exc, exc_info=True)
        return None


def update_batch_slack_message(slack_message_ts: str, reply_text: str) -> None:
    """Replaces the batch message's buttons with the decision outcome, in place."""
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.cora_throughput_slack_channel
    if not token or not channel or not slack_message_ts:
        return
    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        client.chat_update(
            channel=channel,
            ts=slack_message_ts,
            text=reply_text,
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": reply_text}}],
        )
    except Exception as exc:
        logger.error("[Through] chat.update failed for ts=%s: %s", slack_message_ts, exc)
