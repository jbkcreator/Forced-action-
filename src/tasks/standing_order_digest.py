"""
THROUGH-v2.2 T4 — Monthly Standing Order Digest.

Posts a Slack summary of all ratified standing orders so Josh can prune
stale rules. Each rule gets Keep/Archive buttons; archiving calls
/slack/standing-order/interact with action="archive".

Cron entry (add to scripts/cron/crontab.txt):
  0 9 1 * * $PROJECT/scripts/cron/run.sh src.tasks.standing_order_digest

Called directly: python -m src.tasks.standing_order_digest
"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def run_digest() -> int:
    """Post monthly digest to Slack. Returns count of ratified orders posted."""
    from config.settings import get_settings
    from src.services.standing_order_compiler import list_ratified

    orders = list_ratified()
    if not orders:
        logger.info("[SODigest] No ratified standing orders — nothing to post.")
        return 0

    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.relay_slack_channel
    if not token or not channel:
        logger.warning("[SODigest] Slack not configured — digest skipped.")
        return 0

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        blocks: list[dict] = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Monthly Standing Order Review* — {len(orders)} active rule(s).\n"
                        "Review each and archive any that are no longer valid."
                    ),
                },
            },
            {"type": "divider"},
        ]

        for order in orders:
            ratified_date = (
                order["ratified_at"].strftime("%Y-%m-%d") if order["ratified_at"] else "unknown"
            )
            keep_val = json.dumps({"so_id": order["id"], "action": "keep"})
            archive_val = json.dumps({"so_id": order["id"], "action": "archive"})
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*#{order['id']}* `{order['action_type']}` / `{order['vertical']}`\n"
                        f"Ratified: {ratified_date} · "
                        f"Approved {order['approval_count_at_proposal']}x before proposal"
                    ),
                },
            })
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Keep"},
                        "action_id": f"so_keep_{order['id']}",
                        "value": keep_val,
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Archive"},
                        "style": "danger",
                        "action_id": f"so_archive_{order['id']}",
                        "value": archive_val,
                    },
                ],
            })
            blocks.append({"type": "divider"})

        client.chat_postMessage(
            channel=channel,
            text=f"Monthly Standing Order Review — {len(orders)} active rule(s)",
            blocks=blocks,
        )
        logger.info("[SODigest] Posted digest for %d standing order(s).", len(orders))
    except Exception as exc:
        logger.error("[SODigest] Slack post failed: %s", exc, exc_info=True)

    return len(orders)


if __name__ == "__main__":
    run_digest()
