"""
THROUGH-v2.2 T4 — Monthly Standing-Order Digest.

Posts a Slack summary of every ACTIVE (ratified) standing order on the 1st of
each month so Josh can prune stale auto-approval rules — the review cadence he
asked for ("a monthly digest of active ones so I can prune, not a permanent
set-and-forget").

Each rule gets Keep / Archive buttons. Keep is a no-op (inaction means keep).
Archive routes the same value shape as ratify/decline, through the existing
/admin/slack/cora-batch/interact endpoint, dispatching to
decisions.record_standing_order_decision(action="archive_standing_order"),
which DELETEs the active row so that cell_id can re-earn a proposal after 5
more clean approvals.

Cron (scripts/cron/crontab.txt):
  0 9 1 * * $PROJECT/scripts/cron/run.sh src.services.cora_throughput.standing_order_digest

Direct:  python -m src.services.cora_throughput.standing_order_digest
"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def run_digest() -> int:
    """Post the monthly digest of active standing orders. Returns count posted."""
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.services.cora_throughput.standing_order_compiler import (
        list_active_standing_orders,
    )

    with get_db_context() as db:
        orders = list_active_standing_orders(db)

    if not orders:
        logger.info("[Through] No active standing orders — monthly digest skipped.")
        return 0

    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.cora_throughput_slack_channel
    if not token or not channel:
        logger.warning("[Through] Slack not configured — monthly digest skipped.")
        return 0

    try:
        from slack_sdk import WebClient
        from config.cora_cell_grid import CELL_GRID

        client = WebClient(token=token.get_secret_value())

        blocks: list[dict] = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Monthly Standing-Order Review* — {len(orders)} active rule(s).\n"
                        "Archive any that no longer earn their auto-approval. "
                        "Ignoring this leaves every rule active."
                    ),
                },
            },
            {"type": "divider"},
        ]

        for o in orders:
            created = o["created_at"].strftime("%Y-%m-%d") if o.get("created_at") else "unknown"
            cell = CELL_GRID.get(o["cell_id"], {})
            label = f"{cell.get('offer', '?')} · {cell.get('avenue', '?')} · {cell.get('angle', '?')}"
            count = o.get("approval_count_at_proposal", 0)
            archive_val = json.dumps(
                {"standing_order_id": o["id"], "action": "archive_standing_order"}
            )
            keep_val = json.dumps(
                {"standing_order_id": o["id"], "action": "keep_standing_order"}
            )
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*#{o['id']}* `{o['cell_id']}`  _{label}_\n"
                        f"{o['rule_text']}\n"
                        f"Ratified: {created} · earned by {count} clean approval(s)"
                    ),
                },
            })
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Keep"},
                        "action_id": f"keep_standing_order_{o['id']}",
                        "value": keep_val,
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Archive"},
                        "style": "danger",
                        "action_id": f"archive_standing_order_{o['id']}",
                        "value": archive_val,
                    },
                ],
            })
            blocks.append({"type": "divider"})

        client.chat_postMessage(
            channel=channel,
            text=f"Monthly Standing-Order Review — {len(orders)} active rule(s)",
            blocks=blocks,
        )
        logger.info("[Through] Posted monthly digest for %d standing order(s).", len(orders))
    except Exception as exc:
        logger.error("[Through] Monthly digest Slack post failed: %s", exc, exc_info=True)

    return len(orders)


if __name__ == "__main__":
    run_digest()
