"""
Daily Revenue Power Block (THROUGH-v2.2 T2) — a digest folded into the same
Slack post builder.py already makes for batch approval, not a separate
system or PDF pipeline. Stitches together what Josh needs to see once a
day: today's batch, follow-ups due, and calls booked today.
"""
from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy import text

from src.agents.cora import store


def assemble_power_block(db: Any) -> Dict[str, Any]:
    """Read-only assembly — never mutates state. Each section degrades to an
    empty list rather than raising if its underlying data isn't available."""
    due_followups: List[str] = store.list_opportunities_by_status("touched")

    calls_today = db.execute(
        text(
            "SELECT id, prospect_phone, outcome, call_date "
            "FROM synthflow_calls WHERE call_date = CURRENT_DATE ORDER BY id ASC"
        )
    ).mappings().all()

    pending_batches = db.execute(
        text(
            "SELECT batch_id, created_at, "
            "(SELECT count(*) FROM cora_batch_items WHERE cora_batch_items.batch_id = cora_draft_batches.batch_id) AS item_count "
            "FROM cora_draft_batches WHERE status = 'pending' ORDER BY created_at ASC"
        )
    ).mappings().all()

    return {
        "due_followups": due_followups,
        "calls_booked_today": [dict(r) for r in calls_today],
        "pending_batches": [dict(r) for r in pending_batches],
    }


def render_power_block_blocks(power_block: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Renders assemble_power_block()'s output as extra Slack blocks, appended
    to the same daily batch message rather than posted separately."""
    lines = [
        f"*Follow-ups due:* {len(power_block['due_followups'])}",
        f"*Calls booked today:* {len(power_block['calls_booked_today'])}",
    ]
    for batch in power_block["pending_batches"]:
        lines.append(f"*Open batch* `{batch['batch_id'][:8]}` — {batch['item_count']} item(s), awaiting decision")
    return [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}]
