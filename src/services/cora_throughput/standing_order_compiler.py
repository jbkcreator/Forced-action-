"""
Standing-Order Compiler (THROUGH-v2.2 T4) — proposes letting future drafts
of a cell_id auto-approve without a Slack tap, once the same action has been
approved cleanly 5+ times in a row with zero exception-rejects.

No "existing amendment-diff mechanism" was found anywhere in this repo to
build on top of (confirmed by grep during planning) — this is genuinely new,
not a reuse of any prior pattern.

An item's outcome is derived from its own decision plus its batch's final
status, since 'included' alone doesn't distinguish "approved" from "batch
expired before anyone decided it":
    decision == 'exception_rejected'                        -> rejected
    decision == 'included' AND batch.status IN (approved,
                                                  partial)    -> approved
    decision == 'included' AND batch.status IN (pending,
                                                  expired)     -> undecided (excluded)

Only one proposal is ever pending per cell_id at a time — a fresh sweep
skips any cell_id that already has a cora_standing_orders row (active or
not yet ratified), so Josh isn't re-pinged every sweep interval for the
same proposal.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from config.settings import get_settings

logger = logging.getLogger(__name__)

STREAK_THRESHOLD = 5


def _cell_ids_with_decided_history(db: Any) -> List[str]:
    rows = db.execute(
        text(
            """
            SELECT DISTINCT d.cell_id
            FROM cora_batch_items bi
            JOIN cora_draft_batches cb ON cb.batch_id = bi.batch_id
            JOIN outbound_drafts d ON d.draft_id = bi.draft_id
            WHERE (bi.decision = 'exception_rejected')
               OR (bi.decision = 'included' AND cb.status IN ('approved', 'partial'))
            """
        )
    ).all()
    return [r[0] for r in rows]


def _recent_outcomes_for_cell(db: Any, cell_id: str, limit: int = STREAK_THRESHOLD) -> List[str]:
    """Most recent decided outcomes for a cell_id, newest first. 'undecided'
    items (still-open or expired-without-decision) are excluded entirely —
    they neither count toward nor break a streak."""
    rows = db.execute(
        text(
            """
            SELECT bi.decision, cb.status AS batch_status
            FROM cora_batch_items bi
            JOIN cora_draft_batches cb ON cb.batch_id = bi.batch_id
            JOIN outbound_drafts d ON d.draft_id = bi.draft_id
            WHERE d.cell_id = :cell_id
              AND (
                    bi.decision = 'exception_rejected'
                    OR (bi.decision = 'included' AND cb.status IN ('approved', 'partial'))
                  )
            ORDER BY COALESCE(bi.decided_at, bi.created_at) DESC
            LIMIT :limit
            """
        ),
        {"cell_id": cell_id, "limit": limit},
    ).mappings().all()
    outcomes = []
    for row in rows:
        outcomes.append("rejected" if row["decision"] == "exception_rejected" else "approved")
    return outcomes


def _has_existing_proposal(db: Any, cell_id: str) -> bool:
    row = db.execute(
        text("SELECT 1 FROM cora_standing_orders WHERE cell_id = :cell_id LIMIT 1"), {"cell_id": cell_id},
    ).first()
    return row is not None


def _post_standing_order_proposal(standing_order_id: int, cell_id: str, rule_text: str) -> Optional[str]:
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.cora_throughput_slack_channel
    if not token or not channel:
        logger.info(
            "[Through] Slack not configured — standing-order proposal for cell_id=%s stays unposted", cell_id,
        )
        return None

    text_body = f"*Standing-order proposal* — {rule_text}"
    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        response = client.chat_postMessage(
            channel=channel,
            text=text_body,
            blocks=[
                {"type": "section", "text": {"type": "mrkdwn", "text": text_body}},
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Ratify"},
                            "style": "primary",
                            "action_id": "ratify_standing_order",
                            "value": json.dumps({"standing_order_id": standing_order_id, "action": "ratify_standing_order"}),
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Decline"},
                            "style": "danger",
                            "action_id": "decline_standing_order",
                            "value": json.dumps({"standing_order_id": standing_order_id, "action": "decline_standing_order"}),
                        },
                    ],
                },
            ],
        )
        return response["ts"]
    except Exception as exc:
        logger.error("[Through] Slack post failed for standing-order proposal cell_id=%s: %s", cell_id, exc, exc_info=True)
        return None


def compile_standing_orders(db: Any) -> Dict[str, Any]:
    """One sweep pass. Returns a dict of what was proposed, for logging/tests."""
    proposed: List[Dict[str, Any]] = []
    for cell_id in _cell_ids_with_decided_history(db):
        if _has_existing_proposal(db, cell_id):
            continue
        outcomes = _recent_outcomes_for_cell(db, cell_id)
        if len(outcomes) < STREAK_THRESHOLD or any(o != "approved" for o in outcomes):
            continue

        rule_text = f"Auto-approve future '{cell_id}' drafts without batch review"
        row = db.execute(
            text(
                "INSERT INTO cora_standing_orders (cell_id, rule_text, active) "
                "VALUES (:cell_id, :rule_text, false) RETURNING id"
            ),
            {"cell_id": cell_id, "rule_text": rule_text},
        ).first()
        standing_order_id = row[0]
        slack_message_ts = _post_standing_order_proposal(standing_order_id, cell_id, rule_text)
        if slack_message_ts:
            db.execute(
                text("UPDATE cora_standing_orders SET slack_message_ts = :ts WHERE id = :id"),
                {"ts": slack_message_ts, "id": standing_order_id},
            )
        proposed.append({"standing_order_id": standing_order_id, "cell_id": cell_id})

    return {"proposed": proposed}


def run_periodic(stop_event, interval_seconds: int = 30 * 60) -> None:
    from src.core.database import get_db_context

    logger.info("cora_throughput.standing_order_compiler: starting periodic sweep every %ds", interval_seconds)
    while not stop_event.is_set():
        try:
            with get_db_context() as db:
                result = compile_standing_orders(db)
                if result["proposed"]:
                    logger.info("cora_throughput.standing_order_compiler: proposed %d standing order(s)", len(result["proposed"]))
        except Exception:  # noqa: BLE001
            logger.exception("cora_throughput.standing_order_compiler: sweep failed — will retry next interval")
        stop_event.wait(interval_seconds)
    logger.info("cora_throughput.standing_order_compiler: stopped")
