"""
THROUGH-v2.2 T4 — Standing-Order Compiler.

Watches relay_approval_queue for repeated approvals of the same
(action_type, vertical) pair. When a pair accumulates PROPOSAL_THRESHOLD
approvals and has no active (proposed/ratified) standing order, proposes
one via Slack and persists a 'proposed' row to standing_orders.

Called by:
  - src/tasks/standing_order_sweep.py  (cron, after each relay sweep)
  - CLI: python -m src.services.standing_order_compiler --scan

action_type and vertical are read from relay_approval_queue.payload
JSONB fields ("action_type" and "vertical"). Cora stamps these when
enqueueing; seed data may omit them (rows without both fields are
excluded from compiler counts).

Hard gates (DNC, suppression, TCPA, price floor) are NOT relaxed by a
standing order — Relay's guards.evaluate() still runs on every execution.
A standing order only bypasses Josh's per-item Slack review queue.

Monthly digest (active standing order review) lives in
src/tasks/standing_order_digest.py.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import StandingOrder

logger = logging.getLogger(__name__)

PROPOSAL_THRESHOLD = 5


@dataclass
class ProposedOrder:
    action_type: str
    vertical: str
    template_id: Optional[str]
    approval_count: int
    standing_order_id: int


def scan_and_propose() -> list[ProposedOrder]:
    """Find (action_type, vertical) pairs that have crossed the threshold
    and have no active standing order. Creates a 'proposed' row and posts
    to Slack for each new candidate. Returns the list of newly proposed orders.
    """
    candidates = _find_candidates()
    proposed: list[ProposedOrder] = []
    for c in candidates:
        order = _propose(c["action_type"], c["vertical"], c["template_id"], c["approval_count"])
        if order is not None:
            proposed.append(order)
            _post_to_slack(order)
    return proposed


def record_ratify(standing_order_id: int, *, ratified_by: str) -> bool:
    """Flip a 'proposed' standing order to 'ratified'. Returns True if the
    row was in 'proposed' state and was updated, False if already decided."""
    with get_db_context() as session:
        result = session.execute(
            text(
                "UPDATE standing_orders "
                "SET status = 'ratified', ratified_at = now(), "
                "    ratified_by = :by, updated_at = now() "
                "WHERE id = :id AND status = 'proposed'"
            ),
            {"by": ratified_by, "id": standing_order_id},
        )
        updated = result.rowcount > 0
    if updated:
        logger.info("[StandingOrder] %d ratified by %s", standing_order_id, ratified_by)
    return updated


def record_decline(standing_order_id: int, *, declined_by: str) -> bool:
    """Flip a 'proposed' standing order to 'declined'."""
    with get_db_context() as session:
        result = session.execute(
            text(
                "UPDATE standing_orders "
                "SET status = 'declined', declined_at = now(), updated_at = now() "
                "WHERE id = :id AND status = 'proposed'"
            ),
            {"id": standing_order_id},
        )
        updated = result.rowcount > 0
    if updated:
        logger.info("[StandingOrder] %d declined by %s", standing_order_id, declined_by)
    return updated


def record_archive(standing_order_id: int) -> bool:
    """Flip a 'ratified' standing order to 'archived' (monthly digest prune)."""
    with get_db_context() as session:
        result = session.execute(
            text(
                "UPDATE standing_orders "
                "SET status = 'archived', archived_at = now(), updated_at = now() "
                "WHERE id = :id AND status = 'ratified'"
            ),
            {"id": standing_order_id},
        )
        updated = result.rowcount > 0
    if updated:
        logger.info("[StandingOrder] %d archived", standing_order_id)
    return updated


def set_slack_ts(standing_order_id: int, slack_message_ts: str) -> None:
    with get_db_context() as session:
        session.execute(
            text(
                "UPDATE standing_orders SET slack_message_ts = :ts, updated_at = now() "
                "WHERE id = :id"
            ),
            {"ts": slack_message_ts, "id": standing_order_id},
        )


def list_ratified() -> list[dict]:
    """All currently ratified standing orders — used by monthly digest."""
    with get_db_context() as session:
        rows = session.execute(
            text(
                "SELECT id, action_type, vertical, template_id, conditions, "
                "       ratified_at, approval_count_at_proposal "
                "FROM standing_orders WHERE status = 'ratified' "
                "ORDER BY ratified_at ASC"
            )
        ).mappings().all()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _find_candidates() -> list[dict]:
    """Query relay_approval_queue for pairs that have crossed the threshold
    and have no active (proposed or ratified) standing order."""
    with get_db_context() as session:
        rows = session.execute(
            text("""
                WITH counts AS (
                    SELECT
                        payload->>'action_type'  AS action_type,
                        payload->>'vertical'     AS vertical,
                        payload->>'template_id'  AS template_id,
                        count(*)                 AS approval_count
                    FROM relay_approval_queue
                    WHERE status IN ('approved', 'sent')
                      AND payload->>'action_type' IS NOT NULL
                      AND payload->>'vertical'    IS NOT NULL
                    GROUP BY
                        payload->>'action_type',
                        payload->>'vertical',
                        payload->>'template_id'
                    HAVING count(*) >= :threshold
                )
                SELECT c.action_type, c.vertical, c.template_id, c.approval_count
                FROM counts c
                WHERE NOT EXISTS (
                    SELECT 1 FROM standing_orders so
                    WHERE so.action_type = c.action_type
                      AND so.vertical    = c.vertical
                      AND so.status IN ('proposed', 'ratified')
                )
            """),
            {"threshold": PROPOSAL_THRESHOLD},
        ).mappings().all()
        return [dict(r) for r in rows]


def _propose(
    action_type: str,
    vertical: str,
    template_id: Optional[str],
    approval_count: int,
) -> Optional[ProposedOrder]:
    """Insert a 'proposed' standing_orders row. Returns None if a row for
    this pair already exists (race-safe via the unique constraint)."""
    try:
        with get_db_context() as session:
            order = StandingOrder(
                action_type=action_type,
                vertical=vertical,
                template_id=template_id,
                conditions=_default_conditions(action_type),
                status="proposed",
                approval_count_at_proposal=approval_count,
            )
            session.add(order)
            session.flush()
            order_id = order.id
    except Exception as exc:
        if "uq_standing_orders_action_vertical" in str(exc):
            logger.debug(
                "[StandingOrder] proposal skipped — active order already exists "
                "for %s/%s", action_type, vertical,
            )
            return None
        raise

    logger.info(
        "[StandingOrder] proposed id=%d action_type=%s vertical=%s count=%d",
        order_id, action_type, vertical, approval_count,
    )
    return ProposedOrder(
        action_type=action_type,
        vertical=vertical,
        template_id=template_id,
        approval_count=approval_count,
        standing_order_id=order_id,
    )


def _default_conditions(action_type: str) -> list[str]:
    """Base hard-gate conditions always included in every standing order
    proposal — these mirror what Relay's guards already enforce but are
    written explicitly into the rule text so Josh's ratification is informed."""
    base = ["dnc_clear", "suppression_clear", "tcpa_consent_verified"]
    if "price" in action_type or "offer" in action_type:
        base.append("price_floor_met")
    return base


def _post_to_slack(order: ProposedOrder) -> None:
    from config.settings import get_settings
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.relay_slack_channel
    if not token or not channel:
        logger.info(
            "[StandingOrder] Slack not configured — order %d stays proposed without Slack post",
            order.standing_order_id,
        )
        return

    ratify_val = json.dumps({"so_id": order.standing_order_id, "action": "ratify"})
    decline_val = json.dumps({"so_id": order.standing_order_id, "action": "decline"})
    summary = (
        f"*Standing Order Proposal* (#{order.standing_order_id})\n"
        f"Action type: `{order.action_type}` · Vertical: `{order.vertical}`\n"
        f"Josh has approved this pattern *{order.approval_count} times*. "
        f"Ratify to let future matches skip individual review."
    )
    if order.template_id:
        summary += f"\nTemplate: `{order.template_id}`"

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        response = client.chat_postMessage(
            channel=channel,
            text=summary,
            blocks=[
                {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Ratify Standing Order"},
                            "style": "primary",
                            "action_id": "so_ratify",
                            "value": ratify_val,
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Decline"},
                            "style": "danger",
                            "action_id": "so_decline",
                            "value": decline_val,
                        },
                    ],
                },
            ],
        )
        set_slack_ts(order.standing_order_id, response["ts"])
    except Exception as exc:
        logger.error(
            "[StandingOrder] Slack post failed for order %d: %s",
            order.standing_order_id, exc, exc_info=True,
        )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--scan", action="store_true", help="Scan and propose standing orders")
    args = parser.parse_args()
    if args.scan:
        results = scan_and_propose()
        print(f"Proposed {len(results)} standing order(s).")
