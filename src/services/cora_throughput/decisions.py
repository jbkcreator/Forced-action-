"""
Batch decision mutations (THROUGH-v2.2 T1) — the actual bridge from a
founder's Slack tap into Relay's execution queue.

approve_all: for every item still 'included' in the batch, enqueue()s +
record_decision(approved=True) into relay_approval_queue (both untouched,
existing Relay primitives — src.services.relay.queue), then marks the
draft 'approved_pending_send'. Relay's own execute_batch() cron sweep
(unmodified) picks it up from there.

reject_item: an exception-reject for exactly one draft within an otherwise-
approved batch — marks that one item/draft rejected, leaves the batch
'pending' for the remaining tap. Idempotent: re-processing an
already-decided item/batch is a no-op, not an error, so a double Slack
button-press or a retried webhook can't double-enqueue or double-reject.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from src.agents.cora import contracts, store
from src.services.relay import queue as relay_queue

logger = logging.getLogger(__name__)


def _get_batch(db: Any, batch_id: str) -> Optional[Dict[str, Any]]:
    row = db.execute(
        text(
            "SELECT batch_id, status, slack_message_ts, slack_channel, decided_by, decided_at "
            "FROM cora_draft_batches WHERE batch_id = :batch_id"
        ),
        {"batch_id": batch_id},
    ).mappings().first()
    return dict(row) if row else None


def _get_batch_items_with_drafts(db: Any, batch_id: str) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT bi.id AS item_id, bi.draft_id, bi.decision,
                   d.opportunity_thread_id, d.recommended_channel, d.subject, d.body,
                   d.booking_link, d.payment_link, d.contact_email, d.contact_phone, d.cell_id
            FROM cora_batch_items bi
            JOIN outbound_drafts d ON d.draft_id = bi.draft_id
            WHERE bi.batch_id = :batch_id
            ORDER BY bi.id ASC
            """
        ),
        {"batch_id": batch_id},
    ).mappings().all()
    return [dict(r) for r in rows]


def _enqueue_to_relay(item: Dict[str, Any], decided_by: str) -> bool:
    """Returns True if a Relay queue item was enqueued+approved, False if
    skipped (e.g. no recipient for the draft's channel — logged, not raised,
    since one bad draft in a batch must never block the rest)."""
    recipient = item["contact_email"] if item["recommended_channel"] == "email" else item["contact_phone"]
    if not recipient:
        logger.warning(
            "cora_throughput.decisions: draft %s has no recipient for channel=%s — skipping Relay enqueue",
            item["draft_id"], item["recommended_channel"],
        )
        return False

    payload = dict(contracts.to_relay_handoff_payload({
        "draft_id": item["draft_id"],
        "opportunity_thread_id": item["opportunity_thread_id"],
        "recommended_channel": item["recommended_channel"],
        "subject": item["subject"],
        "body": item["body"],
        "booking_link": item["booking_link"],
        "payment_link": item["payment_link"],
    }))
    payload["approved_by"] = decided_by
    payload["approved_at"] = store.now().isoformat()

    queue_item = relay_queue.enqueue(
        idempotency_key=f"cora_draft:{item['draft_id']}",
        channel=item["recommended_channel"],
        recipient=recipient,
        payload=payload,
        thread_id=item["opportunity_thread_id"],
    )
    relay_queue.record_decision(queue_item.id, approved=True, decided_by=decided_by)
    return True


def auto_approve_draft(db: Any, draft: Dict[str, Any], decided_by: str = "standing_order") -> bool:
    """
    Bypasses Slack entirely for a draft whose cell_id is covered by an active
    cora_standing_orders rule (T4) — called from builder.py before a draft
    is ever added to a batch, not after. Reuses the exact same Relay-enqueue
    + draft-status path as an approve_all decision so a standing-order
    auto-approval is indistinguishable from a manual one downstream.
    """
    item = {
        "draft_id": draft["draft_id"],
        "opportunity_thread_id": draft["opportunity_thread_id"],
        "recommended_channel": draft["recommended_channel"],
        "subject": draft["subject"],
        "body": draft["body"],
        "booking_link": draft.get("booking_link"),
        "payment_link": draft.get("payment_link"),
        "contact_email": draft.get("contact_email"),
        "contact_phone": draft.get("contact_phone"),
    }
    if _enqueue_to_relay(item, decided_by):
        store.mark_draft_status(db, draft["draft_id"], "approved_pending_send")
        return True
    return False


def record_batch_decision(
    db: Any, batch_id: str, action: str, decided_by: str, draft_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    action: "approve_all" | "reject_item" (draft_id required for reject_item).
    Never raises for a normal "already decided" race — returns
    {"ok": False, "reason": ...} instead, since a double button-press or a
    replayed Slack webhook is an expected, not exceptional, event.
    """
    batch = _get_batch(db, batch_id)
    if batch is None:
        return {"ok": False, "reason": "batch_not_found"}

    if action == "reject_item":
        if not draft_id:
            return {"ok": False, "reason": "draft_id_required"}
        result = db.execute(
            text(
                "UPDATE cora_batch_items SET decision = 'exception_rejected', decided_at = now() "
                "WHERE batch_id = :batch_id AND draft_id = :draft_id AND decision = 'included'"
            ),
            {"batch_id": batch_id, "draft_id": draft_id},
        )
        if result.rowcount == 0:
            return {"ok": False, "reason": "already_decided_or_not_found"}
        store.mark_draft_status(db, draft_id, "rejected", reject_reason="batch_exception_reject")
        return {"ok": True, "action": "reject_item", "batch_id": batch_id, "draft_id": draft_id}

    if action == "approve_all":
        if batch["status"] != "pending":
            return {"ok": False, "reason": "batch_already_decided"}

        items = _get_batch_items_with_drafts(db, batch_id)
        approved_count = 0
        for item in items:
            if item["decision"] != "included":
                continue
            if _enqueue_to_relay(item, decided_by):
                store.mark_draft_status(db, item["draft_id"], "approved_pending_send")
                db.execute(
                    text("UPDATE cora_batch_items SET decided_at = now() WHERE id = :id"),
                    {"id": item["item_id"]},
                )
                approved_count += 1

        rejected_count = sum(1 for i in items if i["decision"] == "exception_rejected")
        new_status = "partial" if rejected_count > 0 else "approved"
        db.execute(
            text(
                "UPDATE cora_draft_batches SET status = :status, decided_by = :decided_by, decided_at = now() "
                "WHERE batch_id = :batch_id"
            ),
            {"status": new_status, "decided_by": decided_by, "batch_id": batch_id},
        )
        return {
            "ok": True, "action": "approve_all", "batch_id": batch_id,
            "approved_count": approved_count, "rejected_count": rejected_count,
        }

    return {"ok": False, "reason": "unknown_action"}


def record_standing_order_decision(db: Any, standing_order_id: int, action: str, decided_by: str) -> Dict[str, Any]:
    """
    action: "ratify_standing_order" | "decline_standing_order"
            | "archive_standing_order".
    Ratifying flips the row `active=true` — builder.py's next batch-
    construction pass will then auto-approve matching drafts (see
    builder._active_standing_order_cell_ids). Declining deletes the row
    entirely, allowing a future clean approval streak to propose again
    rather than being permanently blocked by one declined proposal.
    Archiving (monthly-digest prune) deletes an already-active row — same
    reasoning as decline: the cell_id can re-earn a proposal after 5 more
    clean approvals rather than being locked out forever.
    """
    row = db.execute(
        text("SELECT slack_message_ts FROM cora_standing_orders WHERE id = :id"), {"id": standing_order_id},
    ).first()
    slack_message_ts = row[0] if row else None

    if action == "ratify_standing_order":
        result = db.execute(
            text(
                "UPDATE cora_standing_orders SET active = true, created_by = :decided_by "
                "WHERE id = :id AND active = false"
            ),
            {"decided_by": decided_by, "id": standing_order_id},
        )
        if result.rowcount == 0:
            return {"ok": False, "reason": "already_decided_or_not_found"}
        return {"ok": True, "action": "ratify_standing_order", "standing_order_id": standing_order_id, "slack_message_ts": slack_message_ts}

    if action == "decline_standing_order":
        result = db.execute(
            text("DELETE FROM cora_standing_orders WHERE id = :id AND active = false"),
            {"id": standing_order_id},
        )
        if result.rowcount == 0:
            return {"ok": False, "reason": "already_decided_or_not_found"}
        return {"ok": True, "action": "decline_standing_order", "standing_order_id": standing_order_id, "slack_message_ts": slack_message_ts}

    if action == "archive_standing_order":
        result = db.execute(
            text("DELETE FROM cora_standing_orders WHERE id = :id AND active = true"),
            {"id": standing_order_id},
        )
        if result.rowcount == 0:
            return {"ok": False, "reason": "not_active_or_not_found"}
        return {"ok": True, "action": "archive_standing_order", "standing_order_id": standing_order_id, "slack_message_ts": slack_message_ts}

    return {"ok": False, "reason": "unknown_action"}
