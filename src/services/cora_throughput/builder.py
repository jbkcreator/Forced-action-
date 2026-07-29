"""
Batch construction (THROUGH-v2.2 T1/T2) — the periodic job that turns Cora's
pending drafts into one Slack-reviewable batch for Josh.

Mirrors src.agents.cora.ingestion.target_producer's in-process periodic-
thread pattern. Only ever builds a NEW batch while none is 'pending' — this
is deliberately the entire backpressure mechanism (T2): drafts simply queue
up in outbound_drafts (status='draft') rather than flooding Josh, and no
change is needed in Cora's own producers to get this for free.

MAX_BATCH_SIZE mirrors src.services.human_close_routing.MAX_CANDIDATES_PER_SWEEP's
10-item cap pattern — a separate constant, not imported, since it's a
different domain/queue.
"""
from __future__ import annotations

import logging
import threading
import uuid
from typing import Any, Dict, List

from sqlalchemy import text

from src.agents.cora import store
from src.services.cora_throughput import batch_slack, power_block
from src.services.cora_throughput.decisions import auto_approve_draft

logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = 10
DEFAULT_INTERVAL_SECONDS = 15 * 60
STALE_BATCH_MAX_AGE_HOURS = 24


def _has_pending_batch(db: Any) -> bool:
    row = db.execute(
        text("SELECT 1 FROM cora_draft_batches WHERE status = 'pending' LIMIT 1")
    ).first()
    return row is not None


def _active_standing_order_cell_ids(db: Any) -> set:
    rows = db.execute(
        text("SELECT DISTINCT cell_id FROM cora_standing_orders WHERE active = true")
    ).all()
    return {r[0] for r in rows}


def build_batch(db: Any) -> Dict[str, Any]:
    """
    Runs one batch-construction pass. Returns a dict describing what
    happened (for logging/tests) — never raises for the normal "nothing to
    do" case.
    """
    if _has_pending_batch(db):
        return {"created": False, "reason": "batch_already_pending"}

    eligible: List[Dict[str, Any]] = store.read_drafts(db, status="draft")
    if not eligible:
        return {"created": False, "reason": "no_eligible_drafts"}

    standing_order_cells = _active_standing_order_cell_ids(db)
    auto_approved_count = 0
    remaining: List[Dict[str, Any]] = []
    for draft in eligible:
        if draft.get("cell_id") in standing_order_cells:
            if auto_approve_draft(db, draft):
                auto_approved_count += 1
            continue
        remaining.append(draft)

    batch_drafts = remaining[:MAX_BATCH_SIZE]
    if not batch_drafts:
        return {"created": False, "reason": "all_covered_by_standing_orders", "auto_approved_count": auto_approved_count}

    batch_id = str(uuid.uuid4())
    db.execute(
        text("INSERT INTO cora_draft_batches (batch_id, status) VALUES (:batch_id, 'pending')"),
        {"batch_id": batch_id},
    )
    for draft in batch_drafts:
        db.execute(
            text(
                "INSERT INTO cora_batch_items (batch_id, draft_id, decision) "
                "VALUES (:batch_id, :draft_id, 'included')"
            ),
            {"batch_id": batch_id, "draft_id": draft["draft_id"]},
        )

    extra_blocks = power_block.render_power_block_blocks(power_block.assemble_power_block(db))
    slack_message_ts = batch_slack.post_batch_for_approval(batch_id, batch_drafts, extra_blocks=extra_blocks)
    if slack_message_ts:
        from config.settings import get_settings
        db.execute(
            text(
                "UPDATE cora_draft_batches SET slack_message_ts = :ts, slack_channel = :channel "
                "WHERE batch_id = :batch_id"
            ),
            {"ts": slack_message_ts, "channel": get_settings().cora_throughput_slack_channel, "batch_id": batch_id},
        )

    return {
        "created": True, "batch_id": batch_id, "item_count": len(batch_drafts),
        "auto_approved_count": auto_approved_count,
    }


def expire_stale_batches(db: Any) -> int:
    """
    Default-action timer (T2) — a batch left 'pending' past
    STALE_BATCH_MAX_AGE_HOURS auto-expires. Its still-'included' items need
    no explicit change: they were never moved off outbound_drafts
    status='draft' in the first place (only an approve/reject decision ever
    changes that), so they are automatically eligible for the next
    build_batch() pass once this one is no longer 'pending'.
    """
    result = db.execute(
        text(
            "UPDATE cora_draft_batches SET status = 'expired' "
            "WHERE status = 'pending' AND created_at < now() - make_interval(hours => :max_age_hours)"
        ),
        {"max_age_hours": STALE_BATCH_MAX_AGE_HOURS},
    )
    return result.rowcount


def run_periodic(stop_event: threading.Event, interval_seconds: int = DEFAULT_INTERVAL_SECONDS) -> None:
    from src.core.database import get_db_context

    logger.info("cora_throughput.builder: starting periodic sweep every %ds", interval_seconds)
    while not stop_event.is_set():
        try:
            with get_db_context() as db:
                expired = expire_stale_batches(db)
                if expired:
                    logger.info("cora_throughput.builder: expired %d stale batch(es)", expired)
                result = build_batch(db)
                if result.get("created"):
                    logger.info(
                        "cora_throughput.builder: created batch %s with %d item(s) (%d auto-approved via standing orders)",
                        result["batch_id"], result["item_count"], result.get("auto_approved_count", 0),
                    )
        except Exception:  # noqa: BLE001
            logger.exception("cora_throughput.builder: sweep failed — will retry next interval")
        stop_event.wait(interval_seconds)
    logger.info("cora_throughput.builder: stopped")
