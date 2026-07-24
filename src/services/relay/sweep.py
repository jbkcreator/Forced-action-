"""
Relay approval-queue sweep (RELAY-v2.2 sub-task R1).

Reads relay_approval_queue rows Josh approved via Slack and executes them
as one batch. Mirrors src/tasks/auto_mode_followup.py's cron-sweep shape
(cadence: every 30 minutes) — no new scheduler, per the confirmed design
in Relay-logic.md.
"""
from __future__ import annotations

import logging
import uuid

from src.services.relay import queue
from src.services.relay.engine import BatchResult, execute_batch

logger = logging.getLogger(__name__)


def run_sweep(*, limit: int = 50) -> BatchResult:
    """Query relay_approval_queue WHERE status='approved', execute them as
    one batch tagged with a fresh batch_id. Returns the BatchResult (also
    what --sweep prints)."""
    items = queue.approved_batch(limit=limit)
    if not items:
        logger.info("[Relay] sweep: no approved items")
        return BatchResult()

    batch_id = f"batch-{uuid.uuid4().hex[:12]}"
    logger.info("[Relay] sweep: executing %d approved item(s) as %s", len(items), batch_id)
    return execute_batch(items, batch_id=batch_id)
