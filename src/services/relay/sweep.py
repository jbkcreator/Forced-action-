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

# Import for its registration side effect only — makes the real 'email'
# channel (RELAY-v2.2 R2) available in DISPATCHERS whenever this module is
# imported directly (e.g. by a caller other than __main__.py, which
# imports it too).
import src.services.relay.channels_email  # noqa: F401,E402

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
