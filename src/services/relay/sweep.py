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

from config.venture_template import DEFAULT_VENTURE_KEY
from src.services.relay import queue
from src.services.relay.engine import BatchResult, execute_batch
from src.services.relay.slack_post import post_completion_receipt
from src.services.relay.suppression_sync import sync_unsubscribes
from src.utils.venture_config import get_venture_config

# Import for its registration side effect only — makes the real 'email'
# channel (RELAY-v2.2 R2) available in DISPATCHERS whenever this module is
# imported directly (e.g. by a caller other than __main__.py, which
# imports it too).
import src.services.relay.channels_email  # noqa: F401,E402

logger = logging.getLogger(__name__)


def run_sweep(*, limit: int = 50, venture_key: str = DEFAULT_VENTURE_KEY) -> BatchResult:
    """Query relay_approval_queue WHERE status='approved', execute them as
    one batch tagged with a fresh batch_id. Returns the BatchResult (also
    what --sweep prints).

    Syncs Relay unsubscribes from Instantly first (RELAY-v2.2 R3, client
    Q1) so a fresh opt-out is already in email_opt_outs before this same
    tick's guards.evaluate() suppression recheck runs. Scoped to this same
    venture_key (CLONE-v2.2 / CL3) since each venture sends through its own
    Instantly campaign — syncing the wrong one would leave a venture's real
    unsubscribes unsuppressed. A dead Instantly API must not stop
    already-approved sends, so failures here are logged and swallowed rather
    than propagated.

    One sweep run covers exactly one venture (CLONE-v2.2 / CL3): the batch
    is filtered to that venture's rows and executed under that venture's
    resolved config, because the send window, daily ceiling and kill-switch
    key all differ per venture. A second venture means a second cron line
    (`--sweep --venture <key>`), not a wider batch.

    PR #195 review: a deactivated venture (ventures.is_active = false) must
    refuse the whole batch rather than execute it. This is the second,
    independent gate — src.utils.venture_config already resolves a
    deactivated venture to a config with no Instantly campaign/sender, but
    checking is_active here too means an approved item is never even handed
    to execute_batch for a venture that is supposed to be off.
    """
    try:
        n = sync_unsubscribes(venture_key=venture_key)
        if n:
            logger.info("[Relay] sweep: synced %d new suppression(s) from Instantly", n)
    except Exception:
        logger.error("[Relay] unsubscribe sync failed — continuing to execute batch", exc_info=True)

    items = queue.approved_batch(limit=limit, venture_key=venture_key)
    if not items:
        logger.info("[Relay] sweep: no approved items for venture %s", venture_key)
        return BatchResult()

    venture = get_venture_config(venture_key)
    if not venture.is_active:
        logger.warning(
            "[Relay] sweep: venture %s is deactivated — refusing to execute "
            "%d approved item(s); left untouched for a human to resolve "
            "(reactivate the venture or cancel the items)",
            venture_key, len(items),
        )
        result = BatchResult()
        result.halted = True
        return result

    batch_id = f"batch-{uuid.uuid4().hex[:12]}"
    logger.info(
        "[Relay] sweep: executing %d approved item(s) for venture %s as %s",
        len(items), venture_key, batch_id,
    )
    result = execute_batch(items, batch_id=batch_id, venture=venture)
    post_completion_receipt(batch_id, result, venture_key=venture_key)
    return result
