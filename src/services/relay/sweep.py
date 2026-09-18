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
from src.services.relay import exceptions_alert_queue, queue
from src.services.relay.engine import BatchResult, execute_batch
from src.services.relay.slack_post import post_completion_receipt
from src.services.relay.suppression_sync import SuppressionSyncFailed, sync_unsubscribes
from src.utils.venture_config import get_venture_config

# Import for their registration side effect only — makes the real 'email'
# channel (RELAY-v2.2 R2) and the 'sms' channel (WP-T2-1) available in
# DISPATCHERS whenever this module is imported directly (e.g. by a caller
# other than __main__.py, which imports both too).
import src.services.relay.channels_email  # noqa: F401,E402
import src.services.relay.channels_sms  # noqa: F401,E402

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
    unsubscribes unsuppressed.

    (production-execution review, finding 3) A failed sync means this
    batch's suppression view may be stale — the entire reason this sync
    exists is to catch an opt-out landing in Instantly moments before this
    sweep runs. Continuing to execute_batch() on a poll that just failed
    would defeat that purpose silently. So a SuppressionSyncFailed here
    defers the whole batch (approved rows are left completely untouched —
    execute_batch() is never called, so nothing is claimed) and pages
    EXCEPTIONS; the next sweep tick retries the sync and, if it recovers,
    executes normally. A "not_configured" result (venture's email channel
    genuinely isn't wired up yet) is not a failure and does not defer.

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
        result = sync_unsubscribes(venture_key=venture_key)
        if result.status == "not_configured":
            logger.info("[Relay] sweep: venture %s email channel not configured — skipping unsubscribe sync", venture_key)
        elif result.count:
            logger.info("[Relay] sweep: synced %d new suppression(s) from Instantly", result.count)
    except SuppressionSyncFailed as exc:
        # Sync must run and fail (or succeed) BEFORE the queue is even
        # queried — test_sweep_calls_sync_before_execute pins this order,
        # and a stale suppression view is the reason to defer before ever
        # looking at what's approved, not after. The approved_batch() call
        # below is therefore genuinely a separate one for this branch, not
        # a redundant duplicate of the one after this try/except: exactly
        # one of the two ever executes per run_sweep() call, since this one
        # returns immediately.
        logger.error("[Relay] unsubscribe sync failed — deferring batch for venture %s: %s", venture_key, exc)
        # Durable (WP-T2-1 go-live review, 2026-09): a sustained Instantly
        # outage means this branch fires every ~30-min sweep tick until it
        # recovers -- exceptions_alert_queue's own dedup (not this call
        # site) is what stops that from creating a new pending row every
        # tick; see its module docstring.
        exceptions_alert_queue.enqueue_and_attempt(
            venture_key=venture_key,
            rule="relay_suppression_sync_failed",
            message=f"Suppression sync failed for venture {venture_key}; batch deferred to avoid sending against a possibly stale suppression list.\n{exc}",
        )
        deferred = BatchResult()
        deferred.deferred = len(queue.approved_batch(limit=limit, venture_key=venture_key))
        return deferred

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
