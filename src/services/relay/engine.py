"""
Relay execution engine (RELAY-v2.2 sub-task R1; reworked for PR #179
review findings #1 and #2).

execute_batch() is RELAY's core: it takes a list of already-approved
relay_approval_queue rows and dispatches each one exactly, deterministically,
with no model in the path (build spec §9.1: "No model in the execution
path — no drift, no interpretation, no agent ever touching a prospect").

Three safety guarantees, all DB/Redis-enforced rather than in-memory:
  1. Kill command — checked before the batch and before every single item;
     halts within one cycle (build spec §9.1).
  2. Idempotency — a row is only dispatched if try_claim_for_batch()
     atomically claims it as 'approved' and unclaimed; this holds even if
     two sweep runs overlap or a run is retried mid-batch. A lost claim
     leaves the row completely untouched (never marked 'skipped') so the
     winning worker's eventual mark_sent()/mark_failed() -- guarded on
     that worker's own batch_id -- is never silently overwritten.
  3. Daily ceiling — reserved atomically per item via
     guards.reserve_daily_slot() BEFORE the row is claimed, so concurrent
     workers can never jointly exceed the configured per-channel cap. A
     reservation that ends up unused (lost claim, failed dispatch) is
     refunded via guards.release_daily_slot().
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.services.relay import guards, queue
from src.services.relay.channels import DISPATCHERS
from src.services.relay.queue import QueueItem
from src.services.kill_switch_service import get_kill_switch_status
from src.utils.venture_config import get_venture_config

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    sent: int = 0
    skipped: int = 0
    failed: int = 0
    deferred: int = 0
    halted: bool = False
    processed_ids: list[int] = field(default_factory=list)


def _kill_switch_is_red(feature: str) -> bool:
    status = get_kill_switch_status(feature)
    return status.get("color") == "red"


def execute_batch(
    items: list[QueueItem],
    *,
    batch_id: str,
    now: datetime | None = None,
    venture=None,
) -> BatchResult:
    """Execute a batch of already-approved queue rows deterministically.

    Each item must already be status='approved' (the caller — the cron
    sweep — is responsible for selecting only approved rows). For every
    item, in order:
      1. Re-check the venture's kill switch (relay_global unless the venture
         opts into its own key); on red, stop immediately. Undispatched
         items are left untouched (still 'approved', unclaimed) so the next
         sweep tick safely resumes them.
      2. Run guards.evaluate() — send window, execution-time suppression
         recheck. DEFER leaves the row untouched (still 'approved',
         unclaimed) for a later sweep; BLOCK marks it 'skipped', terminal.
      3. Atomically reserve one of today's per-channel send slots
         (guards.reserve_daily_slot(), PR #179 review finding #2) BEFORE
         claiming the row — the ceiling is shared state across every
         concurrent worker, so it must be reserved before, not after, a
         claim that might turn out to be lost.
      4. Atomically claim the row for this batch_id. If another run
         already claimed it, the row is left completely untouched (PR #179
         review finding #1 — it is never marked 'skipped'; either the
         owning worker finishes it correctly, or it becomes reclaimable
         once try_claim_for_batch's staleness window elapses) and the
         reservation from step 3 is refunded.
      5. Dispatch via the registered channel handler. An unknown channel,
         or any exception the handler raises, marks the item 'failed'
         (guarded on this worker's own batch_id, per finding #1) and
         refunds the reservation — one bad item never aborts the batch.

    `now` defaults to the real clock; tests inject a fixed value so guard
    behavior (and every pre-R3 test unrelated to guards) doesn't depend on
    what time of day the suite happens to run.

    `venture` is the resolved VentureConfig governing this batch (CLONE-v2.2
    / CL3) — it supplies the kill-switch key, send window and daily ceiling.
    Omitted, venture #1 is resolved, which reads the same values Relay used
    before CL3. A batch must be homogeneous: the caller (sweep.run_sweep)
    selects rows for one venture and passes that venture's config.
    """
    result = BatchResult()

    venture = venture if venture is not None else get_venture_config()
    kill_switch_feature = venture.kill_switch_feature

    if _kill_switch_is_red(kill_switch_feature):
        logger.warning(
            "[Relay] kill switch RED at batch preflight (feature=%s) — "
            "executing zero items", kill_switch_feature,
        )
        result.halted = True
        return result

    now = now if now is not None else datetime.now(timezone.utc)

    for item in items:
        if _kill_switch_is_red(kill_switch_feature):
            remaining = len(items) - len(result.processed_ids)
            logger.warning(
                "[Relay] kill switch RED mid-batch (feature=%s) — halting; "
                "%d/%d items left unclaimed for the next sweep",
                kill_switch_feature, remaining, len(items),
            )
            result.halted = True
            break

        if item.venture_key == "fa_max_lending" and queue.mark_uncertain_if_stale(item.id):
            from src.services.relay.slack_post import post_uncertain_action
            post_uncertain_action(item)
            result.failed += 1
            result.processed_ids.append(item.id)
            continue

        verdict = guards.evaluate(item, now=now, venture=venture)
        if verdict.outcome == guards.DEFER:
            if (item.venture_key == "fa_max_lending" and
                    verdict.reason == "fa_max_opportunity_link_requires_review"):
                from src.services.relay import exceptions_alert_queue
                exceptions_alert_queue.enqueue_and_attempt(
                    venture_key="fa_max_lending",
                    rule="fa_max_opportunity_link_requires_review",
                    message=f"Relay item {item.id} is held for an operator-verified opportunity link.",
                )
            logger.info("[Relay] item %d deferred: %s", item.id, verdict.reason)
            result.deferred += 1
            continue
        if verdict.outcome == guards.BLOCK:
            skipped = queue.mark_skipped(item.id, verdict.reason)
            if skipped and item.venture_key == "fa_max_lending":
                # Notification is observability only.  The durable skipped
                # transition above is the enforcement boundary.
                from src.services.relay.slack_post import post_blocked_action
                post_blocked_action(item, verdict.reason)
            logger.warning("[Relay] item %d blocked: %s", item.id, verdict.reason)
            result.skipped += 1
            result.processed_ids.append(item.id)
            continue

        if not guards.reserve_daily_slot(item.channel, now, venture):
            logger.info(
                "[Relay] item %d deferred: daily ceiling reached (or Redis unavailable) for channel %s",
                item.id, item.channel,
            )
            result.deferred += 1
            continue

        if not queue.try_claim_for_batch(item.id, batch_id):
            logger.info(
                "[Relay] item %d already claimed/moved — leaving untouched for its owner (idempotency)",
                item.id,
            )
            guards.release_daily_slot(item.channel, now, venture)
            result.skipped += 1
            result.processed_ids.append(item.id)
            continue

        dispatcher = DISPATCHERS.get(item.channel)
        if dispatcher is None:
            guards.release_daily_slot(item.channel, now, venture)
            queue.mark_failed(item.id, f"unknown_channel:{item.channel}", batch_id=batch_id)
            logger.error(
                "[Relay] item %d has unknown channel %r — marked failed",
                item.id, item.channel,
            )
            result.failed += 1
            result.processed_ids.append(item.id)
            continue

        try:
            dispatcher(item)
        except Exception as exc:
            guards.release_daily_slot(item.channel, now, venture)
            queue.mark_failed(item.id, str(exc), batch_id=batch_id)
            logger.error(
                "[Relay] item %d dispatch failed on channel %r: %s",
                item.id, item.channel, exc, exc_info=True,
            )
            result.failed += 1
        else:
            queue.mark_sent(item.id, batch_id=batch_id)
            result.sent += 1
        result.processed_ids.append(item.id)

    return result
