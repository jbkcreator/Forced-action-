"""
Relay execution engine (RELAY-v2.2 sub-task R1).

execute_batch() is RELAY's core: it takes a list of already-approved
relay_approval_queue rows and dispatches each one exactly, deterministically,
with no model in the path (build spec §9.1: "No model in the execution
path — no drift, no interpretation, no agent ever touching a prospect").

Two safety guarantees, both DB-enforced rather than in-memory:
  1. Kill command — checked before the batch and before every single item;
     halts within one cycle (build spec §9.1).
  2. Idempotency — a row is only dispatched if try_claim_for_batch()
     atomically claims it as 'approved' and unclaimed; this holds even if
     two sweep runs overlap or a run is retried mid-batch.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from config.settings import get_settings
from src.services.relay import guards, queue
from src.services.relay.channels import DISPATCHERS
from src.services.relay.config import KILL_SWITCH_FEATURE
from src.services.relay.queue import QueueItem
from src.services.kill_switch_service import get_kill_switch_status

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    sent: int = 0
    skipped: int = 0
    failed: int = 0
    deferred: int = 0
    halted: bool = False
    processed_ids: list[int] = field(default_factory=list)


def _kill_switch_is_red() -> bool:
    status = get_kill_switch_status(KILL_SWITCH_FEATURE)
    return status.get("color") == "red"


def execute_batch(items: list[QueueItem], *, batch_id: str, now: datetime | None = None) -> BatchResult:
    """Execute a batch of already-approved queue rows deterministically.

    Each item must already be status='approved' (the caller — the cron
    sweep — is responsible for selecting only approved rows). For every
    item, in order:
      1. Re-check the relay_global kill switch; on red, stop immediately.
         Undispatched items are left untouched (still 'approved',
         unclaimed) so the next sweep tick safely resumes them.
      2. Run guards.evaluate() (RELAY-v2.2 sub-task R3) — send window,
         daily ceiling, execution-time suppression recheck. DEFER leaves
         the row untouched (still 'approved', unclaimed) for a later
         sweep; BLOCK marks it 'skipped', terminal.
      3. Atomically claim the row for this batch_id. If another run
         already claimed it, skip without dispatching (idempotency).
      4. Dispatch via the registered channel handler. An unknown channel,
         or any exception the handler raises, marks the item 'failed' and
         moves on — one bad item never aborts the batch.

    `now` defaults to the real clock; tests inject a fixed value so guard
    behavior (and every pre-R3 test unrelated to guards) doesn't depend on
    what time of day the suite happens to run.
    """
    result = BatchResult()

    if _kill_switch_is_red():
        logger.warning(
            "[Relay] kill switch RED at batch preflight (feature=%s) — "
            "executing zero items", KILL_SWITCH_FEATURE,
        )
        result.halted = True
        return result

    now = now if now is not None else datetime.now(timezone.utc)
    settings = get_settings()
    sent_today = queue.sent_counts_today(now, timezone_name=settings.relay_send_window_timezone)

    for item in items:
        if _kill_switch_is_red():
            remaining = len(items) - len(result.processed_ids)
            logger.warning(
                "[Relay] kill switch RED mid-batch (feature=%s) — halting; "
                "%d/%d items left unclaimed for the next sweep",
                KILL_SWITCH_FEATURE, remaining, len(items),
            )
            result.halted = True
            break

        verdict = guards.evaluate(item, now=now, sent_today=sent_today)
        if verdict.outcome == guards.DEFER:
            logger.info("[Relay] item %d deferred: %s", item.id, verdict.reason)
            result.deferred += 1
            continue
        if verdict.outcome == guards.BLOCK:
            queue.mark_skipped(item.id, verdict.reason)
            logger.warning("[Relay] item %d blocked: %s", item.id, verdict.reason)
            result.skipped += 1
            result.processed_ids.append(item.id)
            continue

        if not queue.try_claim_for_batch(item.id, batch_id):
            logger.info(
                "[Relay] item %d already claimed/moved — skipping (idempotency)",
                item.id,
            )
            queue.mark_skipped(item.id, "claim_lost_to_concurrent_run")
            result.skipped += 1
            result.processed_ids.append(item.id)
            continue

        dispatcher = DISPATCHERS.get(item.channel)
        if dispatcher is None:
            queue.mark_failed(item.id, f"unknown_channel:{item.channel}")
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
            queue.mark_failed(item.id, str(exc))
            logger.error(
                "[Relay] item %d dispatch failed on channel %r: %s",
                item.id, item.channel, exc, exc_info=True,
            )
            result.failed += 1
        else:
            queue.mark_sent(item.id)
            result.sent += 1
            sent_today[item.channel] = sent_today.get(item.channel, 0) + 1
        result.processed_ids.append(item.id)

    return result
