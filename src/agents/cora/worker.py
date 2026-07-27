"""
Cora's worker loop.

    XREADGROUP (queue.read_batch)
        -> idempotency check (queue-level, Redis SET NX EX)
        -> per-opportunity_thread_id lock (locks.opportunity_lock)
        -> run_cora_main(event_type, payload, thread_id)   [checkpointed]
        -> XACK only after that call returns without raising
             (durable persistence already happened inside the graph's own
             persist node before it returns, per each subgraph's contract)
        -> periodic claim_stale() sweep for abandoned/stuck messages

Traps SIGINT/SIGTERM for graceful shutdown: stops claiming new work, lets
the in-flight message finish, then exits without acking anything further.
An in-flight message that doesn't finish before the process exits stays
unacked and gets picked up by claim_stale() (this or another consumer)
once its idle time crosses CLAIM_MIN_IDLE_MS.
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import time
import uuid
from typing import Any, Dict, Optional

from src.agents.cora import locks, metrics, queue
from src.agents.cora.kill_switch import cora_halted
from src.agents.cora.main_graph import run_cora_main

logger = logging.getLogger(__name__)

CLAIM_MIN_IDLE_MS = 60_000
CLAIM_SWEEP_EVERY_N_LOOPS = 12  # roughly once/minute at the default 5s block


def _consumer_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _extract_thread_id(event_type: str, payload: Dict[str, Any], message_id: str) -> str:
    if event_type == "target.ready":
        thread_id = (payload.get("buyer_entity") or {}).get("opportunity_thread_id")
    else:
        thread_id = payload.get("opportunity_thread_id")
    # reply.received with no resolvable thread (unmatched) still needs a
    # checkpoint key — scope it to the message so it never collides with a
    # real opportunity_thread_id and never repeats across retries of the
    # SAME message (XCLAIM redelivers the same message_id).
    return thread_id or f"unmatched:{message_id}"


def _already_processed(idempotency_key: str) -> bool:
    """True only once a PRIOR attempt at this idempotency_key already succeeded (see _record_processed)."""
    from src.core.redis_client import get_redis, redis_available

    if not redis_available():
        return False
    return bool(get_redis().exists(f"cora:processed:{idempotency_key}"))


def _record_processed(idempotency_key: str, ttl_seconds: int = 86_400) -> None:
    """
    Marks an idempotency_key as durably done. Called ONLY after run_cora_main
    returns without raising — never before attempting it. Marking on attempt
    (rather than success) would permanently block every retry of a message
    that failed once: XCLAIM redelivers the SAME message_id/idempotency_key
    to retry it, and that redelivery must actually reach run_cora_main again,
    not be silently swallowed as a "duplicate." True duplicates (the same
    event genuinely published twice) are a different case, already handled
    by this check running before a mark exists only for keys that finished.
    """
    from src.core.redis_client import get_redis, redis_available

    if not redis_available():
        return
    get_redis().set(f"cora:processed:{idempotency_key}", "1", ex=ttl_seconds)


class Worker:
    def __init__(self, consumer_name: Optional[str] = None) -> None:
        self.consumer_name = consumer_name or _consumer_name()
        self._stop = False
        self._loop_count = 0

    def request_stop(self, *_args: Any) -> None:
        logger.info("cora.worker: shutdown requested (consumer=%s) — draining in-flight work", self.consumer_name)
        self._stop = True

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def _process_one(self, message: "queue.QueueMessage") -> None:
        thread_id = _extract_thread_id(message.event_type, message.payload, message.message_id)
        started = time.monotonic()

        if _already_processed(message.idempotency_key):
            logger.info(
                "cora.worker: duplicate idempotency_key=%s event_type=%s — acking without reprocessing",
                message.idempotency_key, message.event_type,
            )
            queue.ack(message.message_id)
            return

        with locks.opportunity_lock(thread_id, owner=self.consumer_name) as acquired:
            if not acquired:
                logger.info(
                    "cora.worker: thread_id=%s locked by another worker — leaving message %s unacked for retry",
                    thread_id, message.message_id,
                )
                return

            try:
                result = run_cora_main(message.event_type, message.payload, thread_id)
                outcome = result.get("terminal_status", "failed")
            except Exception:
                logger.exception(
                    "cora.worker: run_cora_main raised for event_type=%s thread_id=%s message_id=%s — "
                    "leaving unacked (delivery_count=%d)",
                    message.event_type, thread_id, message.message_id, message.delivery_count,
                )
                metrics.record_event(message.event_type, "failed", time.monotonic() - started)
                return

            # The graph's own persist node already wrote the durable business
            # record before returning — mark processed + ack now, never before.
            _record_processed(message.idempotency_key)
            queue.ack(message.message_id)
            metrics.record_event(message.event_type, outcome, time.monotonic() - started)
            logger.info(
                "cora.worker: processed event_type=%s thread_id=%s outcome=%s reject_reason=%s message_id=%s",
                message.event_type, thread_id, outcome, result.get("reject_reason"), message.message_id,
            )

    def _sweep_stale(self) -> None:
        reclaimed = queue.claim_stale(self.consumer_name, min_idle_ms=CLAIM_MIN_IDLE_MS)
        for message in reclaimed:
            logger.info(
                "cora.worker: reclaimed stale message_id=%s event_type=%s delivery_count=%d",
                message.message_id, message.event_type, message.delivery_count,
            )
            self._process_one(message)

    def run_forever(self, block_ms: int = 1000) -> None:
        # block_ms must stay well under the shared Redis client's hardcoded
        # socket_timeout=2 (src.core.redis_client.get_redis()) — see
        # queue.read_batch's docstring for why. The loop below simply calls
        # read_batch repeatedly instead of one long block, same pattern the
        # existing BRPOP consumer (src/agents/events/ingestion.py) uses.
        queue.ensure_group()
        logger.info("cora.worker: starting (consumer=%s)", self.consumer_name)
        while not self._stop:
            if cora_halted():
                logger.warning("cora.worker: kill switch active — idling without claiming new work")
                time.sleep(min(block_ms / 1000, 5))
                continue

            self._loop_count += 1
            try:
                if self._loop_count % CLAIM_SWEEP_EVERY_N_LOOPS == 0:
                    self._sweep_stale()

                messages = queue.read_batch(self.consumer_name, count=1, block_ms=block_ms)
                for message in messages:
                    if self._stop:
                        break
                    self._process_one(message)

                metrics.refresh_queue_gauges()
            except Exception:
                # A transient Redis hiccup (e.g. a blocking read landing right at
                # the shared client's socket_timeout boundary — reproduced
                # directly during load-testing) must not kill the whole worker
                # process. Anything mid-flight that didn't get acked is safe:
                # it stays in the consumer group's PEL for claim_stale() to
                # pick up on a later loop.
                logger.exception("cora.worker: main loop iteration failed — continuing")

        logger.info("cora.worker: stopped (consumer=%s)", self.consumer_name)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    worker = Worker()
    worker.install_signal_handlers()
    worker.run_forever()


if __name__ == "__main__":
    main()
