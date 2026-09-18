"""
FA Max agent worker loop (WP-T2-2).

    claim_next_work_item(queue_name=FA_MAX_QUEUE_NAME)
        -> run_fa_max_agent(...)   [checkpointed bounded tool-call loop]
        -> complete_work_item(status='done'|'failed') only if the loop
           returned without raising
        -> periodic reclaim_expired_work_items() sweep for abandoned/stuck
           claims (a crashed worker's lease simply expires; there is no
           separate retry path — see agent_graph.py's module docstring)

This is a distinct consumer from src.agents.cora.worker (Cora's own Redis
XREADGROUP consumer) and from the Lifecycle supervisor
(src.agents.supervisor, Redis pub/sub + Postgres LISTEN) — FA Max work items
live in the durable fa_max_work_queue table, not a Redis stream, so this
loop polls claim_next_work_item() directly rather than blocking on Redis.

Usage:
    python -m src.agents.fa_max.worker
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import time
import uuid
from typing import Any, Dict, Optional

from src.agents.fa_max.agent_graph import run_fa_max_agent
from src.core.database import get_db_context
from src.services.state_engine import (
    claim_next_work_item,
    complete_work_item,
    reclaim_expired_work_items,
)

logger = logging.getLogger(__name__)

# The queue_name new FA Max agent work items must be enqueued under (via
# src.services.state_engine.enqueue_work_item(queue_name=FA_MAX_QUEUE_NAME, ...))
# for this worker to claim them.
FA_MAX_QUEUE_NAME = "fa_max_agent"

DEFAULT_LEASE_SECONDS = 300
IDLE_POLL_SECONDS = 5
RECLAIM_SWEEP_EVERY_N_LOOPS = 12  # roughly once/minute at the default 5s idle poll


def _worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


class FaMaxWorker:
    def __init__(self, worker_id: Optional[str] = None) -> None:
        self.worker_id = worker_id or _worker_id()
        self._stop = False
        self._loop_count = 0

    def request_stop(self, *_args: Any) -> None:
        logger.info("fa_max.worker: shutdown requested (worker=%s) — draining in-flight work", self.worker_id)
        self._stop = True

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def _claim(self) -> Optional[Dict[str, Any]]:
        with get_db_context() as session:
            return claim_next_work_item(
                session=session,
                queue_name=FA_MAX_QUEUE_NAME,
                worker_id=self.worker_id,
                lease_seconds=DEFAULT_LEASE_SECONDS,
            )

    def _complete(self, work_item_id: str, status: str) -> None:
        with get_db_context() as session:
            completed = complete_work_item(
                session=session, work_item_id=work_item_id, worker_id=self.worker_id, status=status,
            )
        if not completed:
            logger.warning(
                "fa_max.worker: complete_work_item no-op for work_item_id=%s status=%s "
                "(lease likely expired and item was reclaimed by another worker)",
                work_item_id, status,
            )

    def _process_one(self, item: Dict[str, Any]) -> None:
        work_item_id = item["work_item_id"]
        payload = item.get("payload") or {}
        agent_name = payload.get("agent_name") or "fa_max_agent"
        steps = payload.get("steps") or []

        try:
            if payload.get("task_description"):
                from src.agents.fa_max.tool_registry import select_task_tools
                steps = select_task_tools(payload["task_description"], payload.get("context") or {})
            result = run_fa_max_agent(work_item_id=work_item_id, agent_name=agent_name, steps=steps)
        except ValueError as exc:
            logger.warning("fa_max.worker: invalid task work_item_id=%s: %s", work_item_id, exc)
            self._complete(work_item_id, "failed")
            return
        except Exception:
            logger.exception(
                "fa_max.worker: run_fa_max_agent raised for work_item_id=%s agent_name=%s — "
                "leaving claimed for lease expiry (attempt_count=%s)",
                work_item_id, agent_name, item.get("attempt_count"),
            )
            return

        status = "failed" if result.get("error") else "done"
        self._complete(work_item_id, status)
        logger.info(
            "fa_max.worker: processed work_item_id=%s agent_name=%s status=%s error=%s",
            work_item_id, agent_name, status, result.get("error"),
        )

    def _sweep_expired(self) -> None:
        from config.agents import get_agents_settings
        from src.services.fa_max_tool_log import reconcile_expired_send_attempts

        with get_db_context() as session:
            reclaim_expired_work_items(session=session, queue_name=FA_MAX_QUEUE_NAME)
            reconcile_expired_send_attempts(
                session=session,
                timeout_seconds=get_agents_settings().fa_max_agent_tool_timeout_seconds,
            )

    def run_forever(self, idle_poll_seconds: int = IDLE_POLL_SECONDS) -> None:
        logger.info("fa_max.worker: starting (worker=%s)", self.worker_id)
        while not self._stop:
            self._loop_count += 1
            try:
                if self._loop_count % RECLAIM_SWEEP_EVERY_N_LOOPS == 0:
                    self._sweep_expired()

                item = self._claim()
                if item is None:
                    time.sleep(idle_poll_seconds)
                    continue

                self._process_one(item)
            except Exception:
                # A transient DB hiccup must not kill the whole worker process.
                # Anything claimed but not completed keeps its lease and is
                # reclaimed once it expires.
                logger.exception("fa_max.worker: main loop iteration failed — continuing")

        logger.info("fa_max.worker: stopped (worker=%s)", self.worker_id)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    worker = FaMaxWorker()
    worker.install_signal_handlers()
    worker.run_forever()


if __name__ == "__main__":
    main()
