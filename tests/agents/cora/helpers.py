"""
Cora's own test harness. Deliberately separate from tests/scenarios/helpers.py,
which drives the OLD Lifecycle runtime's supervisor.dispatch_event — nothing
in this package ever imports that.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from src.agents.cora import queue, worker


def new_consumer_name() -> str:
    return f"test-consumer-{uuid.uuid4().hex[:8]}"


def drain_all(consumer_name: Optional[str] = None, max_batches: int = 20) -> List[queue.QueueMessage]:
    """Reads + processes every currently-available message via a real Worker. Returns what it saw."""
    w = worker.Worker(consumer_name=consumer_name or new_consumer_name())
    seen: List[queue.QueueMessage] = []
    for _ in range(max_batches):
        messages = w.consumer_name and queue.read_batch(w.consumer_name, count=10, block_ms=200)
        if not messages:
            break
        for message in messages:
            w._process_one(message)
            seen.append(message)
    return seen


def publish_and_drain(event_type: str, payload: Dict[str, Any], idempotency_key: Optional[str] = None) -> queue.QueueMessage:
    """Publishes one event and drains it through a real Worker. Returns the QueueMessage seen."""
    queue.publish(event_type, payload, idempotency_key=idempotency_key)
    seen = drain_all(max_batches=1)
    assert len(seen) == 1, f"expected exactly 1 message drained, got {len(seen)}"
    return seen[0]
