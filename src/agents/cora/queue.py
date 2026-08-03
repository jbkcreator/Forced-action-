"""
Cora's event queue — Redis Streams. New Redis-side usage, not a SQL
migration, so it doesn't conflict with this branch's no-DB-changes
constraint. No Redis Streams (XADD/XREADGROUP) existed anywhere in this
codebase before this module; the existing Cora/Lifecycle event transport
(src/agents/events/ingestion.py) uses plain LPUSH/BRPOP on a different key
("cora:queue") plus Postgres LISTEN/NOTIFY — this module is deliberately
separate infrastructure, reusing only the fact that Redis itself is already
a dependency (src.core.redis_client), not any of the old transport's keys,
channels, or Postgres fallback table.

Stream key: "cora:events" | Consumer group: "cora_workers" | DLQ stream:
"cora:dlq". get_redis() already returns decode_responses=True (real Redis
and fakeredis both), so every field on every method here is a plain str,
never bytes.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from src.core.redis_client import get_redis, redis_available

logger = logging.getLogger(__name__)

STREAM_KEY = "cora:events"
GROUP_NAME = "cora_workers"
DLQ_KEY = "cora:dlq"
MAX_DELIVERIES = 3


@dataclass
class QueueMessage:
    message_id: str
    event_type: str
    idempotency_key: str
    payload: Dict[str, Any]
    delivery_count: int = 1


def ensure_group() -> None:
    """Idempotent — creates the stream + consumer group if either is missing."""
    if not redis_available():
        return
    r = get_redis()
    try:
        r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
    except Exception as exc:  # noqa: BLE001
        if "BUSYGROUP" not in str(exc):
            raise


def make_idempotency_key(event_type: str, opportunity_thread_id: Optional[str], content_hash: str) -> str:
    return f"{event_type}:{opportunity_thread_id or 'none'}:{content_hash}"


def publish(event_type: str, payload: Dict[str, Any], idempotency_key: Optional[str] = None) -> Optional[str]:
    """XADD one event. Returns the Redis message id, or None if Redis is unavailable."""
    if not redis_available():
        logger.warning("queue.publish: Redis unavailable — event_type=%s dropped", event_type)
        return None
    ensure_group()
    r = get_redis()
    fields = {
        "event_type": event_type,
        "idempotency_key": idempotency_key or str(uuid.uuid4()),
        "payload": json.dumps(payload, default=str),
    }
    # Approximate MAXLEN trim on every add — bounds unbounded stream growth
    # (XACK removes a message from the consumer group's pending list, not
    # from the stream itself; without trimming, XLEN would grow forever).
    # approximate=True lets Redis trim in whole macro-nodes for O(1) cost
    # instead of an exact per-entry trim.
    message_id = r.xadd(STREAM_KEY, fields, maxlen=10_000, approximate=True)
    return message_id


def _parse_message(message_id: str, fields: Dict[str, str], delivery_count: int = 1) -> QueueMessage:
    return QueueMessage(
        message_id=message_id,
        event_type=fields.get("event_type", ""),
        idempotency_key=fields.get("idempotency_key", ""),
        payload=json.loads(fields.get("payload", "{}")),
        delivery_count=delivery_count,
    )


def read_batch(consumer_name: str, count: int = 1, block_ms: int = 1000) -> List[QueueMessage]:
    """
    XREADGROUP for new messages only (id='>'). Blocks up to block_ms.

    block_ms MUST stay comfortably under src.core.redis_client.get_redis()'s
    hardcoded socket_timeout=2 (2000ms) — that client is shared app-wide, not
    Cora-specific, and a BLOCK that approaches or exceeds the socket's own
    read timeout raises a spurious redis.exceptions.TimeoutError even though
    Redis itself is behaving correctly (confirmed directly: block_ms=2000
    reproduces this on an idle queue). The existing BRPOP consumer elsewhere
    in this codebase (src/agents/events/ingestion.py) works around the exact
    same constraint the same way — timeout=1 — rather than a single long
    block; the worker loop is expected to call this repeatedly instead of
    blocking for a long single wait.
    """
    if not redis_available():
        return []
    ensure_group()
    r = get_redis()
    result = r.xreadgroup(GROUP_NAME, consumer_name, {STREAM_KEY: ">"}, count=count, block=block_ms)
    if not result:
        return []
    messages: List[QueueMessage] = []
    for _stream_name, entries in result:
        for message_id, fields in entries:
            messages.append(_parse_message(message_id, fields))
    return messages


def ack(message_id: str) -> None:
    """Call only after the corresponding store.py append has durably succeeded."""
    if not redis_available():
        return
    get_redis().xack(STREAM_KEY, GROUP_NAME, message_id)


def claim_stale(consumer_name: str, min_idle_ms: int = 60_000) -> List[QueueMessage]:
    """
    Reclaims pending messages idle longer than min_idle_ms. Messages whose
    delivery count has reached MAX_DELIVERIES are dead-lettered instead of
    reclaimed, and acked off the main stream.
    """
    if not redis_available():
        return []
    r = get_redis()
    pending = r.xpending_range(STREAM_KEY, GROUP_NAME, min="-", max="+", count=100)
    reclaimed: List[QueueMessage] = []
    to_claim: List[str] = []
    for entry in pending:
        message_id = entry["message_id"]
        delivery_count = entry.get("times_delivered", 1)
        idle_ms = entry.get("time_since_delivered", 0)
        if idle_ms < min_idle_ms:
            continue
        if delivery_count >= MAX_DELIVERIES:
            _dead_letter_pending(message_id)
            continue
        to_claim.append(message_id)

    if not to_claim:
        return reclaimed

    claimed = r.xclaim(STREAM_KEY, GROUP_NAME, consumer_name, min_idle_time=min_idle_ms, message_ids=to_claim)
    for message_id, fields in claimed:
        if fields is None:
            continue
        # find delivery count from the pending entry we already fetched
        delivery_count = next(
            (e.get("times_delivered", 1) for e in pending if e["message_id"] == message_id), 1
        )
        reclaimed.append(_parse_message(message_id, fields, delivery_count=delivery_count + 1))
    return reclaimed


def _dead_letter_pending(message_id: str) -> None:
    r = get_redis()
    entries = r.xrange(STREAM_KEY, min=message_id, max=message_id)
    if entries:
        _, fields = entries[0]
        dead_letter(message_id, fields, reason="max_deliveries_exceeded")
    r.xack(STREAM_KEY, GROUP_NAME, message_id)


def dead_letter(message_id: str, fields: Dict[str, Any], reason: str) -> None:
    if not redis_available():
        return
    r = get_redis()
    r.xadd(DLQ_KEY, {**fields, "original_message_id": message_id, "dlq_reason": reason})
    logger.warning("queue: dead-lettered message_id=%s reason=%s", message_id, reason)


def queue_depth() -> int:
    """Total entries currently in the stream (includes already-acked, not-yet-trimmed)."""
    if not redis_available():
        return 0
    try:
        return get_redis().xlen(STREAM_KEY)
    except Exception:  # noqa: BLE001
        return 0


def pending_count() -> int:
    """Unacked backlog for the consumer group — the meaningful 'queue depth' for dashboards."""
    if not redis_available():
        return 0
    try:
        summary = get_redis().xpending(STREAM_KEY, GROUP_NAME)
        return summary.get("pending", 0) if summary else 0
    except Exception:  # noqa: BLE001
        return 0


def dlq_depth() -> int:
    if not redis_available():
        return 0
    try:
        return get_redis().xlen(DLQ_KEY)
    except Exception:  # noqa: BLE001
        return 0
