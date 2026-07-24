"""
Relay approval queue — the single read/write seam for relay_approval_queue
(RELAY-v2.2 sub-task R1).

This module IS "Josh's queue" (build spec §1.1.13): enqueue() is the one
entry point Cora (Phase 2, not yet built) will call to write a proposed
action as a 'pending' row; R1's --seed CLI (src/services/relay/__main__.py)
calls this exact function today to build/prove the engine — identical
schema and call shape, zero change when Cora lands. record_decision() is
called by the Slack decision webhook (src/api/admin_router.py). The
execution engine (src/services/relay/engine.py) claims rows via
try_claim_for_batch() and reports outcomes via mark_sent/mark_failed/
mark_skipped.

Per CLAUDE.md: all data retrieval uses sqlalchemy.text(), never the ORM
query API. The RelayApprovalQueueItem ORM model (src.core.models) is used
only for the single-row INSERT in enqueue().
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.core.database import get_db_context
from src.core.models import RelayApprovalQueueItem
from src.services.relay.config import (
    STATUS_APPROVED,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_REJECTED,
    STATUS_SENT,
    STATUS_SKIPPED,
)


@dataclass
class QueueItem:
    """Read-only view of one relay_approval_queue row."""
    id: int
    idempotency_key: str
    batch_id: Optional[str]
    thread_id: Optional[str]
    channel: str
    recipient: str
    payload: dict
    status: str
    slack_message_ts: Optional[str]
    decided_by: Optional[str]
    decided_at: Optional[datetime]
    error: Optional[str]
    dispatched_at: Optional[datetime]
    created_at: datetime


_QUEUE_ITEM_COLUMNS = tuple(f.name for f in fields(QueueItem))
_COLUMNS_SQL = ", ".join(_QUEUE_ITEM_COLUMNS)


def _row_to_item(row: dict) -> QueueItem:
    return QueueItem(**{col: row[col] for col in _QUEUE_ITEM_COLUMNS})


def enqueue(
    *,
    idempotency_key: str,
    channel: str,
    recipient: str,
    payload: dict,
    thread_id: Optional[str] = None,
) -> QueueItem:
    """Write a new 'pending' row. Called by Cora (Phase 2) and by R1's
    --seed CLI today — identical call, zero code change when Cora lands.

    If idempotency_key already exists (e.g. a caller retries the same
    proposed action), returns the existing row instead of raising or
    creating a duplicate.
    """
    try:
        with get_db_context() as session:
            item = RelayApprovalQueueItem(
                idempotency_key=idempotency_key,
                channel=channel,
                recipient=recipient,
                payload=payload,
                thread_id=thread_id,
                status=STATUS_PENDING,
            )
            session.add(item)
            session.flush()
            item_id = item.id
    except IntegrityError:
        existing = get_item_by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing
        raise
    item = get_item(item_id)
    assert item is not None  # just inserted in the same call
    return item


def get_item(item_id: int) -> Optional[QueueItem]:
    """Fetch one row by id, or None."""
    with get_db_context() as session:
        row = session.execute(
            text(f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue WHERE id = :id"),
            {"id": item_id},
        ).mappings().first()
        return _row_to_item(dict(row)) if row else None


def get_item_by_idempotency_key(idempotency_key: str) -> Optional[QueueItem]:
    """Fetch one row by its idempotency key, or None."""
    with get_db_context() as session:
        row = session.execute(
            text(
                f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue "
                "WHERE idempotency_key = :key"
            ),
            {"key": idempotency_key},
        ).mappings().first()
        return _row_to_item(dict(row)) if row else None


def set_slack_message_ts(item_id: int, slack_message_ts: str) -> None:
    """Record the posted Slack message's ts so the decision webhook can
    edit that message in place once Josh responds."""
    with get_db_context() as session:
        session.execute(
            text(
                "UPDATE relay_approval_queue SET slack_message_ts = :ts, "
                "updated_at = now() WHERE id = :id"
            ),
            {"ts": slack_message_ts, "id": item_id},
        )


def record_decision(item_id: int, *, approved: bool, decided_by: str) -> Optional[QueueItem]:
    """Flip a 'pending' row to approved/rejected. Called by the Slack
    decision webhook.

    Returns None (no-op) if the row was not 'pending' at the moment of
    the update — guards a double button-press or a stale/duplicate Slack
    retry from re-deciding an already-decided row.
    """
    new_status = STATUS_APPROVED if approved else STATUS_REJECTED
    with get_db_context() as session:
        result = session.execute(
            text(
                "UPDATE relay_approval_queue "
                "SET status = :new_status, decided_by = :decided_by, "
                "    decided_at = now(), updated_at = now() "
                "WHERE id = :id AND status = :pending"
            ),
            {
                "new_status": new_status,
                "decided_by": decided_by,
                "id": item_id,
                "pending": STATUS_PENDING,
            },
        )
        if result.rowcount == 0:
            return None
    return get_item(item_id)


def approved_batch(limit: int = 50) -> list[QueueItem]:
    """All status='approved' rows, oldest first — what the cron sweep
    hands to the execution engine."""
    with get_db_context() as session:
        rows = session.execute(
            text(
                f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue "
                "WHERE status = :status ORDER BY created_at ASC LIMIT :limit"
            ),
            {"status": STATUS_APPROVED, "limit": limit},
        ).mappings().all()
        return [_row_to_item(dict(r)) for r in rows]


def try_claim_for_batch(item_id: int, batch_id: str) -> bool:
    """Atomically claim an 'approved', unclaimed row for a batch run.

    Guards against a double sweep pickup — e.g. two overlapping cron runs
    both selecting the same approved row before either dispatches it.
    Returns True if this call claimed the row, False if it was already
    claimed (or moved out of 'approved') by another run.
    """
    with get_db_context() as session:
        result = session.execute(
            text(
                "UPDATE relay_approval_queue "
                "SET batch_id = :batch_id, updated_at = now() "
                "WHERE id = :id AND status = :approved AND batch_id IS NULL"
            ),
            {"batch_id": batch_id, "id": item_id, "approved": STATUS_APPROVED},
        )
        return result.rowcount > 0


def mark_sent(item_id: int) -> None:
    with get_db_context() as session:
        session.execute(
            text(
                "UPDATE relay_approval_queue SET status = :status, "
                "dispatched_at = now(), updated_at = now() WHERE id = :id"
            ),
            {"status": STATUS_SENT, "id": item_id},
        )


def mark_failed(item_id: int, error: str) -> None:
    with get_db_context() as session:
        session.execute(
            text(
                "UPDATE relay_approval_queue SET status = :status, "
                "error = :error, updated_at = now() WHERE id = :id"
            ),
            {"status": STATUS_FAILED, "error": error, "id": item_id},
        )


def mark_skipped(item_id: int, reason: str) -> None:
    with get_db_context() as session:
        session.execute(
            text(
                "UPDATE relay_approval_queue SET status = :status, "
                "error = :error, updated_at = now() WHERE id = :id"
            ),
            {"status": STATUS_SKIPPED, "error": reason, "id": item_id},
        )
