"""
M12 — Event consumer reliability infrastructure.

poll_and_dispatch() polls the events table for unprocessed events of given
types, calls a handler per event, and tracks retries in event_failures.

Delivery guarantee: at-least-once. Events marked processed only after the
handler succeeds. A crash between poll and mark allows re-delivery on the
next poll — handlers must be idempotent (use is_processed() guard).

Permanent failure: after max_retries exhausted, failed_permanently=True is
set on the event_failures row. The event is excluded from future polls and
surfaces for manual inspection.
"""
import logging
from datetime import datetime, timezone
from typing import Callable

from sqlalchemy import text as sa_text

from src.services.event_bus import is_processed, mark_processed

logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 50
_DEFAULT_MAX_RETRIES = 3


def poll_and_dispatch(
    session,
    consumer: str,
    event_types: list[str],
    handler: Callable,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    max_retries: int = _DEFAULT_MAX_RETRIES,
) -> dict:
    """
    Poll unprocessed events and dispatch each to handler.

    Args:
        session:      DB session for polling and state writes.
        consumer:     Consumer name — must match processed_events.consumer.
        event_types:  List of event_type values to poll.
        handler:      Callable(session, event_row) → None. Must be idempotent.
        batch_size:   Max events to poll per call.
        max_retries:  Failures before marking failed_permanently.

    Returns dict with: processed, skipped, failed, permanently_failed counts.
    """
    results = {"processed": 0, "skipped": 0, "failed": 0, "permanently_failed": 0}

    rows = session.execute(sa_text("""
        SELECT e.event_id, e.prospect_id, e.event_type,
               e.actor, e.payload, e.occurred_at, e.source_component
        FROM events e
        LEFT JOIN processed_events pe
            ON pe.event_id = e.event_id AND pe.consumer = :consumer
        LEFT JOIN event_failures ef
            ON ef.event_id = e.event_id AND ef.consumer = :consumer
        WHERE e.event_type = ANY(:types)
          AND pe.event_id IS NULL
          AND (ef.event_id IS NULL OR ef.failed_permanently = FALSE)
        ORDER BY e.occurred_at
        LIMIT :batch_size
        FOR UPDATE OF e SKIP LOCKED
    """), {
        "consumer":   consumer,
        "types":      event_types,
        "batch_size": batch_size,
    }).fetchall()

    if not rows:
        logger.debug("[EventConsumer:%s] no pending events", consumer)
        return results

    logger.info("[EventConsumer:%s] dispatching %d event(s)", consumer, len(rows))

    for row in rows:
        event_id = row.event_id

        # Idempotency guard — skip if race-delivered duplicate
        if is_processed(session, event_id, consumer):
            results["skipped"] += 1
            continue

        try:
            handler(session, row)
            mark_processed(session, event_id, consumer)
            session.commit()
            results["processed"] += 1
            logger.debug("[EventConsumer:%s] event_id=%s processed", consumer, event_id)

        except Exception as exc:
            session.rollback()
            permanently = record_failure(session, event_id, consumer, exc, max_retries)
            session.commit()
            results["failed"] += 1
            if permanently:
                results["permanently_failed"] += 1
            logger.error(
                "[EventConsumer:%s] event_id=%s failed (attempt recorded): %s",
                consumer, event_id, exc,
            )

    logger.info(
        "[EventConsumer:%s] done — processed=%d skipped=%d failed=%d permanent=%d",
        consumer,
        results["processed"],
        results["skipped"],
        results["failed"],
        results["permanently_failed"],
    )
    return results


def record_failure(
    session,
    event_id,
    consumer: str,
    exc: Exception,
    max_retries: int,
) -> bool:
    """
    Upsert a failure record for this (event_id, consumer) pair.
    Increments retry_count; sets failed_permanently when max_retries exhausted.
    Returns True if the event is now permanently failed.
    """
    error_msg = str(exc)[:2000]
    now = datetime.now(timezone.utc)

    session.execute(sa_text("""
        INSERT INTO event_failures (event_id, consumer, retry_count, last_error, last_attempt_at)
        VALUES (CAST(:eid AS uuid), :consumer, 1, :error, :now)
        ON CONFLICT (event_id, consumer) DO UPDATE
            SET retry_count      = event_failures.retry_count + 1,
                last_error       = EXCLUDED.last_error,
                last_attempt_at  = EXCLUDED.last_attempt_at,
                failed_permanently = (event_failures.retry_count + 1) >= :max_retries
    """), {
        "eid":         str(event_id),
        "consumer":    consumer,
        "error":       error_msg,
        "now":         now,
        "max_retries": max_retries,
    })

    row = session.execute(sa_text("""
        SELECT failed_permanently FROM event_failures
        WHERE event_id = CAST(:eid AS uuid) AND consumer = :consumer
    """), {"eid": str(event_id), "consumer": consumer}).fetchone()

    return bool(row and row.failed_permanently)


def is_permanently_failed(session, event_id, consumer: str) -> bool:
    """Return True if this event has been permanently failed for this consumer."""
    row = session.execute(sa_text("""
        SELECT 1 FROM event_failures
        WHERE event_id = CAST(:eid AS uuid)
          AND consumer = :consumer
          AND failed_permanently = TRUE
    """), {"eid": str(event_id), "consumer": consumer}).fetchone()
    return row is not None
