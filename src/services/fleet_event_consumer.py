"""
Fleet event poll-and-dispatch — the read side of the QUALITY-v2.2 Q1
event-trigger dispatcher.

Delivery guarantee: at-least-once, same as src/services/event_consumer.py's
poll_and_dispatch (handlers must be idempotent). The one behavioral
difference: rows are ordered by (priority ASC, occurred_at ASC), not pure
occurred_at — this is spec §9.5's "deadline-aware preemption": an urgent
event (priority=PRIORITY_URGENT) is dispatched ahead of older routine
events in the same batch.

Permanent failure: after max_retries exhausted, failed_permanently=True is
set on the fleet_event_failures row and the event is excluded from future
polls for that consumer.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable

from sqlalchemy import text as sa_text

from src.services.fleet_event_bus import is_processed, mark_processed

logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 50
_DEFAULT_MAX_RETRIES = 3


def poll_and_dispatch_fleet(
    session,
    consumer: str,
    event_types: list[str],
    handler: Callable,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    max_retries: int = _DEFAULT_MAX_RETRIES,
) -> dict:
    """
    Poll unprocessed fleet_events rows and dispatch each to handler, most
    urgent first.

    Args:
        session:      DB session for polling and state writes.
        consumer:     Consumer name — matches fleet_processed_events.consumer.
        event_types:  List of fleet_events.event_type values to poll.
        handler:      Callable(session, event_row) -> None. Must be idempotent.
        batch_size:   Max events to poll per call.
        max_retries:  Failures before marking failed_permanently.

    Returns dict with: processed, skipped, failed, permanently_failed counts.
    """
    results = {"processed": 0, "skipped": 0, "failed": 0, "permanently_failed": 0}

    rows = session.execute(sa_text("""
        SELECT e.id, e.id AS event_id, e.event_type, e.priority, e.source_component,
               e.subscriber_id, e.opportunity_thread_id, e.payload, e.occurred_at
        FROM fleet_events e
        LEFT JOIN fleet_processed_events pe
            ON pe.event_id = e.id AND pe.consumer = :consumer
        LEFT JOIN fleet_event_failures ef
            ON ef.event_id = e.id AND ef.consumer = :consumer
        WHERE e.event_type = ANY(:types)
          AND pe.event_id IS NULL
          AND (ef.event_id IS NULL OR ef.failed_permanently = FALSE)
        ORDER BY e.priority ASC, e.occurred_at ASC
        LIMIT :batch_size
        FOR UPDATE OF e SKIP LOCKED
    """), {
        "consumer":   consumer,
        "types":      event_types,
        "batch_size": batch_size,
    }).fetchall()

    if not rows:
        logger.debug("[FleetEventConsumer:%s] no pending events", consumer)
        return results

    logger.info("[FleetEventConsumer:%s] dispatching %d event(s)", consumer, len(rows))

    for row in rows:
        event_id = row.event_id

        if is_processed(session, event_id, consumer):
            results["skipped"] += 1
            continue

        try:
            handler(session, row)
            mark_processed(session, event_id, consumer)
            session.commit()
            results["processed"] += 1
            logger.debug("[FleetEventConsumer:%s] event_id=%s processed", consumer, event_id)

        except Exception as exc:
            session.rollback()
            permanently = _record_failure(session, event_id, consumer, exc, max_retries)
            session.commit()
            results["failed"] += 1
            if permanently:
                results["permanently_failed"] += 1
            logger.error(
                "[FleetEventConsumer:%s] event_id=%s failed (attempt recorded): %s",
                consumer, event_id, exc,
            )

    logger.info(
        "[FleetEventConsumer:%s] done — processed=%d skipped=%d failed=%d permanent=%d",
        consumer,
        results["processed"],
        results["skipped"],
        results["failed"],
        results["permanently_failed"],
    )
    return results


def _record_failure(
    session,
    event_id: int,
    consumer: str,
    exc: Exception,
    max_retries: int,
) -> bool:
    """Upsert a failure record for this (event_id, consumer) pair.
    Increments retry_count; sets failed_permanently when max_retries exhausted.
    Returns True if the event is now permanently failed."""
    error_msg = str(exc)[:2000]
    now = datetime.now(timezone.utc)

    row = session.execute(sa_text("""
        INSERT INTO fleet_event_failures (event_id, consumer, retry_count, last_error, last_attempt_at)
        VALUES (:eid, :consumer, 1, :error, :now)
        ON CONFLICT (event_id, consumer) DO UPDATE
            SET retry_count      = fleet_event_failures.retry_count + 1,
                last_error       = EXCLUDED.last_error,
                last_attempt_at  = EXCLUDED.last_attempt_at,
                failed_permanently = (fleet_event_failures.retry_count + 1) >= :max_retries
        RETURNING failed_permanently
    """), {
        "eid":         event_id,
        "consumer":    consumer,
        "error":       error_msg,
        "now":         now,
        "max_retries": max_retries,
    }).fetchone()
    return bool(row.failed_permanently)
