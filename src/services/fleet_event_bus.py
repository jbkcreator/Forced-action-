"""
Transactional outbox for the QUALITY-v2.2 fleet event-trigger dispatcher.

Separate from src/services/event_bus.py's `events` table on purpose — that
table's CHECK constraint enumerates a closed set of prospect-lifecycle
event types and requires a NOT NULL prospect_id, neither of which fits a
fleet-wide event (a Stripe cancellation has no prospect_id). This module is
the fleet's own outbox: emit_fleet_event() writes in the caller's
transaction; consumers poll via fleet_event_consumer.poll_and_dispatch_fleet.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

# Lower number = more urgent. PRIORITY_URGENT lets an event (e.g. a fresh
# auction win, per spec §9.5) be dispatched ahead of routine work in the
# same poll_and_dispatch_fleet batch — see fleet_event_consumer.py's
# ORDER BY priority ASC, occurred_at ASC.
PRIORITY_URGENT = 10
PRIORITY_ROUTINE = 100

FLEET_EVENT_TYPES = frozenset({
    "filing.new",
    "payment.received",
    "reply.received",
    "booking.created",
    "subscription.cancelled",
    "source.failure",
})


def emit_fleet_event(
    session: Session,
    *,
    event_type: str,
    source_component: str,
    payload: dict[str, Any],
    priority: int = PRIORITY_ROUTINE,
    subscriber_id: Optional[int] = None,
    opportunity_thread_id: Optional[str] = None,
) -> int:
    """Write a fleet event in the caller's transaction. Returns the new event id.

    Raises ValueError if event_type isn't one of the six spec-named types —
    fail loud here rather than let a typo silently vanish into the DB.
    """
    if event_type not in FLEET_EVENT_TYPES:
        raise ValueError(
            f"emit_fleet_event: unknown event_type={event_type!r} "
            f"(allowed: {sorted(FLEET_EVENT_TYPES)})"
        )

    row = session.execute(
        sa_text("""
            INSERT INTO fleet_events
                (event_type, priority, source_component, subscriber_id,
                 opportunity_thread_id, payload)
            VALUES
                (:event_type, :priority, :source_component, :subscriber_id,
                 :opportunity_thread_id, CAST(:payload AS jsonb))
            RETURNING id
        """),
        {
            "event_type": event_type,
            "priority": priority,
            "source_component": source_component,
            "subscriber_id": subscriber_id,
            "opportunity_thread_id": opportunity_thread_id,
            "payload": json.dumps(payload),
        },
    ).fetchone()
    return row.id


def mark_processed(session: Session, event_id: int, consumer: str) -> None:
    session.execute(
        sa_text("""
            INSERT INTO fleet_processed_events (event_id, consumer)
            VALUES (:eid, :consumer)
            ON CONFLICT DO NOTHING
        """),
        {"eid": event_id, "consumer": consumer},
    )


def is_processed(session: Session, event_id: int, consumer: str) -> bool:
    row = session.execute(
        sa_text("""
            SELECT 1 FROM fleet_processed_events
            WHERE event_id = :eid AND consumer = :consumer
        """),
        {"eid": event_id, "consumer": consumer},
    ).fetchone()
    return row is not None
