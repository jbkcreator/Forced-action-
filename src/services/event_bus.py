"""
Transactional outbox helpers for the M1 event bus.

emit_event() writes to the events table in the same DB transaction as the
caller's state change — atomicity is guaranteed by the caller's session.commit().
Consumers poll via processed_events; mark_processed() records delivery.
"""
import json
import uuid
from sqlalchemy.orm import Session
from sqlalchemy import text as sa_text


def emit_event(
    session: Session,
    *,
    event_type: str,
    actor: str,
    source_component: str,
    payload: dict,
    prospect_id: uuid.UUID | None = None,
) -> uuid.UUID:
    row = session.execute(
        sa_text("""
            INSERT INTO events
                (prospect_id, event_type, actor, payload, source_component)
            VALUES
                (:prospect_id, :event_type, :actor,
                 CAST(:payload AS jsonb), :source_component)
            RETURNING event_id
        """),
        {
            "prospect_id": str(prospect_id) if prospect_id else None,
            "event_type": event_type,
            "actor": actor,
            "payload": json.dumps(payload),
            "source_component": source_component,
        },
    ).fetchone()
    return row.event_id


def mark_processed(
    session: Session,
    event_id: uuid.UUID,
    consumer: str,
) -> None:
    session.execute(
        sa_text("""
            INSERT INTO processed_events (event_id, consumer)
            VALUES (:eid, :consumer)
            ON CONFLICT DO NOTHING
        """),
        {"eid": str(event_id), "consumer": consumer},
    )


def is_processed(
    session: Session,
    event_id: uuid.UUID,
    consumer: str,
) -> bool:
    row = session.execute(
        sa_text("""
            SELECT 1 FROM processed_events
            WHERE event_id = :eid AND consumer = :consumer
        """),
        {"eid": str(event_id), "consumer": consumer},
    ).fetchone()
    return row is not None
