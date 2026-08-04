import pytest
from sqlalchemy import text

from src.services.fleet_event_bus import (
    FLEET_EVENT_TYPES,
    PRIORITY_ROUTINE,
    PRIORITY_URGENT,
    emit_fleet_event,
    is_processed,
    mark_processed,
)


def test_emit_fleet_event_writes_a_row(fresh_db):
    event_id = emit_fleet_event(
        fresh_db,
        event_type="subscription.cancelled",
        source_component="test",
        payload={"foo": "bar"},
    )
    fresh_db.commit()

    row = fresh_db.execute(
        text("SELECT event_type, priority, payload FROM fleet_events WHERE id = :id"),
        {"id": event_id},
    ).fetchone()

    assert row.event_type == "subscription.cancelled"
    assert row.priority == PRIORITY_ROUTINE
    assert row.payload == {"foo": "bar"}


def test_emit_fleet_event_rejects_unknown_type(fresh_db):
    with pytest.raises(ValueError):
        emit_fleet_event(
            fresh_db,
            event_type="not.a.real.type",
            source_component="test",
            payload={},
        )


def test_emit_fleet_event_accepts_urgent_priority(fresh_db):
    event_id = emit_fleet_event(
        fresh_db,
        event_type="source.failure",
        source_component="test",
        payload={},
        priority=PRIORITY_URGENT,
    )
    fresh_db.commit()

    row = fresh_db.execute(
        text("SELECT priority FROM fleet_events WHERE id = :id"), {"id": event_id}
    ).fetchone()
    assert row.priority == PRIORITY_URGENT


def test_mark_processed_then_is_processed_true(fresh_db):
    event_id = emit_fleet_event(
        fresh_db, event_type="booking.created", source_component="test", payload={}
    )
    fresh_db.commit()

    assert is_processed(fresh_db, event_id, "some_consumer") is False
    mark_processed(fresh_db, event_id, "some_consumer")
    fresh_db.commit()
    assert is_processed(fresh_db, event_id, "some_consumer") is True
