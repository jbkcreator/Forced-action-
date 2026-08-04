from sqlalchemy import text as sa_text

from src.services.fleet_event_bus import PRIORITY_ROUTINE, PRIORITY_URGENT, emit_fleet_event
from src.services.fleet_event_consumer import poll_and_dispatch_fleet


def test_urgent_event_dispatched_before_older_routine_event(fresh_db):
    """Proves spec §9.5's 'deadline-aware preemption': an urgent event
    inserted SECOND must still be handled BEFORE an older routine event."""
    routine_id = emit_fleet_event(
        fresh_db, event_type="filing.new", source_component="test",
        payload={"which": "routine"}, priority=PRIORITY_ROUTINE,
    )
    fresh_db.commit()

    urgent_id = emit_fleet_event(
        fresh_db, event_type="source.failure", source_component="test",
        payload={"which": "urgent"}, priority=PRIORITY_URGENT,
    )
    fresh_db.commit()

    dispatch_order = []

    def handler(session, row):
        dispatch_order.append(row.event_id)

    poll_and_dispatch_fleet(
        fresh_db, "test_consumer", ["filing.new", "source.failure"], handler,
    )
    fresh_db.commit()

    # Assert ordering of this test's specific events, regardless of leftover events from prior runs
    assert urgent_id in dispatch_order
    assert routine_id in dispatch_order
    assert dispatch_order.index(urgent_id) < dispatch_order.index(routine_id)


def test_at_least_once_delivery_and_dedup(fresh_db):
    event_id = emit_fleet_event(
        fresh_db, event_type="booking.created", source_component="test", payload={},
    )
    fresh_db.commit()

    calls = []
    result = poll_and_dispatch_fleet(
        fresh_db, "dedup_consumer", ["booking.created"],
        lambda s, r: calls.append(r.event_id),
    )
    fresh_db.commit()

    # Verify this specific event was processed (direct DB query)
    processed_row = fresh_db.execute(sa_text("""
        SELECT 1 FROM fleet_processed_events
        WHERE event_id = :eid AND consumer = :consumer
    """), {"eid": event_id, "consumer": "dedup_consumer"}).fetchone()
    assert processed_row is not None, "Event should be marked processed"
    assert event_id in calls, "Handler should have been called for this event"

    # second poll: already processed for this consumer, must not re-dispatch this event
    result2 = poll_and_dispatch_fleet(
        fresh_db, "dedup_consumer", ["booking.created"],
        lambda s, r: calls.append(r.event_id),
    )
    fresh_db.commit()

    # Verify the specific event was not re-processed (still exactly one row for this event/consumer)
    processed_count = fresh_db.execute(sa_text("""
        SELECT COUNT(*) as cnt FROM fleet_processed_events
        WHERE event_id = :eid AND consumer = :consumer
    """), {"eid": event_id, "consumer": "dedup_consumer"}).fetchone()
    assert processed_count.cnt == 1, "Event should only have one processed entry for this consumer"
    # Verify handler wasn't called again for this event
    assert calls.count(event_id) == 1, "Handler should only have been called once for this event"


def test_handler_failure_recorded_and_dead_lettered_after_max_retries(fresh_db):
    event_id = emit_fleet_event(
        fresh_db, event_type="reply.received", source_component="test", payload={},
    )
    fresh_db.commit()

    def failing_handler(session, row):
        raise RuntimeError("boom")

    for _ in range(3):
        result = poll_and_dispatch_fleet(
            fresh_db, "flaky_consumer", ["reply.received"], failing_handler,
            max_retries=3,
        )
        fresh_db.commit()

    # Verify this specific event was marked as permanently failed
    failure_row = fresh_db.execute(sa_text("""
        SELECT failed_permanently, retry_count FROM fleet_event_failures
        WHERE event_id = :eid AND consumer = :consumer
    """), {"eid": event_id, "consumer": "flaky_consumer"}).fetchone()
    assert failure_row is not None, "Failure record should exist"
    assert failure_row.failed_permanently is True, "Event should be marked permanently failed"
    assert failure_row.retry_count >= 3, "Should have at least 3 retry attempts"

    # a 4th poll must not retry a permanently-failed event
    result4 = poll_and_dispatch_fleet(
        fresh_db, "flaky_consumer", ["reply.received"], failing_handler,
        max_retries=3,
    )
    # Verify the specific event was not retried again (retry_count should be unchanged)
    failure_row2 = fresh_db.execute(sa_text("""
        SELECT retry_count FROM fleet_event_failures
        WHERE event_id = :eid AND consumer = :consumer
    """), {"eid": event_id, "consumer": "flaky_consumer"}).fetchone()
    assert failure_row2.retry_count == failure_row.retry_count, "Permanently failed event should not be retried"
