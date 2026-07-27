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

    assert dispatch_order[0] == urgent_id
    assert dispatch_order[1] == routine_id


def test_at_least_once_delivery_and_dedup(fresh_db):
    event_id = emit_fleet_event(
        fresh_db, event_type="booking.created", source_component="test", payload={},
    )
    fresh_db.commit()

    calls = []
    result = poll_and_dispatch_fleet(
        fresh_db, "dedup_consumer", ["booking.created"],
        lambda s, r: calls.append(r.id),
    )
    fresh_db.commit()
    assert result["processed"] == 1
    assert len(calls) == 1

    # second poll: already processed for this consumer, must not re-dispatch
    result2 = poll_and_dispatch_fleet(
        fresh_db, "dedup_consumer", ["booking.created"],
        lambda s, r: calls.append(r.id),
    )
    assert result2["processed"] == 0
    assert len(calls) == 1


def test_handler_failure_recorded_and_dead_lettered_after_max_retries(fresh_db):
    emit_fleet_event(
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

    assert result["permanently_failed"] == 1

    # a 4th poll must not retry a permanently-failed event
    result4 = poll_and_dispatch_fleet(
        fresh_db, "flaky_consumer", ["reply.received"], failing_handler,
        max_retries=3,
    )
    assert result4["failed"] == 0
