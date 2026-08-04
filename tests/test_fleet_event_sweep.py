import logging
from sqlalchemy import text
from src.services.fleet_event_bus import emit_fleet_event
from src.tasks.fleet_event_sweep import run_sweep


def test_sweep_dispatches_pending_events_to_audit_log(fresh_db, caplog):
    # Fix logging: set propagate=True on the ancestor "src" logger so caplog can capture
    # (logging.yaml sets propagate=false on the ancestor, preventing caplog from seeing logs)
    src_logger = logging.getLogger("src")
    original_propagate = src_logger.propagate
    src_logger.propagate = True
    caplog.set_level(logging.INFO)

    try:
        # Emit an event and capture its ID
        event_id = emit_fleet_event(
            fresh_db,
            event_type="payment.received",
            source_component="test",
            payload={"amount": 500},
        )
        fresh_db.commit()

        # Run the sweep
        result = run_sweep(fresh_db)

        # Verify the event was processed by querying the processed_events table
        # (not by counting total table rows, which can include events from other tests)
        processed_row = fresh_db.execute(
            text(
                "SELECT consumer FROM fleet_processed_events "
                "WHERE event_id = :eid AND consumer = :consumer"
            ),
            {"eid": event_id, "consumer": "fleet_audit_log"},
        ).fetchone()

        assert processed_row is not None, f"Event {event_id} not marked as processed for fleet_audit_log"
        assert any("payment.received" in message for message in caplog.messages)

    finally:
        # Restore original propagate setting to avoid leaking state
        src_logger.propagate = original_propagate
