import logging
from sqlalchemy import text
from src.services.fleet_event_bus import emit_fleet_event
from src.tasks.fleet_event_sweep import run_sweep


def test_sweep_dispatches_pending_events_to_audit_log(fresh_db, caplog):
    # Set up caplog to capture logs despite logging.yaml propagate=false
    caplog.set_level(logging.INFO)
    sweep_logger = logging.getLogger("src.tasks.fleet_event_sweep")
    sweep_logger.propagate = True

    # Clean up any leftover events from previous test runs (shared dev DB)
    fresh_db.execute(text("DELETE FROM fleet_events"))
    fresh_db.commit()

    emit_fleet_event(
        fresh_db, event_type="payment.received", source_component="test", payload={"amount": 500},
    )
    fresh_db.commit()

    result = run_sweep(fresh_db)

    assert result["fleet_audit_log"]["processed"] == 1
    assert any("payment.received" in message for message in caplog.messages)
