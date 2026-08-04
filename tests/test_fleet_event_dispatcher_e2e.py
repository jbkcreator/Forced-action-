import logging
import uuid
from unittest.mock import patch

from sqlalchemy import text

from src.services.fleet_event_bus import PRIORITY_ROUTINE, emit_fleet_event
from src.services.stripe_webhooks import _on_subscription_deleted
from src.tasks.fleet_event_sweep import run_sweep
from tests.test_stripe_webhooks_fleet_event import _make_subscriber


def test_cancellation_reaches_dispatch_ahead_of_older_routine_event(fresh_db, caplog):
    """Full acceptance path: a routine filing event lands first, then a real
    Stripe cancellation comes in — the cancellation must still dispatch
    first (priority preemption), and both must be visible in fleet_events.

    Deviates from the brief in two ways (see task-7-brief.md context):
    - Uses a uuid-suffixed stripe_customer_id instead of the brief's hardcoded
      "cus_e2e_test" — fresh_db.commit() flattens through the SAVEPOINT to the
      real shared dev DB (tests/conftest.py:78-91), so a hardcoded id would
      collide with a leftover row from a prior run (Task 4's lesson).
    - Scopes every assertion to this test's own two event ids rather than
      aggregate counts or caplog[0]/[1] position, since run_sweep's
      fleet_audit_log consumer polls ALL six event types and may process
      leftover unprocessed rows from other tests in this shared DB
      (Task 5's lesson).
    """
    src_logger = logging.getLogger("src")
    original_propagate = src_logger.propagate
    src_logger.propagate = True
    caplog.set_level(logging.INFO)

    try:
        routine_id = emit_fleet_event(
            fresh_db, event_type="filing.new", source_component="test",
            payload={"which": "routine"}, priority=PRIORITY_ROUTINE,
        )
        fresh_db.commit()

        customer_id = f"cus_e2e_{uuid.uuid4().hex[:8]}"
        subscription_id = f"sub_e2e_{uuid.uuid4().hex[:8]}"
        subscriber = _make_subscriber(fresh_db, stripe_customer_id=customer_id)
        fresh_db.commit()

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"):
            _on_subscription_deleted({"customer": customer_id, "id": subscription_id}, fresh_db)
        fresh_db.commit()

        cancellation_row = fresh_db.execute(text("""
            SELECT id FROM fleet_events
            WHERE subscriber_id = :sid AND event_type = 'subscription.cancelled'
        """), {"sid": subscriber.id}).fetchone()
        assert cancellation_row is not None, "cancellation event was not emitted"
        cancellation_id = cancellation_row.id

        result = run_sweep(fresh_db)
        assert result["fleet_audit_log"]["processed"] >= 2

        processed_rows = fresh_db.execute(text("""
            SELECT event_id, processed_at FROM fleet_processed_events
            WHERE consumer = 'fleet_audit_log' AND event_id IN (:routine_id, :cancellation_id)
        """), {"routine_id": routine_id, "cancellation_id": cancellation_id}).fetchall()
        processed_by_id = {r.event_id: r.processed_at for r in processed_rows}

        assert routine_id in processed_by_id, "routine event was never dispatched"
        assert cancellation_id in processed_by_id, "cancellation event was never dispatched"
        assert processed_by_id[cancellation_id] <= processed_by_id[routine_id], (
            "cancellation must dispatch no later than the older routine event"
        )

        log_lines = [m for m in caplog.messages if "FleetAuditLog" in m]
        routine_marker = f"event_id={routine_id} "
        cancellation_marker = f"event_id={cancellation_id} "
        routine_positions = [i for i, m in enumerate(log_lines) if routine_marker in m]
        cancellation_positions = [i for i, m in enumerate(log_lines) if cancellation_marker in m]
        assert routine_positions, "routine event never logged by fleet_audit_log"
        assert cancellation_positions, "cancellation event never logged by fleet_audit_log"
        assert cancellation_positions[0] < routine_positions[0], (
            "cancellation must be logged before the older routine event"
        )

        rows = fresh_db.execute(text("""
            SELECT id, event_type FROM fleet_events
            WHERE id IN (:routine_id, :cancellation_id)
            ORDER BY priority ASC, occurred_at ASC
        """), {"routine_id": routine_id, "cancellation_id": cancellation_id}).fetchall()
        assert [r.event_type for r in rows] == ["subscription.cancelled", "filing.new"]

    finally:
        src_logger.propagate = original_propagate
