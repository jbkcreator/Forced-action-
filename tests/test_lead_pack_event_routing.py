"""
Unit tests for the event-driven Lead Pack fulfillment path (ADR 0018 hybrid).

DB-free: verifies the supervisor routes `lead_pack_reserved` to the fulfillment
worker, and that handle_reserved_event spawns work only for a valid payload.
The actual enrichment/delivery is covered against Postgres in
test_lead_pack_e2e.py via fulfill_purchase().
"""
from unittest.mock import patch

from src.agents.supervisor import dispatch_event
from src.tasks import lead_pack_fulfillment_sweep as sweep


def test_supervisor_routes_reserved_event_to_worker():
    # dispatch_event imports handle_reserved_event lazily from the sweep module,
    # so patching the source symbol is what intercepts the call.
    with patch.object(sweep, "handle_reserved_event") as handler:
        out = dispatch_event({
            "event_type": "lead_pack_reserved",
            "subscriber_id": 7,
            "payload": {"purchase_id": 123},
        })
    assert out["outcome"] == "routed"
    assert out["graph_name"] == "lead_pack_fulfillment"
    handler.assert_called_once()
    assert handler.call_args.args[0] == {"purchase_id": 123}


def test_handle_reserved_event_spawns_for_valid_payload():
    with patch.object(sweep, "_fulfill_when_visible") as target:
        sweep.handle_reserved_event({"purchase_id": 99})
        # Daemon thread targets _fulfill_when_visible with the purchase id.
        # Join briefly so the assertion isn't racy.
        import time
        for _ in range(20):
            if target.called:
                break
            time.sleep(0.05)
    target.assert_called_once_with(99)


def test_handle_reserved_event_ignores_bad_payload():
    with patch.object(sweep, "_fulfill_when_visible") as target:
        sweep.handle_reserved_event({})            # no purchase_id
        sweep.handle_reserved_event({"purchase_id": "nope"})
    target.assert_not_called()
