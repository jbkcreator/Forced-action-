"""Loan-lane sweep wiring test (WS-C gap #4).

The commission/lane-closer consumers only run if a scheduled sweep polls
broker.transition and dispatches to them. This verifies that wiring.
"""
from __future__ import annotations


def test_sweep_polls_both_consumers_on_broker_transition(monkeypatch):
    import src.tasks.loan_lane_sweep as sweep

    calls = []

    def fake_poll(session, consumer, event_types, handler, **kw):
        calls.append((consumer, tuple(event_types), handler.__name__))
        return {"processed": 0, "skipped": 0, "failed": 0, "permanently_failed": 0}

    monkeypatch.setattr(sweep, "poll_and_dispatch", fake_poll)

    sweep.run_sweep(session=object())

    assert ("lane_closer", ("broker.transition",), "handle_lane_closer") in calls
    assert ("commission_poster", ("broker.transition",), "handle_commission_poster") in calls
