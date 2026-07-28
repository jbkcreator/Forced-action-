"""
Fleet event dispatch sweep — QUALITY-v2.2 Q1.

Polls fleet_events (emitted via src/services/fleet_event_bus.py) for the
six spec-named types (filing, payment, reply, booking, cancellation,
source failure) and dispatches each to every registered consumer, most
urgent first (see fleet_event_consumer.poll_and_dispatch_fleet).

`fleet_audit_log` is the one consumer registered here — it logs every
dispatched event so the pipeline has a working, testable end-to-end proof
independent of any other not-yet-built agent subtask. Later subtasks
(Cora's reply-intent classifier, Relay's quiet-hour recheck, Hunter's
auction fast-follow) register their own consumers the same way, by adding
another poll_and_dispatch_fleet(session, "<consumer_name>", [...], handler)
call below.

Run every 2 minutes via cron (matches the existing outcome_dispatch_sweep /
loan_lane_sweep cadence in scripts/cron/crontab.txt).

Usage:
    python -m src.tasks.fleet_event_sweep
"""
from __future__ import annotations

import json
import logging

from src.core.database import get_db_context
from src.services.fleet_event_bus import FLEET_EVENT_TYPES
from src.services.fleet_event_consumer import poll_and_dispatch_fleet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_ALL_EVENT_TYPES = sorted(FLEET_EVENT_TYPES)


def _audit_log_handler(session, row) -> None:
    logger.info(
        "[FleetAuditLog] event_id=%s type=%s priority=%s source=%s payload=%s",
        row.event_id, row.event_type, row.priority, row.source_component, row.payload,
    )


def run_sweep(session) -> dict:
    """Poll all six fleet event types for every registered consumer.
    Returns per-consumer dispatch counts."""
    audit = poll_and_dispatch_fleet(
        session, "fleet_audit_log", _ALL_EVENT_TYPES, _audit_log_handler,
    )
    return {"fleet_audit_log": audit}


def main() -> None:
    logger.info("Starting fleet_event_sweep")
    with get_db_context() as session:
        result = run_sweep(session)
    logger.info("fleet_event_sweep complete: %s", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
