"""Outcome-event dispatch sweep — Block 13 (T-B13-02).

Polls `outcome.recorded` events (emitted by the one-tap deal-capture) and
dispatches each to the decoupled recalculation consumers:
  - outcome_snapshot     → learning-loop pre-decision score snapshot
  - outcome_loss_autopsy → dead + lead-fault → LLM loss autopsy

Both are idempotent (processed_events guard), so at-least-once re-delivery is
safe. Run on a short cron interval (every 1-2 min) so the learning loop lands
near-real-time without coupling it to the buyer's tap latency (event-bus
decoupling, §7.1).

Usage:
    python -m src.tasks.outcome_dispatch_sweep
"""
from __future__ import annotations

import json
import logging

from src.consumers.outcome_consumers import (
    handle_outcome_loss_autopsy,
    handle_outcome_snapshot,
)
from src.core.database import get_db_context
from src.services.event_consumer import poll_and_dispatch
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_EVENT_TYPES = ["outcome.recorded"]


def run_sweep(session) -> dict:
    """Poll outcome.recorded for both consumers. Returns per-consumer counts."""
    snapshot = poll_and_dispatch(session, "outcome_snapshot", _EVENT_TYPES, handle_outcome_snapshot)
    autopsy = poll_and_dispatch(session, "outcome_loss_autopsy", _EVENT_TYPES, handle_outcome_loss_autopsy)
    return {"outcome_snapshot": snapshot, "outcome_loss_autopsy": autopsy}


def main() -> None:
    logger.info("Starting outcome_dispatch_sweep")
    with get_db_context() as session:
        result = run_sweep(session)
    logger.info("outcome_dispatch_sweep complete: %s", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
