"""Loan-lane event sweep — drives the broker.transition consumers.

Polls `broker.transition` events and dispatches each to the two reactions:
  - lane_closer       → closed_won → lane funded, closed_lost → lane dead
  - commission_poster → closed_won → post a commission ledger entry

Both are idempotent (processed_events guard + UNIQUE keys), so re-delivery is
safe. Run on a short cron interval (every 1-2 min) so commissions/closes land
near-real-time without coupling WS-B → WS-A/WS-C directly (event-bus decoupling).

Usage:
    python -m src.tasks.loan_lane_sweep
"""
from __future__ import annotations

import json
import logging

from src.consumers.loan_lane_consumers import handle_commission_poster, handle_lane_closer
from src.core.database import get_db_context
from src.services.event_consumer import poll_and_dispatch
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_EVENT_TYPES = ["broker.transition"]


def run_sweep(session) -> dict:
    """Poll broker.transition for both consumers. Returns per-consumer result counts."""
    lane = poll_and_dispatch(session, "lane_closer", _EVENT_TYPES, handle_lane_closer)
    commission = poll_and_dispatch(session, "commission_poster", _EVENT_TYPES, handle_commission_poster)
    return {"lane_closer": lane, "commission_poster": commission}


def main() -> None:
    logger.info("Starting loan_lane_sweep")
    with get_db_context() as session:
        result = run_sweep(session)
    logger.info("loan_lane_sweep complete: %s", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
