"""Print the pilot rollup: reach / reply / booked / wrong person / paid off per campaign.

Read-only.

Usage:
    PYTHONPATH=. python -m src.tasks.campaign_pilot_rollup
"""
from __future__ import annotations

import logging

from src.core.database import get_db_context
from src.services.campaign_pilot_rollup import format_rollup, pilot_rollup

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    with get_db_context() as session:
        rows = pilot_rollup(session)
        session.rollback()
    logger.info("%s", format_rollup(rows))


if __name__ == "__main__":
    main()
