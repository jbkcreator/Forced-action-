"""
NBRA population sweep (REVINT I1).

Fills `opportunity_scores` from the current whale list using REVINT-v2.2's
scoring primitives, so `nbra_engine.get_ranked_queue()` has rows to rank. Runs
AFTER `hunter_nightly_sweep` (which refreshes the whale flags), so it scores a
fresh, correctly-split whale list.

Honors the Hunter kill switch — a halted data pipeline must not feed a stale
ranked queue to the founder.

Usage (cron, staggered after hunter_nightly_sweep):
    PYTHONPATH=. python -m src.tasks.nbra_populate_sweep --county-id hillsborough
"""
from __future__ import annotations

import argparse
from typing import Optional

from src.agents.hunter.kill_switch import hunter_halted
from src.core.database import get_db_context
from src.services.nbra_populate import populate_opportunity_scores
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def run_sweep(county_id: Optional[str] = None) -> dict:
    if hunter_halted():
        logger.warning("[NbraPopulate] Hunter kill switch active -- skipping sweep, no DB mutation.")
        return {"county_id": county_id, "halted": True}

    with get_db_context() as session:
        summary = populate_opportunity_scores(session, county_id=county_id)
        session.commit()

    result = {"county_id": county_id, **summary}
    logger.info("[NbraPopulate] %s", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="NBRA population sweep — score current whales into opportunity_scores.")
    parser.add_argument("--county-id", default=None, help="Scope whale source to one county (default: all).")
    args = parser.parse_args()
    print(run_sweep(county_id=args.county_id))


if __name__ == "__main__":
    main()
