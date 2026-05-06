"""
Human close routing sweep task.

Runs weekdays at 0 13 * * 1-5 (1 PM UTC — closer working hours).
Finds high-intent non-converting subscribers and routes to Slack.

Usage:
    python -m src.tasks.human_close_sweep [--dry-run]
"""
import logging
import sys

from src.core.database import get_db_context
from src.services.human_close_routing import find_candidates, route_candidate

logger = logging.getLogger(__name__)


def run_sweep(dry_run: bool = False) -> dict:
    results = {
        "candidates_found": 0,
        "routed": 0,
        "route_failed": 0,
        "errors": 0,
    }

    with get_db_context() as db:
        candidates = find_candidates(db)
        results["candidates_found"] = len(candidates)

        for cand in candidates:
            try:
                logger.info(
                    "human_close: candidate sub=%s score=%d interactions=%d",
                    cand.subscriber_id,
                    cand.revenue_signal_score,
                    cand.interactions_count,
                )
                if dry_run:
                    continue
                success = route_candidate(db, cand)
                if success:
                    results["routed"] += 1
                else:
                    results["route_failed"] += 1
            except Exception as exc:
                logger.error("human_close_sweep error sub=%s: %s", cand.subscriber_id, exc)
                results["errors"] += 1

    logger.info(
        "[HumanCloseSweep] candidates=%d routed=%d failed=%d errors=%d dry_run=%s",
        results["candidates_found"],
        results["routed"],
        results["route_failed"],
        results["errors"],
        dry_run,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_sweep(dry_run=dry))
