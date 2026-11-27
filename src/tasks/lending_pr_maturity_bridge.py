"""CLI driver: bridge PropertyRadar maturity records into the dialer calling pool.

Dry run by default (builds + counts, no write). Pass --apply to write the rows.

    python -m src.tasks.lending_pr_maturity_bridge              # dry run, all states
    python -m src.tasks.lending_pr_maturity_bridge --state GA   # dry run, GA only
    python -m src.tasks.lending_pr_maturity_bridge --apply      # write the pool

Writes a sixth pool (pr_maturity) into lending.calling_pool_staging; the dialer
load (src/tasks/lending_dialer_load.py) picks it up from the latest run like any
other pool. Run the live trace first so records have phones to join.
"""
from __future__ import annotations

import argparse
import logging

from src.core.database import get_db_context
from src.lending.pool_source import stale_message, stale_segments
from src.lending.pr_maturity_bridge import extract_pr_maturity_pool

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="write rows (default: dry run)")
    ap.add_argument("--state", choices=["FL", "GA"], help="limit to one state")
    ap.add_argument("--if-enabled", action="store_true",
                    help="cron: do nothing while PROPERTY_RADAR_STATEWIDE_ENABLED is false")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.if_enabled:
        from config.settings import get_settings

        if not get_settings().property_radar_statewide_enabled:
            logger.info("lending_pr_maturity_bridge: PROPERTY_RADAR_STATEWIDE_ENABLED is false, nothing done")
            return 0
    with get_db_context() as session:
        stale = stale_segments(session)
        if stale:  # R1 0.6: never re-stage a segment whose nightly pull failed; the fresh segments still stage
            logger.warning("lending_pr_maturity_bridge: %s", stale_message(stale))
            if args.apply:
                from src.lending.ops_alerts import post_ops_alert

                post_ops_alert(f"PropertyRadar bridge: {stale_message(stale)}")
        summary = extract_pr_maturity_pool(session, dry_run=not args.apply, state=args.state, exclude=stale)
        if args.apply:
            session.commit()
    logger.info("lending_pr_maturity_bridge: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
