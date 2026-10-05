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
from src.lending.pr_maturity_bridge import extract_pr_maturity_pool

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="write rows (default: dry run)")
    ap.add_argument("--state", choices=["FL", "GA"], help="limit to one state")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    with get_db_context() as session:
        summary = extract_pr_maturity_pool(session, dry_run=not args.apply, state=args.state)
        if args.apply:
            session.commit()
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
