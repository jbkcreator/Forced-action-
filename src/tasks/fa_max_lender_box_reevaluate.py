"""WP-T2-10/11 — re-run the Lender Box over every open opportunity now.

Run right after loading new Backflip program rules into lender_box_programs,
so a rule change shows up immediately instead of at the next nightly GYR
sweep (which also re-evaluates, as the backstop). Posts only opportunities
whose color changed, plus an EXCEPTIONS re-engagement card for any deal that
moved from out-of-box to green/yellow.

Usage:
    PYTHONPATH=. python -m src.tasks.fa_max_lender_box_reevaluate
    PYTHONPATH=. python -m src.tasks.fa_max_lender_box_reevaluate --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys

sys.path.insert(0, ".")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _run(dry_run: bool = False) -> None:
    from sqlalchemy import text

    from src.core.database import get_db_context
    from src.services.opportunity_router import reevaluate

    with get_db_context() as db:
        ids = list(db.execute(
            text("SELECT opportunity_id::text FROM fa_max_opportunities WHERE outcome = 'open'")
        ).scalars())
        logger.info("Lender Box re-evaluation: %d open opportunities (dry_run=%s)", len(ids), dry_run)
        if dry_run:
            return
        changed = reevaluate(ids, db)

    logger.info("Lender Box re-evaluation complete: %d opportunities changed color", changed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-run the Lender Box over all open opportunities")
    parser.add_argument("--dry-run", action="store_true", help="Count open opportunities only")
    args = parser.parse_args()
    _run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
