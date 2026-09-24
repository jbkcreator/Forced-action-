"""FA Max Fundability Agent — daily backstop sweep (WP-T3-8).

Scheduled after arv_sweep (10:00 UTC) so WP-8B's published ARV rows are
fresh when this sweep queries them. Runs at 10:30 UTC.

    python -m src.tasks.fa_max_fundability_sweep
    python -m src.tasks.fa_max_fundability_sweep --dry-run

DEPENDENCY ON T3-7 (PR #300):
  This task requires fa_max_opportunity_facts and fa_max_qualification_decisions
  tables from apply_fa_max_opportunity_facts.py. A pre-flight check aborts
  gracefully when those tables are absent so a cron run before PR #300 is
  merged does not fail the whole job runner.
"""
from __future__ import annotations

import argparse
import logging
import sys

from src.core.database import get_db_context

logger = logging.getLogger(__name__)


def _tables_exist(session) -> bool:
    """Return True if T3-7's tables have been applied.

    WP-T3-8 cannot run without fa_max_opportunity_facts and
    fa_max_qualification_decisions. This pre-flight check makes the cron
    entry safe to apply before PR #300 is merged — it exits cleanly rather
    than failing with a Postgres relation-does-not-exist error.
    """
    from sqlalchemy import text

    row = session.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.tables"
            " WHERE table_schema = 'public'"
            "   AND table_name IN ('fa_max_opportunity_facts',"
            "                      'fa_max_qualification_decisions')"
        )
    ).scalar()
    return int(row) == 2


def run_sweep(*, dry_run: bool = False) -> int:
    """Run the fundability backstop sweep. Returns exit code (0 = ok, 1 = err)."""
    from src.services.fa_max_fundability_agent import run_fundability_sweep

    with get_db_context() as session:
        if not _tables_exist(session):
            logger.warning(
                "fa_max.fundability_sweep: T3-7 tables (fa_max_opportunity_facts,"
                " fa_max_qualification_decisions) not found — PR #300 has not been"
                " merged and applied yet. Exiting cleanly; re-run after migration."
            )
            return 0

        if dry_run:
            logger.info("fa_max.fundability_sweep: dry-run mode — no writes")
            from sqlalchemy import text

            count = session.execute(
                text(
                    "SELECT COUNT(DISTINCT qd.opportunity_id)"
                    " FROM fa_max_qualification_decisions qd"
                    " JOIN fa_max_opportunities opp"
                    "   ON opp.opportunity_id = qd.opportunity_id"
                    " WHERE opp.outcome = 'open'"
                    "   AND qd.gaps @> '[{\"gap_type\": \"pending_enrichment\"}]'"
                )
            ).scalar()
            logger.info(
                "fa_max.fundability_sweep: dry-run — %d opportunities would be evaluated",
                count,
            )
            return 0

        stats = run_fundability_sweep(session)

    return 1 if stats.errors > 0 and stats.arv_written == 0 else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="FA Max Fundability Agent sweep")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report candidate count without writing any facts.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(run_sweep(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
