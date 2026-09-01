"""
Add scraper outcome classification to scraper_run_stats.

Additive only: three new nullable columns (outcome_category,
attempt_started_at, completed_at) plus a CheckConstraint scoped to non-null
outcome_category values. NULL outcome_category means clean success with
real data — the same role error_type=NULL/'none' already plays; nothing
here is populated until a call site opts into the new
record_scraper_stats(outcome=...) parameter (see config/scraper_outcomes.py,
src/utils/scraper_outcome_classifier.py).

No backfill: ~200+ historical rows (including the two undocumented legacy
error_type values 'export_unavailable' and 'connector_error') keep
outcome_category=NULL forever and remain readable via error_type, untouched.

Unlike check_run_stats_source_type (many migrations append to that
constraint over time, so a union-read-then-rewrite is required — see
apply_cde10_run_stats_source_type.py), check_run_stats_outcome_category is
owned solely by this migration, so an unconditional drop+recreate is safe.

Usage:
    PYTHONPATH=. python migrations/apply_scraper_run_outcome_category.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        conn.execute(text(
            "ALTER TABLE scraper_run_stats ADD COLUMN IF NOT EXISTS outcome_category VARCHAR(20)"
        ))
        conn.execute(text(
            "ALTER TABLE scraper_run_stats ADD COLUMN IF NOT EXISTS attempt_started_at TIMESTAMP"
        ))
        conn.execute(text(
            "ALTER TABLE scraper_run_stats ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP"
        ))
        conn.execute(text(
            "ALTER TABLE scraper_run_stats DROP CONSTRAINT IF EXISTS check_run_stats_outcome_category"
        ))
        conn.execute(text(
            "ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_outcome_category "
            "CHECK (outcome_category IS NULL OR outcome_category IN "
            "('NO_DATA','TIMEOUT','SOURCE_ERROR','INTERNAL_ERROR','UNKNOWN'))"
        ))

    logger.info(
        "scraper_run_stats: outcome_category/attempt_started_at/completed_at columns "
        "+ check_run_stats_outcome_category constraint ready."
    )


if __name__ == "__main__":
    main()
