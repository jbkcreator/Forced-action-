"""Add 'partner_mining' to scraper_run_stats.check_run_stats_source_type.

src/tasks/partner_mining_sweep.py records its run under source_type
'partner_mining'; without this value every insert is rejected by the CHECK,
so the sweep never appears in scraper_run_stats and Vera/heartbeat treat it
as never-run.

Takes the UNION of the live constraint's values and REQUIRED_SOURCE_TYPES
(same pattern as apply_cde07/apply_cde10) so values added by sibling
migrations are never dropped. Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_partner_mining_run_stats_source_type.py
"""
from __future__ import annotations

import logging
import re

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REQUIRED_SOURCE_TYPES = {"partner_mining"}


def _existing_check_values(conn: Connection, constraint_name: str, table: str) -> set[str]:
    row = conn.execute(
        text(
            "SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint "
            "WHERE conname = :name AND conrelid = CAST(:table AS regclass)"
        ),
        {"name": constraint_name, "table": table},
    ).first()
    if row is None:
        return set()
    return set(re.findall(r"'([^']+)'", row._mapping["def"]))


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        current = _existing_check_values(conn, "check_run_stats_source_type", "scraper_run_stats")
        if not current:
            raise RuntimeError(
                "check_run_stats_source_type not found or empty -- refusing to "
                "replace it with a partner_mining-only constraint"
            )
        added = REQUIRED_SOURCE_TYPES - current
        if not added:
            logger.info("partner_mining already allowed -- nothing to do.")
            return
        values_sql = ",".join(f"'{v}'" for v in sorted(current | REQUIRED_SOURCE_TYPES))
        conn.execute(text("ALTER TABLE scraper_run_stats DROP CONSTRAINT IF EXISTS check_run_stats_source_type;"))
        conn.execute(
            text(f"ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type "
                 f"CHECK (source_type IN ({values_sql}));")
        )
        logger.info("union applied (%d existing, added: %s)", len(current), sorted(added))


if __name__ == "__main__":
    main()
