"""Seed county_sources rows for the new 'voter_registry' signal (fa077 / Task 4).

A row per (county, voter_registry) is needed so:
  - the MediaFire auto-refresh cron can persist its last-processed folder key
    in special_flags (src/tasks/voter_registry_refresh.py), and
  - ColumnMapper can attach an approved header mapping for the upload endpoint.

Idempotent: ON CONFLICT (county_id, signal_type) DO NOTHING. Safe to re-run.

Usage:
    PYTHONPATH=. python scripts/seed_voter_registry_sources.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_ROWS = [
    {
        "county_id": "hillsborough",
        "source_name": "Hillsborough SOE — All Eligible Voters (MediaFire)",
        "url": "https://www.votehillsborough.gov/212/Data-Records-Requests",
        "description": "Monthly voter registry export; auto-refreshed from the SOE MediaFire folder.",
        "output_format": "csv",
        "frequency": "monthly",
        "scrape_mode": "static_download",
    },
    {
        "county_id": "pinellas",
        "source_name": "Pinellas SOE — Voter Registry (manual upload)",
        "url": "mailto:Election@VotePinellas.gov",
        "description": "Voter registry export; manual upload until SOE confirms delivery method.",
        "output_format": "csv",
        "frequency": "monthly",
        "scrape_mode": "static_download",
    },
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for row in _ROWS:
            result = session.execute(
                text("""
                    INSERT INTO county_sources
                        (county_id, signal_type, source_name, url, description,
                         output_format, frequency, scrape_mode, is_active,
                         date_range_available, special_flags, created_at)
                    VALUES
                        (:county_id, 'voter_registry', :source_name, :url, :description,
                         :output_format, :frequency, :scrape_mode, true,
                         false, '{}'::jsonb, now())
                    ON CONFLICT (county_id, signal_type) DO NOTHING
                """),
                row,
            )
            action = "inserted" if result.rowcount else "already exists"
            logger.info("voter_registry source for %s: %s", row["county_id"], action)
    logger.info("Done.")


if __name__ == "__main__":
    run()
    sys.exit(0)
