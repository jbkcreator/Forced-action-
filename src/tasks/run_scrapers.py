"""
Daily scraper runner — M1-F

Runs all 5 Week 1 scrapers in sequence for a given county.
Designed to be called by cron or a scheduler.

Usage:
    python -m src.tasks.run_scrapers [county_id]

Cron (daily at 2am):
    0 2 * * * cd /path/to/app && python -m src.tasks.run_scrapers hillsborough
"""

import logging
import sys
from datetime import date, timedelta

from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def run_all_scrapers(county_id: str = "hillsborough") -> dict:
    """
    Run all 5 M1-F scrapers for a county.
    Returns dict of scraper_name → records_created.
    """
    from src.scrappers.roofing_permits.roofing_permit_engine import scrape_roofing_permits
    from src.scrappers.storm.storm_engine import scrape_storm_damage
    from src.scrappers.fire.fire_engine import scrape_fire_incidents
    from src.scrappers.flood.flood_engine import scrape_flood_damage
    from src.scrappers.insurance.insurance_engine import scrape_insurance_claims

    date_range = (date.today() - timedelta(days=1), date.today())

    scrapers = [
        ("insurance_claims",    lambda: scrape_insurance_claims(county_id, date_range)),
        ("storm_damage",        lambda: scrape_storm_damage(county_id, date_range)),
        ("fire_incidents",      lambda: scrape_fire_incidents(county_id, date_range)),
        ("flood_damage",        lambda: scrape_flood_damage(county_id, date_range)),
        ("roofing_permits",     lambda: scrape_roofing_permits(county_id, date_range)),
    ]

    results = {}
    for name, fn in scrapers:
        try:
            logger.info(f"[run_scrapers] Starting {name} for {county_id}")
            count = fn()
            results[name] = count
            logger.info(f"[run_scrapers] {name} → {count} records")
        except Exception as e:
            logger.error(f"[run_scrapers] {name} FAILED: {e}", exc_info=True)
            results[name] = f"ERROR: {e}"

    logger.info(f"[run_scrapers] Completed. Results: {results}")
    return results


if __name__ == "__main__":
    county = sys.argv[1] if len(sys.argv) > 1 else "hillsborough"
    results = run_all_scrapers(county)
    for name, count in results.items():
        print(f"  {name:25} {count}")
