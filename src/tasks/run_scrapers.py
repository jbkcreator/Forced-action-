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
import time
from datetime import date, datetime, timedelta, timezone

from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def run_all_scrapers(county_id: str = "hillsborough") -> dict:
    """
    Run all 5 M1-F scrapers for a county.
    Returns dict of scraper_name → records_created (int) or "ERROR: ..." (str).
    After completion: persists results to ScraperRunStats and alerts on failures.
    """
    from src.scrappers.roofing_permits.roofing_permit_engine import scrape_roofing_permits
    from src.scrappers.storm.storm_engine import scrape_storm_damage
    from src.scrappers.fire.fire_engine import scrape_fire_incidents
    from src.scrappers.flood.flood_engine import scrape_flood_damage
    from src.scrappers.insurance.insurance_engine import scrape_insurance_claims

    run_date = date.today()
    date_range = (run_date - timedelta(days=1), run_date)

    scrapers = [
        ("insurance_claims",    lambda: scrape_insurance_claims(county_id, date_range)),
        ("storm_damage",        lambda: scrape_storm_damage(county_id, date_range)),
        ("fire_incidents",      lambda: scrape_fire_incidents(county_id, date_range)),
        ("flood_damage",        lambda: scrape_flood_damage(county_id, date_range)),
        ("roofing_permits",     lambda: scrape_roofing_permits(county_id, date_range)),
    ]

    results = {}
    timings = {}
    for name, fn in scrapers:
        logger.info("[run_scrapers] Starting %s for %s", name, county_id)
        t0 = time.monotonic()
        try:
            count = fn()
            results[name] = count
            logger.info("[run_scrapers] %s → %s records", name, count)
        except Exception as e:
            logger.error("[run_scrapers] %s FAILED: %s", name, e, exc_info=True)
            results[name] = f"ERROR: {e}"
        finally:
            timings[name] = round(time.monotonic() - t0, 2)

    logger.info("[run_scrapers] Completed. Results: %s", results)

    _persist_scraper_stats(run_date, county_id, results, timings)

    failures = {k: v for k, v in results.items() if isinstance(v, str) and v.startswith("ERROR:")}
    if failures:
        _alert_scraper_failures(run_date, failures)

    return results


def _persist_scraper_stats(
    run_date: date,
    county_id: str,
    results: dict,
    timings: dict,
) -> None:
    """
    Upsert one ScraperRunStats row per scraper.
    Uses INSERT ... ON CONFLICT DO UPDATE matching the uq_scraper_run_stats constraint.
    Opens a fresh session — independent of any session held by the scrapers themselves.
    Failure here never crashes the scraper run.
    """
    try:
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from src.core.database import get_db_context
        from src.core.models import ScraperRunStats

        with get_db_context() as db:
            for source_type, result in results.items():
                is_error = isinstance(result, str) and result.startswith("ERROR:")
                row = {
                    "run_date":         run_date,
                    "source_type":      source_type,
                    "county_id":        county_id,
                    "total_scraped":    0 if is_error else int(result or 0),
                    "matched":          0,
                    "unmatched":        0,
                    "skipped":          0,
                    "scored":           0,
                    "run_success":      not is_error,
                    "error_message":    str(result) if is_error else None,
                    "duration_seconds": timings.get(source_type),
                }
                stmt = (
                    pg_insert(ScraperRunStats)
                    .values(**row)
                    .on_conflict_do_update(
                        constraint="uq_scraper_run_stats",
                        set_={
                            "total_scraped":    row["total_scraped"],
                            "run_success":      row["run_success"],
                            "error_message":    row["error_message"],
                            "duration_seconds": row["duration_seconds"],
                        },
                    )
                )
                db.execute(stmt)
    except Exception as exc:
        logger.error("[run_scrapers] Failed to persist ScraperRunStats: %s", exc, exc_info=True)


def _alert_scraper_failures(run_date: date, failures: dict) -> None:
    """Send an ops alert listing all failed scrapers and their error messages."""
    try:
        from src.services.email import send_alert

        lines = [f"  {name}: {msg}" for name, msg in sorted(failures.items())]
        send_alert(
            subject=f"[Forced Action] Scraper failures — {run_date}",
            body=(
                f"{len(failures)} scraper(s) failed on {run_date}:\n\n"
                + "\n".join(lines)
                + "\n\nCheck logs for full tracebacks. "
                  "Yesterday's data for these sources may be missing from lead scoring."
            ),
        )
        logger.warning("[run_scrapers] Failure alert sent for %d scraper(s)", len(failures))
    except Exception as exc:
        logger.error("[run_scrapers] Could not send failure alert: %s", exc)


if __name__ == "__main__":
    county = sys.argv[1] if len(sys.argv) > 1 else "hillsborough"
    results = run_all_scrapers(county)
    for name, count in results.items():
        print(f"  {name:25} {count}")
