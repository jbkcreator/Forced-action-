"""
Daily scraper runner — M1-F

Runs all signal scrapers in two groups:

  Group A — Major signal scrapers (two-step: scrape → CSV → loader → DB)
    violations, foreclosures, liens, evictions, probate, bankruptcy
    These drive the core high-volume signals (code_violations, foreclosures,
    judgment_liens, probate, evictions, bankruptcy) that account for the bulk
    of Gold+ scoring.  Each gets a _run_X() wrapper that combines the scraper
    and the corresponding loader so the runner stays uniform.

  Group B — Incident scrapers (self-contained: write directly to DB)
    insurance_claims, storm_damage, fire_incidents, flood_damage, roofing_permits
    These call a single function that handles scraping + DB write internally.

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


# ── Major-scraper wrappers (scrape → load → return matched count) ──────────────
# Each wrapper is responsible for:
#   1. Running the scraper (sync or async via asyncio.run())
#   2. Running the corresponding loader on the output
#   3. Returning the matched record count for ScraperRunStats
# Imports are deferred inside each function to avoid circular imports at module
# load time and to keep startup fast when only a subset of scrapers is used.

def _run_violations(county_id: str, run_date: date) -> int:
    """Code violations: async Playwright → CSV → ViolationLoader → DB."""
    import asyncio
    from src.scrappers.violation.violation_engine import scrape_violations_with_playwright
    from src.loaders.violations import ViolationLoader
    from src.core.database import get_db_context

    start = (run_date - timedelta(days=1)).strftime("%Y-%m-%d")
    end   = run_date.strftime("%Y-%m-%d")
    result = asyncio.run(scrape_violations_with_playwright(start_date=start, end_date=end))
    # scraper returns: (csv_path,) on new data | True if nothing new | None if no rows
    if not result or result is True:
        return 0
    csv_path = result[0] if isinstance(result, tuple) else result
    with get_db_context() as session:
        matched, _, _ = ViolationLoader(session, county_id=county_id).load_from_csv(str(csv_path))
    return matched


def _run_foreclosures(county_id: str, run_date: date) -> int:
    """Foreclosures: async Playwright → CSV → ForeclosureLoader → DB."""
    import asyncio
    from src.scrappers.foreclosures.foreclosure_engine import scrape_realforeclose_calendar
    from src.loaders.foreclosures import ForeclosureLoader
    from src.core.database import get_db_context

    csv_path = asyncio.run(
        scrape_realforeclose_calendar(auction_date=run_date, county_id=county_id)
    )
    if not csv_path:
        return 0
    with get_db_context() as session:
        matched, _, _ = ForeclosureLoader(session, county_id=county_id).load_from_csv(str(csv_path))
    return matched


def _run_liens(county_id: str, run_date: date) -> int:
    """Liens/judgments: async Playwright → CSVs in RAW_LIEN_DIR → LienLoader → DB.

    The lien pipeline writes one CSV per document type (LIEN, JUD, etc.) to
    RAW_LIEN_DIR.  We timestamp before the run, then load every CSV that was
    written during it — handles the multi-file output without hardcoding filenames.
    """
    import asyncio
    from pathlib import Path
    from src.scrappers.liens.lien_engine import run_lien_pipeline
    from src.loaders.liens import LienLoader
    from src.core.database import get_db_context
    from config.constants import RAW_LIEN_DIR

    run_started = time.time()
    asyncio.run(run_lien_pipeline(
        start_date=(run_date - timedelta(days=1)).strftime("%Y-%m-%d"),
        end_date=run_date.strftime("%Y-%m-%d"),
        county_id=county_id,
    ))
    new_csvs = [
        p for p in Path(RAW_LIEN_DIR).glob("*.csv")
        if p.stat().st_mtime >= run_started
    ]
    if not new_csvs:
        return 0
    total = 0
    with get_db_context() as session:
        loader = LienLoader(session, county_id=county_id)
        for csv_path in new_csvs:
            matched, _, _ = loader.load_from_csv(str(csv_path))
            total += matched
    return total


def _run_evictions(county_id: str, run_date: date) -> int:
    """Evictions: HTTP CSV download → filter evictions → EvictionLoader → DB."""
    from src.scrappers.evictions.evictions_engine import (
        download_latest_civil_filing, process_civil_data, filter_evictions,
    )
    from src.loaders.legal_proceedings import EvictionLoader
    from src.core.database import get_db_context

    csv_path = download_latest_civil_filing(
        target_date=run_date.strftime("%Y-%m-%d"), county_id=county_id,
    )
    if not csv_path:
        return 0
    civil_df = process_civil_data(csv_path)
    evictions_df = filter_evictions(civil_df)
    if evictions_df.empty:
        return 0
    with get_db_context() as session:
        matched, _, _ = EvictionLoader(session, county_id=county_id).load_from_dataframe(evictions_df)
    return matched


def _run_probate(county_id: str, run_date: date) -> int:
    """Probate: HTTP CSV download → ProbateLoader → DB."""
    from src.scrappers.probate.probate_engine import download_latest_probate_filing
    from src.loaders.legal_proceedings import ProbateLoader
    from src.core.database import get_db_context

    csv_path = download_latest_probate_filing(
        target_date=run_date.strftime("%Y-%m-%d"), county_id=county_id,
    )
    if not csv_path:
        return 0
    with get_db_context() as session:
        matched, _, _ = ProbateLoader(session, county_id=county_id).load_from_csv(str(csv_path))
    return matched


def _run_bankruptcy(county_id: str, run_date: date) -> int:
    """Bankruptcy: CourtListener API → DataFrame → BankruptcyLoader → DB."""
    import pandas as pd
    from src.scrappers.bankruptcy.bankruptcy_engine import fetch_bankruptcy_filings
    from src.loaders.legal_proceedings import BankruptcyLoader
    from src.core.database import get_db_context

    records = fetch_bankruptcy_filings(lookback_days=1)
    if not records:
        return 0
    df = pd.DataFrame(records)
    with get_db_context() as session:
        matched, _, _ = BankruptcyLoader(session, county_id=county_id).load_from_dataframe(df)
    return matched


# ── Main runner ────────────────────────────────────────────────────────────────

def run_all_scrapers(county_id: str = "hillsborough") -> dict:
    """
    Run all scrapers for a county — major signal sources first, incident scrapers second.
    Returns dict of scraper_name → records_created (int) or "ERROR: ..." (str).
    After completion: persists results to ScraperRunStats and alerts on failures.
    """
    from src.scrappers.roofing_permits.roofing_permit_engine import scrape_roofing_permits
    from src.scrappers.storm.storm_engine import scrape_storm_damage
    from src.scrappers.fire.fire_engine import scrape_fire_incidents
    from src.scrappers.flood.flood_engine import scrape_flood_damage
    from src.scrappers.insurance.insurance_engine import scrape_insurance_claims

    run_date  = date.today()
    date_range = (run_date - timedelta(days=1), run_date)

    scrapers = [
        # ── Group A: major signal scrapers (scrape → CSV → loader) ───────────
        # These drive the core high-volume signals; must run before CDS scoring.
        ("violations",       lambda: _run_violations(county_id, run_date)),
        ("foreclosures",     lambda: _run_foreclosures(county_id, run_date)),
        ("liens",            lambda: _run_liens(county_id, run_date)),
        ("evictions",        lambda: _run_evictions(county_id, run_date)),
        ("probate",          lambda: _run_probate(county_id, run_date)),
        ("bankruptcy",       lambda: _run_bankruptcy(county_id, run_date)),
        # ── Group B: incident scrapers (write directly to DB) ─────────────────
        ("insurance_claims", lambda: scrape_insurance_claims(county_id, date_range)),
        ("storm_damage",     lambda: scrape_storm_damage(county_id, date_range)),
        ("fire_incidents",   lambda: scrape_fire_incidents(county_id, date_range)),
        ("flood_damage",     lambda: scrape_flood_damage(county_id, date_range)),
        ("roofing_permits",  lambda: scrape_roofing_permits(county_id, date_range)),
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
