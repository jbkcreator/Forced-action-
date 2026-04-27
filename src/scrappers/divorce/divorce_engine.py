"""
Divorce / Dissolution-of-Marriage Filing Scraper — Hillsborough County Clerk

Downloads the daily civil filing CSV from the Hillsborough County Clerk's
public records portal, filters for domestic-relations / dissolution-of-marriage
case types, deduplicates against existing DB records, and loads matches into
the LegalProceeding table with record_type='Divorce'.

The civil filing CSV is the same source used by the eviction scraper —
each day's file contains all civil case types, and this engine filters to the
DR (Domestic Relations) subset. One download per day is shared across both
scrapers at the file level (each saves to its own raw directory).

Entry point:
    python -m src.scrappers.divorce.divorce_engine --load-to-db
"""

import re
import time
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config.constants import (
    RAW_DIVORCE_DIR,
    DIVORCE_CASE_PATTERNS,
    CIVIL_FILING_PATTERN,
    DEFAULT_USER_AGENT,
    REQUEST_TIMEOUT_DEFAULT,
    REQUEST_TIMEOUT_LONG,
)
from src.utils.county_config import get_county as _get_county
from src.utils.http_helpers import requests_get_with_retry
from src.utils.logger import setup_logging, get_logger
from src.utils.db_deduplicator import filter_new_records

setup_logging()
logger = get_logger(__name__)


def download_latest_civil_filing(target_date: str = None, county_id: str = "hillsborough") -> Path:
    """
    Download the latest civil filing CSV from the county clerk.
    Saves to RAW_DIVORCE_DIR. Mirrors the evictions engine download function
    with a different destination directory.
    """
    _county = _get_county(county_id)
    _civil_filings_url = _county["portals"]["civil_filings_url"]
    _clerk_base_url = _county["portals"]["clerk_base_url"]

    logger.info("[divorce] Fetching civil filings list from: %s", _civil_filings_url)

    RAW_DIVORCE_DIR.mkdir(parents=True, exist_ok=True)

    response = requests_get_with_retry(
        _civil_filings_url,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=REQUEST_TIMEOUT_DEFAULT,
    )

    soup = BeautifulSoup(response.content, "html.parser")
    file_links = [
        link["href"]
        for link in soup.find_all("a", href=True)
        if "CivilFiling_" in link["href"] and link["href"].endswith(".csv")
    ]

    if not file_links:
        raise ValueError("[divorce] No civil filing CSV files found on the page")

    date_pattern = re.compile(CIVIL_FILING_PATTERN)
    files_with_dates = []
    for href in file_links:
        m = date_pattern.search(href)
        if m:
            try:
                files_with_dates.append((datetime.strptime(m.group(1), "%Y%m%d"), href))
            except ValueError:
                continue

    if not files_with_dates:
        raise ValueError("[divorce] No valid dated civil files found")

    files_with_dates.sort(key=lambda x: x[0], reverse=True)

    if target_date:
        target_dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        matches = [(d, f) for d, f in files_with_dates if d.date() == target_dt.date()]
        if not matches:
            from src.utils.scraper_exceptions import ScraperNoDataError
            raise ScraperNoDataError(f"No civil filing found for date: {target_date}")
        latest_date, latest_file = matches[0]
    else:
        latest_date, latest_file = files_with_dates[0]

    logger.info("[divorce] Selected civil filing: %s (%s)", latest_file, latest_date.strftime("%Y-%m-%d"))

    download_url = (
        f"{_clerk_base_url}{latest_file}"
        if latest_file.startswith("/")
        else f"{_civil_filings_url.rstrip('/')}/{latest_file}"
    )

    output_path = RAW_DIVORCE_DIR / Path(latest_file).name
    file_response = requests_get_with_retry(
        download_url,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=REQUEST_TIMEOUT_LONG,
    )
    output_path.write_bytes(file_response.content)
    logger.info("[divorce] Downloaded to: %s (%.1f KB)", output_path, output_path.stat().st_size / 1024)
    return output_path


def filter_divorce_cases(csv_path: Path) -> pd.DataFrame:
    """
    Load the civil filing CSV and return only domestic-relations / dissolution rows.
    Matches against DIVORCE_CASE_PATTERNS (case-insensitive substring match on
    CaseTypeDescription).
    """
    encodings = ["utf-8", "latin1", "cp1252"]
    df = None
    for enc in encodings:
        try:
            df = pd.read_csv(csv_path, encoding=enc)
            break
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    if df is None:
        raise ValueError(f"[divorce] Could not read CSV: {csv_path}")

    logger.info("[divorce] Raw civil CSV: %d rows", len(df))

    if "CaseTypeDescription" not in df.columns:
        logger.warning("[divorce] 'CaseTypeDescription' column not found — columns: %s", list(df.columns))
        return pd.DataFrame()

    pattern = "|".join(re.escape(p) for p in DIVORCE_CASE_PATTERNS)
    mask = df["CaseTypeDescription"].str.contains(pattern, case=False, na=False)
    df_divorce = df[mask].copy()
    logger.info("[divorce] Dissolution/DR rows: %d / %d total", len(df_divorce), len(df))
    return df_divorce


def run_divorce_pipeline(target_date: str = None, county_id: str = "hillsborough") -> bool:
    """
    Full pipeline: download → filter → dedup → save.
    Returns True on success.
    """
    t0 = time.monotonic()
    logger.info("=" * 60)
    logger.info("HILLSBOROUGH DIVORCE / DISSOLUTION FILINGS")
    logger.info("=" * 60)

    try:
        csv_path = download_latest_civil_filing(target_date=target_date, county_id=county_id)
        df = filter_divorce_cases(csv_path)

        if df.empty:
            logger.info("[divorce] No dissolution-of-marriage cases in today's civil filing")
            _record_stats(0, 0, 0, 0, True, t0, county_id)
            return True

        # Normalize column name for deduplicator
        if "CaseNumber" in df.columns and "Case Number" not in df.columns:
            df = df.rename(columns={"CaseNumber": "Case Number"})

        initial_count = len(df)
        df_new = filter_new_records(df, "divorce", record_type="Divorce")

        if df_new.empty:
            logger.info("[divorce] All dissolution cases already in DB — nothing new")
            _record_stats(initial_count, 0, initial_count, 0, True, t0, county_id)
            return True

        new_dir = RAW_DIVORCE_DIR / "new"
        new_dir.mkdir(parents=True, exist_ok=True)
        out_path = new_dir / "divorce_filings.csv"
        df_new.to_csv(out_path, index=False)
        logger.info("[divorce] Saved %d new dissolution cases to %s", len(df_new), out_path)

        _record_stats(initial_count, 0, initial_count - len(df_new), 0, True, t0, county_id)
        return True

    except Exception as exc:
        logger.error("[divorce] Pipeline failed: %s", exc)
        logger.debug(traceback.format_exc())
        _record_stats(0, 0, 0, 0, False, t0, county_id, error=str(exc))
        return False


def _record_stats(total, matched, skipped, unmatched, success, t0, county_id, error=None):
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        kwargs = dict(
            source_type="divorce_filings",
            total_scraped=total,
            matched=matched,
            unmatched=unmatched,
            skipped=skipped,
            run_success=success,
            duration_seconds=round(time.monotonic() - t0, 2),
            county_id=county_id,
        )
        if error:
            kwargs["error_message"] = error[:500]
        record_scraper_stats(**kwargs)
    except Exception as _se:
        logger.warning("[divorce] Could not record scraper stats: %s", _se)


if __name__ == "__main__":
    import sys
    import argparse
    from src.utils.scraper_db_helper import load_scraped_data_to_db, add_load_to_db_arg

    parser = argparse.ArgumentParser(description="Scrape Hillsborough County divorce/dissolution filings")
    parser.add_argument("--date", type=str, default=None, help="Target date YYYY-MM-DD (default: latest)")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    add_load_to_db_arg(parser)
    args = parser.parse_args()

    success = run_divorce_pipeline(target_date=args.date, county_id=args.county_id)

    if success and args.load_to_db:
        try:
            new_dir = RAW_DIVORCE_DIR / "new"
            csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
            if csv_files:
                load_scraped_data_to_db("divorce_filings", csv_files[0], destination_dir=RAW_DIVORCE_DIR)
            else:
                logger.warning("[divorce] No new divorce records to load")
        except Exception as exc:
            logger.error("[divorce] DB load failed: %s", exc)
            sys.exit(1)
    elif args.load_to_db:
        logger.warning("[divorce] Skipping DB load due to scraping failure")

    sys.exit(0 if success else 1)
