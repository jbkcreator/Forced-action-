"""
Divorce / Dissolution-of-Marriage Filing Scraper — county-agnostic

Downloads civil filings from the county clerk portal, filters for
domestic-relations / dissolution-of-marriage case types, deduplicates
against existing DB records, and loads matches into the LegalProceeding
table with record_type='Divorce'.

For Hillsborough: requests-based directory listing of daily CSV files.
For Pinellas (output_format=excel): browser-use agent navigates the clerk
portal, searches by date range, and downloads the Excel export.

Usage:
    python -m src.scrappers.divorce.divorce_engine --county-id hillsborough --load-to-db
    python -m src.scrappers.divorce.divorce_engine --county-id pinellas --load-to-db --headful
"""

import asyncio
import re
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config.constants import (
    RAW_DIVORCE_DIR,
    DIVORCE_CASE_PATTERNS,
    CIVIL_FILING_PATTERN,
    CIVIL_FILINGS_URL,
    HILLSCLERK_BASE_URL,
    DEFAULT_USER_AGENT,
    REQUEST_TIMEOUT_DEFAULT,
    REQUEST_TIMEOUT_LONG,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.utils.county_config import get_county_config as _get_county
from src.utils.http_helpers import requests_get_with_retry
from src.utils.logger import setup_logging, get_logger
from src.utils.db_deduplicator import filter_new_records

setup_logging()
logger = get_logger(__name__)

_STEALTH_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)


def _make_llm():
    from browser_use import ChatAnthropic
    from config.settings import get_settings
    settings = get_settings()
    return ChatAnthropic(
        model=BROWSER_MODEL,
        temperature=BROWSER_TEMPERATURE,
        api_key=settings.anthropic_api_key.get_secret_value(),
    )


def _get_court_source(county_id: str) -> dict:
    """Return the court_records source dict for county_id (empty dict if absent)."""
    cfg = _get_county(county_id)
    return cfg.get("sources", {}).get("court_records", {})


async def _download_civil_filing_browser(
    county_id: str, source: dict, target_date: str | None, dest_dir: Path
) -> Path:
    """
    Browser-use agent download for counties whose civil portal requires a browser
    (output_format='excel', e.g. Pinellas courtrecords.mypinellasclerk.gov).
    Returns the path to the downloaded file.
    """
    from browser_use import Agent, Browser
    from playwright_stealth import Stealth
    from src.utils.http_helpers import get_browser_use_proxy

    if target_date:
        target_dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        start_str = end_str = target_dt.strftime("%m/%d/%Y")
    else:
        end_dt = datetime.now()
        start_str = (end_dt - timedelta(days=1)).strftime("%m/%d/%Y")
        end_str = end_dt.strftime("%m/%d/%Y")

    url = source.get("url", "")
    nav_hint = source.get("navigation_hint") or ""
    task = (
        f"Go to {url}. "
        f"Search for civil court filings filed between {start_str} and {end_str}. "
        f"Export or download the full results as a file (Excel or CSV). "
        f"Wait for the download to complete. "
        f"Do not open new tabs or navigate away from the portal."
    )
    if nav_hint:
        task += f"\n\nPortal navigation hint: {nav_hint}"

    dest_dir.mkdir(parents=True, exist_ok=True)
    browser = Browser(
        headless=True,
        disable_security=True,
        proxy=get_browser_use_proxy(),
        downloads_path=str(dest_dir),
        user_agent=_STEALTH_UA,
        ignore_default_args=["--enable-automation"],
        enable_default_extensions=True,
        minimum_wait_page_load_time=1.5,
        wait_between_actions=1.0,
        args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--window-size=1920,1080"],
    )
    await browser.start()
    stealth = Stealth(
        chrome_runtime=True, navigator_webdriver=True, navigator_plugins=True, webgl_vendor=True,
        webgl_vendor_override="Google Inc. (Intel)",
        webgl_renderer_override="ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    )
    await browser._cdp_add_init_script(stealth.script_payload)
    logger.info("[divorce] Stealth fingerprint patches injected")

    start_time = time.time()
    agent = Agent(task=task, llm=_make_llm(), browser=browser, max_steps=60, use_judge=False)
    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[divorce] Browser agent did not complete within step budget")
    except Exception as e:
        logger.error("[divorce] Browser agent failed: %s", e)
        raise

    await asyncio.sleep(5)
    candidates = [
        p for p in dest_dir.iterdir()
        if p.stat().st_mtime >= start_time and p.suffix.lower() in (".xlsx", ".xls", ".csv")
    ]
    if not candidates:
        raise FileNotFoundError(f"[divorce] No downloaded civil filing found in {dest_dir}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def download_latest_civil_filing(target_date: str = None, county_id: str = "hillsborough") -> Path:
    """
    Download the latest civil filing from the county clerk.
    Hillsborough: requests-based directory listing → CSV.
    Other counties (output_format=excel, e.g. Pinellas): browser-use → Excel.
    """
    source = _get_court_source(county_id)
    output_format = source.get("output_format", "csv")

    if output_format == "excel":
        logger.info("[divorce] County '%s' uses browser download (output_format=excel)", county_id)
        return asyncio.run(
            _download_civil_filing_browser(county_id, source, target_date, RAW_DIVORCE_DIR)
        )

    # Hillsborough / CSV directory-listing path
    _county = _get_county(county_id)
    _civil_filings_url = _county["urls"].get("civil") or CIVIL_FILINGS_URL
    _clerk_base_url = _county["urls"].get("clerk_base") or HILLSCLERK_BASE_URL

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


def filter_divorce_cases(file_path: Path, county_id: str = "hillsborough") -> pd.DataFrame:
    """
    Load the civil filing and return only domestic-relations / dissolution rows.
    Uses style_col from county source config as the filter column
    (default: CaseTypeDescription for Hillsborough).
    Reads CSV or Excel based on file extension / county output_format.
    """
    source = _get_court_source(county_id)
    style_col = source.get("style_col", "CaseTypeDescription")
    output_format = source.get("output_format", "csv")

    if output_format == "excel" or file_path.suffix.lower() in (".xlsx", ".xls"):
        try:
            df = pd.read_excel(file_path)
        except Exception as e:
            raise ValueError(f"[divorce] Could not read Excel file {file_path}: {e}")
    else:
        encodings = ["utf-8", "latin1", "cp1252"]
        df = None
        for enc in encodings:
            try:
                df = pd.read_csv(file_path, encoding=enc)
                break
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        if df is None:
            raise ValueError(f"[divorce] Could not read CSV: {file_path}")

    logger.info("[divorce] Raw civil filing: %d rows (filter col: '%s')", len(df), style_col)

    if style_col not in df.columns:
        logger.warning("[divorce] '%s' column not found — columns: %s", style_col, list(df.columns))
        return pd.DataFrame()

    pattern = "|".join(re.escape(p) for p in DIVORCE_CASE_PATTERNS)
    mask = df[style_col].str.contains(pattern, case=False, na=False)
    df_divorce = df[mask].copy()
    logger.info("[divorce] Dissolution/DR rows: %d / %d total", len(df_divorce), len(df))
    return df_divorce


def run_divorce_pipeline(target_date: str = None, county_id: str = "hillsborough") -> bool:
    """Full pipeline: download → filter → dedup → save. Returns True on success."""
    t0 = time.monotonic()
    logger.info("=" * 60)
    logger.info("%s DIVORCE / DISSOLUTION FILINGS", county_id.upper())
    logger.info("=" * 60)

    try:
        file_path = download_latest_civil_filing(target_date=target_date, county_id=county_id)
        df = filter_divorce_cases(file_path, county_id=county_id)

        if df.empty:
            logger.info("[divorce] No dissolution-of-marriage cases in today's civil filing")
            _record_stats(0, 0, 0, 0, True, t0, county_id)
            return True

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

    parser = argparse.ArgumentParser(description="Scrape county divorce/dissolution filings")
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
                load_scraped_data_to_db(
                    "divorce_filings", csv_files[0],
                    destination_dir=RAW_DIVORCE_DIR, county_id=args.county_id,
                )
            else:
                logger.warning("[divorce] No new divorce records to load")
        except Exception as exc:
            logger.error("[divorce] DB load failed: %s", exc)
            sys.exit(1)
    elif args.load_to_db:
        logger.warning("[divorce] Skipping DB load due to scraping failure")

    sys.exit(0 if success else 1)
