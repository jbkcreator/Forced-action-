"""
Eviction Filing Data Collection Pipeline — county-agnostic

Downloads civil filings from the county clerk portal, filters for eviction
case types, deduplicates against existing DB records, and saves results.

For Hillsborough: requests-based directory listing of daily CSV files.
For Pinellas (output_format=excel): browser-use agent navigates the clerk
portal, searches by date range, and downloads the Excel export.

Usage:
    python -m src.scrappers.evictions.evictions_engine --county-id hillsborough --load-to-db
    python -m src.scrappers.evictions.evictions_engine --county-id pinellas --load-to-db --headful
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

from src.utils.http_helpers import requests_get_with_retry

from config.constants import (
    RAW_EVICTIONS_DIR,
    EVICTION_CASE_PATTERNS,
    CIVIL_FILING_PATTERN,
    CIVIL_FILINGS_URL,
    HILLSCLERK_BASE_URL,
    DEFAULT_USER_AGENT,
    REQUEST_TIMEOUT_DEFAULT,
    REQUEST_TIMEOUT_LONG,
    OUTPUT_DATE_FORMAT,
    OUTPUT_SEPARATOR,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.utils.county_config import get_county_config as _get_county
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
    county_id: str, source: dict, target_date: str | None, dest_dir: Path,
    headful: bool = False,
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
    elif source.get("start_date") and source.get("end_date"):
        start_str = datetime.strptime(source["start_date"], "%Y-%m-%d").strftime("%m/%d/%Y")
        end_str   = datetime.strptime(source["end_date"],   "%Y-%m-%d").strftime("%m/%d/%Y")
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
        headless=not headful,
        disable_security=True,
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
    logger.info("[evictions] Stealth fingerprint patches injected")

    start_time = time.time()
    agent = Agent(task=task, llm=_make_llm(), browser=browser, max_steps=60, use_judge=False)
    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[evictions] Browser agent did not complete within step budget")
    except Exception as e:
        logger.error("[evictions] Browser agent failed: %s", e)
        raise

    await asyncio.sleep(5)
    candidates = [
        p for p in dest_dir.iterdir()
        if p.stat().st_mtime >= start_time and p.suffix.lower() in (".xlsx", ".xls", ".csv")
    ]
    if not candidates:
        raise FileNotFoundError(f"[evictions] No downloaded civil filing found in {dest_dir}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def download_latest_civil_filing(
    target_date: str = None, county_id: str = "hillsborough", headful: bool = False,
    start_date: str = None, end_date: str = None,
) -> Path:
    """
    Download the latest civil filing from the county clerk.
    Hillsborough: requests-based directory listing → CSV.
    Other counties (output_format=excel, e.g. Pinellas): browser-use → Excel.
    """
    source = _get_court_source(county_id)
    output_format = source.get("output_format", "csv")

    if start_date:
        source = dict(source, start_date=start_date, end_date=end_date or start_date)

    if output_format == "excel":
        logger.info("[evictions] County '%s' uses browser download (output_format=excel)", county_id)
        return asyncio.run(
            _download_civil_filing_browser(county_id, source, target_date, RAW_EVICTIONS_DIR, headful=headful)
        )

    # Hillsborough / CSV directory-listing path
    _county = _get_county(county_id)
    _civil_filings_url = _county["urls"].get("civil") or CIVIL_FILINGS_URL
    _clerk_base_url = _county["urls"].get("clerk_base") or HILLSCLERK_BASE_URL

    logger.info("[evictions] Fetching civil filings list from: %s", _civil_filings_url)
    RAW_EVICTIONS_DIR.mkdir(parents=True, exist_ok=True)

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
        raise ValueError("[evictions] No civil filing CSV files found on the page")

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
        raise ValueError("[evictions] No valid dated civil files found")
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

    logger.info("[evictions] Selected civil filing: %s (%s)", latest_file, latest_date.strftime("%Y-%m-%d"))
    download_url = (
        f"{_clerk_base_url}{latest_file}"
        if latest_file.startswith("/")
        else f"{_civil_filings_url.rstrip('/')}/{latest_file}"
    )
    output_path = RAW_EVICTIONS_DIR / Path(latest_file).name
    file_response = requests_get_with_retry(
        download_url,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=REQUEST_TIMEOUT_LONG,
    )
    output_path.write_bytes(file_response.content)
    logger.info("[evictions] Downloaded to: %s (%.1f KB)", output_path, output_path.stat().st_size / 1024)
    return output_path


def _normalize_style_col_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Pinellas-style one-row-per-case court export to the multi-row
    format that Hillsborough uses and downstream loaders expect.

    Pinellas columns: Case Type, Case #, Filed, Style/Description, Status, Judicial Officer
    Canonical format: CaseTypeDescription, Case Number, FilingDate, Title, PartyType,
                      LastName/CompanyName, PartyAddress

    Style/Description format: "PLAINTIFF NAME\nVs.\nDEFENDANT NAME"
    Each case row is expanded into two rows (Plaintiff + Defendant).
    """
    rename = {
        "Case #":        "Case Number",
        "Filed":         "FilingDate",
        "Case Type":     "CaseTypeDescription",
        "Status":        "Title",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "Style/Description" not in df.columns:
        return df

    expanded_rows = []
    for _, row in df.iterrows():
        style = str(row.get("Style/Description", "") or "")
        # Split on "Vs." variants — newline-separated or just " Vs. "
        parts = re.split(r'\n[Vv][Ss]\.\n|[\s]+[Vv][Ss]\.[\s]+', style, maxsplit=1)
        plaintiff = parts[0].strip() if len(parts) >= 1 else ""
        defendant = parts[1].strip().rstrip(".").strip() if len(parts) >= 2 else ""
        # Strip common suffixes like "et al"
        for suffix in (" et al", " ET AL", " Et Al"):
            defendant = defendant.removesuffix(suffix).strip()

        for party_type, name in (("Plaintiff", plaintiff), ("Defendant", defendant)):
            if not name:
                continue
            new_row = row.to_dict()
            new_row["PartyType"] = party_type
            new_row["LastName/CompanyName"] = name
            new_row["FirstName"] = ""
            new_row["PartyAddress"] = None
            expanded_rows.append(new_row)

    if not expanded_rows:
        return df

    result = pd.DataFrame(expanded_rows)
    logger.info(
        "[evictions] Pinellas normalizer: %d cases → %d party rows",
        len(df), len(result),
    )
    return result


def process_civil_data(file_path: Path, county_id: str = "hillsborough") -> pd.DataFrame:
    """Load the civil filing (CSV or Excel) with multi-encoding support."""
    source = _get_court_source(county_id)
    output_format = source.get("output_format", "csv")

    if output_format == "excel" or file_path.suffix.lower() in (".xlsx", ".xls"):
        logger.info("[evictions] Reading Excel civil filing: %s", file_path)
        df = pd.read_excel(file_path)
    else:
        logger.info("[evictions] Loading civil filing from: %s", file_path)
        encodings = ["utf-8", "latin1", "cp1252"]
        df = None
        for enc in encodings:
            try:
                df = pd.read_csv(file_path, encoding=enc)
                logger.info("[evictions] Loaded %d records (%s)", len(df), enc)
                break
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        if df is None:
            raise ValueError(f"[evictions] Could not read civil filing with any standard encoding: {file_path}")

    # Apply Pinellas-style normalizer when Style/Description column parsing is needed
    if source.get("style_col") == "Style/Description":
        df = _normalize_style_col_format(df)

    return df


def filter_evictions(df: pd.DataFrame, county_id: str = "hillsborough") -> pd.DataFrame:
    """
    Filter civil filing data to include only eviction-related cases.
    Uses style_col from county source config as the filter column
    (default: CaseTypeDescription for Hillsborough).
    """
    source = _get_court_source(county_id)
    style_col = source.get("style_col", "CaseTypeDescription")

    if style_col not in df.columns:
        logger.error("[evictions] Column '%s' not found — columns: %s", style_col, list(df.columns))
        raise KeyError(f"Column '{style_col}' not found in civil filing")

    mask = df[style_col].str.contains("|".join(EVICTION_CASE_PATTERNS), case=False, na=False)
    evictions_df = df[mask].copy()
    logger.info("[evictions] Filtered %d eviction records from %d total (col: %s)",
                len(evictions_df), len(df), style_col)
    return evictions_df


def save_processed_evictions(df: pd.DataFrame, output_filename: str = "eviction_leads.csv") -> Path:
    """Save processed eviction data with dedup against DB."""
    RAW_EVICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    initial_count = len(df)
    df_new = filter_new_records(df, "evictions", record_type="Eviction")

    if df_new.empty:
        logger.info("[evictions] All evictions already in DB — nothing new")
        return None

    new_dir = RAW_EVICTIONS_DIR / "new"
    new_dir.mkdir(parents=True, exist_ok=True)
    final_file = new_dir / output_filename
    df_new.to_csv(final_file, index=False)
    logger.info("[evictions] Saved %d new evictions (filtered %d existing)", len(df_new), initial_count - len(df_new))
    return final_file


def run_eviction_pipeline(
    target_date: str = None, county_id: str = "hillsborough", headful: bool = False,
    start_date: str = None, end_date: str = None,
) -> bool:
    """Full pipeline: download → load → filter → dedup → save. Returns True on success."""
    t0 = time.monotonic()
    try:
        logger.info(OUTPUT_SEPARATOR)
        logger.info("%s EVICTION DATA COLLECTION PIPELINE", county_id.upper())
        logger.info(OUTPUT_SEPARATOR)

        file_path = download_latest_civil_filing(
            target_date=target_date, county_id=county_id, headful=headful,
            start_date=start_date, end_date=end_date,
        )
        civil_df = process_civil_data(file_path, county_id=county_id)
        evictions_df = filter_evictions(civil_df, county_id=county_id)

        if len(evictions_df) == 0:
            logger.warning("[evictions] No eviction cases found")
            try:
                from src.utils.scraper_db_helper import record_scraper_stats
                record_scraper_stats(
                    source_type="evictions", total_scraped=0, matched=0, unmatched=0, skipped=0,
                    run_success=True, error_type="no_data",
                    duration_seconds=round(time.monotonic() - t0, 2), county_id=county_id,
                )
            except Exception as _se:
                logger.warning("[evictions] Could not record scraper stats: %s", _se)
            return False

        today = datetime.now().strftime(OUTPUT_DATE_FORMAT)
        output_path = save_processed_evictions(evictions_df, f"eviction_leads_{today}.csv")

        logger.info(OUTPUT_SEPARATOR)
        logger.info("EVICTION PIPELINE COMPLETED — %d records, output: %s", len(evictions_df), output_path)
        logger.info(OUTPUT_SEPARATOR)

        try:
            from src.utils.scraper_db_helper import record_scraper_stats
            record_scraper_stats(
                source_type="evictions", total_scraped=len(evictions_df), matched=0, unmatched=0, skipped=0,
                run_success=True, duration_seconds=round(time.monotonic() - t0, 2), county_id=county_id,
            )
        except Exception as _se:
            logger.warning("[evictions] Could not record scraper stats: %s", _se)

        return True

    except Exception as e:
        logger.error("[evictions] Pipeline failed: %s", e)
        logger.debug(traceback.format_exc())
        try:
            from src.utils.scraper_db_helper import record_scraper_stats
            record_scraper_stats(
                source_type="evictions", total_scraped=0, matched=0, unmatched=0, skipped=0,
                run_success=False, error_message=str(e)[:500],
                duration_seconds=round(time.monotonic() - t0, 2), county_id=county_id,
            )
        except Exception as _se:
            logger.warning("[evictions] Could not record scraper stats: %s", _se)
        return False


if __name__ == "__main__":
    import sys
    import argparse
    from src.utils.scraper_db_helper import load_scraped_data_to_db, add_load_to_db_arg

    parser = argparse.ArgumentParser(description="Scrape county eviction filings")
    parser.add_argument("--date", type=str, default=None, help="Single target date YYYY-MM-DD")
    parser.add_argument("--start-date", dest="start_date", type=str, default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", dest="end_date", type=str, default=None, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough",
                        help="County identifier (default: hillsborough)")
    parser.add_argument("--headful", action="store_true", default=False,
                        help="Run browser in headed (visible) mode for debugging")
    add_load_to_db_arg(parser)
    args = parser.parse_args()

    success = run_eviction_pipeline(
        target_date=args.date, county_id=args.county_id, headful=args.headful,
        start_date=args.start_date, end_date=args.end_date,
    )

    if success and args.load_to_db:
        try:
            new_dir = RAW_EVICTIONS_DIR / "new"
            csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
            if csv_files:
                load_scraped_data_to_db(
                    "evictions", csv_files[0],
                    destination_dir=RAW_EVICTIONS_DIR, county_id=args.county_id,
                )
            else:
                logger.error("[evictions] No eviction CSV file found to load")
                sys.exit(1)
        except Exception as e:
            logger.error("[evictions] Failed to load data to database: %s", e)
            sys.exit(1)
    elif args.load_to_db:
        logger.warning("[evictions] Skipping database load due to scraping failure")

    sys.exit(0 if success else 1)
