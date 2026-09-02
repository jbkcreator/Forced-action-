"""
Probate Filing Data Collection Pipeline — county-agnostic

Downloads probate filings from the county clerk portal, deduplicates
against existing DB records, and saves results.

For Hillsborough: requests-based directory listing of daily CSV files
at {clerk_base}/Probate/dailyfilings/.
For Pinellas (output_format=excel): browser-use agent navigates the clerk
portal, searches by date range, downloads the Excel export, and filters
for probate case types via style_col.

Usage:
    python -m src.scrappers.probate.probate_engine --county-id hillsborough --load-to-db
    python -m src.scrappers.probate.probate_engine --county-id pinellas --load-to-db --headful
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

from src.utils.http_helpers import (
    requests_get_with_retry, STEALTH_UA, STEALTH_ARGS,
    apply_stealth_to_browser_use, apply_stealth_to_page,
    get_playwright_proxy, get_browser_use_proxy,
)

from config.constants import (
    RAW_PROBATE_DIR,
    PROBATE_FILING_PATTERN,
    PROBATE_FILINGS_URL,
    PROBATE_CASE_PATTERNS,
    PINELLAS_CASE_TYPE_KEYWORDS,
    HILLSCLERK_BASE_URL,
    DEFAULT_USER_AGENT,
    REQUEST_TIMEOUT_DEFAULT,
    REQUEST_TIMEOUT_LONG,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.utils.county_config import get_county_config
from src.utils.logger import setup_logging, get_logger
from src.utils.db_deduplicator import filter_new_records

setup_logging()
logger = get_logger(__name__)



def _make_llm():
    from browser_use import ChatAnthropic
    from config.settings import get_settings
    settings = get_settings()
    return ChatAnthropic(
        model=BROWSER_MODEL,
        temperature=BROWSER_TEMPERATURE,
        api_key=settings.anthropic_api_key.get_secret_value(),
    )



def _get_probate_source(county_id: str) -> dict:
    """Return the probate source dict, falling back to court_records if absent."""
    cfg = get_county_config(county_id)
    sources = cfg.get("sources", {})
    return sources.get("probate") or sources.get("court_records") or {}


async def _scrape_with_playwright(
    source: dict, county_id: str, target_date: str | None,
    headful: bool = False, no_proxy: bool = False,
) -> Path:
    """Run the source's playwright_code and save the resulting DataFrame to disk."""
    from playwright.async_api import async_playwright
    from src.utils.action_sequence import execute_playwright_code, PlaywrightCodeError

    playwright_code = source.get("playwright_code", "")
    if not playwright_code:
        raise ValueError(f"[probate] playwright_code is empty for '{county_id}' source")

    if target_date:
        dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        start_str = end_str = dt.strftime("%Y%m%d")
    else:
        yesterday = datetime.now() - timedelta(days=1)
        start_str = end_str = yesterday.strftime("%Y%m%d")

    url = source.get("url", "")
    RAW_PROBATE_DIR.mkdir(parents=True, exist_ok=True)

    _proxy = None if no_proxy else get_playwright_proxy()
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not headful,
            downloads_path=str(RAW_PROBATE_DIR),
            args=STEALTH_ARGS,
        )
        context = await browser.new_context(
            user_agent=STEALTH_UA, accept_downloads=True, proxy=_proxy,
        )
        page = await context.new_page()
        await apply_stealth_to_page(page)
        try:
            df = await execute_playwright_code(
                playwright_code, page, RAW_PROBATE_DIR,
                placeholders={"url": url, "start_date": start_str, "end_date": end_str},
                county_id=county_id,
            )
        except PlaywrightCodeError as e:
            logger.error("[probate] Playwright scrape failed: %s", e)
            raise
        finally:
            await browser.close()

    if df is None or df.empty:
        raise ValueError(f"[probate] Playwright returned no data for '{county_id}'")

    out_path = RAW_PROBATE_DIR / f"probate_playwright_{county_id}_{start_str}.xlsx"
    df.to_excel(out_path, index=False)
    logger.info("[probate] Playwright data saved: %s (%d rows)", out_path.name, len(df))
    return out_path


async def _download_probate_via_browser(
    county_id: str, source: dict, target_date: str | None, dest_dir: Path,
    headful: bool = False, no_proxy: bool = False,
) -> Path:
    """
    Browser-use agent download for counties whose civil/probate portal requires a browser
    (output_format='excel', e.g. Pinellas courtrecords.mypinellasclerk.gov).
    Downloads the combined civil filing; the caller filters for probate case types.
    Returns the path to the downloaded file.
    """
    from browser_use import Agent, Browser

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
        f"Search for court filings filed between {start_str} and {end_str}. "
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
        user_agent=STEALTH_UA,
        ignore_default_args=["--enable-automation"],
        enable_default_extensions=True,
        minimum_wait_page_load_time=1.5,
        wait_between_actions=1.0,
        args=STEALTH_ARGS,
        proxy=None if no_proxy else get_browser_use_proxy(),
    )
    await browser.start()
    await apply_stealth_to_browser_use(browser)
    logger.info("[probate] Stealth fingerprint patches injected")

    start_time = time.time()
    agent = Agent(task=task, llm=_make_llm(), browser=browser, max_steps=60, use_judge=False)
    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[probate] Browser agent did not complete within step budget")
    except Exception as e:
        logger.error("[probate] Browser agent failed: %s", e)
        raise

    await asyncio.sleep(5)
    candidates = [
        p for p in dest_dir.iterdir()
        if p.stat().st_mtime >= start_time and p.suffix.lower() in (".xlsx", ".xls", ".csv")
    ]
    if not candidates:
        raise FileNotFoundError(f"[probate] No downloaded filing found in {dest_dir}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _static_download(source: dict, target_date: str = None) -> Path:
    """
    Download a dated file directly from a URL pattern stored in source["url"].
    The URL must contain the literal placeholder {date} which is replaced with YYYYMMDD.
    Tries the target date first; if none given, walks back up to 7 days to find the
    latest available file (skips empty files — 151-byte placeholder from weekends).
    """
    url_pattern = source.get("url", "")
    if "{date}" not in url_pattern:
        raise ValueError(f"static_download source url must contain {{date}}: {url_pattern!r}")

    if target_date:
        dates_to_try = [datetime.strptime(target_date.replace("-", ""), "%Y%m%d")]
    else:
        today = datetime.now().date()
        dates_to_try = [
            datetime.combine(today - timedelta(days=i), datetime.min.time())
            for i in range(1, 8)
        ]

    RAW_PROBATE_DIR.mkdir(parents=True, exist_ok=True)
    for dt in dates_to_try:
        date_str = dt.strftime("%Y%m%d")
        download_url = url_pattern.replace("{date}", date_str)
        try:
            resp = requests_get_with_retry(
                download_url,
                headers={"User-Agent": DEFAULT_USER_AGENT},
                timeout=REQUEST_TIMEOUT_DEFAULT,
            )
            if resp.status_code != 200 or len(resp.content) <= 200:
                logger.debug("[probate] %s — empty or missing (size=%d)", date_str, len(resp.content))
                continue
            out_path = RAW_PROBATE_DIR / f"ProbateFiling_{date_str}.csv"
            out_path.write_bytes(resp.content)
            logger.info("[probate] Downloaded %s (%.1f KB)", out_path.name, out_path.stat().st_size / 1024)
            return out_path
        except Exception as e:
            logger.warning("[probate] Could not fetch %s: %s", download_url, e)

    raise FileNotFoundError("[probate] No probate filing found in last 7 days")


def download_latest_probate_filing(
    target_date: str = None, county_id: str = "hillsborough",
    headful: bool = False, no_proxy: bool = False,
) -> Path:
    """
    Download the latest probate filing from the county clerk.
    static_download: direct HTTP GET from URL pattern in DB source config.
    playwright_only / playwright_then_ai: Playwright selector mode (with AI fallback for latter).
    excel (browser-use): Pinellas combined civil filing downloaded via browser-use agent.
    csv (default): Hillsborough requests-based directory listing fallback.
    """
    source = _get_probate_source(county_id)
    scrape_mode = source.get("scrape_mode", "")
    output_format = source.get("output_format", "csv")

    # Pinellas courtrecords: deterministic Playwright + 2captcha. Checked BEFORE
    # scrape_mode (mirrors evictions_engine) so this proven path always wins for
    # output_format=excel. Replaces the old browser-use AI agent that could not
    # solve the reCAPTCHA.
    if output_format == "excel":
        # Merged single-session: ONE captcha search exports the filing Excel AND
        # clicks each case for docket detail (written to <dir>/probate_*_detail.json).
        from src.scrappers.court_docket.pinellas.civil_filing import (
            scrape_pinellas_civil_with_detail, reconstruct_filing_list_from_detail,
        )
        kws = PINELLAS_CASE_TYPE_KEYWORDS.get("probate", ["estate", "guardianship"])
        logger.info("[probate] County '%s' — Pinellas courtrecords merged scrape+detail", county_id)
        excel_path, results = asyncio.run(scrape_pinellas_civil_with_detail(
            "probate", kws, source.get("url", ""),
            target_date=target_date, headful=headful, no_proxy=no_proxy,
            dest_dir=RAW_PROBATE_DIR,
        ))
        if excel_path is not None:
            return excel_path
        # Excel export step failed (site timeout/layout hiccup) but the
        # click-through docket-detail scrape may still have succeeded —
        # reconstruct the same raw filing-list shape from it instead of
        # losing the day's data. NOTE: only eviction's header shape was
        # live-verified (2026-07-08); probate's single-party "IN RE: ESTATE
        # OF..." style shares the same scrape_pinellas_civil_with_detail /
        # normalize_style_col("probate") consumer but wasn't independently
        # confirmed live — style_plaintiff is expected to carry the full
        # "IN RE:..." string with style_defendant empty for this case type.
        fallback_df = reconstruct_filing_list_from_detail(results)
        if fallback_df.empty:
            return None
        RAW_PROBATE_DIR.mkdir(parents=True, exist_ok=True)
        fallback_path = RAW_PROBATE_DIR / (
            f"probate_reconstructed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        fallback_df.to_excel(fallback_path, index=False)
        logger.warning(
            "[probate] Excel export unavailable — reconstructed %d case(s) "
            "from docket-detail scrape instead: %s",
            len(fallback_df), fallback_path.name,
        )
        return fallback_path

    if scrape_mode == "static_download":
        logger.info("[probate] Using static_download mode for '%s'", county_id)
        return _static_download(source, target_date)

    if scrape_mode in ("playwright_only", "playwright_then_ai"):
        logger.info("[probate] Using playwright mode for '%s'", county_id)
        try:
            return asyncio.run(
                _scrape_with_playwright(source, county_id, target_date, headful, no_proxy=no_proxy)
            )
        except Exception as pw_exc:
            if scrape_mode != "playwright_then_ai":
                raise
            logger.warning(
                "[probate] Playwright failed for '%s' (%s) — falling back to browser-use AI agent",
                county_id, pw_exc,
            )
            return asyncio.run(
                _download_probate_via_browser(
                    county_id, source, target_date, RAW_PROBATE_DIR,
                    headful=headful, no_proxy=no_proxy,
                )
            )

    # Hillsborough / probate directory-listing path
    county_cfg = get_county_config(county_id)
    probate_url = county_cfg["urls"].get("probate") or PROBATE_FILINGS_URL
    clerk_base_url = county_cfg["urls"].get("clerk_base") or HILLSCLERK_BASE_URL

    logger.info("[probate] Fetching probate filings list from: %s", probate_url)
    RAW_PROBATE_DIR.mkdir(parents=True, exist_ok=True)

    response = requests_get_with_retry(
        probate_url,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=REQUEST_TIMEOUT_DEFAULT,
    )
    soup = BeautifulSoup(response.content, "html.parser")
    file_links = [
        link["href"]
        for link in soup.find_all("a", href=True)
        if "ProbateFiling_" in link["href"] and link["href"].endswith(".csv")
    ]
    if not file_links:
        raise ValueError("[probate] No probate filing CSV files found on the page")

    date_pattern = re.compile(PROBATE_FILING_PATTERN)
    files_with_dates = []
    for href in file_links:
        m = date_pattern.search(href)
        if m:
            try:
                files_with_dates.append((datetime.strptime(m.group(1), "%Y%m%d"), href))
            except ValueError:
                continue
    if not files_with_dates:
        raise ValueError("[probate] No valid dated probate files found")
    files_with_dates.sort(key=lambda x: x[0], reverse=True)

    if target_date:
        target_dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        matches = [(d, f) for d, f in files_with_dates if d.date() == target_dt.date()]
        if not matches:
            from src.utils.scraper_exceptions import ScraperNoDataError
            raise ScraperNoDataError(f"No probate filing found for date: {target_date}")
        latest_date, latest_file = matches[0]
    else:
        latest_date, latest_file = files_with_dates[0]

    logger.info("[probate] Selected: %s (%s)", latest_file, latest_date.strftime("%Y-%m-%d"))
    download_url = (
        f"{clerk_base_url}{latest_file}"
        if latest_file.startswith("/")
        else f"{probate_url.rstrip('/')}/{latest_file}"
    )
    output_path = RAW_PROBATE_DIR / Path(latest_file).name
    file_response = requests_get_with_retry(
        download_url,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=REQUEST_TIMEOUT_LONG,
    )
    output_path.write_bytes(file_response.content)
    logger.info("[probate] Downloaded to: %s (%.1f KB)", output_path, output_path.stat().st_size / 1024)
    return output_path


def process_probate_data(file_path: Path, county_id: str = "hillsborough") -> pd.DataFrame:
    """
    Load the probate filing (CSV or Excel) and return probate rows.
    For Hillsborough: the file is already probate-only (no filtering needed).
    For Pinellas (Excel, combined civil filing): filters by style_col using
    PROBATE_CASE_PATTERNS.
    """
    source = _get_probate_source(county_id)
    output_format = source.get("output_format", "csv")
    style_col = source.get("style_col")

    if output_format == "excel" or file_path.suffix.lower() in (".xlsx", ".xls"):
        logger.info("[probate] Reading Excel civil filing: %s", file_path)
        df = pd.read_excel(file_path)
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
            raise ValueError(f"[probate] Could not read file with any standard encoding: {file_path}")

    logger.info("[probate] Loaded %d rows from %s", len(df), file_path.name)

    # Pinellas courtrecords export: case types already filtered in-browser
    # (Estate/Guardianship), so DON'T re-filter on Style/Description — the
    # "IN RE: <name>" styles won't contain PROBATE_CASE_PATTERNS and would be
    # wrongly dropped. Instead expand "IN RE: ..." into Decedent party rows.
    if "Style/Description" in df.columns:
        from src.scrappers.court_docket.pinellas.civil_filing import normalize_style_col
        return normalize_style_col(df, "probate")

    # Other counties with a combined civil filing (style_col present): filter for probate
    if style_col and style_col in df.columns:
        pattern = "|".join(re.escape(p) for p in PROBATE_CASE_PATTERNS)
        mask = df[style_col].str.contains(pattern, case=False, na=False)
        df = df[mask].copy()
        logger.info("[probate] Filtered %d probate rows via '%s'", len(df), style_col)
    elif style_col:
        logger.warning("[probate] style_col '%s' not found — columns: %s", style_col, list(df.columns))

    return df


def save_processed_probate(df: pd.DataFrame, county_id: str = "hillsborough", output_filename: str = "probate_leads.csv") -> Path:
    """Save processed probate data with dedup against DB."""
    RAW_PROBATE_DIR.mkdir(parents=True, exist_ok=True)

    if "CaseNumber" in df.columns and "Case Number" not in df.columns:
        df = df.rename(columns={"CaseNumber": "Case Number"})

    initial_count = len(df)
    df_new = filter_new_records(df, "probate", record_type="Probate", county_id=county_id)

    if df_new.empty:
        logger.info("[probate] All probate cases already in DB — nothing new")
        return None

    new_dir = RAW_PROBATE_DIR / "new"
    new_dir.mkdir(parents=True, exist_ok=True)
    final_file = new_dir / output_filename
    df_new.to_csv(final_file, index=False)
    logger.info("[probate] Saved %d new probate cases (filtered %d existing)", len(df_new), initial_count - len(df_new))
    return final_file


def run_probate_pipeline(
    target_date: str = None, county_id: str = "hillsborough",
    headful: bool = False, no_proxy: bool = False,
):
    """Full pipeline: download → process → dedup → save.

    Returns True when new records were written (caller should load them),
    "no_data" when the run succeeded but genuinely found nothing (not a
    failure — exit code should still be 0, but there is no CSV to load), or
    False on an actual pipeline failure. Mirrors evictions_engine.py's
    tri-state contract."""
    t0 = time.monotonic()
    county_cfg = get_county_config(county_id)
    logger.info("=" * 60)
    logger.info("%s PROBATE FILINGS — DATA COLLECTION", county_cfg["display_name"].upper())
    logger.info("=" * 60)

    try:
        file_path = download_latest_probate_filing(
            target_date=target_date, county_id=county_id,
            headful=headful, no_proxy=no_proxy,
        )
        if file_path is None:
            # Pinellas merged scrape+detail found no exportable filing list this
            # run (Excel export button or post-export grid recheck timed out) —
            # a real data-collection gap, not a confirmed zero-case day, so this
            # stays a failure (retried by run.sh, alerted after 3 attempts) but
            # with a clear, specific reason instead of a NoneType crash trying
            # to read a file that was never produced.
            logger.error(
                "[probate] Pinellas civil filing export unavailable this run "
                "(Excel export/grid did not load in time) — 0 cases collected"
            )
            try:
                from src.utils.scraper_db_helper import record_scraper_stats
                from config.scraper_outcomes import ScraperOutcome
                record_scraper_stats(
                    source_type="probate", total_scraped=0, matched=0, unmatched=0, skipped=0,
                    error_type="export_unavailable", outcome=ScraperOutcome.TIMEOUT.value,
                    duration_seconds=round(time.monotonic() - t0, 2), county_id=county_id,
                )
            except Exception as _se:
                logger.warning("[probate] Could not record scraper stats: %s", _se)
            return False
        df = process_probate_data(file_path, county_id=county_id)
        output_path = save_processed_probate(df, county_id=county_id)

        logger.info("=" * 60)
        logger.info("PROBATE PIPELINE COMPLETE — %d records, output: %s", len(df), output_path)
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error("[probate] Pipeline failed: %s", e)
        logger.debug(traceback.format_exc())
        from src.utils.scraper_outcome_classifier import classify_exception
        from config.scraper_outcomes import ScraperOutcome
        # No hardcoded run_success=False here on purpose: a ScraperNoDataError
        # (download_latest_probate_filing's "no probate filing found for
        # date") legitimately reaches this branch, and forcing False would
        # misreport a genuine no-data day as a failure.
        classified = classify_exception(e)
        try:
            from src.utils.scraper_db_helper import record_scraper_stats
            record_scraper_stats(
                source_type="probate", total_scraped=0, matched=0, unmatched=0, skipped=0,
                outcome=classified, error_message=str(e)[:500],
                duration_seconds=round(time.monotonic() - t0, 2), county_id=county_id,
            )
        except Exception as _se:
            logger.warning("[probate] Could not record scraper stats: %s", _se)
        # Match the DB row's classification: a NO_DATA-classified exception is
        # a clean no-data day, not a pipeline failure — keep the return value
        # and the stats row in agreement (mirrors evictions_engine.py's
        # tri-state contract).
        return "no_data" if classified == ScraperOutcome.NO_DATA.value else False


if __name__ == "__main__":
    import sys
    import argparse
    from src.utils.scraper_db_helper import load_scraped_data_to_db, add_load_to_db_arg

    parser = argparse.ArgumentParser(description="Scrape county probate filings")
    parser.add_argument("--date", type=str, default=None, help="Target date YYYY-MM-DD (default: latest)")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough",
                        help="County identifier (default: hillsborough)")
    parser.add_argument("--headful", action="store_true", default=False,
                        help="Run browser in headed (visible) mode for debugging")
    parser.add_argument("--no-proxy", dest="no_proxy", action="store_true", default=False,
                        help="Disable Oxylabs proxy for all requests")
    parser.add_argument("--skip-docket", dest="skip_docket", action="store_true", default=False,
                        help="Skip Stage-2 court-docket detail enrichment (Pinellas only)")
    add_load_to_db_arg(parser)
    args = parser.parse_args()

    result = run_probate_pipeline(
        target_date=args.date, county_id=args.county_id,
        headful=args.headful, no_proxy=args.no_proxy,
    )
    success = result is True           # new records were written — proceed to load
    pipeline_ok = result is not False  # True or "no_data" both count as a clean run

    if success and args.load_to_db:
        try:
            new_dir = RAW_PROBATE_DIR / "new"
            csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
            if csv_files:
                load_scraped_data_to_db(
                    "probate", csv_files[0],
                    destination_dir=RAW_PROBATE_DIR, county_id=args.county_id,
                )
            else:
                logger.warning("[probate] No new probate records to load")
        except Exception as e:
            logger.error("[probate] DB load failed: %s", e)
            sys.exit(1)
    elif args.load_to_db and not pipeline_ok:
        logger.warning("[probate] Skipping database load due to scraping failure")
    elif args.load_to_db:
        logger.info("[probate] No new probate cases today — nothing to load")

    # Stage 2 — court-docket detail enrichment (Pinellas only, same daily run).
    # Forward-only (today's rows); never fails the run (enrich returns, never raises).
    if success and args.load_to_db and args.county_id == "pinellas" and not args.skip_docket:
        # Apply the docket detail scraped during the merged search (no re-search,
        # no captcha) onto the rows the loader just matched/inserted.
        from src.scrappers.court_docket.pinellas.detail_enrichment import apply_detail_from_json
        _dj = sorted(RAW_PROBATE_DIR.glob("probate_*_detail.json"),
                     key=lambda p: p.stat().st_mtime, reverse=True)
        if _dj:
            apply_detail_from_json("Probate", _dj[0], county_id=args.county_id)
        else:
            logger.warning("[probate] no docket detail JSON found — skipping detail apply")

    # pipeline_ok (True or "no_data"), not success — a genuine no-data day is
    # not a failure and must exit 0, or run.sh's retry/alert logic pages on
    # a clean run.
    from src.utils.scraper_run_tracking import pipeline_exit_code
    sys.exit(pipeline_exit_code(result))
