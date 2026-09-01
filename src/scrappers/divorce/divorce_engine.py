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
    PINELLAS_CASE_TYPE_KEYWORDS,
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
from src.utils.http_helpers import (
    requests_get_with_retry, STEALTH_UA, STEALTH_ARGS,
    apply_stealth_to_browser_use, apply_stealth_to_page,
    get_playwright_proxy, get_browser_use_proxy,
)
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


def _get_divorce_source(county_id: str) -> dict:
    """Return the divorce or court_records source dict for county_id (empty dict if absent)."""
    cfg = _get_county(county_id)
    sources = cfg.get("sources", {})
    return sources.get("divorce") or sources.get("court_records") or {}


def _static_download(source: dict, target_date: str | None = None) -> Path:
    """Direct HTTP download using the {date} URL pattern (no browser needed)."""
    url_pattern = source.get("url", "")
    if "{date}" not in url_pattern:
        raise ValueError(f"[divorce] static_download URL must contain {{date}}: {url_pattern!r}")

    RAW_DIVORCE_DIR.mkdir(parents=True, exist_ok=True)

    if target_date:
        dates_to_try = [datetime.strptime(target_date.replace("-", ""), "%Y%m%d")]
    else:
        today = datetime.now()
        dates_to_try = [today - timedelta(days=i) for i in range(1, 8)]

    for dt in dates_to_try:
        date_str = dt.strftime("%Y%m%d")
        download_url = url_pattern.replace("{date}", date_str)
        resp = requests_get_with_retry(
            download_url,
            headers={"User-Agent": DEFAULT_USER_AGENT},
            timeout=REQUEST_TIMEOUT_DEFAULT,
        )
        if resp.status_code != 200 or len(resp.content) <= 200:
            logger.debug("[divorce] Skipping %s (status=%s size=%s)", date_str, resp.status_code, len(resp.content))
            continue
        out_path = RAW_DIVORCE_DIR / f"CivilFiling_{date_str}.csv"
        out_path.write_bytes(resp.content)
        logger.info("[divorce] Downloaded CivilFiling_%s.csv (%.1f KB)", date_str, len(resp.content) / 1024)
        return out_path

    raise FileNotFoundError("[divorce] No civil filing found in last 7 days")


async def _scrape_with_playwright(
    source: dict, county_id: str, target_date: str | None,
    headful: bool = False, no_proxy: bool = False,
) -> Path:
    """Run the source's playwright_code and save the resulting DataFrame to disk."""
    from playwright.async_api import async_playwright
    from src.utils.action_sequence import execute_playwright_code, PlaywrightCodeError

    playwright_code = source.get("playwright_code", "")
    if not playwright_code:
        raise ValueError(f"[divorce] playwright_code is empty for '{county_id}' source")

    if target_date:
        dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        start_str = end_str = dt.strftime("%Y%m%d")
    else:
        yesterday = datetime.now() - timedelta(days=1)
        start_str = end_str = yesterday.strftime("%Y%m%d")

    url = source.get("url", "")
    RAW_DIVORCE_DIR.mkdir(parents=True, exist_ok=True)

    _proxy = None if no_proxy else get_playwright_proxy()
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not headful,
            downloads_path=str(RAW_DIVORCE_DIR),
            args=STEALTH_ARGS,
        )
        context = await browser.new_context(
            user_agent=STEALTH_UA, accept_downloads=True, proxy=_proxy,
        )
        page = await context.new_page()
        await apply_stealth_to_page(page)
        try:
            df = await execute_playwright_code(
                playwright_code, page, RAW_DIVORCE_DIR,
                placeholders={"url": url, "start_date": start_str, "end_date": end_str},
                county_id=county_id,
            )
        except PlaywrightCodeError as e:
            logger.error("[divorce] Playwright scrape failed: %s", e)
            raise
        finally:
            await browser.close()

    if df is None or df.empty:
        raise ValueError(f"[divorce] Playwright returned no data for '{county_id}'")

    out_path = RAW_DIVORCE_DIR / f"divorce_playwright_{county_id}_{start_str}.xlsx"
    df.to_excel(out_path, index=False)
    logger.info("[divorce] Playwright data saved: %s (%d rows)", out_path.name, len(df))
    return out_path


async def _download_civil_filing_browser(
    county_id: str, source: dict, target_date: str | None, dest_dir: Path,
    headful: bool = False, no_proxy: bool = False,
) -> Path:
    """
    Browser-use agent download for counties whose civil portal requires a browser
    (output_format='excel', e.g. Pinellas courtrecords.mypinellasclerk.gov).
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


def download_latest_civil_filing(
    target_date: str = None, county_id: str = "hillsborough",
    headful: bool = False, no_proxy: bool = False,
) -> Path:
    """
    Download the latest civil filing from the county clerk.
    static_download: direct HTTP GET via {date} URL pattern.
    playwright_only / playwright_then_ai: Playwright selector mode (with AI fallback for latter).
    excel: browser-use agent (e.g. Pinellas).
    Default: requests-based directory listing → CSV.
    """
    source = _get_divorce_source(county_id)
    scrape_mode = source.get("scrape_mode", "")
    output_format = source.get("output_format", "csv")

    # Pinellas courtrecords: deterministic Playwright + 2captcha. Checked BEFORE
    # scrape_mode (mirrors evictions_engine). Replaces the browser-use AI agent
    # that could not solve the reCAPTCHA.
    if output_format == "excel":
        # Merged single-session: ONE captcha search exports the filing Excel AND
        # clicks each case for docket detail (written to <dir>/divorce_*_detail.json).
        from src.scrappers.court_docket.pinellas.civil_filing import (
            scrape_pinellas_civil_with_detail, reconstruct_filing_list_from_detail,
        )
        kws = PINELLAS_CASE_TYPE_KEYWORDS.get("divorce", ["dissolution"])
        logger.info("[divorce] County '%s' — Pinellas courtrecords merged scrape+detail", county_id)
        excel_path, results = asyncio.run(scrape_pinellas_civil_with_detail(
            "divorce", kws, source.get("url", ""),
            target_date=target_date, headful=headful, no_proxy=no_proxy,
            dest_dir=RAW_DIVORCE_DIR,
        ))
        if excel_path is not None:
            return excel_path
        # Excel export step failed (site timeout/layout hiccup) but the
        # click-through docket-detail scrape may still have succeeded —
        # reconstruct the same raw filing-list shape from it instead of
        # losing the day's data. Shares the exact reconstruction verified
        # live against evictions (2026-07-08) — same header/parties shape,
        # same normalize_style_col() consumer, different record_type label.
        fallback_df = reconstruct_filing_list_from_detail(results)
        if fallback_df.empty:
            return None
        RAW_DIVORCE_DIR.mkdir(parents=True, exist_ok=True)
        fallback_path = RAW_DIVORCE_DIR / (
            f"divorce_reconstructed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        fallback_df.to_excel(fallback_path, index=False)
        logger.warning(
            "[divorce] Excel export unavailable — reconstructed %d case(s) "
            "from docket-detail scrape instead: %s",
            len(fallback_df), fallback_path.name,
        )
        return fallback_path

    if scrape_mode == "static_download":
        logger.info("[divorce] Using static_download mode for '%s'", county_id)
        return _static_download(source, target_date)

    if scrape_mode in ("playwright_only", "playwright_then_ai"):
        logger.info("[divorce] Using playwright mode for '%s'", county_id)
        try:
            return asyncio.run(
                _scrape_with_playwright(source, county_id, target_date, headful, no_proxy=no_proxy)
            )
        except Exception as pw_exc:
            if scrape_mode != "playwright_then_ai":
                raise
            logger.warning(
                "[divorce] Playwright failed for '%s' (%s) — falling back to browser-use AI agent",
                county_id, pw_exc,
            )
            return asyncio.run(
                _download_civil_filing_browser(
                    county_id, source, target_date, RAW_DIVORCE_DIR,
                    headful=headful, no_proxy=no_proxy,
                )
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
    source = _get_divorce_source(county_id)
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

    # Pinellas courtrecords export: case type already filtered in-browser
    # (Dissolution Of Marriage). Expand "PETITIONER Vs. RESPONDENT" into
    # Petitioner/Respondent party rows; skip the pattern re-filter.
    if "Style/Description" in df.columns:
        from src.scrappers.court_docket.pinellas.civil_filing import normalize_style_col
        return normalize_style_col(df, "divorce")

    if style_col not in df.columns:
        logger.warning("[divorce] '%s' column not found — columns: %s", style_col, list(df.columns))
        return pd.DataFrame()

    pattern = "|".join(re.escape(p) for p in DIVORCE_CASE_PATTERNS)
    mask = df[style_col].str.contains(pattern, case=False, na=False)
    df_divorce = df[mask].copy()
    logger.info("[divorce] Dissolution/DR rows: %d / %d total", len(df_divorce), len(df))
    return df_divorce


def run_divorce_pipeline(
    target_date: str = None, county_id: str = "hillsborough",
    headful: bool = False, no_proxy: bool = False,
):
    """Full pipeline: download → filter → dedup → save.

    Returns True when new records were written (caller should load them),
    "no_data" when the run succeeded but genuinely found nothing (not a
    failure — exit code should still be 0, but there is no CSV to load), or
    False on an actual pipeline failure. Mirrors evictions_engine.py's
    tri-state contract."""
    t0 = time.monotonic()
    logger.info("=" * 60)
    logger.info("%s DIVORCE / DISSOLUTION FILINGS", county_id.upper())
    logger.info("=" * 60)

    try:
        file_path = download_latest_civil_filing(
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
                "[divorce] Pinellas civil filing export unavailable this run "
                "(Excel export/grid did not load in time) — 0 cases collected"
            )
            from config.scraper_outcomes import ScraperOutcome
            _record_stats(0, 0, 0, 0, False, t0, county_id, error="export_unavailable", outcome=ScraperOutcome.TIMEOUT.value)
            return False
        df = filter_divorce_cases(file_path, county_id=county_id)

        if df.empty:
            logger.info("[divorce] No dissolution-of-marriage cases in today's civil filing")
            from config.scraper_outcomes import ScraperOutcome
            _record_stats(0, 0, 0, 0, True, t0, county_id, outcome=ScraperOutcome.NO_DATA.value)
            return True

        if "CaseNumber" in df.columns and "Case Number" not in df.columns:
            df = df.rename(columns={"CaseNumber": "Case Number"})

        initial_count = len(df)
        df_new = filter_new_records(df, "divorce", record_type="Divorce", county_id=county_id)

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
        from src.utils.scraper_outcome_classifier import classify_exception
        from config.scraper_outcomes import ScraperOutcome
        # success=None (not False) on purpose: a ScraperNoDataError
        # (download_latest_civil_filing's "no civil filing found for date")
        # legitimately reaches this branch, and record_scraper_stats derives
        # the real run_success from outcome= below — passing None instead of
        # a hardcoded False avoids a spurious mismatch warning on that path
        # while every other exception still correctly derives to False.
        classified = classify_exception(exc)
        _record_stats(0, 0, 0, 0, None, t0, county_id, error=str(exc), outcome=classified)
        # Match the DB row's classification: a NO_DATA-classified exception is
        # a clean no-data day, not a pipeline failure — keep the return value
        # and the stats row in agreement (mirrors evictions_engine.py's
        # tri-state contract).
        return "no_data" if classified == ScraperOutcome.NO_DATA.value else False


def _record_stats(total, matched, skipped, unmatched, success, t0, county_id, error=None, outcome=None):
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        kwargs = dict(
            source_type="divorce_filings",
            total_scraped=total,
            matched=matched,
            unmatched=unmatched,
            skipped=skipped,
            run_success=success,
            outcome=outcome,
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
    parser.add_argument("--headful", action="store_true", default=False,
                        help="Run browser in headed (visible) mode for debugging")
    parser.add_argument("--no-proxy", dest="no_proxy", action="store_true", default=False,
                        help="Disable Oxylabs proxy for all requests")
    parser.add_argument("--skip-docket", dest="skip_docket", action="store_true", default=False,
                        help="Skip Stage-2 court-docket detail enrichment (Pinellas only)")
    add_load_to_db_arg(parser)
    args = parser.parse_args()

    result = run_divorce_pipeline(
        target_date=args.date, county_id=args.county_id,
        headful=args.headful, no_proxy=args.no_proxy,
    )
    success = result is True           # new records were written — proceed to load
    pipeline_ok = result is not False  # True or "no_data" both count as a clean run

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
    elif args.load_to_db and not pipeline_ok:
        logger.warning("[divorce] Skipping DB load due to scraping failure")
    elif args.load_to_db:
        logger.info("[divorce] No new dissolution cases today — nothing to load")

    # Stage 2 — apply docket detail scraped during the merged search (no re-search/captcha).
    if success and args.load_to_db and args.county_id == "pinellas" and not args.skip_docket:
        from src.scrappers.court_docket.pinellas.detail_enrichment import apply_detail_from_json
        _dj = sorted(RAW_DIVORCE_DIR.glob("divorce_*_detail.json"),
                     key=lambda p: p.stat().st_mtime, reverse=True)
        if _dj:
            apply_detail_from_json("Divorce", _dj[0], county_id=args.county_id)
        else:
            logger.warning("[divorce] no docket detail JSON found — skipping detail apply")

    sys.exit(0 if success else 1)
