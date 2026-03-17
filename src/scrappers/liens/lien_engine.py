"""
Lien and Judgment Document Collection Pipeline

This module automates the download and processing of lien and judgment records from
the Hillsborough County Clerk's public access system. It uses browser automation via
browser_use and Claude Sonnet 4.5 to navigate the document search interface and
download multiple document types.

The pipeline supports six document types:
    - LIEN: General Liens
    - LNCORPTX: Corporate Tax Liens
    - JUD: Judgments
    - CCJ: Certified Judgments
    - D: Deeds
    - TAXDEED: Tax Deeds

Execution modes:
    - Sequential (default): Downloads one document type at a time to conserve API credits
    - Parallel (--all flag): Downloads all six types concurrently for faster execution

Author: Distressed Property Intelligence Platform
"""

import asyncio
import os
import shutil
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
# REVERTED: Using your original imports to match your library version
from browser_use import Agent, ChatAnthropic, Browser

from config.settings import settings
from config.constants import (
    RAW_LIEN_DIR,
    PROCESSED_DATA_DIR,
    PROCESSED_LIENS_DIR,
    PROCESSED_DEEDS_DIR,
    PROCESSED_JUDGMENTS_DIR,
    DOWNLOAD_FILE_PATTERNS,
    TEMP_DOWNLOADS_DIR,
    BROWSER_DOWNLOAD_TEMP_PATTERN,
    HILLSCLERK_PUBLIC_ACCESS_URL,
    get_county_config,
)
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt
from src.utils.csv_deduplicator import deduplicate_csv, get_unique_keys_for_type


# Initialize logging
setup_logging()
logger = get_logger(__name__)

# Model + agent configuration
llm = ChatAnthropic(
    model="claude-sonnet-4-5-20250929",
    timeout=150,
    api_key=settings.anthropic_api_key.get_secret_value(),
    temperature=0,
)

# Ensure directories exist
RAW_LIEN_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_LIENS_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DEEDS_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_JUDGMENTS_DIR.mkdir(parents=True, exist_ok=True)


def _locate_download(start_time: float) -> Optional[Path]:
    """
    Search for recently downloaded files in configured directories.
    
    This function searches both the raw data directory and browser-use temporary
    directories for files matching download patterns that were created after the
    specified start time.
    
    Args:
        start_time: Unix timestamp representing when the download started
        
    Returns:
        Optional[Path]: Path to the most recently modified downloaded file, or None if not found
        
    Note:
        Searches for files matching patterns: *.csv, *.xls, *.xlsx, *.zip
    """
    
    def recent_candidates(folder: Path):
        """Find files in folder that match download patterns and were created after start_time."""
        paths = []
        if not folder.exists():
            return paths
        for pattern in DOWNLOAD_FILE_PATTERNS:
            for candidate in folder.glob(pattern):
                try:
                    if candidate.stat().st_mtime >= start_time:
                        paths.append(candidate)
                except FileNotFoundError:
                    logger.debug(f"File disappeared during check: {candidate}")
                    continue
        return paths
    
    candidates = recent_candidates(RAW_LIEN_DIR)
    logger.debug(f"Found {len(candidates)} candidate files in {RAW_LIEN_DIR}")
    
    # Check browser-use temp directories
    temp_base = TEMP_DOWNLOADS_DIR
    if temp_base.exists():
        for download_dir in temp_base.glob(BROWSER_DOWNLOAD_TEMP_PATTERN):
            temp_candidates = recent_candidates(download_dir)
            candidates.extend(temp_candidates)
            logger.debug(f"Found {len(temp_candidates)} candidate files in {download_dir}")
    
    if not candidates:
        logger.warning("No recent download files found")
        return None
    
    # Return the most recently modified file
    most_recent = max(candidates, key=lambda path: path.stat().st_mtime)
    logger.debug(f"Selected most recent file: {most_recent}")
    return most_recent

async def _safe_close_browser(browser: Optional[Browser]):
    """
    Safely closes the browser instance. 
    Prevents 'BrowserSession' attribute errors from interrupting the pipeline.
    """
    if browser:
        try:
            # Try multiple close methods common in different browser-use versions
            if hasattr(browser, 'close'):
                await browser.close()
            elif hasattr(browser, 'stop'):
                await browser.stop()
            logger.debug("Browser session closed successfully.")
        except Exception as e:
            logger.debug(f"Non-critical cleanup error (ignored): {e}")

async def _playwright_download_document(
    doc_type: str,
    start_date_str: str,
    end_date_str: str,
    clerk_access_url: str = HILLSCLERK_PUBLIC_ACCESS_URL,
) -> Optional[Path]:
    """
    Download all county records for a date range — no DocType filter.
    The county export ignores DocType filters anyway; filtering is done in Python after download.

    Flow:
        1. Navigate to the public access URL
        2. Click the "Document Type" search-type link
        3. Wait for the search form (no option selected — all types returned)
        4. Fill date range via JS and submit
        5. If results exceed 6000 → raise OverflowError (caller handles day-by-day retry)
        6. Download via Export button
    """
    from playwright.async_api import async_playwright

    logger.info(f"[Playwright][{doc_type}] Searching {start_date_str} → {end_date_str}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
        )
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            # 1. Navigate
            await page.goto(clerk_access_url, wait_until="domcontentloaded", timeout=60_000)

            # 2. Click "Document Type" search-type link
            await page.click('div[id="ORI-Document Type"]')

            # 3. Wait for search form — no DocType selection, all types will be returned
            await page.wait_for_selector('#OBKey__1285_1', state='attached', timeout=25_000)

            # 4. Fill date range via JS (chosen dropdown may be open and intercept pointer events)
            await page.evaluate(
                """([startDate, endDate]) => {
                    const setField = (id, val) => {
                        const el = document.querySelector(id);
                        if (!el) return;
                        el.value = val;
                        el.dispatchEvent(new Event('input',  {bubbles: true}));
                        el.dispatchEvent(new Event('change', {bubbles: true}));
                    };
                    setField('#OBKey__1634_1', startDate);
                    setField('#OBKey__1634_2', endDate);
                }""",
                [start_date_str, end_date_str],
            )
            logger.info(f"[Playwright][{doc_type}] Dates set: {start_date_str} → {end_date_str}")

            # 5. Submit search via JS
            await page.evaluate("document.querySelector('#sub').click()")
            logger.info(f"[Playwright][{doc_type}] Search submitted, waiting for results...")

            # 6. Wait for result indicators
            await page.wait_for_selector(
                '.jsgrid-pager, .alert-danger, #exportResults',
                timeout=45_000,
            )

            # 7. Check for error alerts
            error_el = await page.query_selector('.alert-danger')
            if error_el:
                error_text = (await error_el.inner_text()).strip()
                if 'exceeded the limit' in error_text:
                    raise OverflowError(f"[Playwright][{doc_type}] Results exceeded 6000 — needs day-by-day split")
                if 'No results found' in error_text:
                    logger.info(f"[Playwright][{doc_type}] No results found")
                    return None

            # 8. Check export button
            export_btn = page.locator('#exportResults')
            if not await export_btn.is_visible():
                logger.warning(f"[Playwright][{doc_type}] Export button not visible, retrying search...")
                await page.evaluate("document.querySelector('#sub').click()")
                await page.wait_for_timeout(5_000)
                if not await export_btn.is_visible():
                    raise RuntimeError(f"[Playwright][{doc_type}] Export button not visible after retry")

            # 9. Log result count
            pager_el = await page.query_selector('.jsgrid-pager')
            if pager_el:
                logger.info(f"[Playwright][{doc_type}] {(await pager_el.inner_text()).strip()}")

            # 10. Download
            logger.info(f"[Playwright][{doc_type}] Downloading...")
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            safe_name = doc_type.lower().replace(" ", "_").replace("/", "_")
            dest_path = RAW_LIEN_DIR / f"{safe_name}_{ts}.csv"
            async with page.expect_download(timeout=90_000) as dl_info:
                await export_btn.click()
            download = await dl_info.value
            await download.save_as(str(dest_path))
            size_kb = dest_path.stat().st_size / 1024
            logger.info(f"[Playwright][{doc_type}] Saved to {dest_path} ({size_kb:.1f} KB)")
            return dest_path

        finally:
            await context.close()
            await browser.close()


async def _playwright_download_combined(start_str: str, end_str: str, clerk_access_url: str = HILLSCLERK_PUBLIC_ACCESS_URL) -> Optional[pd.DataFrame]:
    """
    Single combined download: one Playwright session covers the full date range.
    Retries up to 5 times on error or no-results (temporary site issue).
    If the result count hits 6000, falls back to day-by-day downloads and merges them.
    Returns a DataFrame of all records, or None if nothing was found.
    """
    MAX_RETRIES = 5
    overflow = False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            path = await _playwright_download_document("Combined", start_str, end_str, clerk_access_url=clerk_access_url)
            if path is None:
                # No results from site — could be temporary; retry before giving up
                if attempt < MAX_RETRIES:
                    logger.warning(f"[Combined] No results (attempt {attempt}/{MAX_RETRIES}) — retrying in 5s...")
                    await asyncio.sleep(5)
                    continue
                logger.info(f"[Combined] No results after {MAX_RETRIES} attempts — none in this date range")
                return None
            df = process_lien_data(path)
            logger.info(f"[Combined] Downloaded {len(df)} records ({start_str} → {end_str})")
            return df
        except OverflowError:
            # >6000 results — day-by-day fallback (handled below, no retry needed)
            overflow = True
            break
        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"[Combined] Attempt {attempt}/{MAX_RETRIES} failed: {e} — retrying in 5s...")
                await asyncio.sleep(5)
                continue
            logger.error(f"[Combined] All {MAX_RETRIES} retries exhausted: {e}")
            return None

    if not overflow:
        return None

    # >6000 results — split into individual days and merge
    logger.warning(f"[Combined] 6000 limit hit — switching to day-by-day")
    start_dt = datetime.strptime(start_str, "%m/%d/%Y")
    end_dt   = datetime.strptime(end_str,   "%m/%d/%Y")
    total_days = (end_dt - start_dt).days
    frames = []
    for offset in range(total_days + 1):
        day = start_dt + timedelta(days=offset)
        day_str = day.strftime("%m/%d/%Y")
        logger.info(f"[Combined] Fetching single day: {day_str}")
        try:
            path = await _playwright_download_document(f"Combined-{day_str}", day_str, day_str, clerk_access_url=clerk_access_url)
            if path is not None:
                frames.append(process_lien_data(path))
        except OverflowError:
            logger.error(f"[Combined] Single day {day_str} still >6000 — skipping")
        except Exception as e:
            logger.warning(f"[Combined] Day {day_str} failed: {e}")

    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['Instrument'], keep='first')
    logger.info(f"[Combined] Day-by-day merge: {len(combined)} unique records")
    return combined


async def download_document_by_type(
    doc_type: str,
    doc_type_code: str,
    start_date_str: str,
    end_date_str: str,
    wait_after_download: int = 40,
    option_value: str = "",
    clerk_access_url: str = HILLSCLERK_PUBLIC_ACCESS_URL,
) -> Optional[Path]:
    """
    Download a specific document type from Hillsborough County Clerk.

    Tries Playwright first (deterministic, no AI credits). Retries up to 5 times on
    error or no-results. Falls back to the browser-use AI agent only if all Playwright
    retries fail with an actual error (not no-results).
    """
    # ── Playwright (primary, with retries) ────────────────────────────────────
    if option_value:
        logger.info(f"\n[{doc_type}] Attempting Playwright scraper (primary)...")
        MAX_RETRIES = 5
        playwright_error = None
        playwright_no_results = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = await _playwright_download_document(doc_type, start_date_str, end_date_str, clerk_access_url=clerk_access_url)
                if result is None:
                    # No results — retry (could be temporary)
                    if attempt < MAX_RETRIES:
                        logger.warning(f"[{doc_type}] No results (attempt {attempt}/{MAX_RETRIES}) — retrying in 5s...")
                        await asyncio.sleep(5)
                        continue
                    logger.warning(f"[{doc_type}] No results after {MAX_RETRIES} attempts — triggering AI fallback")
                    playwright_no_results = True
                    break
                logger.info(f"[{doc_type}] Playwright scraper succeeded")
                return result
            except Exception as e:
                playwright_error = e
                if attempt < MAX_RETRIES:
                    logger.warning(f"[{doc_type}] Playwright attempt {attempt}/{MAX_RETRIES} failed: {e} — retrying in 5s...")
                    await asyncio.sleep(5)
                    continue
                logger.warning(f"[{doc_type}] All Playwright retries failed: {e}")
        if playwright_error is None and not playwright_no_results:
            return None  # no-results that were confirmed by the site
        logger.info(f"[{doc_type}] Falling back to browser-use AI scraper...")

    # ── AI fallback (only reached on error, not on no-results) ────────────────
    logger.info(f"[{doc_type}] Running browser-use AI scraper...")

    try:
        RAW_LIEN_DIR.mkdir(parents=True, exist_ok=True)

        save_dir = os.path.abspath(str(RAW_LIEN_DIR))
        start_time = time.time()

        logger.info(f"[{doc_type}] Fetching records from {start_date_str} to {end_date_str}")
        
        # Load task prompt from YAML configuration
        try:
            task = get_prompt(
                "lien_prompts.yaml",
                "document_search.task_template",
                doc_type=doc_type,
                url=clerk_access_url,
                doc_type_code=doc_type_code,
                start_date_str=start_date_str,
                end_date_str=end_date_str,
                wait_after_download=wait_after_download
            )
        except Exception as e:
            logger.error(f"[{doc_type}] Failed to load prompt from YAML: {e}")
            raise
        
        logger.info(f"[{doc_type}] Launching browser agent to download documents")
        logger.debug(f"[{doc_type}] Download directory: {save_dir}")
        
        # REVERTED: Configure browser using direct kwargs for your older library version
        browser = Browser(
            headless=True,
            disable_security=True,
            downloads_path=save_dir,  # <-- FIX: Forces Playwright to download directly to your raw folder
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--no-first-run',
                '--no-zygote',
                '--single-process',
                '--disable-blink-features=AutomationControlled',
            ]
        )
        
        agent = Agent(
            task=task,
            llm=llm,
            browser=browser,
        )
        
        try:
            history = await agent.run(max_steps=30)
            
            if not history.is_done():
                logger.warning(f"[{doc_type}] Agent could not finish the workflow within step limit. Check browser logs.")
                return None
            
            logger.info(f"[{doc_type}] Agent workflow completed. Waiting for download to finalize...")
            await asyncio.sleep(wait_after_download)
            
            downloaded_file = None
            
            # SAFE CHECK: Try to get from history if your version supports it
            if hasattr(history, 'downloaded_files'):
                try:
                    downloaded_files = history.downloaded_files()
                    if downloaded_files:
                        downloaded_file = Path(downloaded_files[-1])
                        logger.info(f"[{doc_type}] File found via agent history: {downloaded_file}")
                except Exception as e:
                    logger.debug(f"Could not extract from history: {e}")
            
            # FALLBACK: If history method isn't available, rely on your robust locate function
            if not downloaded_file or not downloaded_file.exists():
                logger.info(f"[{doc_type}] Searching disk for downloaded file...")
                downloaded_file = _locate_download(start_time)
            
            if not downloaded_file or not downloaded_file.exists():
                logger.error(f"[{doc_type}] Could not detect the downloaded file after automation completed")
                return None
            
            # Create safe filename from doc_type
            start_dt_parsed = datetime.strptime(start_date_str, "%m/%d/%Y")
            end_dt_parsed   = datetime.strptime(end_date_str,   "%m/%d/%Y")
            safe_doc_name = doc_type.lower().replace(" ", "_").replace("/", "_")
            final_filename = f"{safe_doc_name}_{start_dt_parsed.strftime('%Y%m%d')}_{end_dt_parsed.strftime('%Y%m%d')}{downloaded_file.suffix}"
            dest_file = RAW_LIEN_DIR / final_filename
            
            # Move/Rename file to final destination
            if downloaded_file != dest_file:
                logger.info(f"[{doc_type}] Moving download to: {dest_file}")
                shutil.move(str(downloaded_file), str(dest_file))
                downloaded_file = dest_file
            
            file_size_kb = downloaded_file.stat().st_size / 1024
            logger.info(f"[{doc_type}] Downloaded file: {downloaded_file} (Size: {file_size_kb:.2f} KB)")
            
            return downloaded_file
            
        except Exception as e:
            logger.error(f"[{doc_type}] Browser agent execution failed: {e}")
            logger.debug(traceback.format_exc())
            return None
        finally:
            # Memory cleanup
            await _safe_close_browser(browser)
        
    except Exception as e:
        logger.error(f"[{doc_type}] Error during document download: {e}")
        logger.debug(traceback.format_exc())
        return None


async def download_lien_records(
    start_date_str: str,
    end_date_str: str,
    wait_after_download: int = 40,
) -> Optional[Path]:
    """
    Convenience wrapper to download general lien records.
    """
    return await download_document_by_type(
        doc_type="General Liens",
        doc_type_code="LIEN",
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        wait_after_download=wait_after_download,
    )


def process_lien_data(file_path: Path) -> pd.DataFrame:
    """
    Load and process the lien/judgment data file with multi-format and multi-encoding support.
    """
    logger.info(f"Loading lien/judgment data from: {file_path}")
    
    if not file_path.exists():
        logger.error(f"Lien/judgment data file not found: {file_path}")
        raise FileNotFoundError(f"Lien/judgment data file not found: {file_path}")
    
    df = None
    
    try:
        if file_path.suffix.lower() == ".csv":
            encodings_to_try = ["utf-8", "latin1", "cp1252"]
            for encoding in encodings_to_try:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"Successfully loaded CSV with {encoding} encoding")
                    break
                except (UnicodeDecodeError, pd.errors.ParserError) as e:
                    logger.debug(f"Failed to load with {encoding} encoding: {e}")
                    continue
            
            if df is None:
                error_msg = f"Could not read CSV with any standard encoding: {file_path}"
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        elif file_path.suffix.lower() in [".xls", ".xlsx"]:
            df = pd.read_excel(file_path)
            logger.info("Successfully loaded Excel file")
        
        else:
            error_msg = f"Unsupported file type: {file_path.suffix}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Loaded {len(df)} lien/judgment records")
        logger.debug(f"DataFrame columns: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing lien/judgment data: {e}")
        raise


def categorize_and_split_data(combined_df: pd.DataFrame) -> dict:
    """
    Route records to liens/deeds/judgments using the actual county DocType column.
    The county export returns all document types regardless of any search filter,
    so we filter here in Python using the DocType value from the CSV.
    """
    logger.info("Categorizing documents into liens, deeds, and judgments...")

    hoa_keywords = ['ASSOCIATION', 'HOA', 'CONDO', 'COMMUNITY', 'VILLAGE', 'TOWNHOME', 'PROPERTY OWNERS']
    irs_keywords = ['UNITED STATES', 'INTERNAL REVENUE', 'STATE OF FLORIDA', 'DEPARTMENT OF REVENUE']

    # DocType values the county puts in exported CSVs
    DEED_DOCTYPES     = {'(D) DEED', '(TAXDEED) TAX DEED', '(DPL) DEED PLAT'}
    JUDGMENT_DOCTYPES = {'(JUD) JUDGMENT', '(CCJ) CERTIFIED COPY OF A COURT JUDGMENT'}
    LIEN_DOCTYPES     = {'(LN) LIEN', '(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA'}
    LP_DOCTYPES       = {'(LP) LIS PENDENS', 'LIS PENDENS'}

    def categorize_record(row):
        doc_type = str(row.get('DocType', '') or '').strip()

        if doc_type in DEED_DOCTYPES:
            return 'Deeds'

        if doc_type in LP_DOCTYPES or 'LIS PENDENS' in doc_type.upper():
            return 'LIS PENDENS'

        if doc_type in JUDGMENT_DOCTYPES:
            # Judgments filed by City of Tampa are code enforcement liens, not civil judgments
            grantor = str(row.get('Grantor', '')).upper()
            grantee = str(row.get('Grantee', '')).upper()
            if 'CITY OF TAMPA' in grantor or 'CITY OF TAMPA' in grantee:
                return 'TAMPA CODE LIENS (TCL)'
            return doc_type  # preserve original for downstream logging

        if doc_type in LIEN_DOCTYPES:
            grantor = str(row.get('Grantor', '')).upper()
            grantee = str(row.get('Grantee', '')).upper()
            if doc_type == '(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA':
                return 'TAX LIENS (TL)'
            if any(k in grantor or k in grantee for k in hoa_keywords):
                return 'HOA LIENS (HL)'
            if 'CITY OF TAMPA' in grantee or 'CITY OF TAMPA' in grantor:
                return 'TAMPA CODE LIENS (TCL)'
            if 'HILLSBOROUGH COUNTY' in grantee or 'HILLSBOROUGH COUNTY' in grantor:
                return 'COUNTY CODE LIENS (CCL)'
            if any(k in grantor or k in grantee for k in irs_keywords):
                return 'TAX LIENS (TL)'
            return 'MECHANICS LIENS (ML)'

        return 'SKIP'  # everything else (mortgages, court papers, orders…) discarded

    combined_df['document_type'] = combined_df.apply(categorize_record, axis=1)

    lien_types     = ['HOA LIENS (HL)', 'TAMPA CODE LIENS (TCL)', 'COUNTY CODE LIENS (CCL)',
                      'TAX LIENS (TL)', 'MECHANICS LIENS (ML)', 'LIS PENDENS']
    deed_types     = ['Deeds']
    judgment_types = ['(JUD) JUDGMENT', '(CCJ) CERTIFIED COPY OF A COURT JUDGMENT']

    skipped = (combined_df['document_type'] == 'SKIP').sum()
    logger.info(f"Categorization: {len(combined_df) - skipped} kept, {skipped} discarded (non-relevant DocTypes)")
    
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_counts = {}
    
    today_str = datetime.now().strftime('%Y%m%d')

    # Save liens — isolated to PROCESSED_LIENS_DIR/new/
    liens_df = combined_df[combined_df['document_type'].isin(lien_types)]
    if not liens_df.empty:
        new_dir = PROCESSED_LIENS_DIR / "new"
        new_dir.mkdir(parents=True, exist_ok=True)
        liens_path = new_dir / f"all_liens_{today_str}.csv"
        liens_df.to_csv(liens_path, index=False)
        file_counts[liens_path.name] = len(liens_df)
        logger.info(f"Saved {len(liens_df)} lien records to {liens_path.name}")
        for doc_type in liens_df['document_type'].unique():
            count = len(liens_df[liens_df['document_type'] == doc_type])
            logger.info(f"  - {doc_type}: {count} records")

    # Save deeds — isolated to PROCESSED_DEEDS_DIR/new/
    deeds_df = combined_df[combined_df['document_type'].isin(deed_types)]
    if not deeds_df.empty:
        new_dir = PROCESSED_DEEDS_DIR / "new"
        new_dir.mkdir(parents=True, exist_ok=True)
        deeds_path = new_dir / f"all_deeds_{today_str}.csv"
        deeds_df.to_csv(deeds_path, index=False)
        file_counts[deeds_path.name] = len(deeds_df)
        logger.info(f"Saved {len(deeds_df)} deed records to {deeds_path.name}")
        for doc_type in deeds_df['document_type'].unique():
            count = len(deeds_df[deeds_df['document_type'] == doc_type])
            logger.info(f"  - {doc_type}: {count} records")

    # Save judgments — isolated to PROCESSED_JUDGMENTS_DIR/new/
    judgments_df = combined_df[combined_df['document_type'].isin(judgment_types)]
    if not judgments_df.empty:
        new_dir = PROCESSED_JUDGMENTS_DIR / "new"
        new_dir.mkdir(parents=True, exist_ok=True)
        judgments_path = new_dir / f"all_judgments_{today_str}.csv"
        judgments_df.to_csv(judgments_path, index=False)
        file_counts[judgments_path.name] = len(judgments_df)
        logger.info(f"Saved {len(judgments_df)} judgment records to {judgments_path.name}")
        for doc_type in judgments_df['document_type'].unique():
            count = len(judgments_df[judgments_df['document_type'] == doc_type])
            logger.info(f"  - {doc_type}: {count} records")
    
    return file_counts


def save_processed_liens(df: pd.DataFrame, output_filename: str = "lien_data.csv") -> Path:
    """
    Save processed lien/judgment data to the output directory with deduplication.
    """
    try:
        RAW_LIEN_DIR.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured lien directory exists: {RAW_LIEN_DIR}")
        
        temp_dir = RAW_LIEN_DIR / "temp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_file = temp_dir / "liens_temp.csv"
        
        df.to_csv(temp_file, index=False)
        logger.info(f"Saved {len(df)} records to temporary file: {temp_file}")
        
        try:
            if 'document_type' in df.columns and not df.empty:
                sample_doc_type = str(df['document_type'].iloc[0]).upper()
                if 'JUDGMENT' in sample_doc_type:
                    data_type = 'judgments'
                elif 'DEED' in sample_doc_type:
                    data_type = 'deeds'
                else:
                    data_type = 'liens'
            else:
                data_type = 'liens'  # default
            
            unique_keys = get_unique_keys_for_type(data_type)
            deduplicated_file = deduplicate_csv(
                new_csv_path=temp_file,
                destination_dir=RAW_LIEN_DIR,
                unique_key_columns=unique_keys,
                output_filename=output_filename,
                keep_original=False 
            )
            
            logger.info(f"Lien/judgment records saved to: {deduplicated_file}")
            return deduplicated_file
            
        except Exception as e:
            logger.error(f"Deduplication failed: {e}")
            logger.debug(traceback.format_exc())
            logger.warning("Falling back to non-deduplicated save")
            output_path = RAW_LIEN_DIR / output_filename
            df.to_csv(output_path, index=False)
            temp_file.unlink(missing_ok=True)
            return output_path
        
    except PermissionError as e:
        logger.error(f"Permission error while saving file: {e}")
        raise
    except IOError as e:
        logger.error(f"I/O error while saving file: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while saving processed data: {e}")
        raise


async def run_lien_pipeline(start_date: str = None, end_date: str = None, run_all: bool = False, mode: str = 'all', county_id: str = "hillsborough"):
    """
    Execute the lien and judgment data collection pipeline.
    """
    county_cfg = get_county_config(county_id)
    clerk_access_url = county_cfg["urls"]["clerk_access"]

    logger.info("=" * 60)
    logger.info(f"{county_cfg['display_name'].upper()} LIEN & JUDGMENT RECORDS - DATA COLLECTION")
    logger.info("=" * 60)

    # Compute date range (YYYY-MM-DD → MM/DD/YYYY for site forms)
    _today = datetime.now()
    _end_dt   = datetime.strptime(end_date,   "%Y-%m-%d") if end_date   else _today
    _start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else (_end_dt - timedelta(days=3))
    start_str = _start_dt.strftime("%m/%d/%Y")
    end_str   = _end_dt.strftime("%m/%d/%Y")
    logger.info(f"Date range: {start_str} → {end_str}")

    # County export ignores the DocType filter — every download returns all record types.
    # Map doc_code → the DocType value(s) that appear in the exported CSV's DocType column.
    # After download, rows not matching are dropped before any further processing.
    DOC_TYPE_FILTER = {
        "D":    ["(D) DEED"],
        "JUD":  ["(JUD) JUDGMENT"],
        "CCJ":  ["(CCJ) CERTIFIED COPY OF A COURT JUDGMENT"],
        "LIEN": ["(LN) LIEN"],
    }

    # (name, code, option_value)  — option_value is the exact HTML <option> value for Playwright
    MODE_CONFIGS = {
        'liens':     [("General Liens",       "LIEN",     "(LN) LIEN")],
        'deeds':     [("Deeds",               "D",        "(D) DEED")],
        'judgments': [("Judgments",           "JUD",      "(JUD) JUDGMENT"),
                      ("Certified Judgments", "CCJ",      "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT")],
        'all':       [("General Liens",       "LIEN",     "(LN) LIEN"),
                      ("Judgments",           "JUD",      "(JUD) JUDGMENT"),
                      ("Certified Judgments", "CCJ",      "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT"),
                      ("Deeds",               "D",        "(D) DEED")],
    }
    doc_configs = MODE_CONFIGS.get(mode, MODE_CONFIGS['all'])
    logger.info(f"Mode: {mode.upper()} - running {len(doc_configs)} document type(s): "
                f"{', '.join(n for n, _, __ in doc_configs)}")

    try:
        if mode == 'combined':
            logger.info("Execution mode: COMBINED — single Playwright download, Python-side DocType routing")
            combined_df = await _playwright_download_combined(start_str, end_str, clerk_access_url=clerk_access_url)
            if combined_df is None:
                logger.error("Combined download returned no data — check county site or date range")
                return
            category_counts = categorize_and_split_data(combined_df)
            logger.info("Cleaning up raw download directory...")
            for raw_file in RAW_LIEN_DIR.glob("*"):
                if raw_file.is_file():
                    try:
                        raw_file.unlink()
                        logger.debug(f"Deleted raw file: {raw_file.name}")
                    except Exception as e:
                        logger.warning(f"Could not delete raw file {raw_file.name}: {e}")
            logger.info("=" * 60)
            logger.info("COMBINED LIEN/DEED/JUDGMENT PIPELINE COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Total records downloaded: {len(combined_df)}")
            logger.info("Breakdown by category:")
            for category, count in category_counts.items():
                logger.info(f"  - {category}: {count} records")
            return

        if run_all:
            logger.info(f"Execution mode: PARALLEL ({len(doc_configs)} document types concurrently)")
            logger.warning("Parallel mode uses multiple API credits")

            tasks = [
                download_document_by_type(doc_name, doc_code, start_str, end_str, option_value=opt_val)
                for doc_name, doc_code, opt_val in doc_configs
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            all_dataframes = []
            successful_downloads = 0

            for idx, result in enumerate(results):
                doc_name = doc_configs[idx][0]
                
                if isinstance(result, Exception):
                    logger.error(f"{doc_name} failed with error: {result}")
                    continue
                
                if result is None:
                    logger.warning(f"{doc_name} download returned None")
                    continue
                
                logger.info(f"Processing {doc_name} data...")
                try:
                    df = process_lien_data(result)
                    # County export ignores the DocType filter — drop rows that don't match
                    doc_code = doc_configs[idx][1]
                    allowed = DOC_TYPE_FILTER.get(doc_code)
                    if allowed and 'DocType' in df.columns:
                        before = len(df)
                        df = df[df['DocType'].isin(allowed)]
                        logger.info(f"{doc_name} DocType filter: {before} → {len(df)} records (kept {allowed})")
                    df["document_type"] = doc_name
                    all_dataframes.append(df)
                    successful_downloads += 1
                    logger.info(f"{doc_name} processing complete: {len(df)} records")
                except Exception as e:
                    logger.error(f"Failed to process {doc_name}: {e}")
                    logger.debug(traceback.format_exc())
            
            if not all_dataframes:
                logger.error("No data was successfully downloaded and processed from any document type")
                return
            
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            category_counts = categorize_and_split_data(combined_df)
            
            logger.info("Cleaning up raw download directory...")
            for raw_file in RAW_LIEN_DIR.glob("*"):
                if raw_file.is_file():
                    try:
                        raw_file.unlink()
                        logger.debug(f"Deleted raw file: {raw_file.name}")
                    except Exception as e:
                        logger.warning(f"Could not delete raw file {raw_file.name}: {e}")
            
            logger.info("=" * 60)
            logger.info("LIEN & JUDGMENT PIPELINE COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Successful downloads: {successful_downloads}/{len(doc_configs)}")
            logger.info(f"Total records processed: {len(combined_df)}")
            logger.info("Final Breakdown by Source:")
            for category, count in category_counts.items():
                logger.info(f"  - {category}: {count} records")

        else:
            logger.info(f"Execution mode: SEQUENTIAL ({len(doc_configs)} document type(s) one at a time)")
            logger.info("Sequential mode conserves API credits - recommended for automated runs")

            all_dataframes = []
            successful_downloads = 0

            for idx, (doc_name, doc_code, opt_val) in enumerate(doc_configs, start=1):
                logger.info("=" * 60)
                logger.info(f"[{idx}/{len(doc_configs)}] Starting document type: {doc_name}")
                logger.info("=" * 60)

                result = await download_document_by_type(doc_name, doc_code, start_str, end_str, option_value=opt_val)
                
                if result is None:
                    logger.warning(f"{doc_name} download failed, continuing to next document type")
                    continue
                
                logger.info(f"Processing {doc_name} data...")
                try:
                    df = process_lien_data(result)
                    # County export ignores the DocType filter — drop rows that don't match
                    allowed = DOC_TYPE_FILTER.get(doc_code)
                    if allowed and 'DocType' in df.columns:
                        before = len(df)
                        df = df[df['DocType'].isin(allowed)]
                        logger.info(f"{doc_name} DocType filter: {before} → {len(df)} records (kept {allowed})")
                    df["document_type"] = doc_name
                    all_dataframes.append(df)
                    successful_downloads += 1
                    logger.info(f"{doc_name} completed successfully: {len(df)} records")
                except Exception as e:
                    logger.error(f"Failed to process {doc_name}: {e}")
                    logger.debug(traceback.format_exc())
            
            if not all_dataframes:
                logger.error("No data was successfully downloaded and processed from any document type")
                return
            
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            category_counts = categorize_and_split_data(combined_df)
            
            logger.info("Cleaning up raw download directory...")
            for raw_file in RAW_LIEN_DIR.glob("*"):
                if raw_file.is_file():
                    try:
                        raw_file.unlink()
                        logger.debug(f"Deleted raw file: {raw_file.name}")
                    except Exception as e:
                        logger.warning(f"Could not delete raw file {raw_file.name}: {e}")
            
            logger.info("=" * 60)
            logger.info("LIEN & JUDGMENT PIPELINE COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Successful downloads: {successful_downloads}/{len(doc_configs)}")
            logger.info(f"Total records processed: {len(combined_df)}")
            logger.info("Final Breakdown by Source:")
            for category, count in category_counts.items():
                logger.info(f"  - {category}: {count} records")
    except Exception as e:
        logger.error(f"Lien pipeline failed with error: {e}")
        logger.debug(traceback.format_exc())
        raise


if __name__ == "__main__":
    import sys
    import argparse
    from src.utils.scraper_db_helper import load_scraped_data_to_db, add_load_to_db_arg
    
    parser = argparse.ArgumentParser(description="Scrape Hillsborough County liens, deeds, and judgments")
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format (default: 3 days ago). Example: 2026-02-28"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format (default: today). Example: 2026-03-03"
    )
    parser.add_argument(
        "--mode",
        choices=["liens", "deeds", "judgments", "all", "combined"],
        default="combined",
        help="Which document types to scrape: liens, deeds, judgments, all, or combined (default: combined — single download, Python-side routing)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run selected document types in parallel (faster but uses more API credits)"
    )
    add_load_to_db_arg(parser)
    parser.add_argument(
        "--county-id",
        dest="county_id",
        default="hillsborough",
        help="County identifier (default: hillsborough)",
    )

    args = parser.parse_args()

    # Maps each mode to its isolated processed directory and DB data type
    MODE_DIR_MAP = {
        'liens':     (PROCESSED_LIENS_DIR,     'liens'),
        'deeds':     (PROCESSED_DEEDS_DIR,     'deeds'),
        'judgments': (PROCESSED_JUDGMENTS_DIR, 'judgments'),
    }
    # Modes that populate all three output directories
    MULTI_OUTPUT_MODES = {'all', 'combined'}

    try:
        asyncio.run(run_lien_pipeline(start_date=args.start_date, end_date=args.end_date, run_all=args.all, mode=args.mode, county_id=args.county_id))

        if args.load_to_db:
            load_modes = list(MODE_DIR_MAP.keys()) if args.mode in MULTI_OUTPUT_MODES else [args.mode]
            any_loaded = False
            for load_mode in load_modes:
                type_dir, data_type = MODE_DIR_MAP[load_mode]
                new_dir = type_dir / "new"
                csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
                if csv_files:
                    csv_to_load = csv_files[0]
                    logger.info(f"Loading {load_mode} to database: {csv_to_load}")
                    load_scraped_data_to_db(data_type, csv_to_load, destination_dir=type_dir)
                    any_loaded = True
                else:
                    logger.warning(f"No {load_mode} CSV found in {new_dir}, skipping")
            if not any_loaded:
                logger.warning("No new records to load for any requested type - nothing new today")
                sys.exit(0)

        sys.exit(0)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)