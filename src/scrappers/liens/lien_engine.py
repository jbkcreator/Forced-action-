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
    LIEN_DOCUMENT_TYPES,
)
from src.core.database import get_db_context
from src.loaders.liens import LienLoader
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt, get_config
from src.utils.csv_deduplicator import deduplicate_csv, get_unique_keys_for_type
from src.utils.db_deduplicator import filter_new_records

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

async def download_document_by_type(
    doc_type: str,
    doc_type_code: str,
    lookback_days: int = 1,
    wait_after_download: int = 40,
) -> Optional[Path]:
    """
    Automate browser to download specific document type from Hillsborough County Clerk.
    
    This function launches a browser agent powered by Claude Sonnet 4.5 to navigate
    the county clerk's public access system, search for a specific document type within
    a date range, and download the results.
    """
    
    try:
        RAW_LIEN_DIR.mkdir(parents=True, exist_ok=True)
        
        save_dir = os.path.abspath(str(RAW_LIEN_DIR))
        start_time = time.time()
        
        # Calculate date range
        today = datetime.now()
        start_date = today - timedelta(days=lookback_days)
        
        # Format dates for the form (MM/DD/YYYY format)
        start_date_str = start_date.strftime("%m/%d/%Y")
        end_date_str = today.strftime("%m/%d/%Y")
        
        logger.info(f"[{doc_type}] Fetching records from {start_date_str} to {end_date_str} ({lookback_days} days)")
        
        # Load task prompt from YAML configuration
        try:
            task = get_prompt(
                "lien_prompts.yaml",
                "document_search.task_template",
                doc_type=doc_type,
                url=HILLSCLERK_PUBLIC_ACCESS_URL,
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
            safe_doc_name = doc_type.lower().replace(" ", "_").replace("/", "_")
            final_filename = f"{safe_doc_name}_{start_date.strftime('%Y%m%d')}_{today.strftime('%Y%m%d')}{downloaded_file.suffix}"
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
    lookback_days: int = 1,
    wait_after_download: int = 40,
) -> Optional[Path]:
    """
    Convenience wrapper to download general lien records.
    """
    return await download_document_by_type(
        doc_type="General Liens",
        doc_type_code="LIEN",
        lookback_days=lookback_days,
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
    Applies business logic to categorize and organize documents into 3 files.
    """
    logger.info("Categorizing documents into liens, deeds, and judgments...")
    
    hoa_keywords = ['ASSOCIATION', 'HOA', 'CONDO', 'COMMUNITY', 'VILLAGE', 'TOWNHOME', 'PROPERTY OWNERS']
    irs_keywords = ['UNITED STATES', 'INTERNAL REVENUE', 'STATE OF FLORIDA', 'DEPARTMENT OF REVENUE']
    
    def categorize_record(row):
        doc_type = row.get('document_type', '')
        
        if doc_type == "Corp Tax Liens":
            return "TAX LIENS (TL)"
        
        if doc_type == "General Liens":
            grantor = str(row.get('Grantor', '')).upper()
            grantee = str(row.get('Grantee', '')).upper()
            
            if any(k in grantor or k in grantee for k in hoa_keywords):
                return "HOA LIENS (HL)"
            if 'CITY OF TAMPA' in grantee or 'CITY OF TAMPA' in grantor:
                return "TAMPA CODE LIENS (TCL)"
            if 'HILLSBOROUGH COUNTY' in grantee or 'HILLSBOROUGH COUNTY' in grantor:
                return "COUNTY CODE LIENS (CCL)"
            if any(k in grantor or k in grantee for k in irs_keywords):
                return "TAX LIENS (TL)"
            
            return "MECHANICS LIENS (ML)"
        
        return doc_type
    
    combined_df['document_type'] = combined_df.apply(categorize_record, axis=1)
    
    lien_types = ["HOA LIENS (HL)", "TAMPA CODE LIENS (TCL)", "COUNTY CODE LIENS (CCL)", 
                  "TAX LIENS (TL)", "MECHANICS LIENS (ML)"]
    deed_types = ["Deeds", "Tax Deeds"]
    judgment_types = ["Judgments", "Certified Judgments"]
    
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    temp_dir = PROCESSED_DATA_DIR / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    file_counts = {}
    
    # Save liens with deduplication — isolated to PROCESSED_LIENS_DIR
    liens_df = combined_df[combined_df['document_type'].isin(lien_types)]
    if not liens_df.empty:
        temp_file = temp_dir / "liens_temp.csv"
        liens_df.to_csv(temp_file, index=False)
        try:
            unique_keys = get_unique_keys_for_type('liens')
            today = datetime.now().strftime('%Y%m%d')
            liens_path = deduplicate_csv(
                new_csv_path=temp_file,
                destination_dir=PROCESSED_LIENS_DIR,
                unique_key_columns=unique_keys,
                output_filename=f"all_liens_{today}.csv",
                keep_original=False
            )
            file_counts[liens_path.name] = len(pd.read_csv(liens_path))
            logger.info(f"Saved {file_counts[liens_path.name]} lien records to {liens_path.name}")
        except Exception as e:
            logger.error(f"Lien deduplication failed: {e}, saving without deduplication")
            liens_path = PROCESSED_LIENS_DIR / "all_liens.csv"
            liens_df.to_csv(liens_path, index=False)
            file_counts['all_liens.csv'] = len(liens_df)
            logger.info(f"Saved {len(liens_df)} lien records to {liens_path.name}")

        for doc_type in liens_df['document_type'].unique():
            count = len(liens_df[liens_df['document_type'] == doc_type])
            logger.info(f"  - {doc_type}: {count} records")

    # Save deeds with deduplication — isolated to PROCESSED_DEEDS_DIR
    deeds_df = combined_df[combined_df['document_type'].isin(deed_types)]
    if not deeds_df.empty:
        temp_file = temp_dir / "deeds_temp.csv"
        deeds_df.to_csv(temp_file, index=False)
        try:
            unique_keys = get_unique_keys_for_type('deeds')
            today = datetime.now().strftime('%Y%m%d')
            deeds_path = deduplicate_csv(
                new_csv_path=temp_file,
                destination_dir=PROCESSED_DEEDS_DIR,
                unique_key_columns=unique_keys,
                output_filename=f"all_deeds_{today}.csv",
                keep_original=False
            )
            file_counts[deeds_path.name] = len(pd.read_csv(deeds_path))
            logger.info(f"Saved {file_counts[deeds_path.name]} deed records to {deeds_path.name}")
        except Exception as e:
            logger.error(f"Deed deduplication failed: {e}, saving without deduplication")
            deeds_path = PROCESSED_DEEDS_DIR / "all_deeds.csv"
            deeds_df.to_csv(deeds_path, index=False)
            file_counts['all_deeds.csv'] = len(deeds_df)
            logger.info(f"Saved {len(deeds_df)} deed records to {deeds_path.name}")

        for doc_type in deeds_df['document_type'].unique():
            count = len(deeds_df[deeds_df['document_type'] == doc_type])
            logger.info(f"  - {doc_type}: {count} records")

    # Save judgments with deduplication — isolated to PROCESSED_JUDGMENTS_DIR
    judgments_df = combined_df[combined_df['document_type'].isin(judgment_types)]
    if not judgments_df.empty:
        temp_file = temp_dir / "judgments_temp.csv"
        judgments_df.to_csv(temp_file, index=False)
        try:
            unique_keys = get_unique_keys_for_type('judgments')
            today = datetime.now().strftime('%Y%m%d')
            judgments_path = deduplicate_csv(
                new_csv_path=temp_file,
                destination_dir=PROCESSED_JUDGMENTS_DIR,
                unique_key_columns=unique_keys,
                output_filename=f"all_judgments_{today}.csv",
                keep_original=False
            )
            file_counts[judgments_path.name] = len(pd.read_csv(judgments_path))
            logger.info(f"Saved {file_counts[judgments_path.name]} judgment records to {judgments_path.name}")
        except Exception as e:
            logger.error(f"Judgment deduplication failed: {e}, saving without deduplication")
            judgments_path = PROCESSED_JUDGMENTS_DIR / "all_judgments.csv"
            judgments_df.to_csv(judgments_path, index=False)
            file_counts['all_judgments.csv'] = len(judgments_df)
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


async def run_lien_pipeline(lookback_days: int = 1, run_all: bool = False, mode: str = 'all'):
    """
    Execute the lien and judgment data collection pipeline.
    """
    logger.info("=" * 60)
    logger.info("HILLSBOROUGH COUNTY LIEN & JUDGMENT RECORDS - DATA COLLECTION")
    logger.info("=" * 60)
    
    MODE_CONFIGS = {
        'liens':     [("General Liens", "LIEN")],
        'deeds':     [("Deeds", "D")],
        'judgments': [("Judgments", "JUD"), ("Certified Judgments", "CCJ")],
        'all':       [("General Liens", "LIEN"), ("Judgments", "JUD"),
                      ("Certified Judgments", "CCJ"), ("Deeds", "D")],
    }
    doc_configs = MODE_CONFIGS.get(mode, MODE_CONFIGS['all'])
    logger.info(f"Mode: {mode.upper()} - running {len(doc_configs)} document type(s): "
                f"{', '.join(n for n, _ in doc_configs)}")

    try:
        if run_all:
            logger.info(f"Execution mode: PARALLEL ({len(doc_configs)} document types concurrently)")
            logger.warning("Parallel mode uses multiple API credits")

            tasks = [
                download_document_by_type(doc_name, doc_code, lookback_days)
                for doc_name, doc_code in doc_configs
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

            for idx, (doc_name, doc_code) in enumerate(doc_configs, start=1):
                logger.info("=" * 60)
                logger.info(f"[{idx}/{len(doc_configs)}] Starting document type: {doc_name}")
                logger.info("=" * 60)
                
                result = await download_document_by_type(doc_name, doc_code, lookback_days)
                
                if result is None:
                    logger.warning(f"{doc_name} download failed, continuing to next document type")
                    continue
                
                logger.info(f"Processing {doc_name} data...")
                try:
                    df = process_lien_data(result)
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
        "--lookback",
        type=int,
        default=1,
        help="Number of days to look back (default: 1)"
    )
    parser.add_argument(
        "--mode",
        choices=["liens", "deeds", "judgments", "all"],
        default="all",
        help="Which document types to scrape: liens, deeds, judgments, or all (default: all)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run selected document types in parallel (faster but uses more API credits)"
    )
    add_load_to_db_arg(parser)

    args = parser.parse_args()

    # Maps each mode to its isolated processed directory and DB data type
    MODE_DIR_MAP = {
        'liens':     (PROCESSED_LIENS_DIR,     'liens'),
        'deeds':     (PROCESSED_DEEDS_DIR,     'deeds'),
        'judgments': (PROCESSED_JUDGMENTS_DIR, 'judgments'),
    }

    try:
        asyncio.run(run_lien_pipeline(lookback_days=args.lookback, run_all=args.all, mode=args.mode))

        if args.load_to_db:
            load_modes = list(MODE_DIR_MAP.keys()) if args.mode == 'all' else [args.mode]
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
                logger.error("No CSV files found to load for any requested type")
                sys.exit(1)

        sys.exit(0)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)