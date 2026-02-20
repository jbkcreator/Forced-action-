"""
Foreclosure Auction Data Collection Pipeline

This module automates the scraping of foreclosure auction records from the
Hillsborough County RealForeclose auction calendar. It uses browser automation
via browser_use and Claude Sonnet 4.5 to navigate the calendar, extract detailed
case information, and compile comprehensive auction datasets.

The pipeline performs the following steps:
    1. Launches a browser agent to navigate to the RealForeclose calendar for a specific date
    2. Iterates through each auction case, clicking into detail pages
    3. Extracts all case fields (status, plaintiff, defendant, address, amounts, raw data)
    4. Compiles data into a structured CSV file

Author: Distressed Property Intelligence Platform
"""

import argparse
import asyncio
import datetime as dt
import shutil
import time
import traceback
from pathlib import Path
from typing import Optional

from browser_use import Agent, ChatAnthropic

from config.settings import settings
from config.constants import (
	REALFORECLOSE_BASE_URL,
	AUCTION_DATE_FORMAT,
	RAW_FORECLOSURE_DIR,
	PROCESSED_DATA_DIR,
	DOWNLOAD_FILE_PATTERNS,
	TEMP_DOWNLOADS_DIR,
	BROWSER_DOWNLOAD_TEMP_PATTERN,
	BROWSER_MODEL,
	BROWSER_TEMPERATURE,
	OUTPUT_SEPARATOR,
)
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt

# Initialize logging
setup_logging()
logger = get_logger(__name__)

# LLM configuration for browser automation
llm = ChatAnthropic(
    model=BROWSER_MODEL,
    timeout=180,
    api_key=settings.anthropic_api_key.get_secret_value(),
    temperature=BROWSER_TEMPERATURE,
)

RAW_FORECLOSURE_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def build_preview_url(auction_date: dt.date) -> str:
    """
    Build the RealForeclose calendar preview URL for a given auction date.
    
    Args:
        auction_date: Date object representing the auction date
        
    Returns:
        str: Fully constructed URL for the auction calendar preview
        
    Example:
        >>> url = build_preview_url(dt.date(2026, 2, 19))
        >>> print(url)
        https://www.hillsborough.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=02/19/2026
    """
    date_str = auction_date.strftime(AUCTION_DATE_FORMAT)
    return f"{REALFORECLOSE_BASE_URL}?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"


def _locate_recent_download(start_time: float) -> Optional[Path]:
    """
    Search for recently downloaded files after the specified start time.
    
    This function searches the processed directory, raw directory, and browser-use
    temporary directories for files matching download patterns that were created
    after the specified timestamp.
    
    Args:
        start_time: Unix timestamp representing when the download started
        
    Returns:
        Optional[Path]: Path to the most recently modified downloaded file, or None if not found
        
    Note:
        Searches for files matching patterns: *.csv, *.json, *.xls, *.xlsx
    """
    
    def scan(folder: Path):
        """Find files in folder that match download patterns and were created after start_time."""
        found = []
        if not folder.exists():
            return found
        for pattern in DOWNLOAD_FILE_PATTERNS:
            for candidate in folder.glob(pattern):
                try:
                    if candidate.stat().st_mtime >= start_time:
                        found.append(candidate)
                except FileNotFoundError:
                    logger.debug(f"File disappeared during check: {candidate}")
                    continue
        return found
    
    candidates = scan(PROCESSED_DATA_DIR)
    candidates.extend(scan(RAW_FORECLOSURE_DIR))
    logger.debug(f"Found {len(candidates)} candidate files in data directories")
    
    temp_base = TEMP_DOWNLOADS_DIR
    if temp_base.exists():
        for download_dir in temp_base.glob(BROWSER_DOWNLOAD_TEMP_PATTERN):
            temp_candidates = scan(download_dir)
            candidates.extend(temp_candidates)
            logger.debug(f"Found {len(temp_candidates)} candidate files in {download_dir}")
    
    if not candidates:
        logger.warning("No recent download files found")
        return None
    
    # Return the most recently modified file
    most_recent = max(candidates, key=lambda path: path.stat().st_mtime)
    logger.debug(f"Selected most recent file: {most_recent}")
    return most_recent


async def scrape_realforeclose_calendar(
    auction_date: dt.date,
    wait_after_scrape: int = 10,
) -> Optional[Path]:
    """
    Scrape RealForeclose auction calendar with detailed case information.
    
    This function launches a browser agent powered by Claude Sonnet 4.5 to navigate
    the RealForeclose auction calendar for a specific date, iterate through each
    auction case to extract detailed information, and compile the data into a CSV file.
    
    Args:
        auction_date: Date object representing the auction date to scrape
        wait_after_scrape: Seconds to wait for file write to complete (default: 10)
        
    Returns:
        Optional[Path]: Path to the saved CSV file if successful, None otherwise
        
    Raises:
        Exception: Logs and returns None on browser automation or file operation failures
        
    Example:
        >>> result = await scrape_realforeclose_calendar(
        ...     auction_date=dt.date(2026, 2, 19),
        ...     wait_after_scrape=10
        ... )
        >>> if result:
        ...     print(f"Data saved to: {result}")
    """
    
    try:
        start_time = time.time()
        preview_url = build_preview_url(auction_date)
        iso_date = auction_date.strftime("%Y-%m-%d")
        dest_file = PROCESSED_DATA_DIR / f"hillsborough_realforeclose_{auction_date:%Y%m%d}.csv"
        
        # Load task prompt from YAML configuration
        try:
            task_instructions = get_prompt(
                "foreclosure_prompts.yaml",
                "auction_scrape.task_template",
                preview_url=preview_url,
                dest_file=str(dest_file.resolve())
            )
        except Exception as e:
            logger.error(f"Failed to load prompt from YAML: {e}")
            raise
        
        logger.info(f"Launching browser agent to scrape: {preview_url}")
        logger.info(f"Target auction date: {iso_date}")
        logger.debug(f"Output destination: {dest_file}")
        
        agent = Agent(
            task=task_instructions,
            llm=llm,
            max_steps=50,
            browser_context_config={
                "headless": True,
                "save_downloads_path": str(PROCESSED_DATA_DIR.resolve()),
            },
        )
        
        try:
            history = await agent.run()
            
            if not history.is_done():
                logger.warning("Agent could not complete the scraping workflow within 50 steps")
                logger.warning("The page may be unresponsive or the task is too complex")
                return None
            
            logger.info("Agent workflow completed. Waiting for file write...")
            await asyncio.sleep(wait_after_scrape)
            
        except Exception as e:
            logger.error(f"Browser agent execution failed: {e}")
            logger.debug(traceback.format_exc())
            return None
        
        # Check if the expected file exists
        if dest_file.exists():
            file_size_kb = dest_file.stat().st_size / 1024
            logger.info(f"Auction data saved to {dest_file} ({file_size_kb:.1f} KB)")
            return dest_file
        
        # Fallback: look for any recent download
        logger.debug("Expected file not found, searching for alternative downloads")
        downloaded = _locate_recent_download(start_time)
        if downloaded and downloaded.exists():
            logger.info(f"Found alternative download: {downloaded}")
            if downloaded != dest_file:
                shutil.copy(str(downloaded), str(dest_file))
                logger.info(f"Copied to {dest_file}")
            return dest_file
        
        logger.error("Could not locate the scraped data file")
        return None
        
    except Exception as e:
        logger.error(f"Error during foreclosure auction scraping: {e}")
        logger.debug(traceback.format_exc())
        return None


def _parse_auction_date(date_str: Optional[str]) -> dt.date:
    """
    Parse auction date string or default to today.
    
    Args:
        date_str: Date string in YYYY-MM-DD format, or None for today's date
        
    Returns:
        dt.date: Parsed date object
        
    Raises:
        ValueError: If date_str is provided but cannot be parsed
        
    Example:
        >>> date = _parse_auction_date("2026-02-19")
        >>> print(date)
        2026-02-19
        >>> today = _parse_auction_date(None)  # Uses current date
    """
    if date_str:
        try:
            return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError as e:
            logger.error(f"Invalid date format '{date_str}': {e}")
            raise
    return dt.date.today()


def main():
    """
    CLI entrypoint for RealForeclose auction scraping.
    
    This function provides a command-line interface for scraping foreclosure auction
    data from the Hillsborough County RealForeclose calendar. It accepts optional
    arguments for the auction date and wait time.
    
    Command-line arguments:
        --date: Auction date in YYYY-MM-DD format (defaults to today)
        --wait: Seconds to wait after agent completes scraping (default: 10)
        
    Example:
        >>> python -m src.scrappers.foreclosures.foreclosure_engine --date 2026-02-19 --wait 15
    """
    parser = argparse.ArgumentParser(
        description="Scrape Hillsborough RealForeclose auction calendar"
    )
    parser.add_argument(
        "--date",
        help="Auction date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=10,
        help="Seconds to wait after agent completes scraping.",
    )
    args = parser.parse_args()
    
    try:
        auction_date = _parse_auction_date(args.date)
        logger.info(f"Starting foreclosure auction scraper for date: {auction_date.strftime('%Y-%m-%d')}")
        
        result_file = asyncio.run(
            scrape_realforeclose_calendar(
                auction_date=auction_date,
                wait_after_scrape=args.wait,
            )
        )
        
        if not result_file:
            logger.error("Scraping workflow did not produce output")
            return
        
        logger.info(f"Foreclosure auction data collected for {auction_date.strftime('%Y-%m-%d')}")
        logger.info(f"Output file: {result_file}")
        
    except Exception as e:
        logger.error(f"Foreclosure scraping failed: {e}")
        logger.debug(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()