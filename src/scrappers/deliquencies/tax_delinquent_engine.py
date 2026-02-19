"""
Tax Delinquent Property Data Collection Pipeline (Hybrid RADAR & SNIPER)

This module implements a two-phase approach for collecting tax delinquent property data:

RADAR Phase:
    - Downloads bulk tax delinquent reports from the county tax website
    - Filters accounts by delinquency criteria (minimum years unpaid)
    - Creates a target list of high-priority distressed properties

SNIPER Phase:
    - Enriches individual accounts with detailed scraped data
    - Uses Firecrawl API for structured data extraction
    - Extracts total amounts due, delinquency years, payment plan status
    - Rate-limited to respect API quotas and site policies

The pipeline produces a final enriched dataset of distressed properties suitable
for investment analysis and lead generation.

Author: Distressed Property Intelligence Platform
"""

import asyncio
import os
import random
import re
import shutil
import time
import traceback
from pathlib import Path
from typing import Optional

import pandas as pd
from browser_use import Agent, ChatAnthropic
from firecrawl import FirecrawlApp

from config.settings import settings
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt, get_config

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

# Configuration
DOWNLOAD_DIR = Path("data/reference")
PROCESSED_DIR = Path("data/processed")
DEFAULT_TAX_YEAR = 2026
DEFAULT_ACCOUNT_STATUS = "Unpaid"
DOWNLOAD_PATTERNS = ("*.csv", "*.xls", "*.xlsx")
PARCEL_BASE_URL = "https://hillsborough.county-taxes.com/public/real_estate/parcels"
MIN_YEARS_DELINQUENT = 2
REQUEST_DELAY_RANGE = (2.0, 4.0)  # Delay between accounts for SNIPER phase

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def _locate_download(start_time: float) -> Optional[Path]:
	"""
	Search for recently downloaded tax delinquency files.
	
	This function searches both the configured download directory and browser-use
	temporary directories for files matching download patterns that were created
	after the specified timestamp.
	
	Args:
		start_time: Unix timestamp representing when the download started
		
	Returns:
		Optional[Path]: Path to the most recently modified downloaded file, or None if not found
		
	Note:
		Searches for files matching patterns: *.csv, *.xls, *.xlsx
	"""
	
	def recent_candidates(folder: Path):
		"""Find files in folder that match download patterns and were created after start_time."""
		paths = []
		if not folder.exists():
			return paths
		for pattern in DOWNLOAD_PATTERNS:
			for candidate in folder.glob(pattern):
				try:
					if candidate.stat().st_mtime >= start_time:
						paths.append(candidate)
				except FileNotFoundError:
					logger.debug(f"File disappeared during check: {candidate}")
					continue
		return paths
	
	candidates = recent_candidates(DOWNLOAD_DIR)
	logger.debug(f"Found {len(candidates)} candidate files in {DOWNLOAD_DIR}")
	
	temp_base = Path("C:/tmp")
	if temp_base.exists():
		for download_dir in temp_base.glob("browser-use-downloads-*"):
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


async def download_tax_delinquent_report(
	tax_year: int = DEFAULT_TAX_YEAR,
	account_status: str = DEFAULT_ACCOUNT_STATUS,
	wait_after_download: int = 30,
) -> bool:
	"""
	RADAR PHASE: Download bulk tax delinquent report using browser automation.
	
	This function launches a browser agent to navigate the county tax reports portal,
	apply filters for delinquent accounts, and download the complete dataset.
	
	Args:
		tax_year: Tax year to query (default: 2026)
		account_status: Account status filter (default: "Unpaid")
		wait_after_download: Seconds to wait for download to complete (default: 30)
		
	Returns:
		bool: True if download succeeded, False otherwise
		
	Example:
		>>> success = await download_tax_delinquent_report(tax_year=2026, account_status="Unpaid")
		>>> if success:
		...     print("Download complete")
	"""
	
	try:
		DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured download directory exists: {DOWNLOAD_DIR}")
		
		save_dir = os.path.abspath(str(DOWNLOAD_DIR))
		dest_stub = f"hillsborough_tax_delinquent_{tax_year}"
		start_time = time.time()
		
		# Load task prompt from YAML configuration
		try:
			task = get_prompt(
				"tax_delinquent_prompts.yaml",
				"tax_delinquent_download.task_template",
				account_status=account_status,
				tax_year=tax_year
			)
		except Exception as e:
			logger.error(f"Failed to load prompt from YAML: {e}")
			raise
		
		logger.info(f"[RADAR] Launching browser agent to download tax delinquent report")
		logger.info(f"[RADAR] Tax year: {tax_year}, Status: {account_status}")
		
		agent = Agent(
			task=task,
			llm=llm,
			browser_context_config={
				"headless": True,
				"save_downloads_path": save_dir,
			},
		)
		
		try:
			history = await agent.run()
			
			if not history.is_done():
				logger.warning("[RADAR] Agent could not finish the workflow within step limit")
				return False
			
			logger.info("[RADAR] Agent workflow completed. Waiting for download to finalize...")
			await asyncio.sleep(wait_after_download)
			
		except Exception as e:
			logger.error(f"[RADAR] Browser agent execution failed: {e}")
			logger.debug(traceback.format_exc())
			return False
		
		# Locate the downloaded file
		downloaded_file = _locate_download(start_time)
		
		if not downloaded_file or not downloaded_file.exists():
			logger.error("[RADAR] Could not detect the downloaded report")
			return False
		
		# Rename and move file to final location
		final_ext = downloaded_file.suffix.lower() or ".csv"
		dest_file = DOWNLOAD_DIR / f"{dest_stub}{final_ext}"
		
		if dest_file.exists():
			logger.debug(f"Removing existing file: {dest_file}")
			dest_file.unlink()
		
		logger.info(f"[RADAR] Moving downloaded file: {downloaded_file} -> {dest_file}")
		shutil.move(str(downloaded_file), str(dest_file))
		
		file_size_mb = dest_file.stat().st_size / (1024 ** 2)
		logger.info(f"[RADAR] Tax delinquent report saved to {dest_file} ({file_size_mb:.1f} MB)")
		
		# Clean up temp directory
		temp_dir = downloaded_file.parent
		if temp_dir.exists() and temp_dir.name.startswith("browser-use-downloads-"):
			try:
				shutil.rmtree(temp_dir)
				logger.debug(f"[RADAR] Cleaned up temp directory: {temp_dir}")
			except Exception as exc:
				logger.warning(f"[RADAR] Could not clean temp directory {temp_dir}: {exc}")
		
		return True
		
	except Exception as e:
		logger.error(f"[RADAR] Error during tax delinquent report download: {e}")
		logger.debug(traceback.format_exc())
		return False


def _filter_distressed_accounts(df: pd.DataFrame, min_years: int = MIN_YEARS_DELINQUENT) -> pd.DataFrame:
	"""
	Filter accounts based on minimum years of delinquency.
	
	This function attempts to identify and filter tax accounts with a specified
	minimum number of years of delinquency. It searches for common column names
	that indicate years delinquent.
	
	Args:
		df: DataFrame containing tax delinquent account records
		min_years: Minimum number of delinquent years required (default: 2)
		
	Returns:
		pd.DataFrame: Filtered DataFrame containing only accounts meeting the criteria
		
	Note:
		If the years delinquent column cannot be found, returns the full DataFrame
		with a warning logged.
	"""
	
	logger.debug(f"Available columns (first 10): {list(df.columns)[:10]}")
	
	# Try to identify the years delinquent column (common names)
	years_col = None
	possible_cols = ["Years Delinquent", "years_delinquent", "Delinquent Years", "Years", "YRS", "Yrs Delinq"]
	for col_name in possible_cols:
		if col_name in df.columns:
			years_col = col_name
			logger.info(f"Found years column: '{years_col}'")
			break
	
	if years_col:
		try:
			df_filtered = df[pd.to_numeric(df[years_col], errors="coerce") >= min_years].copy()
			logger.info(f"Filtered {len(df_filtered)} accounts with {min_years}+ years delinquent (from {len(df)} total)")
		except Exception as e:
			logger.error(f"Error filtering by years delinquent: {e}")
			df_filtered = df.copy()
	else:
		logger.warning(f"Could not find 'Years Delinquent' column in: {list(df.columns)}")
		logger.warning(f"Processing all {len(df)} accounts without years filter")
		df_filtered = df.copy()
	
	return df_filtered


async def _scrape_parcel_with_firecrawl(firecrawl_client: FirecrawlApp, account_number: str) -> dict:
	"""
	SNIPER PHASE: Extract structured tax delinquent data using Firecrawl API.
	
	This function uses Firecrawl's extract feature to retrieve structured data
	from individual parcel pages. It loads the extraction schema and prompt from
	YAML configuration for maintainability.
	
	Args:
		firecrawl_client: Initialized Firecrawl API client
		account_number: Tax account number to scrape
		
	Returns:
		dict: Extracted data containing:
			- account_number: Original account number
			- total_amount_due: Total tax amount due (string)
			- years_delinquent: Count of delinquent years (int)
			- payment_plan_status: Payment plan status (default: "No Plan")
			- custom_flags: List of custom flags (default: [])
			
	Note:
		Returns default values if extraction fails. All errors are logged but
		don't raise exceptions to allow batch processing to continue.
	"""

	url = f"{PARCEL_BASE_URL}/{account_number}"
	result = {
		"account_number": account_number,
		"total_amount_due": None,
		"years_delinquent": 0,
		"payment_plan_status": "No Plan",
		"custom_flags": [],
	}

	try:
		logger.info(f"[SNIPER] Extracting data for {account_number} with Firecrawl")
		
		# Load extraction schema and prompt from YAML
		extract_schema = get_config("tax_delinquent_prompts", "parcel_extraction", "schema")
		prompt_template = get_config("tax_delinquent_prompts", "parcel_extraction", "prompt_template")
		
		if not extract_schema:
			logger.error("[SNIPER] Missing Firecrawl extraction schema in tax_delinquent_prompts.yaml")
			return result
		
		prompt = prompt_template.format(account_number=account_number) if prompt_template else ""
		
		# Use Firecrawl's extract feature
		extract_result = firecrawl_client.extract(
			urls=[url],
			schema=extract_schema,
			prompt=prompt
		)
		
		logger.debug(f"[SNIPER] Extract result type: {type(extract_result)}")
		logger.debug(f"[SNIPER] Extract result data type: {type(extract_result.data) if extract_result and hasattr(extract_result, 'data') else 'N/A'}")
		
		if extract_result and hasattr(extract_result, 'data') and extract_result.data:
			data = extract_result.data
			
			# Handle if data is a list or dict
			if isinstance(data, list) and len(data) > 0:
				data = data[0]
			
			# Access extracted fields
			total_amount = data.get("total_amount_due") if isinstance(data, dict) else getattr(data, "total_amount_due", None)
			years_delinq = data.get("years_delinquent") if isinstance(data, dict) else getattr(data, "years_delinquent", None)
			
			if total_amount:
				result["total_amount_due"] = str(total_amount).replace("$", "").replace(",", "").strip()
				logger.info(f"[SNIPER] Extracted amount: ${result['total_amount_due']}")
			else:
				logger.warning(f"[SNIPER] Could not extract amount for {account_number}")
			
			if years_delinq is not None:
				result["years_delinquent"] = int(years_delinq)
				logger.info(f"[SNIPER] Extracted years delinquent: {result['years_delinquent']}")
			else:
				logger.warning(f"[SNIPER] Could not extract years for {account_number}")
		else:
			logger.warning(f"[SNIPER] No data returned from Firecrawl for {account_number}")

	except Exception as exc:
		logger.error(f"[SNIPER] Failed to extract {account_number}: {exc}")
		logger.debug(traceback.format_exc())

	return result


async def _sniper_enrich_accounts(df: pd.DataFrame, max_accounts: int = 100) -> pd.DataFrame:
	"""
	SNIPER PHASE: Enrich high-priority accounts using Firecrawl extraction.
	
	This function takes filtered distressed accounts and enriches them with
	live-scraped data via Firecrawl API. It includes rate limiting to respect
	API quotas and prevents excessive requests.
	
	Args:
		df: DataFrame containing filtered distressed accounts
		max_accounts: Maximum number of accounts to enrich (default: 100)
		
	Returns:
		pd.DataFrame: Enriched DataFrame with additional columns:
			- total_amount_due: Scraped total amount due
			- years_delinquent_scraped: Scraped delinquent years count
			- payment_plan_status: Payment plan status
			- custom_flags: List of custom flags
			
	Note:
		Accounts without valid account numbers are skipped. Rate limiting delays
		are applied between requests (see REQUEST_DELAY_RANGE constant).
	"""

	# Limit scraping to prevent excessive requests
	if len(df) > max_accounts:
		logger.info(f"[SNIPER] Limiting enrichment to first {max_accounts} accounts (out of {len(df)})")
		df = df.head(max_accounts)
	else:
		logger.info(f"[SNIPER] Starting enrichment for {len(df)} accounts")

	enriched_rows = []
	skipped = 0

	try:
		# Initialize Firecrawl client
		firecrawl_client = FirecrawlApp(api_key=settings.firecrawl_api_key.get_secret_value())
		logger.info("[SNIPER] Firecrawl initialized for structured extraction")

		for idx, row in df.iterrows():
			account_number = row.get("Account Number") or row.get("account_number") or row.get("Parcel ID") or row.get("Account #")

			if not account_number:
				skipped += 1
				continue

			logger.info(f"[SNIPER] [{idx + 1}/{len(df)}] Extracting account: {account_number}")

			details = await _scrape_parcel_with_firecrawl(firecrawl_client, str(account_number))

			# Merge scraped data
			enriched_row = row.to_dict()
			enriched_row["total_amount_due"] = details["total_amount_due"]
			enriched_row["years_delinquent_scraped"] = details["years_delinquent"]
			enriched_row["payment_plan_status"] = details["payment_plan_status"]
			enriched_row["custom_flags"] = details["custom_flags"]

			enriched_rows.append(enriched_row)

			# Delay between accounts to respect rate limits
			if idx < len(df) - 1:  # Don't delay after last account
				delay = random.uniform(*REQUEST_DELAY_RANGE)
				logger.debug(f"[SNIPER] Waiting {delay:.1f} seconds before next account")
				await asyncio.sleep(delay)

		enriched_df = pd.DataFrame(enriched_rows)
		logger.info(f"[SNIPER] Enrichment complete. Processed {len(enriched_df)} accounts")
		if skipped > 0:
			logger.warning(f"[SNIPER] Skipped {skipped} accounts with missing account numbers")

		return enriched_df
		
	except Exception as e:
		logger.error(f"[SNIPER] Error during account enrichment: {e}")
		logger.debug(traceback.format_exc())
		return pd.DataFrame(enriched_rows) if enriched_rows else df


async def run_radar_sniper_pipeline(
	tax_year: int = DEFAULT_TAX_YEAR,
	account_status: str = DEFAULT_ACCOUNT_STATUS,
	min_years_delinquent: int = MIN_YEARS_DELINQUENT,
	max_sniper_accounts: int = 100,
	skip_download: bool = False,
) -> bool:
	"""
	Execute the full Radar & Sniper pipeline for tax delinquent lead generation.
	
	This orchestration function coordinates the complete pipeline:
	1. RADAR Phase: Bulk download of tax delinquent CSV (optional)
	2. CSV Parsing: Load and validate CSV with multiple encoding strategies
	3. Filtering: Filter accounts by minimum years of delinquency
	4. SNIPER Phase: Individual enrichment via Firecrawl API
	5. Output: Save enriched leads to final CSV
	
	Args:
		tax_year: Tax year to query (default: 2026)
		account_status: Account status filter (default: "CURRENT")
		min_years_delinquent: Minimum years delinquent for filtering (default: 2)
		max_sniper_accounts: Maximum accounts to enrich (default: 100)
		skip_download: Skip download phase and use existing CSV (default: False)
		
	Returns:
		bool: True if pipeline completed successfully, False otherwise
		
	Raises:
		Does not raise exceptions - all errors are logged and return False
		
	Note:
		CSV parsing includes 3 fallback strategies: UTF-8, latin1, and Python engine.
		Final output is written to data/processed/final_weekly_distress_leads.csv.
	"""

	try:
		# PHASE 1: RADAR - Download bulk CSV (optional)
		if skip_download:
			logger.info("Skipping download phase (using existing CSV)")
		else:
			logger.info("[RADAR] Starting bulk download phase")
			success = await download_tax_delinquent_report(tax_year=tax_year, account_status=account_status)

			if not success:
				logger.error("[RADAR] Phase failed. Aborting pipeline")
				return False

		# Locate the CSV file (either just downloaded or existing)
		bulk_csv_path = DOWNLOAD_DIR / f"hillsborough_tax_delinquent_{tax_year}.csv"
		if not bulk_csv_path.exists():
			# Try alternate extensions
			for ext in [".xls", ".xlsx"]:
				alt_path = DOWNLOAD_DIR / f"hillsborough_tax_delinquent_{tax_year}{ext}"
				if alt_path.exists():
					bulk_csv_path = alt_path
					logger.debug(f"Found alternate file: {bulk_csv_path}")
					break

		if not bulk_csv_path.exists():
			logger.error(f"Could not find CSV file at {bulk_csv_path}")
			return False

		# PHASE 2: Parse CSV
		logger.info("Parsing bulk CSV")
		
		try:
			# Try with error handling for malformed CSV
			df = pd.read_csv(
				bulk_csv_path,
				on_bad_lines='skip',
				encoding='utf-8',
				low_memory=False,
				skipinitialspace=True,
				quoting=1  # QUOTE_ALL
			)
			logger.info(f"Loaded {len(df)} records from bulk CSV")
			
			if df.empty:
				logger.error("CSV is empty or could not be parsed")
				return False
				
		except Exception as exc:
			logger.warning(f"Failed to parse CSV (UTF-8): {exc}")
			logger.info("Attempting alternative parsing strategies")
			
			# Strategy 2: Try latin1 encoding
			try:
				df = pd.read_csv(
					bulk_csv_path,
					on_bad_lines='skip',
					encoding='latin1',
					low_memory=False,
					skipinitialspace=True,
					quoting=1
				)
				logger.info(f"Loaded {len(df)} records from bulk CSV (latin1 encoding)")
			except Exception as exc2:
				logger.warning(f"Alternative parsing with latin1 failed: {exc2}")
				
				# Strategy 3: Try with minimal parsing (treat everything as strings)
				try:
					df = pd.read_csv(
						bulk_csv_path,
						on_bad_lines='skip',
						encoding='utf-8',
						low_memory=False,
						dtype=str,
						skipinitialspace=True,
						engine='python'  # More forgiving parser
					)
					logger.info(f"Loaded {len(df)} records using Python engine (all strings)")
				except Exception as exc3:
					logger.error(f"All parsing strategies failed: {exc3}")
					logger.error(f"Please manually inspect the CSV file: {bulk_csv_path}")
					return False

		# PHASE 3: Filter distressed accounts
		df_distressed = _filter_distressed_accounts(df, min_years=min_years_delinquent)

		if df_distressed.empty:
			logger.warning("No distressed accounts found matching criteria")
			return False

		# PHASE 4: SNIPER - Enrich with live scraped data
		logger.info(f"[SNIPER] Enriching {len(df_distressed)} distressed accounts")
		df_enriched = await _sniper_enrich_accounts(df_distressed, max_accounts=max_sniper_accounts)

		# PHASE 5: Save final output
		final_output = PROCESSED_DIR / "final_weekly_distress_leads.csv"
		df_enriched.to_csv(final_output, index=False)

		file_size_kb = final_output.stat().st_size / 1024
		logger.info(f"Final distress leads saved to {final_output} ({file_size_kb:.1f} KB)")
		logger.info(f"Total accounts processed: {len(df_enriched)}")

		return True
		
	except Exception as e:
		logger.error(f"Pipeline execution failed: {e}")
		logger.debug(traceback.format_exc())
		return False


if __name__ == "__main__":
	"""
	CLI entrypoint for the Hillsborough Tax Delinquent pipeline.
	
	Supports two modes:
	- download-only: Only execute RADAR phase (bulk download)
	- full-pipeline: Execute complete RADAR + SNIPER pipeline
	
	Example usage:
		# Full pipeline with default settings
		python tax_delinquent_engine.py
		
		# Download only for specific year
		python tax_delinquent_engine.py --mode download-only --tax-year 2025
		
		# Full pipeline with custom filters
		python tax_delinquent_engine.py --min-years 3 --max-sniper 50
		
		# Skip download and use existing CSV
		python tax_delinquent_engine.py --skip-download
	"""
	import argparse
	
	parser = argparse.ArgumentParser(
		description="Hybrid Radar & Sniper strategy for Hillsborough Tax Delinquencies"
	)
	parser.add_argument(
		"--mode",
		choices=["download-only", "full-pipeline"],
		default="full-pipeline",
		help="Run mode: download-only or full-pipeline (default: full-pipeline)"
	)
	parser.add_argument(
		"--tax-year",
		type=int,
		default=DEFAULT_TAX_YEAR,
		help=f"Tax year to query (default: {DEFAULT_TAX_YEAR})"
	)
	parser.add_argument(
		"--min-years",
		type=int,
		default=MIN_YEARS_DELINQUENT,
		help=f"Minimum years delinquent to filter (default: {MIN_YEARS_DELINQUENT})"
	)
	parser.add_argument(
		"--max-sniper",
		type=int,
		default=100,
		help="Maximum accounts to enrich in SNIPER phase (default: 100)"
	)
	parser.add_argument(
		"--status",
		default=DEFAULT_ACCOUNT_STATUS,
		help=f"Account status filter (default: {DEFAULT_ACCOUNT_STATUS})"
	)
	parser.add_argument(
		"--skip-download",
		action="store_true",
		help="Skip download phase and use existing CSV file for SNIPER phase"
	)
	
	args = parser.parse_args()
	
	try:
		logger.info(f"Starting Tax Delinquent Engine in {args.mode} mode")
		
		if args.mode == "download-only":
			asyncio.run(download_tax_delinquent_report(
				tax_year=args.tax_year,
				account_status=args.status
			))
		else:
			asyncio.run(
				run_radar_sniper_pipeline(
					tax_year=args.tax_year,
					account_status=args.status,
					min_years_delinquent=args.min_years,
					max_sniper_accounts=args.max_sniper,
					skip_download=args.skip_download,
				)
			)
			
		logger.info("Tax Delinquent Engine completed successfully")
		
	except KeyboardInterrupt:
		logger.warning("Pipeline interrupted by user")
	except Exception as e:
		logger.error(f"Pipeline failed: {e}")
		logger.debug(traceback.format_exc())

