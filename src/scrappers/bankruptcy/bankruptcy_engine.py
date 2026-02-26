"""
Bankruptcy Filing Data Collection Pipeline

This module automates the collection of bankruptcy filing records from the
CourtListener API for the Florida Middle Bankruptcy Court (Tampa Division).
It retrieves recent bankruptcy filings, filters for Tampa-specific cases,
and saves them to the processed data directory.

The pipeline performs the following steps:
    1. Fetches bankruptcy dockets from CourtListener API for specified date range
    2. Filters for Tampa Division cases (docket numbers starting with '8:')
    3. Cleans and structures the data for downstream processing
    4. Saves the processed bankruptcy leads to CSV format

Author: Distressed Property Intelligence Platform
"""

import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
import requests

from config.settings import settings
from config.constants import (
	COURTLISTENER_API_URL,
	COURT_CODE_FLORIDA_MIDDLE_BANKRUPTCY,
	TAMPA_DIVISION_PREFIX,
	RAW_BANKRUPTCY_DIR,
	API_USER_AGENT,
	REQUEST_TIMEOUT_DEFAULT,
)
from src.core.database import get_db_context
from src.loaders.legal_proceedings import BankruptcyLoader
from src.utils.logger import setup_logging, get_logger
from src.utils.csv_deduplicator import deduplicate_csv, get_unique_keys_for_type

# Initialize logging
setup_logging()
logger = get_logger(__name__)


def fetch_bankruptcy_filings(lookback_days: int = 1) -> List[Dict[str, Any]]:
	"""
	Fetch bankruptcy filings from CourtListener API.
	
	This function queries the CourtListener API to retrieve bankruptcy docket
	information for the Florida Middle Bankruptcy Court. It fetches all cases
	filed within the specified lookback period.
	
	Args:
		lookback_days: Number of days to look back from today (default: 1)
		
	Returns:
		List[Dict[str, Any]]: List of docket dictionaries from the API response
		
	Raises:
		requests.HTTPError: If the HTTP request fails
		requests.Timeout: If the request times out
		ValueError: If the API returns an unexpected response format
		
	Example:
		>>> filings = fetch_bankruptcy_filings(lookback_days=7)
		>>> print(f"Fetched {len(filings)} bankruptcy filings")
	"""
	start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
	
	logger.info(f"Fetching bankruptcy filings from CourtListener API since {start_date}")
	
	# Construct API URL with query parameters
	params = {
		"court": COURT_CODE_FLORIDA_MIDDLE_BANKRUPTCY,
		"date_filed__gte": start_date,
	}
	
	headers = {
		"Authorization": f"Token {settings.court_listener_api_key.get_secret_value()}",
		"User-Agent": API_USER_AGENT,
	}
	
	try:
		response = requests.get(
			COURTLISTENER_API_URL,
			params=params,
			headers=headers,
			timeout=REQUEST_TIMEOUT_DEFAULT,
		)
		response.raise_for_status()
		logger.debug(f"Successfully fetched API response (status code: {response.status_code})")
		
	except requests.Timeout as e:
		logger.error(f"Request timed out while fetching bankruptcy filings: {e}")
		raise
	except requests.HTTPError as e:
		logger.error(f"HTTP error occurred while fetching bankruptcy filings: {e}")
		logger.error(f"Response content: {response.text}")
		raise
	except requests.RequestException as e:
		logger.error(f"Request error occurred while fetching bankruptcy filings: {e}")
		raise
	
	try:
		data = response.json()
		results = data.get('results', [])
		
		logger.info(f"Fetched {len(results)} bankruptcy dockets from API")
		logger.debug(f"API response includes {data.get('count', 0)} total results")
		
		return results
		
	except ValueError as e:
		logger.error(f"Failed to parse JSON response: {e}")
		raise


def filter_tampa_bankruptcies(dockets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	"""
	Filter bankruptcy dockets for Tampa Division cases only.
	
	This function filters the raw docket data to include only bankruptcy cases
	(case type 'bk') filed in the Tampa Division (docket numbers starting with '8:').
	It also cleans the case names by removing common prefixes.
	
	Args:
		dockets: List of docket dictionaries from the API
		
	Returns:
		List[Dict[str, Any]]: Filtered list of Tampa bankruptcy cases
		
	Example:
		>>> tampa_cases = filter_tampa_bankruptcies(all_dockets)
		>>> print(f"Found {len(tampa_cases)} Tampa bankruptcy cases")
	"""
	logger.info("Filtering for Tampa Division bankruptcy cases")
	
	tampa_bankruptcies = []
	
	for docket in dockets:
		case_type = docket.get('federal_dn_case_type', '')
		docket_num = docket.get('docket_number', '')
		
		# Filter for Bankruptcy ('bk') AND Tampa Division ('8:')
		if case_type == 'bk' and docket_num.startswith(TAMPA_DIVISION_PREFIX):
			# Clean the case name by removing common prefixes
			raw_name = docket.get('case_name', '')
			clean_name = raw_name.replace("In re: ", "").strip()
			
			# Create cleaned record
			cleaned_record = {
				'case_name': clean_name,
				'raw_case_name': raw_name,
				'docket_number': docket_num,
				'date_filed': docket.get('date_filed', ''),
				'case_type': case_type,
				'court': docket.get('court', ''),
				'docket_id': docket.get('id', ''),
				'date_terminated': docket.get('date_terminated', ''),
				'nature_of_suit': docket.get('nature_of_suit', ''),
			}
			
			tampa_bankruptcies.append(cleaned_record)
			
			logger.debug(f"Found Tampa bankruptcy: {clean_name} ({docket_num})")
	
	logger.info(f"Filtered {len(tampa_bankruptcies)} Tampa bankruptcy cases from {len(dockets)} total dockets")
	
	if len(tampa_bankruptcies) == 0:
		logger.warning("No Tampa bankruptcy cases found in the data")
	
	return tampa_bankruptcies


def save_bankruptcy_leads(
	leads: List[Dict[str, Any]], 
	output_filename: str = "tampa_bankruptcy_leads.csv"
) -> Optional[Path]:
	"""
	Save bankruptcy leads to a CSV file with deduplication.
	
	This function creates the processed data directory if it doesn't exist,
	deduplicates against existing CSV files, and saves the bankruptcy leads
	as a CSV file with the specified filename.
	
	Args:
		leads: List of dictionaries containing bankruptcy lead data
		output_filename: Name for the output CSV file (default: "tampa_bankruptcy_leads.csv")
		
	Returns:
		Path: Path object pointing to the saved deduplicated CSV file, or None if no data
		
	Raises:
		IOError: If the file cannot be written to disk
		PermissionError: If there are insufficient permissions to write the file
		
	Example:
		>>> output_path = save_bankruptcy_leads(leads, "bankruptcies_20260220.csv")
		>>> print(f"Saved to: {output_path}")
	"""
	if not leads:
		logger.warning("No bankruptcy leads to save")
		return None
	
	try:
		RAW_BANKRUPTCY_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured bankruptcy directory exists: {RAW_BANKRUPTCY_DIR}")
		
		# Convert to DataFrame
		df = pd.DataFrame(leads)
		
		# Check DB for existing bankruptcy cases (deduplicate BEFORE CSV save)
		logger.info("=" * 60)
		logger.info("DB DEDUPLICATION: Checking for existing bankruptcies")
		logger.info("=" * 60)
		
		initial_count = len(df)
		df_new = filter_new_records(df, 'bankruptcy', record_type='Bankruptcy')
		
		if df_new.empty:
			logger.info("✓ All bankruptcies already exist in database - nothing new")
			return None
		
		# Save only NEW bankruptcies
		new_dir = RAW_BANKRUPTCY_DIR / "new"
		new_dir.mkdir(parents=True, exist_ok=True)
		final_file = new_dir / output_filename
		
		df_new.to_csv(final_file, index=False)
		logger.info(f"Saved {len(df_new)} NEW bankruptcies to {final_file}")
		logger.info(f"Filtered {initial_count - len(df_new)} existing records")
		
		return final_file
		
	except PermissionError as e:
		logger.error(f"Permission denied when writing to {output_filename}: {e}")
		raise
	except IOError as e:
		logger.error(f"I/O error occurred while writing file: {e}")
		raise
	except Exception as e:
		logger.error(f"Unexpected error saving bankruptcy data: {e}")
		logger.debug(traceback.format_exc())
		raise
		logger.error(f"Unexpected error saving bankruptcy leads: {e}")
		raise


def run_bankruptcy_pipeline(lookback_days: int = 1) -> bool:
	"""
	Execute the complete bankruptcy data collection pipeline.
	
	This function orchestrates the entire workflow:
	    1. Fetches bankruptcy filings from CourtListener API
	    2. Filters for Tampa Division cases only
	    3. Saves the filtered results to CSV
	
	Args:
		lookback_days: Number of days to look back for filings (default: 1)
	
	Returns:
		bool: True if the pipeline executed successfully, False otherwise
		
	Example:
		>>> success = run_bankruptcy_pipeline(lookback_days=7)
		>>> if success:
		>>>     print("Bankruptcy pipeline completed successfully")
	"""
	try:
		logger.info("=" * 80)
		logger.info("STARTING BANKRUPTCY DATA COLLECTION PIPELINE")
		logger.info("=" * 80)
		
		# Step 1: Fetch bankruptcy filings from API
		logger.info(f"\n[STEP 1/3] Fetching bankruptcy filings (lookback: {lookback_days} days)...")
		dockets = fetch_bankruptcy_filings(lookback_days=lookback_days)
		
		if not dockets:
			logger.warning("No bankruptcy filings found in the specified date range")
			return False
		
		# Step 2: Filter for Tampa cases
		logger.info("\n[STEP 2/3] Filtering for Tampa Division bankruptcy cases...")
		tampa_leads = filter_tampa_bankruptcies(dockets)
		
		if not tampa_leads:
			logger.warning("No Tampa bankruptcy cases found - pipeline completed but no data to save")
			return False
		
		# Step 3: Save processed leads
		logger.info("\n[STEP 3/3] Saving processed bankruptcy leads...")
		today = datetime.now().strftime("%Y%m%d")
		output_filename = f"tampa_bankruptcy_leads_{today}.csv"
		output_path = save_bankruptcy_leads(tampa_leads, output_filename)
		
		if not output_path:
			logger.error("Failed to save bankruptcy leads")
			return False
		
		logger.info("=" * 80)
		logger.info("BANKRUPTCY PIPELINE COMPLETED SUCCESSFULLY")
		logger.info(f"Output file: {output_path}")
		logger.info(f"Total Tampa bankruptcy leads: {len(tampa_leads)}")
		logger.info("=" * 80)
		
		return True
		
	except Exception as e:
		logger.error("=" * 80)
		logger.error("BANKRUPTCY PIPELINE FAILED")
		logger.error(f"Error: {e}")
		logger.error("Traceback:")
		logger.error(traceback.format_exc())
		logger.error("=" * 80)
		return False


if __name__ == "__main__":
	"""
	Main entry point for the bankruptcy data collection pipeline.
	
	This script can be run directly to execute the complete pipeline:
	    python -m src.scrappers.bankruptcy.bankruptcy_engine
	    
	Optional arguments can be added for lookback days:
	    python -m src.scrappers.bankruptcy.bankruptcy_engine --lookback 7
	    
	Exit codes:
	    0: Pipeline completed successfully
	    1: Pipeline failed or no bankruptcy data found
	"""
	import sys
	import argparse
	
	parser = argparse.ArgumentParser(
		description="Fetch Tampa bankruptcy filings from CourtListener API"
	)
	parser.add_argument(
		"--lookback",
		type=int,
		default=1,
		help="Number of days to look back for filings (default: 1)",
	)
	
	from src.utils.scraper_db_helper import add_load_to_db_arg
	add_load_to_db_arg(parser)
	
	args = parser.parse_args()
	
	success = run_bankruptcy_pipeline(lookback_days=args.lookback)
	
	# Load to database if requested and scraping was successful
	if success and args.load_to_db:
		try:
			from src.utils.scraper_db_helper import load_scraped_data_to_db
			# Find the most recent bankruptcy CSV in new/ subdirectory
			new_dir = RAW_BANKRUPTCY_DIR / "new"
			csv_files = sorted(new_dir.glob("tampa_bankruptcy_leads*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
			if csv_files:
				csv_to_load = csv_files[0]
				logger.info(f"Loading to database: {csv_to_load}")
				load_scraped_data_to_db('bankruptcy', csv_to_load, destination_dir=RAW_BANKRUPTCY_DIR)
				
				# Delete CSV after successful DB load
				try:
					csv_to_load.unlink()
					logger.info(f"✓ Cleaned up CSV file: {csv_to_load.name}")
				except Exception as e:
					logger.warning(f"Could not delete CSV {csv_to_load}: {e}")
			else:
				logger.error("No bankruptcy CSV file found to load")
				sys.exit(1)
		except Exception as e:
			logger.error(f"Failed to load data to database: {e}")
			sys.exit(1)
	elif args.load_to_db:
		logger.warning("Skipping database load due to scraping failure")
	
	sys.exit(0 if success else 1)




