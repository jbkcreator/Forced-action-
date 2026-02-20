"""
Eviction Filing Data Collection Pipeline

This module automates the download and processing of Hillsborough County eviction
daily filings from the Civil Court daily filings page. It retrieves the latest 
civil filing CSV from the county clerk's website, filters for eviction-related cases,
and saves them to the processed data directory.

The pipeline performs the following steps:
    1. Fetches the directory listing of available civil filing CSVs
    2. Identifies and downloads the most recent file based on date
    3. Loads and processes the CSV data with multi-encoding support
    4. Filters for eviction-related cases (LT Residential Eviction types)
    5. Saves the filtered eviction data to the output directory

Author: Distressed Property Intelligence Platform
"""

import re
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config.constants import (
	CIVIL_FILINGS_URL,
	RAW_EVICTIONS_DIR,
	PROCESSED_DATA_DIR,
	HILLSCLERK_BASE_URL,
	EVICTION_CASE_PATTERNS,
	CIVIL_FILING_PATTERN,
	DEFAULT_USER_AGENT,
	REQUEST_TIMEOUT_DEFAULT,
	REQUEST_TIMEOUT_LONG,
	OUTPUT_DATE_FORMAT,
	OUTPUT_SEPARATOR,
)
from src.utils.logger import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger(__name__)


def download_latest_civil_filing() -> Path:
	"""
	Download the latest civil filing CSV from Hillsborough County Clerk.
	
	This function fetches the directory listing from the civil filings page,
	identifies the most recent filing based on date in the filename, and downloads
	the corresponding CSV file to the raw data directory.
	
	Returns:
		Path: Path object pointing to the downloaded CSV file
		
	Raises:
		ValueError: If no civil filing files are found or no valid dates can be parsed
		requests.HTTPError: If the HTTP request fails
		requests.Timeout: If the request times out
		
	Example:
		>>> csv_path = download_latest_civil_filing()
		>>> print(f"Downloaded to: {csv_path}")
	"""
	logger.info(f"Fetching civil filings list from: {CIVIL_FILINGS_URL}")
	
	try:
		# Ensure download directory exists
		RAW_EVICTIONS_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured download directory exists: {RAW_EVICTIONS_DIR}")
		
		# Fetch the directory listing page
		response = requests.get(
			CIVIL_FILINGS_URL,
			headers={
				"User-Agent": DEFAULT_USER_AGENT
			},
			timeout=REQUEST_TIMEOUT_DEFAULT,
		)
		response.raise_for_status()
		logger.debug(f"Successfully fetched directory listing (status code: {response.status_code})")
		
	except requests.Timeout as e:
		logger.error(f"Request timed out while fetching civil filings list: {e}")
		raise
	except requests.HTTPError as e:
		logger.error(f"HTTP error occurred while fetching civil filings list: {e}")
		raise
	except requests.RequestException as e:
		logger.error(f"Request error occurred while fetching civil filings list: {e}")
		raise
	
	try:
		# Parse HTML to find CSV files
		soup = BeautifulSoup(response.content, "html.parser")
		file_links = []
		
		for link in soup.find_all("a", href=True):
			href = link["href"]
			# Look for CivilFiling_YYYYMMDD.csv pattern
			if "CivilFiling_" in href and href.endswith(".csv"):
				file_links.append(href)
		
		if not file_links:
			logger.error("No civil filing CSV files found on the page")
			raise ValueError("No civil filing CSV files found on the page")
		
		logger.info(f"Found {len(file_links)} civil filing files")
		
		# Extract dates from filenames and find the latest
		date_pattern = re.compile(CIVIL_FILING_PATTERN)
		files_with_dates = []
		
		for file_link in file_links:
			match = date_pattern.search(file_link)
			if match:
				date_str = match.group(1)
				try:
					file_date = datetime.strptime(date_str, "%Y%m%d")
					files_with_dates.append((file_date, file_link))
				except ValueError:
					logger.warning(f"Could not parse date from filename: {file_link}")
					continue
		
		if not files_with_dates:
			logger.error("No valid dated civil files found")
			raise ValueError("No valid dated civil files found")
		
		# Sort by date and get the latest
		files_with_dates.sort(key=lambda x: x[0], reverse=True)
		latest_date, latest_file = files_with_dates[0]
		
		logger.info(f"Latest civil filing: {latest_file} (Date: {latest_date.strftime('%Y-%m-%d')})")
		
	except Exception as e:
		logger.error(f"Error parsing HTML or extracting file information: {e}")
		raise
	
	try:
		# Construct full URL
		if latest_file.startswith("/"):
			download_url = f"{HILLSCLERK_BASE_URL}{latest_file}"
		else:
			download_url = f"{CIVIL_FILINGS_URL.rstrip('/')}/{latest_file}"
		
		# Download the file
		filename = Path(latest_file).name
		output_path = RAW_EVICTIONS_DIR / filename
		
		logger.info(f"Downloading civil filing from: {download_url}")
		file_response = requests.get(
			download_url,
			headers={
				"User-Agent": DEFAULT_USER_AGENT
			},
			timeout=REQUEST_TIMEOUT_LONG,
		)
		file_response.raise_for_status()
		
		# Save to disk
		output_path.write_bytes(file_response.content)
		file_size_kb = output_path.stat().st_size / 1024
		logger.info(f"Downloaded to: {output_path} (Size: {file_size_kb:.2f} KB)")
		
		return output_path
		
	except requests.Timeout as e:
		logger.error(f"Request timed out while downloading file: {e}")
		raise
	except requests.HTTPError as e:
		logger.error(f"HTTP error occurred while downloading file: {e}")
		raise
	except IOError as e:
		logger.error(f"I/O error occurred while writing file: {e}")
		raise
	except Exception as e:
		logger.error(f"Unexpected error during file download: {e}")
		raise


def process_civil_data(csv_path: Path) -> pd.DataFrame:
	"""
	Load and process the civil filing CSV file with multi-encoding support.
	
	This function attempts to load the CSV file using multiple encodings
	(UTF-8, Latin-1, and CP1252) to handle various character sets that
	may be present in the source data.
	
	Args:
		csv_path: Path object pointing to the civil filing CSV file
		
	Returns:
		pd.DataFrame: Loaded and parsed civil filing records as a DataFrame
		
	Raises:
		ValueError: If the CSV cannot be read with any standard encoding
		FileNotFoundError: If the specified file does not exist
		pd.errors.ParserError: If the CSV structure is invalid
		
	Example:
		>>> df = process_civil_data(Path("data/raw/evictions/CivilFiling_20260219.csv"))
		>>> print(f"Loaded {len(df)} records")
	"""
	logger.info(f"Loading civil filing data from: {csv_path}")
	
	if not csv_path.exists():
		logger.error(f"Civil filing CSV file not found: {csv_path}")
		raise FileNotFoundError(f"Civil filing CSV file not found: {csv_path}")
	
	# Try multiple encodings
	encodings_to_try = ["utf-8", "latin1", "cp1252"]
	df = None
	
	for encoding in encodings_to_try:
		try:
			df = pd.read_csv(csv_path, encoding=encoding)
			logger.info(f"Successfully loaded CSV with {encoding} encoding")
			break
		except (UnicodeDecodeError, pd.errors.ParserError) as e:
			logger.debug(f"Failed to load with {encoding} encoding: {e}")
			continue
	
	if df is None:
		error_msg = f"Could not read CSV with any standard encoding: {csv_path}"
		logger.error(error_msg)
		raise ValueError(error_msg)
	
	logger.info(f"Loaded {len(df)} civil filing records")
	logger.debug(f"DataFrame columns: {list(df.columns)}")
	
	return df


def filter_evictions(df: pd.DataFrame) -> pd.DataFrame:
	"""
	Filter civil filing data to include only eviction-related cases.
	
	This function filters the DataFrame to retain only records where the
	CaseTypeDescription contains eviction-related keywords. Common patterns
	include "LT Residential Eviction", "LT Commercial Eviction", etc.
	
	Args:
		df: DataFrame containing all civil filing records
		
	Returns:
		pd.DataFrame: Filtered DataFrame containing only eviction cases
		
	Raises:
		KeyError: If the expected 'CaseTypeDescription' column is not found
		
	Example:
		>>> evictions_df = filter_evictions(civil_df)
		>>> print(f"Found {len(evictions_df)} eviction cases")
	"""
	logger.info("Filtering for eviction cases")
	
	# Check if required column exists
	if "CaseTypeDescription" not in df.columns:
		logger.error("Column 'CaseTypeDescription' not found in CSV")
		logger.debug(f"Available columns: {list(df.columns)}")
		raise KeyError("Column 'CaseTypeDescription' not found in CSV")
	
	# Create filter mask for eviction patterns
	eviction_mask = df["CaseTypeDescription"].str.contains(
		"|".join(EVICTION_CASE_PATTERNS),
		case=False,
		na=False
	)
	
	evictions_df = df[eviction_mask].copy()
	
	logger.info(f"Filtered {len(evictions_df)} eviction records from {len(df)} total civil filings")
	
	if len(evictions_df) > 0:
		# Log case type breakdown
		case_type_counts = evictions_df["CaseTypeDescription"].value_counts()
		logger.info("Eviction case type breakdown:")
		for case_type, count in case_type_counts.items():
			logger.info(f"  {case_type}: {count}")
	else:
		logger.warning("No eviction cases found in the civil filing data")
	
	return evictions_df


def save_processed_evictions(df: pd.DataFrame, output_filename: str = "eviction_leads.csv") -> Path:
	"""
	Save processed eviction data to the output directory.
	
	This function creates the processed data directory if it doesn't exist and
	saves the DataFrame as a CSV file with the specified filename.
	
	Args:
		df: DataFrame containing processed eviction records
		output_filename: Name for the output CSV file (default: "eviction_leads.csv")
		
	Returns:
		Path: Path object pointing to the saved CSV file
		
	Raises:
		IOError: If the file cannot be written to disk
		PermissionError: If there are insufficient permissions to write the file
		
	Example:
		>>> output_path = save_processed_evictions(df, "evictions_20260219.csv")
		>>> print(f"Saved to: {output_path}")
	"""
	try:
		PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured processed directory exists: {PROCESSED_DATA_DIR}")
		
		output_path = PROCESSED_DATA_DIR / output_filename
		
		df.to_csv(output_path, index=False)
		logger.info(f"Saved {len(df)} eviction records to: {output_path}")
		
		return output_path
		
	except PermissionError as e:
		logger.error(f"Permission denied when writing to {output_path}: {e}")
		raise
	except IOError as e:
		logger.error(f"I/O error occurred while writing file: {e}")
		raise
	except Exception as e:
		logger.error(f"Unexpected error saving processed evictions: {e}")
		raise


def run_eviction_pipeline() -> bool:
	"""
	Execute the complete eviction data collection pipeline.
	
	This function orchestrates the entire workflow:
	    1. Downloads the latest civil filing CSV
	    2. Loads and processes the data
	    3. Filters for eviction cases only
	    4. Saves the filtered results
	
	Returns:
		bool: True if the pipeline executed successfully, False otherwise
		
	Example:
		>>> success = run_eviction_pipeline()
		>>> if success:
		>>>     print("Eviction pipeline completed successfully")
	"""
	try:
		logger.info(OUTPUT_SEPARATOR)
		logger.info("STARTING EVICTION DATA COLLECTION PIPELINE")
		logger.info(OUTPUT_SEPARATOR)
		
		# Step 1: Download latest civil filing
		logger.info("\n[STEP 1/4] Downloading latest civil filing...")
		csv_path = download_latest_civil_filing()
		
		# Step 2: Load and process the data
		logger.info("\n[STEP 2/4] Loading and processing civil filing data...")
		civil_df = process_civil_data(csv_path)
		
		# Step 3: Filter for evictions
		logger.info("\n[STEP 3/4] Filtering for eviction cases...")
		evictions_df = filter_evictions(civil_df)
		
		if len(evictions_df) == 0:
			logger.warning("No eviction cases found - pipeline completed but no data to save")
			return False
		
		# Step 4: Save processed evictions
		logger.info("\n[STEP 4/4] Saving processed eviction data...")
		# Generate filename with today's date
		today = datetime.now().strftime(OUTPUT_DATE_FORMAT)
		output_filename = f"eviction_leads_{today}.csv"
		output_path = save_processed_evictions(evictions_df, output_filename)
		
		logger.info(OUTPUT_SEPARATOR)
		logger.info("EVICTION PIPELINE COMPLETED SUCCESSFULLY")
		logger.info(f"Output file: {output_path}")
		logger.info(f"Total eviction records: {len(evictions_df)}")
		logger.info(OUTPUT_SEPARATOR)
		
		return True
		
	except Exception as e:
		logger.error(OUTPUT_SEPARATOR)
		logger.error("EVICTION PIPELINE FAILED")
		logger.error(f"Error: {e}")
		logger.error("Traceback:")
		logger.error(traceback.format_exc())
		logger.error(OUTPUT_SEPARATOR)
		return False


if __name__ == "__main__":
	"""
	Main entry point for the eviction data collection pipeline.
	
	This script can be run directly to execute the complete pipeline:
	    python -m src.scrappers.evictions.evictions_engine
	    
	Exit codes:
	    0: Pipeline completed successfully
	    1: Pipeline failed or no eviction data found
	"""
	import sys
	
	success = run_eviction_pipeline()
	sys.exit(0 if success else 1)
