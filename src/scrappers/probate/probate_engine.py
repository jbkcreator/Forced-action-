"""
Probate Filing Data Collection Pipeline

This module automates the download and processing of Hillsborough County probate
daily filings. It retrieves the latest probate filing CSV from the county clerk's
website, processes the data, and saves it to the processed data directory.

The pipeline performs the following steps:
    1. Fetches the directory listing of available probate filing CSVs
    2. Identifies and downloads the most recent file based on date
    3. Loads and processes the CSV data with multi-encoding support
    4. Saves the processed data to the output directory

Author: Distressed Property Intelligence Platform
"""

import re
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils.logger import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger(__name__)

# Configuration
PROBATE_BASE_URL = "https://publicrec.hillsclerk.com/Probate/dailyfilings/"
DOWNLOAD_DIR = Path("data/raw/probate")
PROCESSED_DIR = Path("data/processed")


def download_latest_probate_filing() -> Path:
	"""
	Download the latest probate filing CSV from Hillsborough County Clerk.
	
	This function fetches the directory listing from the probate filings page,
	identifies the most recent filing based on date in the filename, and downloads
	the corresponding CSV file to the raw data directory.
	
	Returns:
		Path: Path object pointing to the downloaded CSV file
		
	Raises:
		ValueError: If no probate filing files are found or no valid dates can be parsed
		requests.HTTPError: If the HTTP request fails
		requests.Timeout: If the request times out
		
	Example:
		>>> csv_path = download_latest_probate_filing()
		>>> print(f"Downloaded to: {csv_path}")
	"""
	logger.info(f"Fetching probate filings list from: {PROBATE_BASE_URL}")
	
	try:
		# Ensure download directory exists
		DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured download directory exists: {DOWNLOAD_DIR}")
		
		# Fetch the directory listing page
		response = requests.get(
			PROBATE_BASE_URL,
			headers={
				"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
			},
			timeout=30,
		)
		response.raise_for_status()
		logger.debug(f"Successfully fetched directory listing (status code: {response.status_code})")
		
	except requests.Timeout as e:
		logger.error(f"Request timed out while fetching probate filings list: {e}")
		raise
	except requests.HTTPError as e:
		logger.error(f"HTTP error occurred while fetching probate filings list: {e}")
		raise
	except requests.RequestException as e:
		logger.error(f"Request error occurred while fetching probate filings list: {e}")
		raise
	
	try:
		# Parse HTML to find CSV files
		soup = BeautifulSoup(response.content, "html.parser")
		file_links = []
		
		for link in soup.find_all("a", href=True):
			href = link["href"]
			# Look for ProbateFiling_YYYYMMDD.csv pattern
			if "ProbateFiling_" in href and href.endswith(".csv"):
				file_links.append(href)
		
		if not file_links:
			logger.error("No probate filing CSV files found on the page")
			raise ValueError("No probate filing CSV files found on the page")
		
		logger.info(f"Found {len(file_links)} probate filing files")
		
		# Extract dates from filenames and find the latest
		date_pattern = re.compile(r"ProbateFiling_(\d{8})\.csv")
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
			logger.error("No valid dated probate files found")
			raise ValueError("No valid dated probate files found")
		
		# Sort by date and get the latest
		files_with_dates.sort(key=lambda x: x[0], reverse=True)
		latest_date, latest_file = files_with_dates[0]
		
		logger.info(f"Latest probate filing: {latest_file} (Date: {latest_date.strftime('%Y-%m-%d')})")
		
	except Exception as e:
		logger.error(f"Error parsing HTML or extracting file information: {e}")
		raise
	
	try:
		# Construct full URL
		if latest_file.startswith("/"):
			download_url = f"https://publicrec.hillsclerk.com{latest_file}"
		else:
			download_url = f"{PROBATE_BASE_URL.rstrip('/')}/{latest_file}"
		
		# Download the file
		filename = Path(latest_file).name
		output_path = DOWNLOAD_DIR / filename
		
		logger.info(f"Downloading probate file from: {download_url}")
		file_response = requests.get(
			download_url,
			headers={
				"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
			},
			timeout=60,
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


def process_probate_data(csv_path: Path) -> pd.DataFrame:
	"""
	Load and process the probate CSV file with multi-encoding support.
	
	This function attempts to load the CSV file using multiple encodings
	(UTF-8, Latin-1, and CP1252) to handle various character sets that
	may be present in the source data.
	
	Args:
		csv_path: Path object pointing to the probate CSV file
		
	Returns:
		pd.DataFrame: Loaded and parsed probate records as a DataFrame
		
	Raises:
		ValueError: If the CSV cannot be read with any standard encoding
		FileNotFoundError: If the specified file does not exist
		pd.errors.ParserError: If the CSV structure is invalid
		
	Example:
		>>> df = process_probate_data(Path("data/raw/probate/ProbateFiling_20260218.csv"))
		>>> print(f"Loaded {len(df)} records")
	"""
	logger.info(f"Loading probate data from: {csv_path}")
	
	if not csv_path.exists():
		logger.error(f"Probate CSV file not found: {csv_path}")
		raise FileNotFoundError(f"Probate CSV file not found: {csv_path}")
	
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
	
	logger.info(f"Loaded {len(df)} probate records")
	logger.debug(f"DataFrame columns: {list(df.columns)}")
	
	return df


def save_processed_probate(df: pd.DataFrame, output_filename: str = "probate_leads.csv") -> Path:
	"""
	Save processed probate data to the output directory.
	
	This function creates the processed data directory if it doesn't exist and
	saves the DataFrame as a CSV file with the specified filename.
	
	Args:
		df: DataFrame containing processed probate records
		output_filename: Name for the output CSV file (default: "probate_leads.csv")
		
	Returns:
		Path: Path object pointing to the saved CSV file
		
	Raises:
		IOError: If the file cannot be written to disk
		PermissionError: If there are insufficient permissions to write the file
		
	Example:
		>>> output_path = save_processed_probate(df, "probate_leads_2026.csv")
		>>> print(f"Saved to: {output_path}")
	"""
	try:
		PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured processed directory exists: {PROCESSED_DIR}")
		
		output_path = PROCESSED_DIR / output_filename
		
		df.to_csv(output_path, index=False)
		logger.info(f"Saved {len(df)} records to: {output_path}")
		
		return output_path
		
	except PermissionError as e:
		logger.error(f "Permission error while saving file: {e}")
		raise
	except IOError as e:
		logger.error(f"I/O error while saving file: {e}")
		raise
	except Exception as e:
		logger.error(f"Unexpected error while saving processed data: {e}")
		raise


def run_probate_pipeline():
	"""
	Execute the complete probate data collection pipeline.
	
	This function orchestrates the entire workflow:
		1. Downloads the latest probate filing CSV
		2. Processes and loads the data
		3. Saves the processed data to the output directory
	
	The function includes comprehensive error handling and logging at each step.
	
	Raises:
		Exception: Re-raises any exceptions that occur during pipeline execution
		           after logging the error details
		
	Example:
		>>> run_probate_pipeline()
		# Logs progress and saves processed probate leads to data/processed/
	"""
	logger.info("=" * 60)
	logger.info("HILLSBOROUGH COUNTY PROBATE FILINGS - DATA COLLECTION")
	logger.info("=" * 60)
	
	try:
		# Step 1: Download latest probate filing
		logger.info("Step 1: Downloading latest probate filing")
		csv_path = download_latest_probate_filing()
		
		# Step 2: Process the data
		logger.info("Step 2: Processing probate data")
		df = process_probate_data(csv_path)
		
		# Step 3: Save processed data
		logger.info("Step 3: Saving processed data")
		output_path = save_processed_probate(df)
		
		logger.info("=" * 60)
		logger.info("PROBATE PIPELINE COMPLETE")
		logger.info("=" * 60)
		logger.info(f"Total records processed: {len(df)}")
		logger.info(f"Output file location: {output_path}")
		
	except Exception as e:
		logger.error(f"Probate pipeline failed with error: {e}")
		logger.debug(traceback.format_exc())
		raise


if __name__ == "__main__":
	run_probate_pipeline()
