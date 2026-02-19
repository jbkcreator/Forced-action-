"""
Building Permit Data Collection Pipeline

This module automates the download and processing of building permit records from
the Hillsborough County Accela system. It uses browser automation via browser_use
and Claude Sonnet 4.5 to navigate the permit search interface, apply date filters,
and export permit data.

The pipeline performs the following steps:
    1. Launches a browser agent with Claude Sonnet 4.5 to navigate the Accela permit portal
    2. Applies date range filters based on lookback period
    3. Exports and downloads permit records (CSV/Excel format)
    4. Processes and saves the data to the processed data directory

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
from browser_use import Agent, ChatAnthropic

from config.settings import settings
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt

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
PERMIT_URL = "https://aca-prod.accela.com/HCFL/Cap/CapHome.aspx?module=Building"
RAW_DIR = Path("data/raw/permit")
PROCESSED_DIR = Path("data/processed")
DOWNLOAD_PATTERNS = ("*.csv", "*.xls", "*.xlsx", "*.zip")

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


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
		for pattern in DOWNLOAD_PATTERNS:
			for candidate in folder.glob(pattern):
				try:
					if candidate.stat().st_mtime >= start_time:
						paths.append(candidate)
				except FileNotFoundError:
					logger.debug(f"File disappeared during check: {candidate}")
					continue
		return paths
	
	candidates = recent_candidates(RAW_DIR)
	logger.debug(f"Found {len(candidates)} candidate files in {RAW_DIR}")
	
	# Check browser-use temp directories
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


async def download_permits(
	lookback_days: int = 365,
	wait_after_download: int = 40,
) -> Optional[Path]:
	"""
	Automate browser to download permit records from the Accela portal.
	
	This function launches a browser agent powered by Claude Sonnet 4.5 to navigate
	the Hillsborough County Accela permit search portal, apply date range filters,
	and download permit records.
	
	Args:
		lookback_days: Number of days to look back from today for permit records (default: 365)
		wait_after_download: Seconds to wait for download to complete after export (default: 40)
		
	Returns:
		Optional[Path]: Path to the downloaded file if successful, None otherwise
		
	Raises:
		Exception: Logs and returns None on browser automation failures
		
	Example:
		>>> file_path = await download_permits(lookback_days=180)
		>>> if file_path:
		...     print(f"Downloaded to: {file_path}")
	"""
	
	try:
		RAW_DIR.mkdir(parents=True, exist_ok=True)
		
		save_dir = os.path.abspath(str(RAW_DIR))
		start_time = time.time()
		
		# Calculate date range
		today = datetime.now()
		start_date = today - timedelta(days=lookback_days)
		
		# Format dates for the form (MM/DD/YYYY format)
		start_date_str = start_date.strftime("%m/%d/%Y")
		end_date_str = today.strftime("%m/%d/%Y")
		
		logger.info(f"Fetching permits from {start_date_str} to {end_date_str} ({lookback_days} days)")
		
		# Load task prompt from YAML configuration
		try:
			task = get_prompt(
				"permit_prompts.yaml",
				"permit_search.task_template",
				url=PERMIT_URL,
				start_date=start_date_str,
				end_date=end_date_str,
				wait_time=wait_after_download
			)
		except Exception as e:
			logger.error(f"Failed to load prompt from YAML: {e}")
			raise
		
		logger.info("Launching browser agent to download permits")
		logger.debug(f"Download directory: {save_dir}")
		
		agent = Agent(
			task=task,
			llm=llm,
			browser_context_config={
				"headless": False,  # Set to True for production
				"save_downloads_path": save_dir,
			},
			max_actions_per_step=10,
		)
		
		try:
			history = await agent.run(max_steps=25)
			
			if not history.is_done():
				logger.warning("Agent could not finish the workflow within step limit. Check browser logs.")
				return None
			
			logger.info("Agent workflow completed. Waiting for download to finalize...")
			await asyncio.sleep(wait_after_download)
			
		except Exception as e:
			logger.error(f"Browser agent execution failed: {e}")
			logger.debug(traceback.format_exc())
			return None
		
		# Look for downloaded file
		downloaded_file = _locate_download(start_time)
		
		if not downloaded_file or not downloaded_file.exists():
			logger.error("Could not detect the downloaded file after automation completed")
			return None
		
		# If file is in temp directory, move it to RAW_DIR
		if not downloaded_file.is_relative_to(RAW_DIR):
			final_filename = f"hillsborough_permits_{start_date.strftime('%Y%m%d')}_{today.strftime('%Y%m%d')}{downloaded_file.suffix}"
			dest_file = RAW_DIR / final_filename
			
			logger.info(f"Moving download from temp directory to: {dest_file}")
			shutil.move(str(downloaded_file), str(dest_file))
			downloaded_file = dest_file
			
			# Clean up temp directory
			temp_dir = downloaded_file.parent
			if temp_dir.name.startswith("browser-use-downloads-"):
				try:
					shutil.rmtree(temp_dir)
					logger.debug(f"Cleaned up temp directory: {temp_dir}")
				except Exception as e:
					logger.warning(f"Could not clean temp directory: {e}")
		
		file_size_kb = downloaded_file.stat().st_size / 1024
		logger.info(f"Downloaded file: {downloaded_file} (Size: {file_size_kb:.2f} KB)")
		
		return downloaded_file
		
	except Exception as e:
		logger.error(f"Error during permit download: {e}")
		logger.debug(traceback.format_exc())
		return None


def process_permit_data(file_path: Path) -> pd.DataFrame:
	"""
	Load and process the permit data file with multi-format and multi-encoding support.
	
	This function handles both CSV and Excel file formats, attempting multiple character
	encodings for CSV files to ensure compatibility with various source data formats.
	
	Args:
		file_path: Path object pointing to the permit data file (CSV, XLS, or XLSX)
		
	Returns:
		pd.DataFrame: Loaded and parsed permit records as a DataFrame
		
	Raises:
		ValueError: If the file type is unsupported or cannot be read with standard encodings
		FileNotFoundError: If the specified file does not exist
		pd.errors.ParserError: If the file structure is invalid
		
	Example:
		>>> df = process_permit_data(Path("data/raw/permit/permits_20260101_20260131.csv"))
		>>> print(f"Loaded {len(df)} permit records")
	"""
	logger.info(f"Loading permit data from: {file_path}")
	
	if not file_path.exists():
		logger.error(f"Permit data file not found: {file_path}")
		raise FileNotFoundError(f"Permit data file not found: {file_path}")
	
	df = None
	
	try:
		# Handle different file types
		if file_path.suffix.lower() == ".csv":
			# Try multiple encodings for CSV files
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
		
		logger.info(f"Loaded {len(df)} permit records")
		logger.debug(f"DataFrame columns: {list(df.columns)}")
		
		return df
		
	except Exception as e:
		logger.error(f"Error processing permit data: {e}")
		raise


def save_processed_permits(df: pd.DataFrame, output_filename: str = "permit_data.csv") -> Path:
	"""
	Save processed permit data to the output directory.
	
	This function creates the processed data directory if it doesn't exist and
	saves the DataFrame as a CSV file with the specified filename.
	
	Args:
		df: DataFrame containing processed permit records
		output_filename: Name for the output CSV file (default: "permit_data.csv")
		
	Returns:
		Path: Path object pointing to the saved CSV file
		
	Raises:
		IOError: If the file cannot be written to disk
		PermissionError: If there are insufficient permissions to write the file
		
	Example:
		>>> output_path = save_processed_permits(df, "hillsborough_permits_2026.csv")
		>>> print(f"Saved to: {output_path}")
	"""
	try:
		PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured processed directory exists: {PROCESSED_DIR}")
		
		output_path = PROCESSED_DIR / output_filename
		
		df.to_csv(output_path, index=False)
		logger.info(f"Saved {len(df)} permit records to: {output_path}")
		
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


async def run_permit_pipeline(lookback_days: int = 365):
	"""
	Execute the complete permit data collection pipeline.
	
	This function orchestrates the entire workflow:
		1. Downloads permit data using browser automation
		2. Processes and loads the downloaded data
		3. Saves the processed data to the output directory
	
	The function includes comprehensive error handling and logging at each step.
	
	Args:
		lookback_days: Number of days to look back for permit records (default: 365)
		
	Raises:
		Exception: Re-raises any exceptions that occur during pipeline execution
		           after logging the error details
		
	Example:
		>>> await run_permit_pipeline(lookback_days=180)
		# Logs progress and saves processed permits to data/processed/
	"""
	logger.info("=" * 60)
	logger.info("HILLSBOROUGH COUNTY BUILDING PERMITS - DATA COLLECTION")
	logger.info("=" * 60)
	
	try:
		# Step 1: Download permit data
		logger.info("Step 1: Downloading permit data via browser automation")
		file_path = await download_permits(lookback_days=lookback_days)
		
		if not file_path:
			logger.error("Download failed. Aborting pipeline.")
			return
		
		# Step 2: Process the data
		logger.info("Step 2: Processing permit data")
		df = process_permit_data(file_path)
		
		# Step 3: Save processed data
		logger.info("Step 3: Saving processed data")
		output_path = save_processed_permits(df)
		
		logger.info("=" * 60)
		logger.info("PERMIT PIPELINE COMPLETE")
		logger.info("=" * 60)
		logger.info(f"Total records processed: {len(df)}")
		logger.info(f"Output file location: {output_path}")
		
	except Exception as e:
		logger.error(f"Permit pipeline failed with error: {e}")
		logger.debug(traceback.format_exc())
		raise


if __name__ == "__main__":
	import sys
	
	# Allow command-line override of lookback days
	lookback_days = 365
	if len(sys.argv) > 1:
		try:
			lookback_days = int(sys.argv[1])
			logger.info(f"Using command-line lookback period: {lookback_days} days")
		except ValueError:
			logger.warning(f"Invalid lookback days: {sys.argv[1]}, using default: 365")
	
	asyncio.run(run_permit_pipeline(lookback_days=lookback_days))
