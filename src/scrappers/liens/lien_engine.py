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
from browser_use import Agent, ChatAnthropic, Browser

from config.settings import settings
from config.constants import (
	RAW_LIEN_DIR,
	PROCESSED_DATA_DIR,
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


async def download_document_by_type(
	doc_type: str,
	doc_type_code: str,
	lookback_days: int = 30,
	wait_after_download: int = 40,
) -> Optional[Path]:
	"""
	Automate browser to download specific document type from Hillsborough County Clerk.
	
	This function launches a browser agent powered by Claude Sonnet 4.5 to navigate
	the county clerk's public access system, search for a specific document type within
	a date range, and download the results.
	
	Args:
		doc_type: Human-readable document type name (e.g., "General Liens", "Corp Tax Liens")
		doc_type_code: Document type code to select in the search form (e.g., "LIEN", "LNCORPTX")
		lookback_days: Number of days to look back from today for document records (default: 30)
		wait_after_download: Seconds to wait for download to complete after export (default: 40)
		
	Returns:
		Optional[Path]: Path to the downloaded file if successful, None otherwise
		
	Raises:
		Exception: Logs and returns None on browser automation failures
		
	Example:
		>>> file_path = await download_document_by_type("General Liens", "LIEN", lookback_days=30)
		>>> if file_path:
		...     print(f"Downloaded to: {file_path}")
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
		
		# Configure browser for headless server environment
		browser = Browser(
			headless=True,
			disable_security=True,
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
			
			# Look for downloaded file BEFORE browser cleanup
			downloaded_file = _locate_download(start_time)
			
			if not downloaded_file or not downloaded_file.exists():
				logger.error(f"[{doc_type}] Could not detect the downloaded file after automation completed")
				return None
			
			# Move file from temp directory to RAW_LIEN_DIR immediately
			if not downloaded_file.is_relative_to(RAW_LIEN_DIR):
				# Create safe filename from doc_type
				safe_doc_name = doc_type.lower().replace(" ", "_").replace("/", "_")
				final_filename = f"{safe_doc_name}_{start_date.strftime('%Y%m%d')}_{today.strftime('%Y%m%d')}{downloaded_file.suffix}"
				dest_file = RAW_LIEN_DIR / final_filename
				
				logger.info(f"[{doc_type}] Moving download from temp directory to: {dest_file}")
				shutil.move(str(downloaded_file), str(dest_file))
				downloaded_file = dest_file
			
			file_size_kb = downloaded_file.stat().st_size / 1024
			logger.info(f"[{doc_type}] Downloaded file: {downloaded_file} (Size: {file_size_kb:.2f} KB)")
			
			return downloaded_file
			
		except Exception as e:
			logger.error(f"[{doc_type}] Browser agent execution failed: {e}")
			logger.debug(traceback.format_exc())
			return None
		
	except Exception as e:
		logger.error(f"[{doc_type}] Error during document download: {e}")
		logger.debug(traceback.format_exc())
		return None


async def download_lien_records(
	lookback_days: int = 30,
	wait_after_download: int = 40,
) -> Optional[Path]:
	"""
	Convenience wrapper to download general lien records.
	
	This function provides a simplified interface for downloading general lien records
	by calling download_document_by_type with pre-configured parameters.
	
	Args:
		lookback_days: Number of days to look back from today (default: 30)
		wait_after_download: Seconds to wait for download to complete (default: 40)
		
	Returns:
		Optional[Path]: Path to the downloaded file if successful, None otherwise
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
	
	This function handles both CSV and Excel file formats, attempting multiple character
	encodings for CSV files to ensure compatibility with various source data formats.
	
	Args:
		file_path: Path object pointing to the lien/judgment data file (CSV, XLS, or XLSX)
		
	Returns:
		pd.DataFrame: Loaded and parsed records as a DataFrame
		
	Raises:
		ValueError: If the file type is unsupported or cannot be read with standard encodings
		FileNotFoundError: If the specified file does not exist
		pd.errors.ParserError: If the file structure is invalid
	"""
	logger.info(f"Loading lien/judgment data from: {file_path}")
	
	if not file_path.exists():
		logger.error(f"Lien/judgment data file not found: {file_path}")
		raise FileNotFoundError(f"Lien/judgment data file not found: {file_path}")
	
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
		
		logger.info(f"Loaded {len(df)} lien/judgment records")
		logger.debug(f"DataFrame columns: {list(df.columns)}")
		
		return df
		
	except Exception as e:
		logger.error(f"Error processing lien/judgment data: {e}")
		raise


def categorize_and_split_data(combined_df: pd.DataFrame) -> dict:
	"""
	Applies business logic to categorize and organize documents into 3 files:
	- all_liens.csv: All lien types (General Liens split by keywords, Corp Tax Liens)
	- all_deeds.csv: All deed transfers (Deeds, Tax Deeds)
	- all_judgments.csv: All judgments (Judgments, Certified Judgments)
	
	Categorization Rules for General Liens:
		- HOA LIENS (HL): Association/HOA/Condo keywords
		- TAMPA CODE LIENS (TCL): City of Tampa in grantor/grantee
		- COUNTY CODE LIENS (CCL): Hillsborough County in grantor/grantee
		- TAX LIENS (TL): IRS/revenue keywords or Corp Tax Liens
		- MECHANICS LIENS (ML): Default for other General Liens
	
	Args:
		combined_df: DataFrame containing all raw document records
		
	Returns:
		dict: Dictionary mapping file names to record counts
		
	Example:
		>>> file_counts = categorize_and_split_data(combined_df)
		>>> print(f"Liens: {file_counts['all_liens.csv']} records")
	"""
	logger.info("Categorizing documents into liens, deeds, and judgments...")
	
	# Keyword patterns for General Liens categorization
	hoa_keywords = ['ASSOCIATION', 'HOA', 'CONDO', 'COMMUNITY', 'VILLAGE', 'TOWNHOME', 'PROPERTY OWNERS']
	irs_keywords = ['UNITED STATES', 'INTERNAL REVENUE', 'STATE OF FLORIDA', 'DEPARTMENT OF REVENUE']
	
	def categorize_record(row):
		"""Categorize each record and update document_type."""
		doc_type = row.get('document_type', '')
		
		# Merge Corp Tax Liens into Tax Liens
		if doc_type == "Corp Tax Liens":
			return "TAX LIENS (TL)"
		
		# Split General Liens based on Grantor/Grantee keywords
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
			
			# Default for General Liens not matching above patterns
			return "MECHANICS LIENS (ML)"
		
		# Keep other document types as-is
		return doc_type
	
	# Apply the categorization logic
	combined_df['document_type'] = combined_df.apply(categorize_record, axis=1)
	
	# Define file groupings
	lien_types = ["HOA LIENS (HL)", "TAMPA CODE LIENS (TCL)", "COUNTY CODE LIENS (CCL)", 
	              "TAX LIENS (TL)", "MECHANICS LIENS (ML)"]
	deed_types = ["Deeds", "Tax Deeds"]
	judgment_types = ["Judgments", "Certified Judgments"]
	
	# Save to 3 separate files with deduplication
	PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
	temp_dir = PROCESSED_DATA_DIR / "temp"
	temp_dir.mkdir(parents=True, exist_ok=True)
	file_counts = {}
	
	# Save liens with deduplication
	liens_df = combined_df[combined_df['document_type'].isin(lien_types)]
	if not liens_df.empty:
		# Save to temp file first
		temp_file = temp_dir / "liens_temp.csv"
		liens_df.to_csv(temp_file, index=False)
		
		# Deduplicate
		try:
			unique_keys = get_unique_keys_for_type('liens')
			today = datetime.now().strftime('%Y%m%d')
			liens_path = deduplicate_csv(
				new_csv_path=temp_file,
				destination_dir=PROCESSED_DATA_DIR,
				unique_key_columns=unique_keys,
				output_filename=f"all_liens_{today}.csv",
				keep_original=False
			)
			file_counts[liens_path.name] = len(pd.read_csv(liens_path))
			logger.info(f"Saved {file_counts[liens_path.name]} lien records to {liens_path.name}")
		except Exception as e:
			logger.error(f"Lien deduplication failed: {e}, saving without deduplication")
			liens_path = PROCESSED_DATA_DIR / "all_liens.csv"
			liens_df.to_csv(liens_path, index=False)
			file_counts['all_liens.csv'] = len(liens_df)
			logger.info(f"Saved {len(liens_df)} lien records to {liens_path.name}")
		
		for doc_type in liens_df['document_type'].unique():
			count = len(liens_df[liens_df['document_type'] == doc_type])
			logger.info(f"  - {doc_type}: {count} records")
	
	# Save deeds with deduplication
	deeds_df = combined_df[combined_df['document_type'].isin(deed_types)]
	if not deeds_df.empty:
		# Save to temp file first
		temp_file = temp_dir / "deeds_temp.csv"
		deeds_df.to_csv(temp_file, index=False)
		
		# Deduplicate
		try:
			unique_keys = get_unique_keys_for_type('deeds')
			today = datetime.now().strftime('%Y%m%d')
			deeds_path = deduplicate_csv(
				new_csv_path=temp_file,
				destination_dir=PROCESSED_DATA_DIR,
				unique_key_columns=unique_keys,
				output_filename=f"all_deeds_{today}.csv",
				keep_original=False
			)
			file_counts[deeds_path.name] = len(pd.read_csv(deeds_path))
			logger.info(f"Saved {file_counts[deeds_path.name]} deed records to {deeds_path.name}")
		except Exception as e:
			logger.error(f"Deed deduplication failed: {e}, saving without deduplication")
			deeds_path = PROCESSED_DATA_DIR / "all_deeds.csv"
			deeds_df.to_csv(deeds_path, index=False)
			file_counts['all_deeds.csv'] = len(deeds_df)
			logger.info(f"Saved {len(deeds_df)} deed records to {deeds_path.name}")
		
		for doc_type in deeds_df['document_type'].unique():
			count = len(deeds_df[deeds_df['document_type'] == doc_type])
			logger.info(f"  - {doc_type}: {count} records")
	
	# Save judgments with deduplication
	judgments_df = combined_df[combined_df['document_type'].isin(judgment_types)]
	if not judgments_df.empty:
		# Save to temp file first
		temp_file = temp_dir / "judgments_temp.csv"
		judgments_df.to_csv(temp_file, index=False)
		
		# Deduplicate
		try:
			unique_keys = get_unique_keys_for_type('judgments')
			today = datetime.now().strftime('%Y%m%d')
			judgments_path = deduplicate_csv(
				new_csv_path=temp_file,
				destination_dir=PROCESSED_DATA_DIR,
				unique_key_columns=unique_keys,
				output_filename=f"all_judgments_{today}.csv",
				keep_original=False
			)
			file_counts[judgments_path.name] = len(pd.read_csv(judgments_path))
			logger.info(f"Saved {file_counts[judgments_path.name]} judgment records to {judgments_path.name}")
		except Exception as e:
			logger.error(f"Judgment deduplication failed: {e}, saving without deduplication")
			judgments_path = PROCESSED_DATA_DIR / "all_judgments.csv"
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
	
	This function creates the processed data directory if it doesn't exist,
	deduplicates against existing CSV files, and saves the DataFrame as a CSV
	file with the specified filename.
	
	Args:
		df: DataFrame containing processed lien/judgment records
		output_filename: Name for the output CSV file (default: "lien_data.csv")
		
	Returns:
		Path: Path object pointing to the saved deduplicated CSV file
		
	Raises:
		IOError: If the file cannot be written to disk
		PermissionError: If there are insufficient permissions to write the file
	"""
	try:
		RAW_LIEN_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured lien directory exists: {RAW_LIEN_DIR}")
		
		# Save to temp file first
		temp_dir = RAW_LIEN_DIR / "temp"
		temp_dir.mkdir(parents=True, exist_ok=True)
		temp_file = temp_dir / "liens_temp.csv"
		
		df.to_csv(temp_file, index=False)
		logger.info(f"Saved {len(df)} records to temporary file: {temp_file}")
		
		# Deduplicate against existing CSVs
		try:
			# Determine data type from content
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
				keep_original=False  # Remove temp file after deduplication
			)
			
			logger.info(f"Lien/judgment records saved to: {deduplicated_file}")
			return deduplicated_file
			
		except Exception as e:
			logger.error(f"Deduplication failed: {e}")
			logger.debug(traceback.format_exc())
			# Fallback: save without deduplication (old behavior)
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


async def run_lien_pipeline(lookback_days: int = 30, run_all: bool = False):
	"""
	Execute the complete lien and judgment data collection pipeline.
	
	This function orchestrates the download and processing of four document types:
	General Liens, Corporate Tax Liens, Judgments, and Certified Judgments.
	
	Execution modes:
		- Sequential (default): Downloads one type at a time to conserve API credits
		  and reduce resource usage. Recommended for regular automated runs.
		- Parallel (run_all=True): Downloads all six types concurrently for faster
		  execution. Uses 6x API credits but completes in ~1/6 the time.
	
	Args:
		lookback_days: Number of days to look back from today for document records (default: 30)
		run_all: If True, downloads all types in parallel; if False, downloads sequentially
		
	Raises:
		Exception: Re-raises any exceptions that occur during pipeline execution
		           after logging the error details
		
	Example:
		>>> # Sequential mode (default, saves API credits)
		>>> await run_lien_pipeline(lookback_days=30, run_all=False)
		>>> 
		>>> # Parallel mode (faster but uses 4x credits)
		>>> await run_lien_pipeline(lookback_days=30, run_all=True)
	"""
	logger.info("=" * 60)
	logger.info("HILLSBOROUGH COUNTY LIEN & JUDGMENT RECORDS - DATA COLLECTION")
	logger.info("=" * 60)
	
	try:
		if run_all:
			# Run all six document types in parallel
			logger.info("Execution mode: PARALLEL (downloading 6 document types concurrently)")
			logger.warning("Parallel mode uses 6x API credits compared to sequential mode")
			
			tasks = [
				download_document_by_type("General Liens", "LIEN", lookback_days),
				download_document_by_type("Corp Tax Liens", "LNCORPTX", lookback_days),
				download_document_by_type("Judgments", "JUD", lookback_days),
				download_document_by_type("Certified Judgments", "CCJ", lookback_days),
				download_document_by_type("Deeds", "D", lookback_days),
				download_document_by_type("Tax Deeds", "TAXDEED", lookback_days),
			]
			
			# Run all downloads concurrently
			results = await asyncio.gather(*tasks, return_exceptions=True)
			
			# Process each result
			all_dataframes = []
			successful_downloads = 0
			
			for idx, result in enumerate(results):
				doc_types = ["General Liens", "Corp Tax Liens", "Judgments", "Certified Judgments", "Deeds", "Tax Deeds"]
				doc_name = doc_types[idx]
				
				if isinstance(result, Exception):
					logger.error(f"{doc_name} failed with error: {result}")
					continue
				
				if result is None:
					logger.warning(f"{doc_name} download returned None")
					continue
				
				# Process the downloaded file
				logger.info(f"Processing {doc_name} data...")
				try:
					df = process_lien_data(result)
					df["document_type"] = doc_name  # Add column to track source
					all_dataframes.append(df)
					successful_downloads += 1
					logger.info(f"{doc_name} processing complete: {len(df)} records")
				except Exception as e:
					logger.error(f"Failed to process {doc_name}: {e}")
					logger.debug(traceback.format_exc())
			
			if not all_dataframes:
				logger.error("No data was successfully downloaded and processed from any document type")
				return
			
			# Combine all raw dataframes
			combined_df = pd.concat(all_dataframes, ignore_index=True)
			
			# Categorize, split, and save to distinct Source files
			category_counts = categorize_and_split_data(combined_df)
			
			# Delete the raw source files now that processing is complete
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
			logger.info(f"Successful downloads: {successful_downloads}/6")
			logger.info(f"Total records processed: {len(combined_df)}")
			logger.info("Final Breakdown by Source:")
			for category, count in category_counts.items():
				logger.info(f"  - {category}: {count} records")
		
		else:
			# Run document types sequentially to save API credits
			logger.info("Execution mode: SEQUENTIAL (downloading 6 document types one at a time)")
			logger.info("Sequential mode conserves API credits - recommended for automated runs")
			
			doc_configs = [
				("General Liens", "LIEN"),
				("Corp Tax Liens", "LNCORPTX"),
				("Judgments", "JUD"),
				("Certified Judgments", "CCJ"),
				("Deeds", "D"),
				("Tax Deeds", "TAXDEED"),
			]
			
			all_dataframes = []
			successful_downloads = 0
			
			for idx, (doc_name, doc_code) in enumerate(doc_configs, start=1):
				logger.info("=" * 60)
				logger.info(f"[{idx}/6] Starting document type: {doc_name}")
				logger.info("=" * 60)
				
				result = await download_document_by_type(doc_name, doc_code, lookback_days)
				
				if result is None:
					logger.warning(f"{doc_name} download failed, continuing to next document type")
					continue
				
				# Process the downloaded file
				logger.info(f"Processing {doc_name} data...")
				try:
					df = process_lien_data(result)
					df["document_type"] = doc_name  # Add column to track source
					all_dataframes.append(df)
					successful_downloads += 1
					logger.info(f"{doc_name} completed successfully: {len(df)} records")
				except Exception as e:
					logger.error(f"Failed to process {doc_name}: {e}")
					logger.debug(traceback.format_exc())
			
			if not all_dataframes:
				logger.error("No data was successfully downloaded and processed from any document type")
				return
			
			# Combine all raw dataframes
			combined_df = pd.concat(all_dataframes, ignore_index=True)
			
			# Categorize, split, and save to distinct Source files
			category_counts = categorize_and_split_data(combined_df)
			
			# Delete the raw source files now that processing is complete
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
			logger.info(f"Successful downloads: {successful_downloads}/6")
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
	
	parser = argparse.ArgumentParser(description="Scrape Hillsborough County liens and judgments")
	parser.add_argument(
		"--lookback",
		type=int,
		default=30,
		help="Number of days to look back (default: 30)"
	)
	parser.add_argument(
		"--all",
		action="store_true",
		help="Run all document types in parallel (faster but uses more API credits)"
	)
	add_load_to_db_arg(parser)
	
	args = parser.parse_args()
	
	try:
		asyncio.run(run_lien_pipeline(lookback_days=args.lookback, run_all=args.all))
		
		# Load to database if requested
		if args.load_to_db:
			# Find the liens CSV in new/ subdirectory
			# Note: Scraper creates 3 files (all_liens, all_judgments, all_deeds)
			# For now, we load just the liens file - judgments and deeds can be loaded separately if needed
			new_dir = PROCESSED_DATA_DIR / "new"
			liens_files = sorted(new_dir.glob("all_liens_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
			if liens_files:
				csv_to_load = liens_files[0]
				logger.info(f"Loading liens to database: {csv_to_load}")
				load_scraped_data_to_db('liens', csv_to_load, destination_dir=PROCESSED_DATA_DIR)
				
				# Delete CSV after successful DB load (DB is single source of truth)
				try:
					csv_to_load.unlink()
					logger.info(f"✓ Cleaned up CSV file: {csv_to_load.name}")
				except Exception as e:
					logger.warning(f"Could not delete CSV {csv_to_load}: {e}")
			else:
				logger.error("No liens CSV file found to load")
				logger.info("Note: Scraper also creates all_judgments and all_deeds files")
				sys.exit(1)
		
		sys.exit(0)
		
	except Exception as e:
		logger.error(f"Pipeline failed: {e}")
		sys.exit(1)
