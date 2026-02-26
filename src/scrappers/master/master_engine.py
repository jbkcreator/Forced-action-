import asyncio
import argparse
import math
import os
import shutil
import sys
import time
import traceback
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Dict, Set

import pandas as pd
from dbfread import DBF
from browser_use import Agent, ChatAnthropic, Browser
from config.settings import settings

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.database import get_db_context
from src.core.models import Property
from src.loaders.master import MasterPropertyLoader
from src.utils.logger import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger(__name__)

# 1. Setup the Brain (Claude)
# We use temperature=0 for scraping to ensure deterministic actions
llm = ChatAnthropic(
	model="claude-sonnet-4-5-20250929", 
	timeout=120,
    api_key=settings.anthropic_api_key.get_secret_value(),
	temperature=0
)

# Configuration
CHUNK_SIZE = 50000
RAW_FILE_ENCODING = "cp1252"
DBF_SIGNATURES = {0x02, 0x03, 0x04, 0x05, 0x83, 0x8B, 0x8C}


# Helper functions for CSV conversion
def _normalize_label(label: str) -> str:
	"""Normalize column labels to lowercase with underscores."""
	normalized = ''.join(ch.lower() if ch.isalnum() else '_' for ch in str(label).strip())
	return '_'.join(filter(None, normalized.split('_')))


def _format_cell_value(value) -> str:
	"""Format cell values for consistent CSV output."""
	if value is None:
		return ""
	if isinstance(value, Decimal):
		formatted = format(value, 'f').rstrip('0').rstrip('.')
		return formatted or "0"
	if isinstance(value, float):
		if math.isnan(value):
			return ""
		if value.is_integer():
			return str(int(value))
	return str(value)


def _detect_file_type(file_path: Path) -> str:
	"""Detect input file type by signature."""
	with open(file_path, 'rb') as fh:
		signature = fh.read(8)

	if not signature:
		raise ValueError(f"Input file {file_path} is empty.")

	first_byte = signature[0]
	if first_byte in DBF_SIGNATURES:
		return 'dbf'
	if signature.startswith(b'\xD0\xCF\x11\xE0'):
		return 'xls'
	if signature.startswith(b'PK\x03\x04'):
		return 'xlsx'
	return 'text'


def _excel_chunk_reader(file_path: Path, chunk_size: int):
	"""Read Excel file in chunks, preserving all columns."""
	import xlrd

	logger.info(f"Opening Excel workbook: {file_path}")
	book = xlrd.open_workbook(str(file_path), on_demand=True)
	sheet = book.sheet_by_index(0)
	
	# Read header row
	header = [sheet.cell_value(0, col) for col in range(sheet.ncols)]
	logger.info(f"Found {len(header)} columns in Excel file")
	
	row_buffer: List[Dict[str, str]] = []
	total_rows = sheet.nrows - 1  # Exclude header
	
	for row_idx in range(1, sheet.nrows):
		row_data: Dict[str, str] = {}
		for col_idx, col_name in enumerate(header):
			cell_value = sheet.cell_value(row_idx, col_idx)
			row_data[col_name] = _format_cell_value(cell_value)
		row_buffer.append(row_data)

		if len(row_buffer) >= chunk_size:
			df = pd.DataFrame(row_buffer)
			df.columns = [_normalize_label(col) for col in df.columns]
			yield df
			row_buffer = []
			logger.info(f"Processed {row_idx}/{total_rows} rows...")

	if row_buffer:
		df = pd.DataFrame(row_buffer)
		df.columns = [_normalize_label(col) for col in df.columns]
		yield df

	logger.info(f"Completed processing {total_rows:,} rows from Excel file")
	book.release_resources()


def _dbf_chunk_reader(file_path: Path, chunk_size: int):
	"""Read DBF file in chunks, preserving all columns."""
	logger.info(f"Opening DBF file: {file_path}")
	
	dbf_table = DBF(str(file_path), encoding=RAW_FILE_ENCODING, ignore_missing_memofile=True)
	
	# Get field names (columns)
	header = dbf_table.field_names
	logger.info(f"Found {len(header)} columns in DBF file: {', '.join(header)}")
	
	row_buffer: List[Dict[str, str]] = []
	total_rows = len(dbf_table)
	logger.info(f"Total rows to process: {total_rows:,}")
	
	for row_idx, record in enumerate(dbf_table, start=1):
		row_data: Dict[str, str] = {}
		for field_name in header:
			value = record.get(field_name)
			row_data[field_name] = _format_cell_value(value)
		row_buffer.append(row_data)
		
		if len(row_buffer) >= chunk_size:
			df = pd.DataFrame(row_buffer)
			df.columns = [_normalize_label(col) for col in df.columns]
			yield df
			row_buffer = []
			logger.info(f"Processed {row_idx:,}/{total_rows:,} rows...")
	
	if row_buffer:
		df = pd.DataFrame(row_buffer)
		df.columns = [_normalize_label(col) for col in df.columns]
		yield df
		logger.info(f"Processed {row_idx:,}/{total_rows:,} rows (final chunk)")
	
	logger.info(f"Completed processing {total_rows:,} rows from DBF file")


async def download_parcel_master():
	"""Download the PARCEL_SPREADSHEET.xls file using browser automation."""
	save_dir = os.path.abspath("data/reference/")
	os.makedirs(save_dir, exist_ok=True)

	task = (
		"Go to https://downloads.hcpafl.org/ "
		"Find and click on 'PARCEL_SPREADSHEET.xls' (536 MB file) to start the download. "
		"IMPORTANT: The click action will timeout after 15 seconds - this is NORMAL for large file downloads. "
		"After clicking, wait exactly 60 seconds for the background download to complete. "
		"Do NOT click again, do NOT try to verify in downloads tab - just wait 60 seconds after clicking and then finish."
	)

	print("[*] Launching browser agent to download PARCEL_SPREADSHEET.xls...")

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

	history = await agent.run()
	
	if not history.is_done():
		print("[!] Agent was unable to complete the task. Check logs.")
		return False
	
	print("[+] Download initiated. Checking for completed file...")
	
	# Agent already waited 60 seconds, just give a small buffer
	await asyncio.sleep(5)
	
	# Search for the downloaded file in browser-use temp directories
	downloaded_file = None
	from config.constants import TEMP_DOWNLOADS_DIR
	temp_base = TEMP_DOWNLOADS_DIR
	
	if temp_base.exists():
		for download_dir in temp_base.glob("browser-use-downloads-*"):
			# Look for completed XLS file (not .crdownload)
			for xls_file in download_dir.glob("*[Pp][Aa][Rr][Cc][Ee][Ll]*.[Xx][Ll][Ss]"):
				if not xls_file.name.endswith('.crdownload'):
					downloaded_file = xls_file
					print(f"[*] Found file: {xls_file}")
					break
			if downloaded_file:
				break
	
	if not downloaded_file or not downloaded_file.exists():
		print(f"[!] Could not locate completed download file")
		return False
	
	print(f"[*] Verifying file size...")
	
	# Quick check - file should be ~536 MB
	final_size_mb = downloaded_file.stat().st_size / (1024**2)
	
	if final_size_mb < 100:
		print(f"[!] WARNING: File size ({final_size_mb:.1f} MB) seems incomplete.")
		print(f"[*] Waiting 30 more seconds for download to finish...")
		
		# Wait and monitor for completion
		start_time = time.time()
		while time.time() - start_time < 30:
			try:
				current_size_mb = downloaded_file.stat().st_size / (1024**2)
				if current_size_mb >= 500:
					print(f"[+] Download complete: {current_size_mb:.1f} MB")
					final_size_mb = current_size_mb
					break
				print(f"[*] Progress: {current_size_mb:.1f} MB")
				time.sleep(3)
			except Exception as e:
				print(f"[!] Error: {e}")
				time.sleep(3)
		
		final_size_mb = downloaded_file.stat().st_size / (1024**2)
		if final_size_mb < 100:
			print(f"[!] ERROR: Download incomplete at {final_size_mb:.1f} MB")
			return False
	
	# Move to final destination
	dest_file = Path(save_dir) / "PARCEL_SPREADSHEET.xls"
	print(f"[*] Moving file to {dest_file}")
	shutil.move(str(downloaded_file), str(dest_file))
	
	# Clean up temp directory
	try:
		temp_dir = downloaded_file.parent
		if temp_dir.exists() and temp_dir.name.startswith("browser-use-downloads-"):
			shutil.rmtree(str(temp_dir))
			print(f"[*] Cleaned up temp directory: {temp_dir}")
	except Exception as e:
		print(f"[!] Warning: Could not clean up temp dir: {e}")
	
	print(f"[+] Success! File saved to {dest_file} ({final_size_mb:.1f} MB)")
	return dest_file


def convert_xls_to_csv(xls_path: Path, output_dir: Path) -> Path:
	"""
	Convert XLS file to CSV format in chunks.
	
	Args:
		xls_path: Path to XLS file
		output_dir: Directory to save CSV file
		
	Returns:
		Path to created CSV file
	"""
	logger.info("=" * 80)
	logger.info("PHASE 2: Converting XLS to CSV")
	logger.info("=" * 80)
	
	if not xls_path.exists():
		raise FileNotFoundError(f"XLS file not found: {xls_path}")
	
	# Create output directory
	output_dir.mkdir(parents=True, exist_ok=True)
	
	# Generate timestamped filename
	timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
	output_csv = output_dir / f"master_{timestamp}.csv"
	
	# Detect file type
	file_type = _detect_file_type(xls_path)
	logger.info(f"Detected file type: {file_type.upper()}")
	
	# Select appropriate reader based on file type
	if file_type == 'xls':
		chunk_reader = _excel_chunk_reader(xls_path, CHUNK_SIZE)
	elif file_type == 'dbf':
		chunk_reader = _dbf_chunk_reader(xls_path, CHUNK_SIZE)
	else:
		raise ValueError(f"Unsupported file type: {file_type}. Expected XLS or DBF.")
	
	# Process chunks
	logger.info(f"Starting conversion to CSV: {output_csv}")
	logger.info(f"Processing in chunks of {CHUNK_SIZE:,} rows...")
	
	processed_count = 0
	chunk_count = 0
	
	for i, chunk in enumerate(chunk_reader):
		chunk_count = i + 1
		
		# Clean up whitespace in string columns
		for col in chunk.columns:
			if chunk[col].dtype == 'object':
				chunk[col] = chunk[col].astype(str).str.strip()
		
		# Write/Append to CSV
		write_header = (i == 0)
		write_mode = 'w' if i == 0 else 'a'
		chunk.to_csv(output_csv, mode=write_mode, index=False, header=write_header)
		
		processed_count += len(chunk)
		
		if i == 0:
			logger.info(f"Output contains {len(chunk.columns)} columns")
	
	size_mb = output_csv.stat().st_size / (1024 ** 2)
	logger.info(f"✓ Conversion complete: {processed_count:,} rows, {size_mb:.1f} MB")
	
	return output_csv


def get_existing_parcel_ids() -> Set[str]:
	"""Query database for all existing parcel IDs."""
	logger.info("=" * 80)
	logger.info("Querying database for existing parcel IDs...")
	logger.info("=" * 80)
	
	with get_db_context() as session:
		# Use bulk query for efficiency
		result = session.query(Property.parcel_id).all()
		existing_ids = {row[0] for row in result if row[0]}
	
	logger.info(f"Found {len(existing_ids):,} existing properties in database")
	return existing_ids


def deduplicate_against_db(csv_path: Path, existing_ids: Set[str], output_dir: Path) -> tuple[Path, int, int]:
	"""
	Filter out existing parcel IDs from CSV.
	
	Args:
		csv_path: Path to full CSV file
		existing_ids: Set of existing parcel IDs from database
		output_dir: Directory to save deduplicated CSV
		
	Returns:
		Tuple of (deduplicated_csv_path, new_count, duplicate_count)
	"""
	logger.info("=" * 80)
	logger.info("PHASE 3: Deduplicating against database")
	logger.info("=" * 80)
	
	timestamp = datetime.now().strftime("%Y%m%d")
	output_csv = output_dir / f"master_new_only_{timestamp}.csv"
	
	new_count = 0
	duplicate_count = 0
	chunk_num = 0
	
	for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE, dtype={'folio': str}):
		chunk_num += 1
		
		# Normalize column names
		chunk.columns = [col.upper() for col in chunk.columns]
		
		# Filter out existing parcel IDs
		parcel_col = 'FOLIO' if 'FOLIO' in chunk.columns else chunk.columns[0]
		
		before_count = len(chunk)
		chunk_filtered = chunk[~chunk[parcel_col].isin(existing_ids)]
		after_count = len(chunk_filtered)
		
		dups_this_chunk = before_count - after_count
		duplicate_count += dups_this_chunk
		new_count += after_count
		
		if after_count > 0:
			# Append new records to output
			write_header = (chunk_num == 1)
			write_mode = 'w' if write_header else 'a'
			chunk_filtered.to_csv(output_csv, mode=write_mode, index=False, header=write_header)
		
		if chunk_num % 10 == 0:
			logger.info(f"Progress: Processed {chunk_num} chunks, found {new_count:,} new records...")
	
	logger.info("=" * 80)
	logger.info(f"Total records in source: {new_count + duplicate_count:,}")
	logger.info(f"New records: {new_count:,}")
	logger.info(f"Duplicates skipped: {duplicate_count:,}")
	logger.info("=" * 80)
	
	if new_count == 0:
		logger.warning("No new records found - database is up to date!")
		if output_csv.exists():
			output_csv.unlink()
		return None, 0, duplicate_count
	
	return output_csv, new_count, duplicate_count


def load_to_database(csv_path: Path) -> tuple[int, int, int]:
	"""
	Load CSV data to database using MasterPropertyLoader.
	
	Args:
		csv_path: Path to CSV file with new records
		
	Returns:
		Tuple of (inserted, unmatched, skipped)
	"""
	logger.info("=" * 80)
	logger.info("PHASE 4: Loading to database")
	logger.info("=" * 80)
	
	with get_db_context() as session:
		loader = MasterPropertyLoader(session)
		inserted, unmatched, skipped = loader.load_from_csv(
			str(csv_path),
			skip_duplicates=True,
			chunksize=10000
		)
		session.commit()
	
	logger.info("=" * 80)
	logger.info("DATABASE LOAD SUMMARY")
	logger.info("=" * 80)
	logger.info(f"Inserted: {inserted:,}")
	logger.info(f"Unmatched: {unmatched:,}")
	logger.info(f"Skipped: {skipped:,}")
	logger.info("=" * 80)
	
	return inserted, unmatched, skipped


def accumulate_to_archive(new_csv: Path, archive_dir: Path):
	"""
	Append new records to cumulative archive file.
	
	Args:
		new_csv: Path to new records CSV
		archive_dir: Directory containing master_cumulative.csv
	"""
	logger.info("=" * 80)
	logger.info("PHASE 5: Accumulating to archive")
	logger.info("=" * 80)
	
	archive_dir.mkdir(parents=True, exist_ok=True)
	cumulative_csv = archive_dir / "master_cumulative.csv"
	
	# Read new records
	new_df = pd.read_csv(new_csv)
	logger.info(f"New records to append: {len(new_df):,}")
	
	# Append to cumulative file
	if cumulative_csv.exists():
		logger.info(f"Appending to existing archive: {cumulative_csv}")
		new_df.to_csv(cumulative_csv, mode='a', index=False, header=False)
		
		# Count total rows
		total_rows = sum(1 for _ in open(cumulative_csv)) - 1  # Subtract header
		logger.info(f"✓ Archive now contains {total_rows:,} total records")
	else:
		logger.info(f"Creating new archive: {cumulative_csv}")
		new_df.to_csv(cumulative_csv, index=False)
		logger.info(f"✓ Archive created with {len(new_df):,} records")


def cleanup_staging(staging_dir: Path):
	"""Delete all files in staging directory after successful processing."""
	logger.info("=" * 80)
	logger.info("Cleaning up staging directory")
	logger.info("=" * 80)
	
	if not staging_dir.exists():
		logger.info("Staging directory does not exist, nothing to clean")
		return
	
	files = list(staging_dir.glob("*.csv"))
	for file in files:
		file.unlink()
		logger.debug(f"Deleted: {file.name}")
	
	logger.info(f"✓ Cleaned up {len(files)} file(s) from staging")


async def run_master_pipeline(skip_download: bool = False, skip_convert: bool = False, load_to_db: bool = False):
	"""
	Run the complete master property data pipeline.
	
	Args:
		skip_download: Skip download phase (use existing XLS)
		skip_convert: Skip conversion phase (use existing CSV)
		load_to_db: Load data to database after deduplication
	"""
	try:
		# Setup paths
		reference_dir = Path("data/reference")
		new_dir = Path("data/raw/master/new")
		old_dir = Path("data/raw/master/old")
		
		xls_file = reference_dir / "PARCEL_SPREADSHEET.xls"
		
		# Phase 1: Download
		if not skip_download:
			logger.info("Starting PHASE 1: Download")
			downloaded_file = await download_parcel_master()
			if not downloaded_file:
				logger.error("Download failed")
				return
			xls_file = downloaded_file
		else:
			logger.info("Skipping download phase (using existing XLS)")
			if not xls_file.exists():
				logger.error(f"XLS file not found: {xls_file}")
				return
		
		# Phase 2: Convert XLS to CSV
		if not skip_convert:
			converted_csv = convert_xls_to_csv(xls_file, new_dir)
		else:
			logger.info("Skipping conversion phase (using existing CSV)")
			csv_files = sorted(new_dir.glob("master_*.csv"))
			if not csv_files:
				logger.error("No CSV files found in staging directory")
				return
			converted_csv = csv_files[-1]  # Use most recent
			logger.info(f"Using existing CSV: {converted_csv}")
		
		# Phase 3: Deduplicate against DB
		existing_ids = get_existing_parcel_ids()
		deduplicated_csv, new_count, dup_count = deduplicate_against_db(
			converted_csv, existing_ids, new_dir
		)
		
		if new_count == 0:
			logger.info("No new records to process. Pipeline complete.")
			cleanup_staging(new_dir)
			return
		
		# Phase 4: Load to database (if requested)
		if load_to_db:
			inserted, unmatched, skipped = load_to_database(deduplicated_csv)
			
			if inserted == 0:
				logger.warning("No records inserted to database")
				return
			
			# Phase 5: Accumulate to archive
			accumulate_to_archive(deduplicated_csv, old_dir)
			
			# Cleanup staging
			cleanup_staging(new_dir)
			
			logger.info("=" * 80)
			logger.info("✓ MASTER PROPERTY PIPELINE COMPLETE")
			logger.info("=" * 80)
			logger.info(f"New properties added: {inserted:,}")
			logger.info(f"Archive location: {old_dir / 'master_cumulative.csv'}")
		else:
			logger.info("=" * 80)
			logger.info("Pipeline complete (skipped database loading)")
			logger.info(f"Deduplicated file ready: {deduplicated_csv}")
			logger.info("Rerun with --load-to-db to insert into database")
			logger.info("=" * 80)
		
	except Exception as e:
		logger.error(f"Pipeline failed: {e}")
		logger.debug(traceback.format_exc())
		raise


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Master Property Data Pipeline')
	parser.add_argument(
		'--skip-download',
		action='store_true',
		help='Skip download phase (use existing XLS file)'
	)
	parser.add_argument(
		'--skip-convert',
		action='store_true',
		help='Skip conversion phase (use existing CSV file in staging)'
	)
	parser.add_argument(
		'--load-to-db',
		action='store_true',
		help='Load deduplicated data to database'
	)
	
	args = parser.parse_args()
	
	asyncio.run(run_master_pipeline(
		skip_download=args.skip_download,
		skip_convert=args.skip_convert,
		load_to_db=args.load_to_db
	))

