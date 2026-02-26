"""
Building Permit Data Collection Pipeline

This module automates the collection of building permit records from
the Hillsborough County Accela system. It uses browser automation via browser_use
and Claude Sonnet 4.5 to navigate the permit search interface, extract table data
directly from the HTML, and process permit records.

The pipeline performs the following steps:
    1. Launches a browser agent with Claude Sonnet 4.5 to navigate the Accela permit portal
    2. Applies date range filters based on lookback period
    3. Extracts permit data directly from HTML table as pipe-delimited text
    4. Parses and saves the data to the processed data directory with deduplication

Author: Distressed Property Intelligence Platform
"""

import asyncio
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from browser_use import Agent, ChatAnthropic, Browser

from config.settings import settings
from config.constants import (
	BROWSER_MODEL,
	BROWSER_TEMPERATURE,
	RAW_PERMIT_DIR,
	PROCESSED_DATA_DIR,
	PERMIT_SEARCH_URL,
)
from src.core.database import get_db_context
from src.loaders.permits import BuildingPermitLoader
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt
from src.utils.db_deduplicator import filter_new_records

# Initialize logging
setup_logging()
logger = get_logger(__name__)

# Model + agent configuration
llm = ChatAnthropic(
	model=BROWSER_MODEL,
	timeout=150,
	api_key=settings.anthropic_api_key.get_secret_value(),
	temperature=BROWSER_TEMPERATURE,
)

# Ensure directories exist
RAW_PERMIT_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


async def scrape_permits_with_browser_use(lookback_days: int = 365) -> Optional[Path]:
	"""
	AI-led table extraction method for permit records.
	
	This method extracts permit data directly from the HTML table by having the
	AI agent read and return the table contents as pipe-delimited text. More reliable
	than file downloads since it doesn't depend on export buttons.
	
	Args:
		lookback_days: Number of days to look back from today (default: 365)
	
	Returns:
		Optional[Path]: Path to saved CSV file if successful, None otherwise
	"""
	try:
		# Calculate date range
		today = datetime.now()
		start_date = today - timedelta(days=lookback_days)
		
		end_date_str = today.strftime("%m/%d/%Y")
		start_date_str = start_date.strftime("%m/%d/%Y")
		
		logger.info(f"Using browser-use to scrape permits from {start_date_str} to {end_date_str}")
		logger.info("Agent will extract table data and return as text - we'll build CSV in Python")
		
		# Load task instructions from YAML configuration
		try:
			task_instructions = get_prompt(
				"permit_prompts.yaml",
				"permit_search.task_template",
				url=PERMIT_SEARCH_URL,
				end_date=end_date_str,
				start_date=start_date_str
			)
		except Exception as e:
			logger.error(f"Failed to load prompt from YAML: {e}")
			raise

		logger.info("Launching browser agent to scrape permit table...")
		
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
			task=task_instructions,
			llm=llm,
			max_steps=75,  # Increased to handle pagination through many pages
			browser=browser,
		)
		
		try:
			history = await agent.run()
			
			# Check completion status
			completed = history.is_done()
			if not completed:
				# history.history is a list property, not a method
				try:
					step_count = len(history.history) if hasattr(history, 'history') else "unknown"
				except Exception:
					step_count = "unknown"
				logger.warning(f"Agent did not complete all steps (stopped at step {step_count})")
				logger.warning("Will attempt to parse any data that was collected")
			else:
				logger.info("Agent workflow completed successfully")
			
			# Get the final result from the agent
			final_result = history.final_result()
			
			if not final_result:
				logger.error("No result returned from browser agent")
				logger.error("This usually means the agent crashed before returning data")
				return None
			
			result_str = str(final_result)
			logger.info(f"Agent returned result of length {len(result_str)} characters")
			
			# Log if we got partial data
			if not completed:
				logger.info("Processing partial data from incomplete run")
			
			logger.debug(f"First 1000 chars: {result_str[:1000]}")
			
			# Parse the pipe-delimited data
			all_permits = []
			lines = result_str.strip().split('\n')
			
			# Find the header line
			header_idx = -1
			header_columns = []
			
			# Look for the header line
			for idx, line in enumerate(lines):
				if '|' in line and any(col in line for col in ['Record Number', 'Status', 'Date']):
					header_columns = [col.strip() for col in line.split('|')]
					header_idx = idx
					logger.info(f"Found header at line {idx}: {header_columns}")
					break
			
			if header_idx == -1:
				logger.error("Could not find pipe-delimited header in agent result")
				logger.error(f"Result content (first 2000 chars): {result_str[:2000]}")
				logger.warning("Will try to parse anyway looking for any pipe-delimited data")
				return None
			
			# Parse ALL data rows after the header
			records_found = 0
			for line_num, line in enumerate(lines[header_idx + 1:], start=header_idx + 1):
				line = line.strip()
				if not line or '|' not in line:
					continue
				
				# Skip separator lines (like |------|------|)
				if line.replace('|', '').replace('-', '').strip() == '':
					continue
				
				values = [val.strip() for val in line.split('|')]
				
				# Only process if we have enough values
				if len(values) >= len(header_columns):
					row_dict = {}
					for i, col in enumerate(header_columns):
						if i < len(values):
							row_dict[col] = values[i]
					all_permits.append(row_dict)
					records_found += 1
					
					# Log progress every 10 records
					if records_found % 10 == 0:
						logger.info(f"Parsed {records_found} records so far...")
			
			logger.info(f"Finished parsing. Total records found: {records_found}")
			
			if not all_permits:
				logger.error("No permit records parsed from agent result")
				logger.error(f"Parsed {len(lines)} lines, header at {header_idx}")
				return None
			
			# Warn if incomplete run but we got some data
			if not completed and records_found > 0:
				logger.warning("=" * 60)
				logger.warning("PARTIAL DATA EXTRACTED")
				logger.warning("=" * 60)
				logger.warning(f"Agent stopped early but collected {records_found} records")
				logger.warning("This data will be saved, but you may want to re-run after fixing the issue")
				logger.warning("(e.g., adding API credits, increasing timeouts, etc.)")
			
			logger.info(f"Successfully parsed {len(all_permits)} permit records")
			
			# Convert to DataFrame
			df = pd.DataFrame(all_permits)
			
			# Save to temporary file first
			# Check DB for existing permits (deduplicate BEFORE CSV save)
			logger.info("=" * 60)
			logger.info("DB DEDUPLICATION: Checking for existing permits")
			logger.info("=" * 60)
			
			initial_count = len(df)
			df_new = filter_new_records(df, 'permits')
			
			if df_new.empty:
				logger.info("✓ All permits already exist in database - nothing new to load")
				return True  # Success, but no new records
			
			# Save only NEW permits to temporary CSV
			new_dir = RAW_PERMIT_DIR / "new"
			new_dir.mkdir(parents=True, exist_ok=True)
			
			today = datetime.now().strftime("%Y%m%d")
			temp_file = new_dir / f"building_permits_new_{today}.csv"
			
			df_new.to_csv(temp_file, index=False)
			size_mb = temp_file.stat().st_size / (1024 ** 2)
			logger.info(f"Saved {len(df_new)} NEW permits to {temp_file} ({size_mb:.2f} MB)")
			logger.info(f"Filtered {initial_count - len(df_new)} existing records")
			
			return temp_file
			
		except Exception as e:
			logger.error(f"Browser agent execution failed: {e}")
			logger.debug(traceback.format_exc())
			
			# Check for common failure reasons
			error_str = str(e).lower()
			if "credit balance" in error_str or "too low" in error_str:
				logger.error("=" * 60)
				logger.error("ANTHROPIC API CREDITS EXHAUSTED")
				logger.error("=" * 60)
				logger.error("Add credits at: https://console.anthropic.com/settings/billing")
				logger.error("The agent may have collected partial data before running out of credits.")
			
			return None
			
	except Exception as e:
		logger.error(f"Browser-use scraping failed: {e}")
		logger.debug(traceback.format_exc())
		return None


async def run_permit_pipeline(lookback_days: int = 365):
	"""
	Execute the complete permit data collection pipeline.
	
	This function orchestrates the entire workflow using direct table extraction:
		1. Scrapes permit data directly from HTML table via browser agent
		2. Parses the pipe-delimited text response
		3. Saves the processed data with deduplication
	
	The function includes comprehensive error handling and logging at each step.
	
	Args:
		lookback_days: Number of days to look back for permit records (default: 365)
		
	Raises:
		Exception: Re-raises any exceptions that occur during pipeline execution
		           after logging the error details
		
	Example:
		>>> await run_permit_pipeline(lookback_days=180)
		# Logs progress and saves processed permits to data/raw/permits/
	"""
	logger.info("=" * 60)
	logger.info("HILLSBOROUGH COUNTY BUILDING PERMITS - DATA COLLECTION")
	logger.info("=" * 60)
	
	try:
		# Scrape permits using direct table extraction
		logger.info(f"Scraping permits from last {lookback_days} days via browser automation")
		file_path = await scrape_permits_with_browser_use(lookback_days=lookback_days)
		
		if not file_path:
			logger.error("Scraping failed. Aborting pipeline.")
			return
		
		logger.info("=" * 60)
		logger.info("PERMIT PIPELINE COMPLETE")
		logger.info("=" * 60)
		logger.info(f"Output file location: {file_path}")
		
	except Exception as e:
		logger.error(f"Permit pipeline failed with error: {e}")
		logger.debug(traceback.format_exc())
		raise


if __name__ == "__main__":
	import sys
	import argparse
	from src.utils.scraper_db_helper import load_scraped_data_to_db, add_load_to_db_arg
	
	parser = argparse.ArgumentParser(description="Scrape Hillsborough County building permits")
	parser.add_argument(
		"--lookback",
		type=int,
		default=365,
		help="Number of days to look back for permits (default: 365)"
	)
	add_load_to_db_arg(parser)
	args = parser.parse_args()
	
	try:
		asyncio.run(run_permit_pipeline(lookback_days=args.lookback))
		
		# Load to database if requested
		if args.load_to_db:
			# Find the most recent permit CSV in new/ subdirectory
			new_dir = RAW_PERMIT_DIR / "new"
			csv_files = sorted(new_dir.glob("building_permits*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
			if csv_files:
				csv_to_load = csv_files[0]
				logger.info(f"Loading to database: {csv_to_load}")
				
				# Load to DB
				load_scraped_data_to_db('permits', csv_to_load, destination_dir=RAW_PERMIT_DIR)
				
				# Delete CSV after successful DB load (DB is single source of truth)
				try:
					csv_to_load.unlink()
					logger.info(f"✓ Cleaned up CSV file: {csv_to_load.name}")
				except Exception as e:
					logger.warning(f"Could not delete CSV {csv_to_load}: {e}")
			else:
				logger.error("No permit CSV file found to load")
				sys.exit(1)
		
		sys.exit(0)
		
	except Exception as e:
		logger.error(f"Pipeline failed: {e}")
		sys.exit(1)
