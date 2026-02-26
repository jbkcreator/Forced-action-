"""
Code Enforcement Violations Data Collection Pipeline

This module automates the download of code enforcement violation records from the
Hillsborough County Accela enforcement portal. It uses browser automation via
browser_use and Claude Sonnet 4.5 to navigate the portal, apply filters, and
extract violation data.

The pipeline performs the following steps:
    1. Launches a browser agent to navigate the Accela enforcement portal
    2. Applies date filters (last 1 day - yesterday to today)
    3. Searches for and extracts all violation records
    4. Saves the violation data as CSV with standardized filename
    5. Violations are mapped to properties using address matching

Author: Distressed Property Intelligence Platform
"""

import argparse
import asyncio
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from browser_use import Agent, ChatAnthropic, Browser

from config.settings import settings
from config.constants import (
	RAW_VIOLATIONS_DIR,
	VIOLATION_SEARCH_URL,
)
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt
from src.utils.db_deduplicator import filter_new_records, check_all_exist
from src.core.database import get_db_context
from src.loaders.violations import ViolationLoader

# Initialize logging
setup_logging()
logger = get_logger(__name__)

# Model + agent configuration
llm = ChatAnthropic(
	model="claude-sonnet-4-5-20250929",
	timeout=180,
	api_key=settings.anthropic_api_key.get_secret_value(),
	temperature=0,
)


async def scrape_violations_with_browser_use(start_date: str = None, end_date: str = None) -> bool:
	"""
	Full AI-led extraction (FALLBACK method).
	
	This is the RESILIENT method - works even if DOM structure changes.
	Only used when the optimized method fails (selector changes).
	More expensive and slower, but bulletproof.
	
	Args:
		start_date: Start date in YYYY-MM-DD format (default: yesterday)
		end_date: End date in YYYY-MM-DD format (default: today)
	
	Returns:
		bool: True if scraping succeeded and file was saved, False otherwise
	"""
	try:
		# Parse dates or use defaults
		if end_date:
			end_dt = datetime.strptime(end_date, "%Y-%m-%d")
		else:
			end_dt = datetime.now()
			
		if start_date:
			start_dt = datetime.strptime(start_date, "%Y-%m-%d")
		else:
			start_dt = end_dt - timedelta(days=1)
		
		end_date_str = end_dt.strftime("%m/%d/%Y")
		start_date_str = start_dt.strftime("%m/%d/%Y")
		
		logger.info(f"Using browser-use to scrape violations from {start_date_str} to {end_date_str}")
		logger.info("Agent will extract table data and return as text - we'll build CSV in Python")
		# Load task instructions from YAML configuration
		try:
			task_instructions = get_prompt(
				"violation_prompts.yaml",
				"violation_browser_use_scrape.task_template",
				url=VIOLATION_SEARCH_URL,
				end_date=end_date_str,
				start_date=start_date_str
			)
		except Exception as e:
			logger.error(f"Failed to load prompt from YAML: {e}")
			raise

		logger.info("Launching browser agent to scrape violation table...")
		
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
			
			if not history.is_done():
				# history.history is a list property, not a method
				try:
					step_count = len(history.history) if hasattr(history, 'history') else "unknown"
				except Exception:
					step_count = "unknown"
				logger.warning(f"Agent could not finish within max steps. Current step: {step_count}")
				# Still try to parse whatever was returned
			else:
				logger.info("Agent workflow completed successfully")
			
			# Get the final result from the agent
			final_result = history.final_result()
			
			if not final_result:
				logger.error("No result returned from browser agent")
				return False
			
			result_str = str(final_result)
			logger.info(f"Agent returned result of length {len(result_str)} characters")
			logger.debug(f"First 1000 chars: {result_str[:1000]}")
			
			# Parse the pipe-delimited data
			all_violations = []
			lines = result_str.strip().split('\n')
			
			# Find ALL pipe-delimited sections (agent might return multiple tables)
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
				# Try to parse any pipe-delimited lines
				for line in lines:
					if line.count('|') >= 5:  # At least 6 fields
						logger.info(f"Found potential data line: {line[:100]}")
				return False
			
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
					all_violations.append(row_dict)
					records_found += 1
					
					# Log progress every 10 records
					if records_found % 10 == 0:
						logger.info(f"Parsed {records_found} records so far...")
			
			logger.info(f"Finished parsing. Total records found: {records_found}")
			
			if not all_violations:
				logger.error("No violation records parsed from agent result")
				logger.error(f"Parsed {len(lines)} lines, header at {header_idx}")
				return False
			
			logger.info(f"Successfully parsed {len(all_violations)} violation records")
			
			# Convert to DataFrame
			df = pd.DataFrame(all_violations)
			
			# Save to temporary file first
			temp_dir = RAW_VIOLATIONS_DIR / "temp"
			temp_dir.mkdir(parents=True, exist_ok=True)
			# Check DB for existing violations (deduplicate BEFORE CSV save)
			logger.info("=" * 60)
			logger.info("DB DEDUPLICATION: Checking for existing violations")
			logger.info("=" * 60)
			
			initial_count = len(df)
			df_new = filter_new_records(df, 'violations')
			
			if df_new.empty:
				logger.info("✓ All violations already exist in database - nothing new to load")
				return True  # Success, but no new records
			
			# Save only NEW violations to temporary CSV
			new_dir = RAW_VIOLATIONS_DIR / "new"
			new_dir.mkdir(parents=True, exist_ok=True)
			
			today = datetime.now().strftime("%Y%m%d")
			temp_file = new_dir / f"violations_new_{today}.csv"
			
			df_new.to_csv(temp_file, index=False)
			size_mb = temp_file.stat().st_size / (1024 ** 2)
			logger.info(f"Saved {len(df_new)} NEW violations to {temp_file} ({size_mb:.2f} MB)")
			logger.info(f"Filtered {initial_count - len(df_new)} existing records")
			
			return temp_file
			
		except Exception as e:
			logger.error(f"Browser agent execution failed: {e}")
			logger.debug(traceback.format_exc())
			return False
			
	except Exception as e:
		logger.error(f"Browser-use scraping failed: {e}")
		logger.debug(traceback.format_exc())
		return False


async def main(args):
	"""Main execution function for violation scraping."""
	logger.info("=" * 60)
	logger.info("Violation Scraping Pipeline Starting")
	logger.info("=" * 60)
	
	if args.start_date or args.end_date:
		logger.info(f"\nCustom date range specified:")
		logger.info(f"  Start: {args.start_date or 'yesterday'}")
		logger.info(f"  End: {args.end_date or 'today'}")
	
	logger.info("\nAttempting browser-use table scraping...")
	logger.info("Extracting violation data directly from HTML table")
	csv_file = await scrape_violations_with_browser_use(
		start_date=args.start_date,
		end_date=args.end_date
	)
	
	if csv_file:
		logger.info("=" * 60)
		logger.info("✓ Scraping completed successfully!")
		logger.info("=" * 60)
		
		# Load to database if requested
		if args.load_to_db:
			logger.info("\n" + "=" * 60)
			logger.info("Loading violations into database...")
			logger.info("=" * 60)
			
			try:
				with get_db_context() as session:
					loader = ViolationLoader(session)
					matched, unmatched, skipped = loader.load_from_csv(
						str(csv_file),
						skip_duplicates=True
					)
					session.commit()
					
					logger.info(f"\n{'='*60}")
					logger.info(f"DATABASE LOAD SUMMARY")
					logger.info(f"{'='*60}")
					logger.info(f"  Matched:   {matched:>6}")
					logger.info(f"  Unmatched: {unmatched:>6}")
					logger.info(f"  Skipped:   {skipped:>6}")
					total = matched + unmatched + skipped
					match_rate = (matched / total * 100) if total > 0 else 0
					logger.info(f"  Match Rate: {match_rate:>5.1f}%")
					logger.info(f"{'='*60}\n")
					
					logger.info("✓ Database load completed!")
					
					# Delete CSV after successful DB insertion (DB is single source of truth)
					try:
						csv_file.unlink()
						logger.info(f"✓ Cleaned up CSV file: {csv_file.name}")
					except Exception as e:
						logger.warning(f"Could not delete CSV {csv_file}: {e}")
			except Exception as e:
				logger.error(f"Failed to load data to database: {e}")
				logger.debug(traceback.format_exc())
		else:
			logger.info("\nSkipping database load (use --load-to-db flag to enable)")
	else:
		logger.error("=" * 60)
		logger.error("✗ Violation report download/scraping failed")
		logger.error("=" * 60)


if __name__ == "__main__":
	# Parse command-line arguments
	parser = argparse.ArgumentParser(description="Scrape code enforcement violations from Hillsborough County portal")
	parser.add_argument(
		"--start-date",
		type=str,
		help="Start date in YYYY-MM-DD format (default: yesterday). Example: 2026-01-03"
	)
	parser.add_argument(
		"--end-date",
		type=str,
		help="End date in YYYY-MM-DD format (default: today). Example: 2026-01-04"
	)
	parser.add_argument(
		"--load-to-db",
		action="store_true",
		help="Automatically load scraped data into database after scraping"
	)
	args = parser.parse_args()

	asyncio.run(main(args))