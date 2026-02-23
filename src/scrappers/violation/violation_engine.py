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

import pandas as pd

from browser_use import Agent, ChatAnthropic

from config.settings import settings
from config.constants import (
	REFERENCE_DATA_DIR,
	VIOLATION_SEARCH_URL,
)
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt

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
	Use browser-use Agent to scrape violation records directly from HTML table.
	
	This approach uses browser automation to navigate the portal, search for violations,
	and extract data. The agent returns data in pipe-delimited format, which we then
	parse and save as CSV in Python. This is more reliable than depending on file
	attachments or downloads.
	
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
		
		today_str = end_dt.strftime("%m/%d/%Y")
		start_date_str = start_dt.strftime("%m/%d/%Y")
		
		logger.info(f"Using browser-use to scrape violations from {start_date_str} to {today_str}")
		logger.info("Agent will extract table data and return as text - we'll build CSV in Python")
		
		# Load task instructions from YAML configuration
		try:
			task_instructions = get_prompt(
				"violation_prompts.yaml",
				"violation_browser_use_scrape.task_template",
				url=VIOLATION_SEARCH_URL,
				today_date=today_str,
				start_date=start_date_str
			)
		except Exception as e:
			logger.error(f"Failed to load prompt from YAML: {e}")
			raise

		logger.info("Launching browser agent to scrape violation table...")
		
		agent = Agent(
			task=task_instructions,
			llm=llm,
			max_steps=30,
			browser_context_config={
				"headless": False,
			},
		)
		
		try:
			history = await agent.run()
			
			if not history.is_done():
				logger.warning("Agent could not finish within 30 steps")
				return False
			
			logger.info("Agent workflow completed successfully")
			
			# Get the final result from the agent
			final_result = history.final_result()
			
			if not final_result:
				logger.error("No result returned from browser agent")
				return False
			
			result_str = str(final_result)
			logger.info(f"Agent returned result of length {len(result_str)} characters")
			logger.debug(f"First 500 chars: {result_str[:500]}")
			
			# Parse the pipe-delimited data
			all_violations = []
			lines = result_str.strip().split('\n')
			
			# Find the header line (should contain column names with | separators)
			header_idx = -1
			header_columns = []
			for idx, line in enumerate(lines):
				if '|' in line and any(col in line for col in ['Record Number', 'Status', 'Date']):
					header_columns = [col.strip() for col in line.split('|')]
					header_idx = idx
					logger.info(f"Found header at line {idx}: {header_columns}")
					break
			
			if header_idx == -1:
				logger.error("Could not find pipe-delimited header in agent result")
				logger.error(f"Result content: {result_str[:1000]}")
				return False
			
			# Parse data rows
			for line in lines[header_idx + 1:]:
				line = line.strip()
				if not line or '|' not in line:
					continue
				
				values = [val.strip() for val in line.split('|')]
				
				# Only process if we have enough values
				if len(values) >= len(header_columns):
					row_dict = {}
					for i, col in enumerate(header_columns):
						if i < len(values):
							row_dict[col] = values[i]
					all_violations.append(row_dict)
			
			if not all_violations:
				logger.error("No violation records parsed from agent result")
				logger.error(f"Parsed {len(lines)} lines, header at {header_idx}")
				return False
			
			logger.info(f"Successfully parsed {len(all_violations)} violation records")
			
			# Convert to DataFrame and save as CSV
			df = pd.DataFrame(all_violations)
			
			# Ensure directory exists
			REFERENCE_DATA_DIR.mkdir(parents=True, exist_ok=True)
			dest_file = REFERENCE_DATA_DIR / "hcfl_code_enforcement_violations.csv"
			
			df.to_csv(dest_file, index=False)
			size_mb = dest_file.stat().st_size / (1024 ** 2)
			logger.info(f"Violation report saved to {dest_file} ({size_mb:.2f} MB)")
			logger.info(f"Columns: {df.columns.tolist()}")
			logger.info(f"Sample of first row: {dict(df.iloc[0]) if len(df) > 0 else 'N/A'}")
			
			return True
			
		except Exception as e:
			logger.error(f"Browser agent execution failed: {e}")
			logger.debug(traceback.format_exc())
			return False
			
	except Exception as e:
		logger.error(f"Browser-use scraping failed: {e}")
		logger.debug(traceback.format_exc())
		return False


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
	args = parser.parse_args()

	logger.info("=" * 60)
	logger.info("Violation Scraping Pipeline Starting")
	logger.info("=" * 60)
	
	if args.start_date or args.end_date:
		logger.info(f"\nCustom date range specified:")
		logger.info(f"  Start: {args.start_date or 'yesterday'}")
		logger.info(f"  End: {args.end_date or 'today'}")
	
	logger.info("\nAttempting browser-use table scraping...")
	logger.info("Extracting violation data directly from HTML table")
	success = asyncio.run(scrape_violations_with_browser_use(
		start_date=args.start_date,
		end_date=args.end_date
	))
	
	if success:
		logger.info("=" * 60)
		logger.info("✓ Pipeline completed successfully!")
		logger.info("=" * 60)
	else:
		logger.error("=" * 60)
		logger.error("✗ Violation report download/scraping failed")
		logger.error("=" * 60)




