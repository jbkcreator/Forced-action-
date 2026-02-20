"""
Code Enforcement Violations Data Collection Pipeline

This module automates the download of code enforcement violation records from the
Hillsborough County Accela enforcement portal. It uses browser automation via
browser_use and Claude Sonnet 4.5 to navigate the portal, apply filters, and
export violation data.

The pipeline performs the following steps:
    1. Launches a browser agent to navigate the Accela enforcement portal
    2. Applies date filters (2025 start year to current date)
    3. Searches for and exports all violation records
    4. Saves the downloaded file with a standardized filename

Author: Distressed Property Intelligence Platform
"""

import asyncio
import os
import shutil
import time
import traceback
from pathlib import Path
from typing import Optional

from browser_use import Agent, ChatAnthropic

from config.settings import settings
from config.constants import (
	REFERENCE_DATA_DIR,
	DOWNLOAD_FILE_PATTERNS,
	VIOLATION_SEARCH_URL,
	TEMP_DOWNLOADS_DIR,
	BROWSER_DOWNLOAD_TEMP_PATTERN,
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


def _recent_download(start_time: float) -> Optional[Path]:
	"""
	Locate the most recently downloaded file after the specified start time.
	
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
	
	def scan(folder: Path):
		"""Find files in folder that match download patterns and were created after start_time."""
		found = []
		if not folder.exists():
			return found
		for pattern in DOWNLOAD_FILE_PATTERNS:
			for candidate in folder.glob(pattern):
				try:
					if candidate.stat().st_mtime >= start_time:
						found.append(candidate)
				except FileNotFoundError:
					logger.debug(f"File disappeared during check: {candidate}")
					continue
		return found
	
	candidates = scan(REFERENCE_DATA_DIR)
	logger.debug(f"Found {len(candidates)} candidate files in {REFERENCE_DATA_DIR}")
	
	temp_base = TEMP_DOWNLOADS_DIR
	if temp_base.exists():
		for download_dir in temp_base.glob(BROWSER_DOWNLOAD_TEMP_PATTERN):
			temp_candidates = scan(download_dir)
			candidates.extend(temp_candidates)
			logger.debug(f"Found {len(temp_candidates)} candidate files in {download_dir}")
	
	if not candidates:
		logger.warning("No recent download files found")
		return None
	
	# Return the most recently modified file
	most_recent = max(candidates, key=lambda path: path.stat().st_mtime)
	logger.debug(f"Selected most recent file: {most_recent}")
	return most_recent


async def download_violation_report(wait_after_download: int = 30) -> bool:
	"""
	Automate the Accela portal to download code enforcement violation records.
	
	This function launches a browser agent powered by Claude Sonnet 4.5 to navigate
	the Hillsborough County Accela enforcement portal, set date filters, and export
	violation records to a CSV or Excel file.
	
	Args:
		wait_after_download: Seconds to wait for download to complete after export (default: 30)
		
	Returns:
		bool: True if download and file processing succeeded, False otherwise
		
	Raises:
		Exception: Logs and returns False on browser automation or file operation failures
		
	Example:
		>>> success = await download_violation_report(wait_after_download=40)
		>>> if success:
		...     print("Violation report downloaded successfully")
	"""
	
	try:
		REFERENCE_DATA_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured download directory exists: {REFERENCE_DATA_DIR}")
		
		start_time = time.time()
		dest_stub = "hcfl_code_enforcement_violations"
		
		today = time.strftime("%m/%d/%Y")
		
		# Load task prompt from YAML configuration
		try:
			instructions = get_prompt(
				"violation_prompts.yaml",
				"violation_export.task_template",
				url=VIOLATION_SEARCH_URL,
				today_date=today
			)
		except Exception as e:
			logger.error(f"Failed to load prompt from YAML: {e}")
			raise
		
		logger.info("Launching browser agent to download code enforcement violation report")
		logger.debug(f"Download directory: {REFERENCE_DATA_DIR}")
		
		agent = Agent(
			task=instructions,
			llm=llm,
			max_steps=15,
			browser_context_config={
				"headless": True,
				"save_downloads_path": str(REFERENCE_DATA_DIR.resolve()),
			},
		)
		
		try:
			history = await agent.run()
			
			if not history.is_done():
				logger.warning("Agent could not finish the Accela workflow within 15 steps")
				logger.warning("Browser may be unresponsive. Check logs.")
				return False
			
			logger.info("Agent workflow completed. Waiting for download to finalize...")
			await asyncio.sleep(wait_after_download)
			
		except Exception as e:
			logger.error(f"Browser agent execution failed: {e}")
			logger.debug(traceback.format_exc())
			return False
		
		# Look for downloaded file
		downloaded = _recent_download(start_time)
		if not downloaded or not downloaded.exists():
			logger.error("Could not locate the downloaded violation report")
			return False
		
		# Rename and move file to final location
		final_ext = downloaded.suffix.lower() or ".csv"
		dest_file = REFERENCE_DATA_DIR / f"{dest_stub}{final_ext}"
		
		if dest_file.exists():
			logger.debug(f"Removing existing file: {dest_file}")
			dest_file.unlink()
		
		logger.info(f"Moving downloaded file: {downloaded} -> {dest_file}")
		shutil.move(str(downloaded), str(dest_file))
		
		size_mb = dest_file.stat().st_size / (1024 ** 2)
		logger.info(f"Violation report saved to {dest_file} ({size_mb:.1f} MB)")
		
		# Clean up temp directory
		temp_dir = downloaded.parent
		if temp_dir.exists() and temp_dir.name.startswith("browser-use-downloads-"):
			try:
				shutil.rmtree(temp_dir)
				logger.debug(f"Cleaned up temp directory: {temp_dir}")
			except Exception as exc:
				logger.warning(f"Failed to clean temp directory {temp_dir}: {exc}")
		
		return True
		
	except Exception as e:
		logger.error(f"Error during violation report download: {e}")
		logger.debug(traceback.format_exc())
		return False


if __name__ == "__main__":
	asyncio.run(download_violation_report())
