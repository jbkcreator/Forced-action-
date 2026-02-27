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


async def scrape_permits_with_playwright(
	start_date: str = None,
	end_date: str = None,
	headless: bool = True,
	debug: bool = False,
) -> Optional[Path]:
	"""
	Primary Playwright scraper — deterministic, no AI credits consumed.

	Navigates the Accela building permit portal, fills date filters, then tries
	the "Download results" button to get all records in one shot. Falls back to
	page-by-page table scraping if the download fails.
	Raises on any failure so the caller can fall back to browser_use.

	Args:
		start_date: Start date in YYYY-MM-DD format (default: yesterday)
		end_date:   End date in YYYY-MM-DD format (default: today)
		headless:   Run browser headlessly (default True). Set False with xvfb-run.
		debug:      Save screenshots + HTML dumps to data/debug/playwright/permits/.

	Returns:
		Path to saved CSV, True if all records already in DB, or raises on failure.
	"""
	from playwright.async_api import async_playwright

	if end_date:
		end_dt = datetime.strptime(end_date, "%Y-%m-%d")
	else:
		end_dt = datetime.now()
	if start_date:
		start_dt = datetime.strptime(start_date, "%Y-%m-%d")
	else:
		start_dt = end_dt - timedelta(days=1)

	end_date_str   = end_dt.strftime("%m/%d/%Y")
	start_date_str = start_dt.strftime("%m/%d/%Y")

	logger.info(f"[Playwright] Scraping permits {start_date_str} → {end_date_str}")

	# Exact element IDs from the Accela portal HTML
	START_DATE_ID   = "ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate"
	END_DATE_ID     = "ctl00_PlaceHolderMain_generalSearchForm_txtGSEndDate"
	SEARCH_BTN_ID   = "ctl00_PlaceHolderMain_btnNewSearch"
	DOWNLOAD_BTN_ID = "ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_gdvPermitListtop4btnExport"

	debug_dir = Path("data/debug/playwright/permits")
	if debug:
		debug_dir.mkdir(parents=True, exist_ok=True)
		logger.info(f"[Playwright][DEBUG] Screenshots/HTML saved to {debug_dir.resolve()}")

	async def snap(page, name):
		if not debug:
			return
		await page.screenshot(path=str(debug_dir / f"{name}.png"), full_page=True)
		(debug_dir / f"{name}.html").write_text(await page.content(), encoding="utf-8")
		logger.info(f"[Playwright][DEBUG] Saved {name}.png + {name}.html")

	async def js_fill_date(page, input_id: str, value: str):
		"""Set a masked date input value and fire all events the portal listens to."""
		await page.evaluate(
			'''([id, val]) => {
				const el = document.getElementById(id);
				if (!el) throw new Error("Input #" + id + " not found");
				el.value = val;
				el.dispatchEvent(new Event("focus",  {bubbles: true}));
				el.dispatchEvent(new Event("input",  {bubbles: true}));
				el.dispatchEvent(new Event("change", {bubbles: true}));
				el.dispatchEvent(new Event("blur",   {bubbles: true}));
			}''',
			[input_id, value],
		)

	all_rows = []

	async with async_playwright() as pw:
		browser = await pw.chromium.launch(
			headless=headless,
			args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--no-zygote', '--single-process'],
		)
		ctx = await browser.new_context(
			viewport={'width': 1280, 'height': 900},
			accept_downloads=True,
		)
		page = await ctx.new_page()

		try:
			# 1. Load the portal
			await page.goto(PERMIT_SEARCH_URL, wait_until='domcontentloaded', timeout=30000)
			await page.wait_for_timeout(2000)
			await snap(page, "01_page_loaded")

			# 2. Expand search form if collapsed
			try:
				toggle = page.locator('a:has-text("Search Applications")').first
				if await toggle.is_visible(timeout=3000):
					await toggle.click()
					await page.wait_for_timeout(1500)
					await snap(page, "02_form_expanded")
			except Exception:
				pass  # Already visible

			# 3. Fill start date using exact known ID
			await page.wait_for_selector(f'#{START_DATE_ID}', timeout=10000)
			await js_fill_date(page, START_DATE_ID, start_date_str)
			logger.info(f"[Playwright] Start date set: {start_date_str}")

			# 4. Fill end date
			await js_fill_date(page, END_DATE_ID, end_date_str)
			logger.info(f"[Playwright] End date set: {end_date_str}")
			await snap(page, "03_dates_filled")

			# 5. Submit search using exact known button ID
			await page.evaluate(f'''() => {{
				const btn = document.getElementById("{SEARCH_BTN_ID}");
				if (!btn) throw new Error("Search button #{SEARCH_BTN_ID} not found");
				btn.click();
			}}''')
			await page.wait_for_load_state('networkidle', timeout=30000)
			await snap(page, "04_search_results")
			logger.info("[Playwright] Search submitted")

			# 6. Try "Download results" button — fetches ALL records in one shot
			downloaded = False
			try:
				async with page.expect_download(timeout=30000) as dl_info:
					await page.evaluate(f'''() => {{
						const btn = document.getElementById("{DOWNLOAD_BTN_ID}");
						if (!btn) throw new Error("Download button not found");
						btn.click();
					}}''')
				download = await dl_info.value
				dl_filename = download.suggested_filename or "permits_download.csv"
				dl_path = RAW_PERMIT_DIR / "temp" / dl_filename
				dl_path.parent.mkdir(parents=True, exist_ok=True)
				await download.save_as(dl_path)
				logger.info(f"[Playwright] Downloaded: {dl_filename} ({dl_path.stat().st_size:,} bytes)")

				if dl_filename.endswith('.csv'):
					df_dl = pd.read_csv(dl_path)
				elif dl_filename.endswith(('.xls', '.xlsx')):
					df_dl = pd.read_excel(dl_path)
				else:
					try:
						df_dl = pd.read_csv(dl_path)
					except Exception:
						df_dl = pd.read_excel(dl_path)

				all_rows = df_dl.to_dict('records')
				logger.info(f"[Playwright] Download method: {len(all_rows)} records")
				downloaded = True

			except Exception as e:
				logger.warning(f"[Playwright] Download button failed ({e}) — scraping page by page")

			# 7. Fallback: page-by-page table scraping
			if not downloaded:
				page_num = 1
				while True:
					logger.info(f"[Playwright] Scraping page {page_num}...")

					rows = await page.evaluate('''() => {
						const result = [];
						const dataRow = document.querySelector("tr.ACA_TabRow_Odd, tr.ACA_TabRow_Even");
						if (!dataRow) return result;
						const table = dataRow.closest("table");
						if (!table) return result;

						const ths = table.querySelectorAll("th");
						const headers = Array.from(ths).map(th =>
							th.classList.contains("ACA_Hide") ? "Address" : th.innerText.trim()
						);

						const dataRows = table.querySelectorAll("tr.ACA_TabRow_Odd, tr.ACA_TabRow_Even");
						for (const row of dataRows) {
							const cells = row.querySelectorAll("td");
							const obj = {};
							cells.forEach((td, i) => {
								const key = headers[i];
								if (key) obj[key] = td.innerText.trim();
							});
							result.push(obj);
						}
						return result;
					}''')

					if rows:
						all_rows.extend(rows)
						logger.info(f"[Playwright] Page {page_num}: {len(rows)} rows (total: {len(all_rows)})")
					else:
						logger.info(f"[Playwright] Page {page_num}: no rows — done")
						await snap(page, f"page_{page_num:02d}_empty")
						break

					# Click the next numbered page link
					has_next = await page.evaluate('''() => {
						const sel = document.querySelector("span.SelectedPageButton");
						if (!sel) return false;
						const cur = parseInt(sel.textContent.trim());
						for (const a of document.querySelectorAll(".aca_pagination_td a")) {
							if (parseInt(a.textContent.trim()) === cur + 1) {
								a.click();
								return true;
							}
						}
						return false;
					}''')
					if not has_next:
						logger.info("[Playwright] Last page reached")
						break

					await page.wait_for_load_state('networkidle', timeout=30000)
					page_num += 1

			logger.info(f"[Playwright] Total records extracted: {len(all_rows)}")

		finally:
			await browser.close()

	if not all_rows:
		raise RuntimeError("Playwright extracted 0 records — portal may be down or selectors changed")

	# Normalize column names
	COLUMN_ALIASES = {
		'Date':            ['Date', 'Filed Date', 'Opened Date', 'Application Date'],
		'Record Number':   ['Record Number', 'Application Number', 'Record #'],
		'Record Type':     ['Record Type', 'Application Type', 'Type'],
		'Description':     ['Description', 'Desc'],
		'Project Name':    ['Project Name', 'Project'],
		'Related Records': ['Related Records', 'Related'],
		'Status':          ['Status', 'Application Status'],
		'Short Notes':     ['Short Notes', 'Notes', 'Short Note'],
		'Address':         ['Address', 'Location', 'Parcel Address'],
	}
	normalized = []
	for row in all_rows:
		norm_row = {}
		for target_col, aliases in COLUMN_ALIASES.items():
			for alias in aliases:
				if alias in row:
					norm_row[target_col] = row[alias]
					break
			if target_col not in norm_row:
				norm_row[target_col] = ''
		normalized.append(norm_row)

	df = pd.DataFrame(normalized)

	logger.info("DB DEDUPLICATION: Checking for existing permits")
	initial_count = len(df)
	df_new = filter_new_records(df, 'permits')

	if df_new.empty:
		logger.info("All permits already exist in database — nothing new to load")
		return True

	new_dir = RAW_PERMIT_DIR / "new"
	new_dir.mkdir(parents=True, exist_ok=True)
	today_str = datetime.now().strftime("%Y%m%d")
	csv_path = new_dir / f"building_permits_new_{today_str}.csv"
	df_new.to_csv(csv_path, index=False)
	size_mb = csv_path.stat().st_size / (1024 ** 2)
	logger.info(f"[Playwright] Saved {len(df_new)} new permits to {csv_path} ({size_mb:.2f} MB)")
	logger.info(f"[Playwright] Filtered {initial_count - len(df_new)} already-existing records")
	return csv_path


async def scrape_permits_with_browser_use(
	start_date: str = None,
	end_date: str = None,
) -> Optional[Path]:
	"""
	AI-led table extraction method for permit records (fallback).

	Args:
		start_date: Start date in YYYY-MM-DD format (default: yesterday)
		end_date:   End date in YYYY-MM-DD format (default: today)

	Returns:
		Optional[Path]: Path to saved CSV file if successful, None otherwise
	"""
	try:
		if end_date:
			end_dt = datetime.strptime(end_date, "%Y-%m-%d")
		else:
			end_dt = datetime.now()
		if start_date:
			start_dt = datetime.strptime(start_date, "%Y-%m-%d")
		else:
			start_dt = end_dt - timedelta(days=1)

		end_date_str   = end_dt.strftime("%m/%d/%Y")
		start_date_str = start_dt.strftime("%m/%d/%Y")
		
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


async def run_permit_pipeline(
	start_date: str = None,
	end_date: str = None,
	scraper: str = "auto",
	headless: bool = True,
	debug: bool = False,
):
	"""
	Execute the complete permit data collection pipeline.

	Args:
		start_date: Start date in YYYY-MM-DD format (default: yesterday)
		end_date:   End date in YYYY-MM-DD format (default: today)
		scraper:    "auto" (playwright→ai fallback), "playwright", or "ai"
		headless:   Run browser headlessly
		debug:      Save screenshots + HTML dumps

	Raises:
		Exception: Re-raises any exceptions after logging
	"""
	logger.info("=" * 60)
	logger.info("HILLSBOROUGH COUNTY BUILDING PERMITS - DATA COLLECTION")
	logger.info("=" * 60)

	try:
		file_path = None
		active_scraper = scraper

		if active_scraper in ("auto", "playwright"):
			logger.info("Scraping permits via Playwright (primary method)")
			try:
				file_path = await scrape_permits_with_playwright(
					start_date=start_date, end_date=end_date,
					headless=headless, debug=debug,
				)
				logger.info("Playwright scraping succeeded")
			except Exception as e:
				if active_scraper == "playwright":
					raise
				logger.warning(f"Playwright scraping failed: {e}")
				logger.warning("Falling back to browser-use AI method...")
				active_scraper = "ai"

		if active_scraper == "ai" and file_path is None:
			logger.info("Running browser-use AI scraper...")
			file_path = await scrape_permits_with_browser_use(
				start_date=start_date, end_date=end_date,
			)

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
		"--start-date",
		type=str,
		help="Start date in YYYY-MM-DD format (default: yesterday). Example: 2026-02-26"
	)
	parser.add_argument(
		"--end-date",
		type=str,
		help="End date in YYYY-MM-DD format (default: today). Example: 2026-02-27"
	)
	add_load_to_db_arg(parser)
	parser.add_argument(
		"--scraper",
		choices=["auto", "playwright", "ai"],
		default="auto",
		help="Which scraper to use: auto (playwright→ai fallback), playwright only, ai only (default: auto)"
	)
	parser.add_argument(
		"--debug",
		action="store_true",
		help="Save screenshots + HTML dumps at each step to data/debug/playwright/permits/ (works headless)"
	)
	parser.add_argument(
		"--headful",
		action="store_true",
		help="Open a real browser window (requires: xvfb-run -a python -m ...)"
	)
	args = parser.parse_args()

	if args.headful:
		logger.info("[HEADFUL] Browser window enabled — requires xvfb-run on server")
	if args.debug:
		logger.info("[DEBUG] Screenshot/HTML dumps enabled → data/debug/playwright/permits/")

	try:
		asyncio.run(run_permit_pipeline(
			start_date=args.start_date,
			end_date=args.end_date,
			scraper=args.scraper,
			headless=not args.headful,
			debug=args.debug,
		))
		
		# Load to database if requested
		if args.load_to_db:
			# Find the most recent permit CSV in new/ subdirectory
			new_dir = RAW_PERMIT_DIR / "new"
			csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
			if csv_files:
				csv_to_load = csv_files[0]
				logger.info(f"Loading to database: {csv_to_load}")
				
				# Load to DB
				load_scraped_data_to_db('permits', csv_to_load, destination_dir=RAW_PERMIT_DIR)
			else:
				logger.error("No permit CSV file found to load")
				sys.exit(1)
		
		sys.exit(0)
		
	except Exception as e:
		logger.error(f"Pipeline failed: {e}")
		sys.exit(1)
