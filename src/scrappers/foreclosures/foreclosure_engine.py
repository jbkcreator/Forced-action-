"""
Foreclosure Auction Data Collection Pipeline

This module automates the scraping of foreclosure auction records from the
Hillsborough County RealForeclose auction calendar. It uses Playwright as the
primary scraper (deterministic, no AI credits) and falls back to browser_use
with Claude Sonnet if Playwright fails.

The pipeline performs the following steps:
    1. Navigates to the RealForeclose calendar preview for a specific date
    2. Waits for #BID_WINDOW_CONTAINER to load
    3. Extracts all .AUCTION_ITEM.PREVIEW cards across Running/Waiting/Closed sections
    4. Handles pagination within each section
    5. Deduplicates against the database and saves new records to CSV

Author: Distressed Property Intelligence Platform
"""

import argparse
import asyncio
import datetime as dt
import traceback
from pathlib import Path
from typing import Optional

import pandas as pd
from browser_use import Agent, ChatAnthropic, Browser

from config.settings import settings
from config.constants import (
	REALFORECLOSE_BASE_URL,
	AUCTION_DATE_FORMAT,
	RAW_FORECLOSURE_DIR,
	PROCESSED_DATA_DIR,
	BROWSER_MODEL,
	BROWSER_TEMPERATURE,
)
from src.core.database import get_db_context
from src.loaders.foreclosures import ForeclosureLoader
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt
from src.utils.db_deduplicator import filter_new_records

# Initialize logging
setup_logging()
logger = get_logger(__name__)

# LLM configuration for browser-use AI fallback
llm = ChatAnthropic(
	model=BROWSER_MODEL,
	timeout=180,
	api_key=settings.anthropic_api_key.get_secret_value(),
	temperature=BROWSER_TEMPERATURE,
)

RAW_FORECLOSURE_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def build_preview_url(auction_date: dt.date) -> str:
	"""
	Build the RealForeclose calendar preview URL for a given auction date.

	Example:
		>>> build_preview_url(dt.date(2026, 2, 19))
		'https://www.hillsborough.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=02/19/2026'
	"""
	date_str = auction_date.strftime(AUCTION_DATE_FORMAT)
	return f"{REALFORECLOSE_BASE_URL}?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"


def _save_new_foreclosures(df: pd.DataFrame, auction_date: dt.date) -> Optional[Path]:
	"""
	Deduplicate DataFrame against the database and save new records to new/ directory.

	Returns the saved file path, or None if all records already exist.
	"""
	logger.info("=" * 60)
	logger.info("DB DEDUPLICATION: Checking for existing foreclosures")
	logger.info("=" * 60)

	initial_count = len(df)
	df_new = filter_new_records(df, 'foreclosures')

	if df_new.empty:
		logger.info("✓ All foreclosures already exist in database - nothing new")
		return None

	new_dir = RAW_FORECLOSURE_DIR / "new"
	new_dir.mkdir(parents=True, exist_ok=True)
	final_file = new_dir / f"foreclosures_new_{auction_date:%Y%m%d}.csv"

	df_new.to_csv(final_file, index=False)
	size_mb = final_file.stat().st_size / (1024 ** 2)
	logger.info(f"Saved {len(df_new)} NEW foreclosures to {final_file} ({size_mb:.2f} MB)")
	logger.info(f"Filtered {initial_count - len(df_new)} existing records")

	return final_file


# ── Playwright scraper ────────────────────────────────────────────────────────

async def _extract_auction_item_playwright(item) -> Optional[dict]:
	"""Extract all fields from a single .AUCTION_ITEM.PREVIEW locator."""
	try:
		record = {}

		# Auction ID from the 'aid' attribute on the card element
		record['Auction ID'] = await item.get_attribute('aid') or ''

		# Status / start-time from AUCTION_STATS section
		status_lbl = item.locator('.ASTAT_MSGA')
		status_val = item.locator('.ASTAT_MSGB')
		label_text = (await status_lbl.inner_text()).strip() if await status_lbl.count() else ''
		value_text = (await status_val.inner_text()).strip() if await status_val.count() else ''

		if 'Auction Starts' in label_text:
			record['Auction Start Date/Time'] = value_text
			record['Auction Status'] = 'Waiting'
		else:
			# "Auction Status" label with value like "Canceled per County"
			record['Auction Status'] = value_text
			record['Auction Start Date/Time'] = ''

		# Detail table: iterate AD_LBL / AD_DTA row pairs
		rows = item.locator('.ad_tab tr')
		row_count = await rows.count()
		address_parts = []

		for i in range(row_count):
			row = rows.nth(i)
			lbl_el = row.locator('.AD_LBL')
			dta_el = row.locator('.AD_DTA')
			if not await lbl_el.count() or not await dta_el.count():
				continue
			lbl = (await lbl_el.inner_text()).strip().rstrip(':')
			dta = (await dta_el.inner_text()).strip()

			if lbl == 'Auction Type':
				record['Auction Type'] = dta
			elif lbl == 'Case #':
				record['Case Number'] = dta
			elif lbl == 'Final Judgment Amount':
				record['Judgment Amount'] = dta
			elif lbl == 'Parcel ID':
				record['Parcel ID'] = dta
			elif lbl == 'Property Address' or (lbl == '' and dta):
				# First row is street address, second blank-label row is city/state/zip
				address_parts.append(dta)
			elif lbl == 'Assessed Value':
				record['Assessed Value'] = dta
			elif lbl == 'Plaintiff Max Bid':
				record['Plaintiff Max Bid'] = dta

		record['Property Address'] = ', '.join(filter(None, address_parts))
		return record

	except Exception as e:
		logger.warning(f"[Playwright] Failed to extract auction item: {e}")
		return None


async def scrape_foreclosures_with_playwright(
	auction_date: dt.date,
	debug: bool = False,
) -> Optional[Path]:
	"""
	Primary Playwright scraper — deterministic, no AI credits consumed.

	Reads #BID_WINDOW_CONTAINER and extracts .AUCTION_ITEM.PREVIEW cards from
	all three sections (Running/Waiting/Closed), handling pagination per section.

	Args:
		auction_date: Date of the auction to scrape.
		debug:        Save screenshots + HTML to data/debug/playwright/foreclosures/.

	Returns:
		Path to the deduplicated CSV, or None if nothing new.

	Raises:
		RuntimeError: If no auction items are found (page empty or selectors changed).
	"""
	from playwright.async_api import async_playwright

	preview_url = build_preview_url(auction_date)
	logger.info(f"[Playwright] Scraping foreclosures for {auction_date}: {preview_url}")

	debug_dir = Path("data/debug/playwright/foreclosures")
	if debug:
		debug_dir.mkdir(parents=True, exist_ok=True)
		logger.info(f"[Playwright][DEBUG] Screenshots/HTML → {debug_dir.resolve()}")

	async def save_debug(page, name: str):
		if debug:
			await page.screenshot(path=str(debug_dir / f"{name}.png"), full_page=True)
			(debug_dir / f"{name}.html").write_text(await page.content(), encoding="utf-8")
			logger.info(f"[Playwright][DEBUG] Saved {name}.png + {name}.html")

	async with async_playwright() as pw:
		browser = await pw.chromium.launch(
			headless=True,
			args=[
				'--no-sandbox',
				'--disable-setuid-sandbox',
				'--disable-dev-shm-usage',
				'--disable-gpu',
			],
		)
		page = await browser.new_page()
		try:
			await page.goto(preview_url, wait_until="domcontentloaded", timeout=60_000)

			# Give JS time to render the auction container
			try:
				await page.wait_for_selector(
					"#BID_WINDOW_CONTAINER", state="attached", timeout=30_000
				)
			except Exception:
				# Container absent → no auctions scheduled for this date
				page_text = await page.inner_text("body")
				logger.info(
					f"[Playwright] #BID_WINDOW_CONTAINER not found for {auction_date} "
					f"— likely no auctions scheduled. Page snippet: {page_text[:300]!r}"
				)
				return None

			await save_debug(page, "01_loaded")

			all_records = []

			# Three auction sections: Running (R), Waiting (W), Closed/Canceled (C)
			# Only W and C have pagination controls.
			sections = [
				("Area_R", "R", None,     None),
				("Area_W", "W", "maxWA",  "Head_W"),
				("Area_C", "C", "maxCA",  "Head_C"),
			]

			for area_id, _letter, max_pg_id, head_class in sections:
				page_num = 1
				while True:
					area = page.locator(f"#{area_id}")
					items = area.locator(".AUCTION_ITEM.PREVIEW")
					count = await items.count()
					logger.info(f"[Playwright] #{area_id} page {page_num}: {count} items")

					for i in range(count):
						record = await _extract_auction_item_playwright(items.nth(i))
						if record:
							all_records.append(record)

					# Stop if no pagination for this section
					if not max_pg_id:
						break

					max_el = page.locator(f"#{max_pg_id}")
					max_text = (await max_el.inner_text()).strip() if await max_el.count() else "1"
					max_pages = int(max_text) if max_text.isdigit() else 1

					if page_num >= max_pages:
						break

					# Click the bottom "Next Page" button for this section
					next_btn = page.locator(f".{head_class} .PageFrame .PageRight").last
					await next_btn.click()
					await page.wait_for_timeout(2_000)
					await save_debug(page, f"{area_id}_page{page_num + 1}")
					page_num += 1

			await save_debug(page, "99_done")

		finally:
			await browser.close()

	if not all_records:
		raise RuntimeError(
			"[Playwright] No auction items found — page may be empty or selectors changed"
		)

	logger.info(f"[Playwright] Extracted {len(all_records)} total auction records")
	df = pd.DataFrame(all_records)
	return _save_new_foreclosures(df, auction_date)


# ── AI fallback scraper ───────────────────────────────────────────────────────

async def _scrape_with_ai(
	auction_date: dt.date,
	wait_after_scrape: int = 10,
) -> Optional[Path]:
	"""
	AI browser-use fallback scraper. Used when Playwright fails or explicitly requested.
	"""
	import time

	start_time = time.time()
	preview_url = build_preview_url(auction_date)
	iso_date = auction_date.strftime("%Y-%m-%d")

	temp_dir = RAW_FORECLOSURE_DIR / "temp"
	temp_dir.mkdir(parents=True, exist_ok=True)
	temp_file = temp_dir / f"foreclosures_temp_{auction_date:%Y%m%d}.csv"

	try:
		task_instructions = get_prompt(
			"foreclosure_prompts.yaml",
			"auction_scrape.task_template",
			preview_url=preview_url,
			dest_file=str(temp_file.resolve()),
		)
	except Exception as e:
		logger.error(f"[AI] Failed to load prompt from YAML: {e}")
		raise

	logger.info(f"[AI] Launching browser agent for {iso_date}: {preview_url}")

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
		],
	)

	agent = Agent(
		task=task_instructions,
		llm=llm,
		max_steps=50,
		browser=browser,
	)

	try:
		history = await agent.run()

		if not history.is_done():
			logger.warning("[AI] Agent could not complete within 50 steps")
			return None

		logger.info("[AI] Agent workflow completed. Waiting for file write...")
		await asyncio.sleep(wait_after_scrape)

	except Exception as e:
		logger.error(f"[AI] Browser agent execution failed: {e}")
		logger.debug(traceback.format_exc())
		return None

	if not temp_file.exists():
		logger.error("[AI] Could not locate the scraped data file")
		return None

	file_size_kb = temp_file.stat().st_size / 1024
	logger.info(f"[AI] Temp file saved: {temp_file} ({file_size_kb:.1f} KB)")

	try:
		df = pd.read_csv(temp_file)
		result = _save_new_foreclosures(df, auction_date)
		temp_file.unlink()
		return result
	except Exception as e:
		logger.error(f"[AI] Post-processing failed: {e}")
		logger.debug(traceback.format_exc())
		return temp_file


# ── Orchestrator ──────────────────────────────────────────────────────────────

async def scrape_realforeclose_calendar(
	auction_date: dt.date,
	wait_after_scrape: int = 10,
	scraper: str = "auto",
) -> Optional[Path]:
	"""
	Scrape RealForeclose auction calendar for a given date.

	Args:
		auction_date:     Date to scrape.
		wait_after_scrape: Seconds to wait after AI agent finishes (AI path only).
		scraper:          'auto' (playwright → AI fallback), 'playwright', or 'ai'.

	Returns:
		Path to deduplicated CSV of new records, or None if nothing new.
	"""
	csv_file = None
	playwright_succeeded = False  # True = Playwright ran cleanly (even if no records)
	MAX_PLAYWRIGHT_RETRIES = 5

	if scraper in ("auto", "playwright"):
		logger.info("\nAttempting Playwright scraping (primary method)...")
		playwright_error = None
		for attempt in range(1, MAX_PLAYWRIGHT_RETRIES + 1):
			try:
				csv_file = await scrape_foreclosures_with_playwright(auction_date)
				playwright_succeeded = True
				logger.info("Playwright scraping succeeded")
				playwright_error = None
				break
			except Exception as e:
				if scraper == "playwright":
					raise
				playwright_error = e
				if attempt < MAX_PLAYWRIGHT_RETRIES:
					logger.warning(f"[Playwright] Attempt {attempt}/{MAX_PLAYWRIGHT_RETRIES} failed: {e} — retrying in 5s...")
					await asyncio.sleep(5)
					continue
				logger.warning(f"[Playwright] All {MAX_PLAYWRIGHT_RETRIES} retries failed")

	# Only fall back to AI if Playwright raised on every attempt (didn't complete cleanly)
	if scraper == "ai" or (scraper == "auto" and not playwright_succeeded):
		logger.info("Running browser-use AI scraper...")
		csv_file = await _scrape_with_ai(auction_date, wait_after_scrape)

	return csv_file


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_auction_date(date_str: Optional[str]) -> dt.date:
	"""Parse YYYY-MM-DD string or default to today."""
	if date_str:
		try:
			return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
		except ValueError as e:
			logger.error(f"Invalid date format '{date_str}': {e}")
			raise
	return dt.date.today()


def main():
	parser = argparse.ArgumentParser(
		description="Scrape Hillsborough RealForeclose auction calendar"
	)
	parser.add_argument(
		"--date",
		help="Auction date (YYYY-MM-DD). Defaults to today.",
	)
	parser.add_argument(
		"--wait",
		type=int,
		default=10,
		help="Seconds to wait after AI agent completes (AI path only, default: 10).",
	)
	parser.add_argument(
		"--scraper",
		choices=["auto", "playwright", "ai"],
		default="auto",
		help="Which scraper to use: auto (playwright → AI fallback), playwright only, ai only (default: auto)",
	)
	parser.add_argument(
		"--debug",
		action="store_true",
		help="Save screenshots + HTML dumps to data/debug/playwright/foreclosures/",
	)
	parser.add_argument(
		"--load-to-db",
		action="store_true",
		help="Automatically load scraped data into database after scraping",
	)

	args = parser.parse_args()

	if args.debug:
		logger.info("[DEBUG] Screenshot/HTML dumps enabled → data/debug/playwright/foreclosures/")

	try:
		auction_date = _parse_auction_date(args.date)

		result_file = asyncio.run(
			scrape_realforeclose_calendar(
				auction_date=auction_date,
				wait_after_scrape=args.wait,
				scraper=args.scraper,
			)
		)

		if not result_file:
			logger.warning("No new foreclosure records to process today")
			return

		logger.info(f"Foreclosure data collected for {auction_date.strftime('%Y-%m-%d')}")
		logger.info(f"Output file: {result_file}")

		if args.load_to_db:
			logger.info("\n" + "=" * 60)
			logger.info("Loading foreclosures into database...")
			logger.info("=" * 60)

			try:
				with get_db_context() as session:
					loader = ForeclosureLoader(session)
					matched, unmatched, skipped = loader.load_from_csv(
						str(result_file),
						skip_duplicates=True,
					)
					session.commit()

					logger.info(f"\n{'='*60}")
					logger.info("DATABASE LOAD SUMMARY")
					logger.info(f"{'='*60}")
					logger.info(f"  Matched:   {matched:>6}")
					logger.info(f"  Unmatched: {unmatched:>6}")
					logger.info(f"  Skipped:   {skipped:>6}")
					total = matched + unmatched + skipped
					match_rate = (matched / total * 100) if total > 0 else 0
					logger.info(f"  Match Rate: {match_rate:>5.1f}%")
					logger.info(f"{'='*60}\n")
					logger.info("✓ Database load completed!")

			except Exception as e:
				logger.error(f"Failed to load data to database: {e}")
				logger.debug(traceback.format_exc())
		else:
			logger.info("\nSkipping database load (use --load-to-db flag to enable)")

	except Exception as e:
		logger.error(f"Error in main execution: {e}")
		logger.debug(traceback.format_exc())


if __name__ == "__main__":
	main()
