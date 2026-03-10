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
from src.utils.db_deduplicator import filter_new_records
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
async def scrape_violations_with_playwright(
    start_date: str = None,
    end_date: str = None,
    headless: bool = True,
    debug: bool = False,
):
    """
    Primary Playwright scraper — deterministic, no AI credits consumed.

    Navigates the Accela enforcement portal, fills date filters, then scrapes
    page-by-page by clicking Next until Next becomes disabled (span), without
    relying on visible page numbers (no "..." handling needed).

    Args:
        start_date: Start date in YYYY-MM-DD format (default: yesterday)
        end_date:   End date in YYYY-MM-DD format (default: today)
        headless:   Run browser headlessly (default True). Set False with xvfb-run.
        debug:      Save screenshots + HTML dumps to data/debug/playwright/ at each step.

    Returns:
        - None if no rows
        - True if nothing new after DB dedupe
        - (csv_path, ) on success (kept compatible with your current return usage)
    """
    from playwright.async_api import async_playwright
    import pandas as pd
    from pathlib import Path
    from datetime import datetime, timedelta

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

    logger.info(f"[Playwright] Scraping violations {start_date_str} → {end_date_str}")

    # Exact element IDs from the Accela portal HTML
    START_DATE_ID = "ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate"
    END_DATE_ID = "ctl00_PlaceHolderMain_generalSearchForm_txtGSEndDate"
    SEARCH_BTN_ID = "ctl00_PlaceHolderMain_btnNewSearch"

    debug_dir = Path("data/debug/playwright/violations")
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
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
                "--single-process",
            ],
        )
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            accept_downloads=True,
        )
        page = await ctx.new_page()

        try:
            # 1. Load the portal
            await page.goto(VIOLATION_SEARCH_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2000)
            await snap(page, "01_page_loaded")

            # 2. Expand search form if collapsed (Accela hides it on first load)
            try:
                toggle = page.locator('a:has-text("Search Applications")').first
                if await toggle.is_visible(timeout=5000):
                    await toggle.click()
                    await page.wait_for_timeout(1500)
                    await snap(page, "02_form_expanded")
            except Exception:
                pass  # Already visible

            # 3. Fill start date using exact known ID
            await page.wait_for_selector(f"#{START_DATE_ID}", timeout=20000)
            await js_fill_date(page, START_DATE_ID, start_date_str)
            logger.info(f"[Playwright] Start date set: {start_date_str}")

            # 4. Fill end date
            await js_fill_date(page, END_DATE_ID, end_date_str)
            logger.info(f"[Playwright] End date set: {end_date_str}")
            await snap(page, "03_dates_filled")

            # 5. Submit search — wait for ASP.NET PostBack (navigation may or may not fire)
            try:
                async with page.expect_navigation(timeout=45000):
                    await page.evaluate(
                        f'''() => {{
                            const btn = document.getElementById("{SEARCH_BTN_ID}");
                            if (!btn) throw new Error("Search button #{SEARCH_BTN_ID} not found");
                            btn.click();
                        }}'''
                    )
            except Exception:
                await page.wait_for_load_state("networkidle", timeout=60000)

            await snap(page, "04_search_results")
            logger.info("[Playwright] Search submitted")

            # 6a. Wait for results table to render (UpdatePanel can delay after postback)
            try:
                await page.wait_for_selector("tr.ACA_TabRow_Odd, tr.ACA_TabRow_Even", timeout=30000)
                logger.info("[Playwright] Results table detected")
            except Exception:
                logger.warning("[Playwright] Timed out waiting for results rows — page may have no results")

            # 6b. Pagination: click Next until it becomes disabled (span).
            visited_pages = set()

            async def get_selected_page_num() -> str:
                """Authoritative current page: <span class='SelectedPageButton'>14</span>"""
                return await page.evaluate(
                    '''() => {
                        const el = document.querySelector("span.SelectedPageButton");
                        return el ? el.textContent.trim() : "";
                    }'''
                )

            async def next_is_clickable() -> bool:
                """
                Next is clickable when it's an <a>. When disabled, it renders as <span>.
                We search pagination 'PrevNext' cells and check if Next cell has an <a>.
                """
                return await page.evaluate(
                    '''() => {
                        const tds = Array.from(document.querySelectorAll("td.aca_pagination_PrevNext"));
                        const nextTd = tds.find(td => td.textContent.includes("Next"));
                        if (!nextTd) return false;
                        return !!nextTd.querySelector("a");
                    }'''
                )

            async def click_next():
                """Click the Next link using DOM click (works best with UpdatePanel)."""
                await page.evaluate(
                    '''() => {
                        const tds = Array.from(document.querySelectorAll("td.aca_pagination_PrevNext"));
                        const nextTd = tds.find(td => td.textContent.includes("Next"));
                        const a = nextTd ? nextTd.querySelector("a") : null;
                        if (!a) throw new Error("Next link not found/clickable");
                        a.click();
                    }'''
                )

            while True:
                # Read current page number
                current_page = await get_selected_page_num()
                if not current_page:
                    logger.warning("[Playwright] SelectedPageButton missing — stopping pagination.")
                    break

                # Prevent duplicates / loops
                if current_page in visited_pages:
                    logger.warning(f"[Playwright] Page {current_page} already visited — stopping to avoid duplicates.")
                    break
                visited_pages.add(current_page)

                logger.info(f"[Playwright] Scraping page {current_page}...")
                if current_page.isdigit():
                    await snap(page, f"page_{int(current_page):02d}")
                else:
                    await snap(page, f"page_{current_page}")

                # Extract rows on current page
                rows = await page.evaluate(
                    '''() => {
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
                    }'''
                )

                if rows:
                    all_rows.extend(rows)
                    logger.info(
                        f"[Playwright] Page {current_page}: scraped {len(rows)} rows (total: {len(all_rows)})"
                    )
                else:
                    logger.info(f"[Playwright] Page {current_page}: no rows found — stopping.")
                    await snap(page, f"page_{current_page}_empty")
                    break

                # Stop if Next is disabled
                if not await next_is_clickable():
                    logger.info("[Playwright] Next is not clickable — last page reached.")
                    break

                # Click Next and wait for actual page change (SelectedPageButton + table refresh)
                old_page = current_page
                old_first_row = await page.evaluate(
                    '''() => {
                        const row = document.querySelector("tr.ACA_TabRow_Odd, tr.ACA_TabRow_Even");
                        return row ? row.innerText.trim() : "";
                    }'''
                )

                await click_next()

                # Wait until selected page number changes
                await page.wait_for_function(
                    '''(oldVal) => {
                        const el = document.querySelector("span.SelectedPageButton");
                        return el && el.textContent.trim() !== oldVal;
                    }''',
                    arg=old_page,
                    timeout=45000,
                )

                # Wait until table changes (guards against scraping old DOM)
                await page.wait_for_function(
                    '''(oldText) => {
                        const row = document.querySelector("tr.ACA_TabRow_Odd, tr.ACA_TabRow_Even");
                        const now = row ? row.innerText.trim() : "";
                        return now && now !== oldText;
                    }''',
                    arg=old_first_row,
                    timeout=45000,
                )

            logger.info(f"[Playwright] Total records extracted: {len(all_rows)}")

        finally:
            await browser.close()

    if not all_rows:
        logger.info("[Playwright] 0 records found — no violations in this date range")
        return None

    # Normalize column names
    COLUMN_ALIASES = {
        "Date": ["Date", "Filed Date", "Opened Date", "Application Date"],
        "Record Number": ["Record Number", "Application Number", "Record #"],
        "Record Type": ["Record Type", "Application Type", "Type"],
        "Description": ["Description", "Desc"],
        "Project Name": ["Project Name", "Project"],
        "Related Records": ["Related Records", "Related"],
        "Status": ["Status", "Application Status"],
        "Short Notes": ["Short Notes", "Notes", "Short Note"],
        "Address": ["Address", "Location", "Parcel Address"],
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
                norm_row[target_col] = ""
        normalized.append(norm_row)

    df = pd.DataFrame(normalized)

    logger.info("DB DEDUPLICATION: Checking for existing violations")
    initial_count = len(df)
    df_new = filter_new_records(df, "violations")

    if df_new.empty:
        logger.info("All violations already exist in database — nothing new to load")
        return True

    new_dir = RAW_VIOLATIONS_DIR / "new"
    new_dir.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d")
    csv_path = new_dir / f"violations_new_{today_str}.csv"
    df_new.to_csv(csv_path, index=False)
    size_mb = csv_path.stat().st_size / (1024 ** 2)
    logger.info(f"[Playwright] Saved {len(df_new)} new violations to {csv_path} ({size_mb:.2f} MB)")
    logger.info(f"[Playwright] Filtered {initial_count - len(df_new)} already-existing records")

    return csv_path

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
			for line in lines[header_idx + 1:]:
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

	headless = not args.headful
	scraper = args.scraper
	if args.headful:
		logger.info("[HEADFUL] Browser window enabled — requires xvfb-run on server")
	if args.debug:
		logger.info("[DEBUG] Screenshot/HTML dumps enabled → data/debug/playwright/violations/")

	csv_file = None
	MAX_PLAYWRIGHT_RETRIES = 5

	if scraper in ("auto", "playwright"):
		logger.info("\nAttempting Playwright scraping (primary method)...")
		playwright_error = None
		for attempt in range(1, MAX_PLAYWRIGHT_RETRIES + 1):
			try:
				csv_file = await scrape_violations_with_playwright(
					start_date=args.start_date,
					end_date=args.end_date,
					headless=headless,
					debug=args.debug,
				)
				if csv_file is None:
					# No results from site — retry (could be temporary)
					if attempt < MAX_PLAYWRIGHT_RETRIES:
						logger.warning(f"[Playwright] No results (attempt {attempt}/{MAX_PLAYWRIGHT_RETRIES}) — retrying in 5s...")
						await asyncio.sleep(5)
						continue
					logger.info(f"[Playwright] No results after {MAX_PLAYWRIGHT_RETRIES} attempts — no violations in date range")
					playwright_error = None
					break
				# Success (csv_path or True — all already in DB)
				logger.info("Playwright scraping succeeded")
				playwright_error = None
				break
			except Exception as e:
				if scraper == "playwright":
					raise  # no fallback when explicitly requested
				playwright_error = e
				if attempt < MAX_PLAYWRIGHT_RETRIES:
					logger.warning(f"[Playwright] Attempt {attempt}/{MAX_PLAYWRIGHT_RETRIES} failed: {e} — retrying in 5s...")
					await asyncio.sleep(5)
					continue
				logger.warning(f"[Playwright] All {MAX_PLAYWRIGHT_RETRIES} retries failed with errors")

		# Only fall back to AI if Playwright failed with an actual error (not no-results)
		if playwright_error is not None and scraper == "auto":
			logger.warning("Falling back to browser-use AI method...")
			scraper = "ai"

	if scraper == "ai" and csv_file is None:
		logger.info("Running browser-use AI scraper...")
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

					# Rescore affected properties immediately
					affected_ids = loader.get_affected_property_ids()
					if affected_ids:
						logger.info(f"Triggering CDS rescore for {len(affected_ids)} affected properties...")
						try:
							from src.services.cds_engine import MultiVerticalScorer
							with get_db_context() as score_session:
								scorer = MultiVerticalScorer(score_session)
								scorer.score_properties_by_ids(affected_ids, save_to_db=True)
								score_session.commit()
							logger.info("✓ CDS rescore completed")
						except Exception as score_err:
							logger.warning(f"⚠ CDS rescore failed (non-critical): {score_err}")

					# Delete CSV after successful insertion; keep on error for debugging
					try:
						Path(str(csv_file)).unlink()
						logger.info("✓ CSV deleted after successful DB insertion")
					except Exception as del_err:
						logger.warning(f"⚠ Could not delete CSV (non-critical): {del_err}")

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
	parser.add_argument(
		"--scraper",
		choices=["auto", "playwright", "ai"],
		default="auto",
		help="Which scraper to use: auto (playwright→ai fallback), playwright only, ai only (default: auto)"
	)
	parser.add_argument(
		"--debug",
		action="store_true",
		help="Save screenshots + HTML dumps at each step to data/debug/playwright/violations/ (works headless)"
	)
	parser.add_argument(
		"--headful",
		action="store_true",
		help="Open a real browser window (requires: xvfb-run -a python -m ...)"
	)
	args = parser.parse_args()

	asyncio.run(main(args))