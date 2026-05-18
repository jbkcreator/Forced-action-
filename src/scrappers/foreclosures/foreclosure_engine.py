"""
Foreclosure Auction Data Collection Pipeline — county-agnostic, browser-use only.

For each county reads the foreclosures source config and runs a single browser-use
Agent to extract auction records for a given date.  The agent task is generated
by Claude from the source metadata so no per-county prompt files are needed.

Hillsborough → hillsborough.realforeclose.com  (same platform)
Pinellas     → pinellas.realforeclose.com      (same platform, different URL)

Usage:
    python -m src.scrappers.foreclosures.foreclosure_engine --county-id hillsborough
    python -m src.scrappers.foreclosures.foreclosure_engine --county-id pinellas --date 2026-05-08
    python -m src.scrappers.foreclosures.foreclosure_engine --county-id hillsborough --load-to-db --headful
"""

import argparse
import asyncio
import datetime as dt
import json
import sys
import traceback
from pathlib import Path
from typing import Optional

import pandas as pd

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import setup_logging, get_logger
from src.utils.county_config import get_county_config
from src.utils.db_deduplicator import filter_new_records
from config.constants import BROWSER_MODEL, BROWSER_TEMPERATURE, RAW_FORECLOSURE_DIR

setup_logging()
logger = get_logger(__name__)

AUCTION_DATE_FORMAT = "%m/%d/%Y"


# ---------------------------------------------------------------------------
# URL helper
# ---------------------------------------------------------------------------

def build_preview_url(base_url: str, auction_date: dt.date) -> str:
	"""Build the RealForeclose calendar preview URL for a given date."""
	date_str = auction_date.strftime(AUCTION_DATE_FORMAT)
	return f"{base_url}?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={date_str}"


# ---------------------------------------------------------------------------
# LLM helpers (mirrors master_engine pattern)
# ---------------------------------------------------------------------------

def _make_llm():
	from browser_use import ChatAnthropic
	from config.settings import get_settings
	settings = get_settings()
	return ChatAnthropic(
		model=BROWSER_MODEL,
		timeout=180,
		api_key=settings.anthropic_api_key.get_secret_value(),
		temperature=BROWSER_TEMPERATURE,
	)


def build_agent_task(source: dict, auction_date: dt.date) -> str:
	"""
	Generate a browser-use agent task from the county source config + auction date.

	Uses Claude to produce step-by-step instructions tailored to the source's
	navigation_hint and description.  Falls back to a template on LLM failure.
	"""
	import anthropic
	from config.settings import get_settings

	base_url = source["url"]
	preview_url = build_preview_url(base_url, auction_date)
	iso_date = auction_date.strftime("%Y-%m-%d")
	description = source.get("description", "")
	nav_hint = source.get("navigation_hint", "") or ""

	meta = {
		"preview_url": preview_url,
		"auction_date": iso_date,
		"description": description,
		"navigation_hint": nav_hint,
	}

	system_prompt = (
		"You generate browser-automation task instructions for a browser-use Agent. "
		"The agent is a Chromium browser controller that can navigate pages and extract text. "
		"Write a concise, step-by-step task in plain English. "
		"Do NOT add any explanation outside the task text."
	)

	user_prompt = f"""Generate a browser-use agent task to extract foreclosure auction records.

Source metadata:
{json.dumps(meta, indent=2)}

Requirements:
- Navigate to preview_url.
- Wait for the auction listing to load.
- Extract ALL auction records visible on the page across all sections (Running, Waiting, Closed/Canceled).
- For each record capture: case_number, property_address, judgment_amount, parcel_id, auction_type, auction_status, case_detail_url.
- For each case_detail_url, navigate to it and extract plaintiff and defendant party names, then go back.
- Return ALL records as a single JSON object in this exact format:
  {{"auctions": [{{"case_number": "", "property_address": "", "judgment_amount": "", "parcel_id": "", "auction_type": "", "auction_status": "", "plaintiff": "", "defendant": "", "case_detail_url": ""}}]}}
- If no auctions are listed for this date, return: {{"auctions": []}}
- Do not save any files. Return the JSON as your final result.
- Keep the task under 250 words.
"""

	try:
		settings = get_settings()
		client = anthropic.Anthropic(
			api_key=settings.anthropic_api_key.get_secret_value()
		)
		response = client.messages.create(
			model="claude-sonnet-4-6",
			max_tokens=512,
			temperature=0,
			system=system_prompt,
			messages=[{"role": "user", "content": user_prompt}],
		)
		task = response.content[0].text.strip()
		logger.info(f"[LLM] Generated agent task:\n{task}")
		return task
	except Exception as e:
		logger.warning(f"[LLM] Task generation failed ({e}) — using template fallback")
		return _template_task(source, auction_date)


def _template_task(source: dict, auction_date: dt.date) -> str:
	"""Template fallback when LLM is unavailable."""
	base_url = source["url"]
	preview_url = build_preview_url(base_url, auction_date)
	nav_hint = source.get("navigation_hint", "") or ""
	return (
		f"Go to {preview_url}.\n"
		f"{nav_hint}\n"
		"Wait for the auction listing to load.\n"
		"Extract all auction records across all sections (Running, Waiting, Closed/Canceled).\n"
		"For each record capture: case_number, property_address, judgment_amount, parcel_id, "
		"auction_type, auction_status, case_detail_url.\n"
		"For each case_detail_url, navigate to it and extract plaintiff and defendant names, then go back.\n"
		'Return all records as JSON: {"auctions": [...]}.\n'
		'If no auctions exist for this date return: {"auctions": []}.'
	)


# ---------------------------------------------------------------------------
# Browser-use agent (mirrors master_engine pattern)
# ---------------------------------------------------------------------------

async def run_browser_agent(task: str, headful: bool = False, no_proxy: bool = False):
	"""
	Run a browser-use Agent with the given task.
	Returns the agent history object, or None on failure.
	"""
	from browser_use import Agent, Browser
	from src.utils.http_helpers import get_browser_use_proxy

	llm = _make_llm()

	browser = Browser(
		headless=not headful,
		disable_security=True,
		proxy=None if no_proxy else get_browser_use_proxy(),
		args=[
			'--no-sandbox',
			'--disable-dev-shm-usage',
			'--disable-gpu',
			'--disable-blink-features=AutomationControlled',
			'--window-size=1920,1080',
		],
	)

	agent = Agent(
		task=task,
		llm=llm,
		browser=browser,
		max_steps=80,
		use_judge=False,
	)

	logger.info("[Agent] Starting browser-use agent...")
	try:
		history = await agent.run()
		if not history.is_done():
			logger.warning("[Agent] Agent did not complete all steps within budget")
		return history
	except Exception as e:
		logger.error(f"[Agent] Run failed: {e}")
		logger.debug(traceback.format_exc())
		return None


# ---------------------------------------------------------------------------
# Result parsing + save
# ---------------------------------------------------------------------------

def _parse_agent_result(
	history,
	auction_date: dt.date,
	county_id: str,
) -> Optional[Path]:
	"""
	Extract the JSON auction list from agent's final_result(),
	normalise column names, deduplicate against DB, and save to CSV.
	"""
	if history is None:
		logger.error("[Parse] No agent history — agent failed to run")
		return None

	# Try final_result() first, fall back to last extracted_content entry
	final = history.final_result()
	if not final:
		contents = history.extracted_content()
		final = contents[-1] if contents else None
	if not final:
		logger.error("[Parse] Agent returned no result")
		return None

	# Pull the JSON object out of the agent's text response
	json_start = final.find('{')
	json_end = final.rfind('}') + 1
	if json_start == -1 or json_end == 0:
		logger.error(f"[Parse] No JSON found in agent result: {final[:300]!r}")
		return None

	try:
		data = json.loads(final[json_start:json_end])
	except json.JSONDecodeError as e:
		logger.error(f"[Parse] JSON decode failed: {e} | snippet: {final[json_start:json_end][:300]!r}")
		return None

	auctions = data.get("auctions", [])
	if not auctions:
		logger.info(f"[Parse] No auctions for {auction_date} — none scheduled or all cancelled")
		return None

	logger.info(f"[Parse] Agent extracted {len(auctions)} auction records")

	df = pd.DataFrame(auctions)

	# Normalise to canonical column names expected by ForeclosureLoader
	rename_map = {
		"case_number":      "Case Number",
		"property_address": "Property Address",
		"judgment_amount":  "Judgment Amount",
		"parcel_id":        "Parcel ID",
		"auction_type":     "Auction Type",
		"auction_status":   "Auction Status",
		"plaintiff":        "Plaintiff",
		"defendant":        "Defendant",
		"case_detail_url":  "Case Detail URL",
	}
	df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
	df["Auction Start Date/Time"] = auction_date.strftime("%Y-%m-%d")
	df["county_id"] = county_id

	return _save_new_foreclosures(df, auction_date, county_id)


def _save_new_foreclosures(
	df: pd.DataFrame,
	auction_date: dt.date,
	county_id: str,
) -> Optional[Path]:
	"""Deduplicate against DB and save new records to the county new/ directory."""
	initial_count = len(df)
	df_new = filter_new_records(df, "foreclosures")

	if df_new.empty:
		logger.info("All records already exist in DB — nothing new to save")
		return None

	new_dir = RAW_FORECLOSURE_DIR / county_id / "new"
	new_dir.mkdir(parents=True, exist_ok=True)
	out_path = new_dir / f"foreclosures_{county_id}_{auction_date:%Y%m%d}.csv"

	df_new.to_csv(out_path, index=False)
	logger.info(
		f"Saved {len(df_new)} new records to {out_path} "
		f"(filtered {initial_count - len(df_new)} existing)"
	)
	return out_path


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

async def run_foreclosure_pipeline(
	county_id: str = "hillsborough",
	auction_date: Optional[dt.date] = None,
	headful: bool = False,
	load_to_db: bool = False,
	no_proxy: bool = False,
) -> Optional[Path]:
	"""
	County-agnostic foreclosure scrape for a single auction date.

	Reads the foreclosures source from county config, generates a browser-use
	agent task via LLM, runs the agent, parses the JSON result, deduplicates,
	and optionally loads to DB.
	"""
	if auction_date is None:
		auction_date = dt.date.today()

	cfg = get_county_config(county_id)
	source = cfg["sources"].get("foreclosures")
	if not source:
		logger.error(f"[{county_id}] No foreclosures source configured — add one via admin UI")
		return None

	if source.get("prr_only"):
		logger.info(f"[{county_id}] Foreclosures source is PRR-only — load CSV manually")
		return None

	logger.info("=" * 70)
	logger.info(f"FORECLOSURE PIPELINE — {cfg['display_name'].upper()}")
	logger.info(f"Date: {auction_date}  headful={headful}  load_to_db={load_to_db}")
	logger.info("=" * 70)

	task = build_agent_task(source, auction_date)
	history = await run_browser_agent(task, headful=headful, no_proxy=no_proxy)
	csv_file = _parse_agent_result(history, auction_date, county_id)

	if not csv_file:
		logger.info("No new foreclosure records — nothing to load")
		return None

	if load_to_db:
		_load_to_database(csv_file, county_id)

	return csv_file


def _load_to_database(csv_file: Path, county_id: str) -> None:
	"""Load scraped CSV into the foreclosures table via ForeclosureLoader."""
	from src.core.database import get_db_context
	from src.loaders.foreclosures import ForeclosureLoader

	logger.info("=" * 60)
	logger.info("Loading foreclosures into database...")
	logger.info("=" * 60)

	try:
		with get_db_context() as session:
			loader = ForeclosureLoader(session, county_id=county_id)
			matched, unmatched, skipped = loader.load_from_csv(
				str(csv_file), skip_duplicates=True
			)
			session.commit()

		total = matched + unmatched + skipped
		match_rate = (matched / total * 100) if total > 0 else 0
		logger.info(
			f"Matched: {matched}  Unmatched: {unmatched}  "
			f"Skipped: {skipped}  Match rate: {match_rate:.1f}%"
		)

		# Rescore affected properties immediately
		try:
			with get_db_context() as session:
				loader2 = ForeclosureLoader(session, county_id=county_id)
				affected_ids = loader2.get_affected_property_ids()
			if affected_ids:
				logger.info(f"Triggering CDS rescore for {len(affected_ids)} properties...")
				from src.services.cds_engine import MultiVerticalScorer
				from src.core.database import get_db_context as _gdb
				with _gdb() as score_session:
					scorer = MultiVerticalScorer(score_session)
					scorer.score_properties_by_ids(affected_ids, save_to_db=True, county_id=county_id)
					score_session.commit()
				logger.info("CDS rescore complete")
		except Exception as e:
			logger.warning(f"CDS rescore failed (non-critical): {e}")

	except Exception as e:
		logger.error(f"Database load failed: {e}")
		logger.debug(traceback.format_exc())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_date(date_str: Optional[str]) -> dt.date:
	if date_str:
		try:
			return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
		except ValueError:
			logger.error(f"Invalid date '{date_str}' — expected YYYY-MM-DD")
			raise
	return dt.date.today()


def main():
	parser = argparse.ArgumentParser(
		description="Foreclosure auction scraper — county-agnostic, browser-use only"
	)
	parser.add_argument("--county-id", default="hillsborough",
		help="County identifier (default: hillsborough)")
	parser.add_argument("--date",
		help="Auction date YYYY-MM-DD (default: today)")
	parser.add_argument("--headful", action="store_true",
		help="Run browser in visible mode")
	parser.add_argument("--load-to-db", action="store_true",
		help="Load scraped records into database after scraping")
	parser.add_argument("--no-proxy", action="store_true",
		help="Disable Oxylabs proxy (for testing direct connectivity)")

	args = parser.parse_args()
	auction_date = _parse_date(args.date)

	result = asyncio.run(run_foreclosure_pipeline(
		county_id=args.county_id,
		auction_date=auction_date,
		headful=args.headful,
		load_to_db=args.load_to_db,
		no_proxy=args.no_proxy,
	))

	if result:
		logger.info(f"Output: {result}")
	else:
		logger.info("No output file — nothing new or agent returned no data")


if __name__ == "__main__":
	main()
