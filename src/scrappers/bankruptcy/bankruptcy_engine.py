"""
Bankruptcy Filing Data Collection Pipeline — PACER Source

Fetches Tampa FLMB bankruptcy cases via two steps:
  1. PACER PCL Party Search (batch REST API, $0.10/page) — case list + debtor names
  2. PACER CM/ECF docket fetch per case (Playwright, $0.10/case) — debtor street address

Debtor address enables address-first property matching in BankruptcyLoader,
lifting match rates from ~15% (name-only) to ~40-60%.

Author: Distressed Property Intelligence Platform
"""

import random
import re
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from config.settings import settings
from src.utils.http_helpers import requests_get_with_retry
from config.constants import (
	COURTLISTENER_API_URL,
	COURT_CODE_FLORIDA_MIDDLE_BANKRUPTCY,
	TAMPA_DIVISION_PREFIX,
	RAW_BANKRUPTCY_DIR,
	API_USER_AGENT,
	DEFAULT_USER_AGENT,
	REQUEST_TIMEOUT_DEFAULT,
	PACER_AUTH_URL,
	PACER_PCL_PARTIES_URL,
	PACER_CMECF_FLMB_URL,
	PACER_COURT_FLMB,
	PACER_CHAPTER_FILTER,
)
from src.utils.county_config import get_county_config
from src.utils.logger import setup_logging, get_logger
from src.utils.db_deduplicator import filter_new_records

# Initialize logging
setup_logging()
logger = get_logger(__name__)


def _record_stats(source_type, total, matched, unmatched, skipped, success, county_id, t0, **kwargs):
	try:
		from src.utils.scraper_db_helper import record_scraper_stats
		record_scraper_stats(
			source_type=source_type, total_scraped=total, matched=matched,
			unmatched=unmatched, skipped=skipped, run_success=success,
			duration_seconds=round(time.monotonic() - t0, 2),
			county_id=county_id, **kwargs,
		)
	except Exception as _se:
		logger.warning("Could not record scraper stats: %s", _se)


def _get_pacer_token() -> str:
	"""
	Authenticate with PACER and return the nextGenCSO session token.
	Token is used as a header on PCL API requests and as a cookie on CM/ECF page fetches.
	Raises RuntimeError if credentials are not configured.
	"""
	if not settings.pacer_username or not settings.pacer_password:
		raise RuntimeError(
			"PACER credentials not configured. Set PACER_USERNAME and PACER_PASSWORD in .env"
		)

	resp = requests.post(
		PACER_AUTH_URL,
		json={
			"loginId": settings.pacer_username,
			"password": settings.pacer_password.get_secret_value(),
			"redactFlag": "1",
		},
		timeout=REQUEST_TIMEOUT_DEFAULT,
		headers={"User-Agent": API_USER_AGENT},
	)
	resp.raise_for_status()

	token = resp.json().get("nextGenCSO")
	if not token:
		raise RuntimeError(f"PACER auth response missing nextGenCSO token: {resp.text[:200]}")

	logger.info("PACER authentication successful")
	return token


def fetch_cases_from_pcl(
	lookback_days: int = 1,
	court_code: str = PACER_COURT_FLMB,
	pacer_token: str = "",
) -> List[Dict[str, Any]]:
	"""
	POST to PACER PCL /parties/find to get all debtor-role cases filed in the
	given date range. Handles pagination. Each page costs $0.10 (PACER billing).
	"""
	today = datetime.now().date()
	date_from = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
	date_to = today.strftime("%Y-%m-%d")

	logger.info(f"Fetching PCL party search: court={court_code}, {date_from} → {date_to}")

	headers = {
		"X-NEXT-GEN-CSO": pacer_token,
		"Content-Type": "application/json",
		"Accept": "application/json",
		"User-Agent": API_USER_AGENT,
	}

	payload = {
		"role": ["db"],
		"courtCase": {
			"courtId": [court_code],
			"dateFiledFrom": date_from,
			"dateFiledTo": date_to,
			"federalBankruptcyChapter": PACER_CHAPTER_FILTER,
		},
	}

	all_results: List[Dict] = []
	page = 0

	while True:
		resp = requests.post(
			PACER_PCL_PARTIES_URL,
			json=payload,
			params={"page": page},
			headers=headers,
			timeout=REQUEST_TIMEOUT_DEFAULT,
		)
		if not resp.ok:
			logger.error(f"PCL API error (page {page}): HTTP {resp.status_code} — {resp.text[:300]}")
			resp.raise_for_status()

		data = resp.json()
		results = data.get("content", [])
		all_results.extend(results)

		total_pages = data.get("totalPages", 1)
		logger.info(f"PCL page {page + 1}/{total_pages}: {len(results)} results")

		if page >= total_pages - 1:
			break
		page += 1

	logger.info(f"PCL total results: {len(all_results)}")
	return all_results


def _parse_cmecf_address(html: str) -> Dict[str, Optional[str]]:
	"""
	Parse debtor address from CM/ECF docket HTML.

	Expected debtor section structure:
	    <TD><I><B>Debtor</B></I><BR>
	    <B>Name</B><BR>123 Main St<BR>Tampa, FL 33601<BR>COUNTY-FL</TD>

	Lines after parsing: ['Debtor', 'Name', '123 Main St', 'Tampa, FL 33601', 'HILLSBOROUGH-FL']
	Returns empty dict for PO Box or unparseable HTML (non-fatal — caller falls through to name match).
	"""
	soup = BeautifulSoup(html, "html.parser")

	debtor_td = None
	for i_tag in soup.find_all("i"):
		b_tag = i_tag.find("b")
		if b_tag and "Debtor" in b_tag.get_text():
			debtor_td = i_tag.find_parent("td")
			break

	if not debtor_td:
		logger.debug("CM/ECF parse: Debtor section not found in HTML")
		return {}

	lines = [ln.strip() for ln in debtor_td.get_text(separator="\n").split("\n") if ln.strip()]
	if len(lines) < 4:
		logger.debug(f"CM/ECF parse: not enough address lines: {lines}")
		return {}

	street = lines[2]
	city_state_zip_raw = lines[3]

	if re.match(r"^P\.?O\.?\s*Box", street, re.IGNORECASE):
		logger.debug(f"CM/ECF parse: PO Box skipped: {street}")
		return {}

	m = re.match(r"^(.+),\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", city_state_zip_raw)
	if not m:
		logger.debug(f"CM/ECF parse: city/state/zip not matched: {city_state_zip_raw!r}")
		return {"street": street, "city": None, "state": None, "zip": None}

	return {
		"street": street,
		"city":   m.group(1).strip(),
		"state":  m.group(2),
		"zip":    m.group(3),
	}


def fetch_debtor_address_cmecf(case_id: str, pacer_token: str) -> Dict[str, Optional[str]]:
	"""
	Fetch debtor address from CM/ECF docket page via Playwright + PACER session cookie.
	Costs $0.10 per call (PACER billing). Returns empty dict on any error (non-fatal).
	"""
	url = f"{PACER_CMECF_FLMB_URL}?{case_id}"
	logger.debug(f"Fetching CM/ECF docket: {url}")

	try:
		with sync_playwright() as pw:
			browser = pw.chromium.launch(headless=True)
			context = browser.new_context(user_agent=DEFAULT_USER_AGENT)
			context.add_cookies([{
				"name":   "nextGenCSO",
				"value":  pacer_token,
				"domain": ".uscourts.gov",
				"path":   "/",
			}])
			page = context.new_page()
			page.goto(url, timeout=30_000, wait_until="domcontentloaded")
			html = page.content()
			browser.close()

		return _parse_cmecf_address(html)

	except Exception as e:
		logger.warning(f"CM/ECF address fetch failed for case_id={case_id}: {e}")
		return {}


def build_bankruptcy_leads(
	pcl_cases: List[Dict[str, Any]],
	pacer_token: str,
	division_prefix: str = TAMPA_DIVISION_PREFIX,
) -> List[Dict[str, Any]]:
	"""
	Filter PCL results for Tampa Division cases and fetch CM/ECF address per case.

	PCL party response shape (PACER PCL API User Guide, Nov 2024):
	    {"firstName": "John", "middleName": "A", "lastName": "Smith", "role": "db",
	     "courtCase": {"caseId": "12345678", "courtId": "flmb",
	                   "docketNum": "8:26-bk-01234", "caseTitle": "Smith, John A",
	                   "dateFiled": "2026-05-25", "chapter": "7"}}

	Field names are from the PDF spec — verify against live API on first run.
	"""
	leads: List[Dict] = []

	for case in pcl_cases:
		court_case = case.get("courtCase") or {}
		docket_num = court_case.get("docketNum", "")

		if not docket_num.startswith(division_prefix):
			continue

		first  = (case.get("firstName")  or "").strip()
		middle = (case.get("middleName") or "").strip()
		last   = (case.get("lastName")   or "").strip()
		lead_name = " ".join(filter(None, [first, middle, last])) or court_case.get("caseTitle", "")

		case_id    = court_case.get("caseId", "")
		chapter    = court_case.get("chapter", "")
		date_filed = court_case.get("dateFiled", "")
		court_id   = court_case.get("courtId", "")

		address: Dict = {}
		if case_id:
			time.sleep(random.uniform(1.0, 2.5))
			address = fetch_debtor_address_cmecf(str(case_id), pacer_token)

		leads.append({
			"Docket Number": docket_num,
			"Lead Name":     lead_name,
			"Date Filed":    date_filed,
			"Case Type":     "bk",
			"Court ID":      court_id,
			"Chapter":       chapter,
			"Debtor Street": address.get("street"),
			"Debtor City":   address.get("city"),
			"Debtor State":  address.get("state"),
			"Debtor Zip":    address.get("zip"),
		})
		logger.debug(f"Lead: {lead_name} ({docket_num}) — address: {address.get('street') or 'none'}")

	logger.info(f"Built {len(leads)} Tampa leads from {len(pcl_cases)} PCL cases")
	if not leads:
		logger.warning("No Tampa Division cases found after filtering")
	return leads


# =============================================================================
# LEGACY — CourtListener source (kept for reference; not called by pipeline)
# =============================================================================

def fetch_bankruptcy_filings(lookback_days: int = 1, court_code: str = COURT_CODE_FLORIDA_MIDDLE_BANKRUPTCY) -> List[Dict[str, Any]]:
	"""
	Fetch bankruptcy filings from CourtListener API.
	
	This function queries the CourtListener API to retrieve bankruptcy docket
	information for the Florida Middle Bankruptcy Court. It fetches all cases
	filed within the specified lookback period.
	
	Args:
		lookback_days: Number of days to look back from today (default: 1)
		
	Returns:
		List[Dict[str, Any]]: List of docket dictionaries from the API response
		
	Raises:
		requests.HTTPError: If the HTTP request fails
		requests.Timeout: If the request times out
		ValueError: If the API returns an unexpected response format
		
	Example:
		>>> filings = fetch_bankruptcy_filings(lookback_days=7)
		>>> print(f"Fetched {len(filings)} bankruptcy filings")
	"""
	start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
	
	logger.info(f"Fetching bankruptcy filings from CourtListener API since {start_date}")
	
	# Construct API URL with query parameters
	params = {
		"court": court_code,
		"date_filed__gte": start_date,
	}
	
	if not settings.court_listener_api_key:
		raise RuntimeError("COURT_LISTENER_API_KEY not configured (legacy CourtListener path)")
	headers = {
		"Authorization": f"Token {settings.court_listener_api_key.get_secret_value()}",
		"User-Agent": API_USER_AGENT,
	}
	
	try:
		response = requests_get_with_retry(
			COURTLISTENER_API_URL,
			params=params,
			headers=headers,
			timeout=REQUEST_TIMEOUT_DEFAULT,
		)
		logger.debug(f"Successfully fetched API response (status code: {response.status_code})")
		
	except requests.Timeout as e:
		logger.error(f"Request timed out while fetching bankruptcy filings: {e}")
		raise
	except requests.HTTPError as e:
		logger.error(f"HTTP error occurred while fetching bankruptcy filings: {e}")
		logger.error(f"Response content: {response.text}")
		raise
	except requests.RequestException as e:
		logger.error(f"Request error occurred while fetching bankruptcy filings: {e}")
		raise
	
	try:
		data = response.json()
		results = data.get('results', [])
		
		logger.info(f"Fetched {len(results)} bankruptcy dockets from API")
		logger.debug(f"API response includes {data.get('count', 0)} total results")
		
		return results
		
	except ValueError as e:
		logger.error(f"Failed to parse JSON response: {e}")
		raise


def filter_tampa_bankruptcies(dockets: List[Dict[str, Any]], division_prefix: str = TAMPA_DIVISION_PREFIX) -> List[Dict[str, Any]]:
	"""
	Filter bankruptcy dockets for Tampa Division cases only.
	
	This function filters the raw docket data to include only bankruptcy cases
	(case type 'bk') filed in the Tampa Division (docket numbers starting with '8:').
	It also cleans the case names by removing common prefixes.
	
	Args:
		dockets: List of docket dictionaries from the API
		
	Returns:
		List[Dict[str, Any]]: Filtered list of Tampa bankruptcy cases
		
	Example:
		>>> tampa_cases = filter_tampa_bankruptcies(all_dockets)
		>>> print(f"Found {len(tampa_cases)} Tampa bankruptcy cases")
	"""
	logger.info("Filtering for Tampa Division bankruptcy cases")
	
	tampa_bankruptcies = []
	
	for docket in dockets:
		case_type = docket.get('federal_dn_case_type', '')
		docket_num = docket.get('docket_number', '')
		
		# Filter for Bankruptcy ('bk') AND Tampa Division ('8:')
		if case_type == 'bk' and docket_num.startswith(division_prefix):
			# Clean the case name by removing common prefixes
			raw_name = docket.get('case_name', '')
			clean_name = raw_name.replace("In re: ", "").strip()
			
			# Create cleaned record
			cleaned_record = {
				'Docket Number': docket_num,
				'Lead Name': clean_name,
				'Date Filed': docket.get('date_filed', ''),
				'Case Type': case_type,
				'Court ID': docket.get('court', ''),
				'docket_id': docket.get('id', ''),
				'date_terminated': docket.get('date_terminated', ''),
				'nature_of_suit': docket.get('nature_of_suit', ''),
			}
			
			tampa_bankruptcies.append(cleaned_record)
			
			logger.debug(f"Found Tampa bankruptcy: {clean_name} ({docket_num})")
	
	logger.info(f"Filtered {len(tampa_bankruptcies)} Tampa bankruptcy cases from {len(dockets)} total dockets")
	
	if len(tampa_bankruptcies) == 0:
		logger.warning("No Tampa bankruptcy cases found in the data")
	
	return tampa_bankruptcies


def save_bankruptcy_leads(
	leads: List[Dict[str, Any]],
	output_filename: str = "tampa_bankruptcy_leads.csv",
	county_id: str = "hillsborough",
) -> Optional[Path]:
	"""
	Save bankruptcy leads to a CSV file with deduplication.
	
	This function creates the processed data directory if it doesn't exist,
	deduplicates against existing CSV files, and saves the bankruptcy leads
	as a CSV file with the specified filename.
	
	Args:
		leads: List of dictionaries containing bankruptcy lead data
		output_filename: Name for the output CSV file (default: "tampa_bankruptcy_leads.csv")
		
	Returns:
		Path: Path object pointing to the saved deduplicated CSV file, or None if no data
		
	Raises:
		IOError: If the file cannot be written to disk
		PermissionError: If there are insufficient permissions to write the file
		
	Example:
		>>> output_path = save_bankruptcy_leads(leads, "bankruptcies_20260220.csv")
		>>> print(f"Saved to: {output_path}")
	"""
	if not leads:
		logger.warning("No bankruptcy leads to save")
		return None
	
	try:
		RAW_BANKRUPTCY_DIR.mkdir(parents=True, exist_ok=True)
		logger.debug(f"Ensured bankruptcy directory exists: {RAW_BANKRUPTCY_DIR}")
		
		# Convert to DataFrame
		df = pd.DataFrame(leads)
		
		# Check DB for existing bankruptcy cases (deduplicate BEFORE CSV save)
		logger.info("=" * 60)
		logger.info("DB DEDUPLICATION: Checking for existing bankruptcies")
		logger.info("=" * 60)
		
		initial_count = len(df)
		df_new = filter_new_records(df, 'bankruptcy', record_type='Bankruptcy', county_id=county_id)
		
		if df_new.empty:
			logger.info("✓ All bankruptcies already exist in database - nothing new")
			return None
		
		# Save only NEW bankruptcies
		new_dir = RAW_BANKRUPTCY_DIR / "new"
		new_dir.mkdir(parents=True, exist_ok=True)
		final_file = new_dir / output_filename
		
		df_new.to_csv(final_file, index=False)
		logger.info(f"Saved {len(df_new)} NEW bankruptcies to {final_file}")
		logger.info(f"Filtered {initial_count - len(df_new)} existing records")
		
		return final_file
		
	except PermissionError as e:
		logger.error(f"Permission denied when writing to {output_filename}: {e}")
		raise
	except IOError as e:
		logger.error(f"I/O error occurred while writing file: {e}")
		raise
	except Exception as e:
		logger.error(f"Unexpected error saving bankruptcy data: {e}")
		logger.debug(traceback.format_exc())
		raise
		logger.error(f"Unexpected error saving bankruptcy leads: {e}")
		raise


def run_bankruptcy_pipeline(lookback_days: int = 1, county_id: str = "hillsborough") -> bool:
	"""Execute the complete bankruptcy data collection pipeline (PACER source)."""
	t0 = time.monotonic()
	try:
		county_cfg = get_county_config(county_id)
		court_cfg = county_cfg.get("court", {})
		court_code = court_cfg.get("bankruptcy_code", PACER_COURT_FLMB)
		division_prefix = court_cfg.get("division_prefix", TAMPA_DIVISION_PREFIX)

		logger.info("=" * 80)
		logger.info(f"STARTING BANKRUPTCY PIPELINE — PACER SOURCE ({county_cfg['display_name'].upper()})")
		logger.info("=" * 80)

		# Step 1: Authenticate with PACER
		logger.info("\n[STEP 1/3] Authenticating with PACER...")
		try:
			pacer_token = _get_pacer_token()
		except RuntimeError as e:
			logger.error(f"PACER auth failed: {e}")
			_record_stats('bankruptcy', 0, 0, 0, 0, False, county_id, t0, error_type='config_error')
			return False

		# Step 2: Fetch cases from PCL ($0.10/page billed to PACER account)
		logger.info(f"\n[STEP 2/3] Fetching from PACER PCL (lookback: {lookback_days} days)...")
		pcl_cases = fetch_cases_from_pcl(
			lookback_days=lookback_days,
			court_code=court_code,
			pacer_token=pacer_token,
		)

		if not pcl_cases:
			logger.warning("No bankruptcy filings found in PCL for the specified date range")
			_record_stats('bankruptcy', 0, 0, 0, 0, True, county_id, t0, error_type='no_data')
			return False

		# Step 3: Build leads — filter Tampa cases + fetch CM/ECF addresses ($0.10/case)
		logger.info(f"\n[STEP 3/3] Building leads + fetching CM/ECF addresses ({len(pcl_cases)} PCL cases)...")
		leads = build_bankruptcy_leads(pcl_cases, pacer_token, division_prefix=division_prefix)

		if not leads:
			logger.warning("No Tampa bankruptcy cases found after division filter")
			_record_stats('bankruptcy', 0, 0, 0, 0, True, county_id, t0, error_type='no_data')
			return False

		# Step 4: Save (dedup + CSV)
		today = datetime.now().strftime("%Y%m%d")
		output_filename = f"tampa_bankruptcy_leads_{today}.csv"
		output_path = save_bankruptcy_leads(leads, output_filename, county_id=county_id)

		if not output_path:
			logger.info("All bankruptcy cases already exist in DB — nothing new to save")
			_record_stats('bankruptcy', len(leads), 0, 0, len(leads), True, county_id, t0)
			return True

		logger.info("=" * 80)
		logger.info("BANKRUPTCY PIPELINE COMPLETED SUCCESSFULLY")
		logger.info(f"Output: {output_path}  |  New leads: {len(leads)}")
		logger.info("=" * 80)

		_record_stats('bankruptcy', len(leads), 0, 0, 0, True, county_id, t0)
		return True

	except Exception as e:
		logger.error("=" * 80)
		logger.error(f"BANKRUPTCY PIPELINE FAILED: {e}")
		logger.error(traceback.format_exc())
		logger.error("=" * 80)
		_record_stats('bankruptcy', 0, 0, 0, 0, False, county_id, t0, error_message=str(e)[:500])
		return False


if __name__ == "__main__":
	"""
	Main entry point for the bankruptcy data collection pipeline.
	
	This script can be run directly to execute the complete pipeline:
	    python -m src.scrappers.bankruptcy.bankruptcy_engine
	    
	Optional arguments can be added for lookback days:
	    python -m src.scrappers.bankruptcy.bankruptcy_engine --lookback 7
	    
	Exit codes:
	    0: Pipeline completed successfully
	    1: Pipeline failed or no bankruptcy data found
	"""
	import sys
	import argparse
	
	parser = argparse.ArgumentParser(
		description="Fetch bankruptcy filings from CourtListener API"
	)
	parser.add_argument(
		"--lookback",
		type=int,
		default=1,
		help="Number of days to look back for filings (default: 1)",
	)
	parser.add_argument(
		"--county-id",
		dest="county_id",
		default="hillsborough",
		help="County identifier (default: hillsborough)",
	)

	from src.utils.scraper_db_helper import add_load_to_db_arg
	add_load_to_db_arg(parser)

	args = parser.parse_args()

	success = run_bankruptcy_pipeline(lookback_days=args.lookback, county_id=args.county_id)
	
	# Load to database if requested and scraping was successful
	if success and args.load_to_db:
		try:
			from src.utils.scraper_db_helper import load_scraped_data_to_db
			# Find the most recent bankruptcy CSV in new/ subdirectory
			new_dir = RAW_BANKRUPTCY_DIR / "new"
			csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
			if csv_files:
				csv_to_load = csv_files[0]
				logger.info(f"Loading to database: {csv_to_load}")
				load_scraped_data_to_db('bankruptcy', csv_to_load, destination_dir=RAW_BANKRUPTCY_DIR)
			else:
				logger.error("No bankruptcy CSV file found to load")
				sys.exit(1)
		except Exception as e:
			logger.error(f"Failed to load data to database: {e}")
			sys.exit(1)
	elif args.load_to_db:
		logger.warning("Skipping database load due to scraping failure")
	
	sys.exit(0 if success else 1)




