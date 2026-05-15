"""
Tax Delinquent Property Data Collection Pipeline — county-agnostic, browser-use only.

RADAR Phase:
    - Downloads bulk tax delinquent report from the county tax portal using browser-use.
    - Filters accounts by delinquency criteria (minimum years unpaid).
    - Creates a target list of high-priority distressed properties.

SNIPER Phase:
    - Enriches individual accounts with detailed data via Firecrawl API.
    - Extracts total amounts due, delinquency years, payment plan status.
    - Rate-limited to respect API quotas and site policies.

Usage:
    python -m src.scrappers.deliquencies.tax_delinquent_engine --county-id hillsborough
    python -m src.scrappers.deliquencies.tax_delinquent_engine --county-id hillsborough --mode download-only
    python -m src.scrappers.deliquencies.tax_delinquent_engine --county-id hillsborough --skip-download --load-to-db
"""

import asyncio
import argparse
import json
import os
import random
import shutil
import sys
import time
import traceback
from pathlib import Path
from typing import Optional

import pandas as pd
from firecrawl import FirecrawlApp

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from config.constants import (
    DEFAULT_TAX_YEAR,
    DEFAULT_ACCOUNT_STATUS,
    MIN_YEARS_DELINQUENT,
    RAW_TAX_DELINQUENCIES_DIR,
    PROCESSED_DATA_DIR,
    REFERENCE_DATA_DIR,
    DOWNLOAD_FILE_PATTERNS,
    PARCEL_LOOKUP_URL,
    REQUEST_DELAY_RANGE,
    TEMP_DOWNLOADS_DIR,
    BROWSER_DOWNLOAD_TEMP_PATTERN,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.utils.county_config import get_county as _get_county
from src.core.database import get_db_context
from src.core.models import TaxDelinquency, Property
from src.utils.logger import setup_logging, get_logger
from src.utils.prompt_loader import get_prompt, get_config

setup_logging()
logger = get_logger(__name__)

RAW_TAX_DELINQUENCIES_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# LLM helpers
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


def build_agent_task(source: dict, tax_year: int, account_status: str) -> str:
    """
    Generate a browser-use agent task from county source metadata.
    When the source has a configured URL/nav_hint, uses LLM with source metadata.
    Falls back to YAML prompt (Hillsborough legacy) only when no source URL is set,
    then falls back to template if both fail.
    """
    import anthropic
    from config.settings import get_settings

    portal_url = source.get("url", "")
    description = source.get("description", "")
    nav_hint = source.get("navigation_hint", "") or ""

    # YAML prompt is Hillsborough-specific — skip it when source has a configured URL
    if not portal_url:
        try:
            task = get_prompt(
                "tax_delinquent_prompts.yaml",
                "tax_delinquent_download.task_template",
                account_status=account_status,
                tax_year=tax_year,
            )
            if task:
                logger.info("[LLM] Loaded agent task from YAML prompt")
                return task
        except Exception:
            pass

    meta = {
        "portal_url": portal_url,
        "tax_year": tax_year,
        "account_status": account_status,
        "description": description,
        "navigation_hint": nav_hint,
    }

    system_prompt = (
        "You generate browser-automation task instructions for a browser-use Agent. "
        "The agent controls a Chromium browser and must trigger a file download (CSV). "
        "Write concise, numbered steps in plain English. "
        "Do NOT add any explanation outside the task text."
    )

    user_prompt = f"""Generate a browser-use agent task to download a bulk tax delinquent report from a county tax portal.

Source metadata:
{json.dumps(meta, indent=2)}

Requirements:
- Keep the task under 350 words.
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
        logger.info("[LLM] Generated agent task:\n%s", task)
        return task
    except Exception as e:
        logger.warning("[LLM] Task generation failed (%s) — using template fallback", e)
        return _template_task(source, tax_year, account_status)


def _template_task(source: dict, tax_year: int, account_status: str) -> str:
    """Template fallback when LLM and YAML are unavailable."""
    portal_url = source.get("url", "")
    nav_hint = source.get("navigation_hint", "") or ""
    return (
        f"Go to {portal_url}.\n"
        "Wait 5 seconds for the page to load.\n"
        f"{nav_hint}\n"
        'Find the "Public - Delinquent Report" or similar delinquent report option and select it.\n'
        f"Set the tax year to {tax_year}.\n"
        f'Set the account status filter to "{account_status}" or "Unpaid".\n'
        "Submit the search and wait for results.\n"
        "Click the CSV download button.\n"
        "Wait 30 seconds for the download to complete.\n"
        "Do not navigate away."
    )


# ---------------------------------------------------------------------------
# RADAR: Download bulk tax delinquent report
# ---------------------------------------------------------------------------

def _locate_download(start_time: float, download_dir: Optional[Path] = None) -> Optional[Path]:
    """Search for recently downloaded tax delinquency files."""
    def recent_candidates(folder: Path):
        paths = []
        if not folder.exists():
            return paths
        for pattern in DOWNLOAD_FILE_PATTERNS:
            for candidate in folder.glob(pattern):
                try:
                    if candidate.stat().st_mtime >= start_time:
                        paths.append(candidate)
                except FileNotFoundError:
                    continue
        return paths

    candidates = recent_candidates(REFERENCE_DATA_DIR)
    if download_dir:
        candidates.extend(recent_candidates(download_dir))

    temp_base = TEMP_DOWNLOADS_DIR
    if temp_base.exists():
        for dl_dir in temp_base.glob(BROWSER_DOWNLOAD_TEMP_PATTERN):
            candidates.extend(recent_candidates(dl_dir))

    if not candidates:
        logger.warning("[RADAR] No downloaded file found. start_time=%.1fs ago", time.time() - start_time)
        return None

    return max(candidates, key=lambda p: p.stat().st_mtime)


async def download_tax_delinquent_report(
    tax_year: int = DEFAULT_TAX_YEAR,
    account_status: str = DEFAULT_ACCOUNT_STATUS,
    wait_after_download: int = 30,
    county_id: str = "hillsborough",
    headful: bool = False,
    cf_bypass: Optional[bool] = None,
) -> bool:
    """
    RADAR PHASE: Download bulk tax delinquent report via browser-use agent.

    When the source has ``cf_bypass_required=true`` in counties.json (or the
    caller passes ``cf_bypass=True``), the Agent is launched against the
    warmed Edge profile — Edge binary + persistent user_data_dir + no proxy
    + no stealth init script — so the warmed cf_clearance + TLS fingerprint
    stays intact. Same pattern the lien engine uses on the Pinellas clerk
    portal.
    """
    from browser_use import Agent, Browser
    from src.utils.http_helpers import get_browser_use_proxy

    _county = _get_county(county_id)
    _file_prefix = _county["file_prefix"]
    REFERENCE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest_path = REFERENCE_DATA_DIR / f"{_file_prefix}_tax_delinquent_{tax_year}.csv"

    source = _county.get("sources", {}).get("tax_delinquency")
    if source is None:
        tax_url = _county.get("urls", {}).get("tax") or ""
        source = {"url": tax_url, "signal_type": "tax_delinquency"}

    # Resolve CF-bypass: explicit CLI flag overrides; otherwise honor the
    # per-source flag set by an admin in counties.json.
    cf_required = bool(source.get("cf_bypass_required")) if cf_bypass is None else cf_bypass
    cf_profile: Optional[dict] = None
    if cf_required:
        from src.utils.cf_persistent_browser import (
            resolve_cf_profile,
            CFProfileNotWarmedError,
        )
        profile_name = source.get("cf_bypass_profile_name") or f"{county_id}_tax"
        try:
            cf_profile = resolve_cf_profile(profile_name)
            logger.info(
                "[RADAR] CF-bypass mode — Edge=%s profile=%s",
                cf_profile["edge_path"], cf_profile["profile_dir"],
            )
        except CFProfileNotWarmedError as exc:
            logger.error("[RADAR] CF-bypass requested but profile unusable: %s", exc)
            return False

    task = build_agent_task(source, tax_year, account_status)

    logger.info("[RADAR] Launching browser-use agent — tax_year=%s county=%s", tax_year, county_id)

    llm = _make_llm()

    browser_kwargs = dict(
        headless=not headful,
        disable_security=True,
        downloads_path=str(REFERENCE_DATA_DIR),
        ignore_default_args=["--enable-automation"],
        minimum_wait_page_load_time=1.5,
        wait_between_actions=1.0,
        args=[
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--window-size=1920,1080',
        ],
    )

    if cf_profile:
        # Persistent Edge profile + no proxy + no stealth — anything else
        # would perturb the fingerprint Cloudflare hashed when issuing
        # cf_clearance.
        browser_kwargs.update(
            executable_path=cf_profile["edge_path"],
            user_data_dir=cf_profile["profile_dir"],
            proxy=None,
            enable_default_extensions=False,
        )
    else:
        proxy = get_browser_use_proxy()
        logger.info("[RADAR] Proxy: %s", "Oxylabs enabled" if proxy else "NO PROXY — running direct")
        browser_kwargs.update(
            proxy=proxy,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            enable_default_extensions=True,
        )

    browser = Browser(**browser_kwargs)

    await browser.start()
    if not cf_profile:
        # Stealth patches the navigator/WebGL signatures Cloudflare hashes
        # when issuing cf_clearance — skip injection in CF mode to keep the
        # warmed profile's fingerprint intact.
        from playwright_stealth import Stealth
        stealth = Stealth(
            chrome_runtime=True, navigator_webdriver=True, navigator_plugins=True, webgl_vendor=True,
            webgl_vendor_override="Google Inc. (Intel)",
            webgl_renderer_override="ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)",
        )
        await browser._cdp_add_init_script(stealth.script_payload)

    agent = Agent(task=task, llm=llm, browser=browser, max_steps=80, use_judge=False)

    start_time = time.time()
    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[RADAR] Agent did not complete within step budget")
    except Exception as e:
        logger.error("[RADAR] Browser agent execution failed: %s", e)
        logger.debug(traceback.format_exc())
        return False

    logger.info("[RADAR] Agent completed. Waiting %ds for download to finalize...", wait_after_download)
    await asyncio.sleep(wait_after_download)

    downloaded_file = _locate_download(start_time)
    if not downloaded_file or not downloaded_file.exists():
        logger.error("[RADAR] Could not detect the downloaded report")
        return False

    final_ext = downloaded_file.suffix.lower() or ".csv"
    dest_file = REFERENCE_DATA_DIR / f"{_file_prefix}_tax_delinquent_{tax_year}{final_ext}"
    if dest_file.exists():
        dest_file.unlink()

    logger.info("[RADAR] Moving %s → %s", downloaded_file, dest_file)
    shutil.move(str(downloaded_file), str(dest_file))

    # Validate file — must not be empty or an HTML error page
    file_size = dest_file.stat().st_size
    if file_size < 1024:
        logger.error("[RADAR] Downloaded file too small (%d bytes) — likely empty or error page", file_size)
        dest_file.unlink(missing_ok=True)
        return False
    with open(dest_file, "r", errors="replace") as _f:
        _head = _f.read(512)
    if "<!DOCTYPE" in _head or "<html" in _head.lower():
        logger.error("[RADAR] Downloaded file is an HTML error page — session may have expired")
        dest_file.unlink(missing_ok=True)
        return False

    size_mb = file_size / (1024 ** 2)
    logger.info("[RADAR] Report saved: %s (%.1f MB)", dest_file, size_mb)

    # Clean up temp dir
    temp_dir = downloaded_file.parent
    if temp_dir.exists() and temp_dir.name.startswith("browser-use-downloads-"):
        try:
            shutil.rmtree(temp_dir)
        except Exception as exc:
            logger.warning("[RADAR] Could not clean temp dir %s: %s", temp_dir, exc)

    return True


# ---------------------------------------------------------------------------
# Support: DB dedup + distress filter
# ---------------------------------------------------------------------------

def _get_existing_tax_records(tax_year: int, county_id: str = "hillsborough") -> set:
    """Query DB for existing tax delinquency account numbers for the given year + county."""
    logger.info("[Dedup] Querying DB for existing tax records (year %s, county=%s)...", tax_year, county_id)
    try:
        with get_db_context() as session:
            results = (
                session.query(Property.parcel_id)
                .join(TaxDelinquency, TaxDelinquency.property_id == Property.id)
                .filter(TaxDelinquency.tax_year == tax_year)
                .filter(Property.county_id == county_id)
                .distinct()
                .all()
            )
            existing = {"A" + row[0] for row in results if row[0]}
            logger.info("[Dedup] Found %d existing records for %s / %s", len(existing), tax_year, county_id)
            return existing
    except Exception as e:
        logger.error("[Dedup] Failed to query existing records: %s", e)
        return set()


def _filter_distressed_accounts(df: pd.DataFrame, min_years: int = MIN_YEARS_DELINQUENT) -> pd.DataFrame:
    """Filter accounts to those with at least `min_years` of delinquency."""
    years_col = None
    for col_name in ("Years Delinquent", "years_delinquent", "Delinquent Years", "Years", "YRS", "Yrs Delinq"):
        if col_name in df.columns:
            years_col = col_name
            break

    if years_col:
        try:
            df_filtered = df[pd.to_numeric(df[years_col], errors="coerce") >= min_years].copy()
            logger.info("[Filter] %d/%d accounts have %d+ years delinquent", len(df_filtered), len(df), min_years)
            return df_filtered
        except Exception as e:
            logger.error("[Filter] Error filtering by years delinquent: %s", e)

    logger.warning("[Filter] 'Years Delinquent' column not found — processing all %d accounts", len(df))
    return df.copy()


# ---------------------------------------------------------------------------
# SNIPER: Firecrawl enrichment
# ---------------------------------------------------------------------------

async def _scrape_parcel_with_firecrawl(
    firecrawl_client: FirecrawlApp,
    account_number: str,
    parcel_lookup_url: str = PARCEL_LOOKUP_URL,
) -> dict:
    """SNIPER PHASE: Extract structured tax data for one account via Firecrawl."""
    url = f"{parcel_lookup_url}/{account_number}"
    result = {
        "account_number": account_number,
        "total_amount_due": None,
        "years_delinquent": 0,
        "payment_plan_status": "No Plan",
        "custom_flags": [],
    }

    try:
        logger.info("[SNIPER] Extracting %s", account_number)
        extract_schema = get_config("tax_delinquent_prompts", "parcel_extraction", "schema")
        prompt_template = get_config("tax_delinquent_prompts", "parcel_extraction", "prompt_template")

        if not extract_schema:
            logger.error("[SNIPER] Missing Firecrawl extraction schema in tax_delinquent_prompts.yaml")
            return result

        prompt = prompt_template.format(account_number=account_number) if prompt_template else ""
        extract_result = firecrawl_client.extract(urls=[url], schema=extract_schema, prompt=prompt)

        if extract_result and hasattr(extract_result, "data") and extract_result.data:
            data = extract_result.data
            if isinstance(data, list) and data:
                data = data[0]

            total_amount = data.get("total_amount_due") if isinstance(data, dict) else getattr(data, "total_amount_due", None)
            years_delinq = data.get("years_delinquent") if isinstance(data, dict) else getattr(data, "years_delinquent", None)

            if total_amount:
                result["total_amount_due"] = str(total_amount).replace("$", "").replace(",", "").strip()
            if years_delinq is not None:
                result["years_delinquent"] = int(years_delinq)
        else:
            logger.warning("[SNIPER] No data returned for %s", account_number)

    except Exception as exc:
        logger.error("[SNIPER] Failed to extract %s: %s", account_number, exc)
        logger.debug(traceback.format_exc())

    return result


async def _sniper_enrich_accounts(
    df: pd.DataFrame,
    max_accounts: int = 100,
    parcel_lookup_url: str = PARCEL_LOOKUP_URL,
) -> pd.DataFrame:
    """SNIPER PHASE: Enrich high-priority accounts using Firecrawl."""
    from config.settings import get_settings

    if len(df) > max_accounts:
        logger.info("[SNIPER] Limiting enrichment to first %d accounts (of %d)", max_accounts, len(df))
        df = df.head(max_accounts)

    enriched_rows = []
    skipped = 0

    try:
        settings = get_settings()
        firecrawl_client = FirecrawlApp(api_key=settings.firecrawl_api_key.get_secret_value())
        logger.info("[SNIPER] Firecrawl initialized — enriching %d accounts", len(df))

        for idx, row in df.iterrows():
            account_number = (
                row.get("Account Number")
                or row.get("account_number")
                or row.get("Parcel ID")
                or row.get("Account #")
            )

            if not account_number:
                skipped += 1
                continue

            if (idx + 1) % 10 == 0 or idx == 0:
                logger.info("[SNIPER] Progress: %d/%d accounts enriched", idx + 1, len(df))

            details = await _scrape_parcel_with_firecrawl(firecrawl_client, str(account_number), parcel_lookup_url)

            enriched_row = row.to_dict()
            enriched_row["total_amount_due"] = details["total_amount_due"]
            enriched_row["years_delinquent_scraped"] = details["years_delinquent"]
            enriched_row["payment_plan_status"] = details["payment_plan_status"]
            enriched_row["custom_flags"] = details["custom_flags"]
            enriched_rows.append(enriched_row)

            if idx < len(df) - 1:
                delay = random.uniform(*REQUEST_DELAY_RANGE)
                await asyncio.sleep(delay)

        if skipped:
            logger.warning("[SNIPER] Skipped %d accounts with missing account numbers", skipped)
        logger.info("[SNIPER] Enrichment complete: %d accounts", len(enriched_rows))
        return pd.DataFrame(enriched_rows)

    except Exception as e:
        logger.error("[SNIPER] Error during enrichment: %s", e)
        logger.debug(traceback.format_exc())
        return pd.DataFrame(enriched_rows) if enriched_rows else df


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def run_radar_sniper_pipeline(
    tax_year: int = DEFAULT_TAX_YEAR,
    account_status: str = DEFAULT_ACCOUNT_STATUS,
    min_years_delinquent: int = MIN_YEARS_DELINQUENT,
    max_sniper_accounts: int = 100,
    skip_download: bool = False,
    county_id: str = "hillsborough",
    load_to_db: bool = False,
    headful: bool = False,
    cf_bypass: Optional[bool] = None,
) -> bool:
    """Execute the full RADAR + SNIPER pipeline for tax delinquent lead generation."""
    try:
        # PHASE 1: RADAR
        if skip_download:
            logger.info("[RADAR] Skipping download (using existing CSV)")
        else:
            logger.info("[RADAR] Starting bulk download phase")
            success = await download_tax_delinquent_report(
                tax_year=tax_year,
                account_status=account_status,
                county_id=county_id,
                headful=headful,
                cf_bypass=cf_bypass,
            )
            if not success:
                logger.error("[RADAR] Phase failed — aborting pipeline")
                return False

        # Locate downloaded file
        _file_prefix = _get_county(county_id)["file_prefix"]
        bulk_csv_path = REFERENCE_DATA_DIR / f"{_file_prefix}_tax_delinquent_{tax_year}.csv"
        if not bulk_csv_path.exists():
            for ext in (".xls", ".xlsx"):
                alt = REFERENCE_DATA_DIR / f"{_file_prefix}_tax_delinquent_{tax_year}{ext}"
                if alt.exists():
                    bulk_csv_path = alt
                    break

        if not bulk_csv_path.exists():
            logger.error("[RADAR] Cannot find CSV at %s", bulk_csv_path)
            return False

        # PHASE 2: Parse CSV
        logger.info("[Parse] Loading bulk CSV: %s", bulk_csv_path)
        df = None
        for enc in ("utf-8", "latin1"):
            try:
                df = pd.read_csv(bulk_csv_path, on_bad_lines="skip", encoding=enc, low_memory=False, skipinitialspace=True, quoting=1)
                break
            except Exception:
                continue
        if df is None:
            try:
                df = pd.read_csv(bulk_csv_path, on_bad_lines="skip", encoding="utf-8", low_memory=False, dtype=str, skipinitialspace=True, engine="python")
            except Exception as exc:
                logger.error("[Parse] All parsing strategies failed: %s", exc)
                return False

        if df.empty:
            logger.error("[Parse] CSV is empty or could not be parsed")
            return False
        logger.info("[Parse] Loaded %d records", len(df))

        # PHASE 3: Filter distressed accounts
        df_distressed = _filter_distressed_accounts(df, min_years=min_years_delinquent)
        if df_distressed.empty:
            logger.warning("[Filter] No distressed accounts found")
            return False

        # PHASE 3.5: DB dedup — only enrich NEW accounts (county-scoped)
        existing_accounts = _get_existing_tax_records(tax_year, county_id=county_id)
        # Detect account column — varies by county portal
        account_col = None
        for _col in ("Account Number", "Account #", "Account", "Parcel ID", "account_number"):
            if _col in df_distressed.columns:
                account_col = _col
                break
        initial_count = len(df_distressed)
        if account_col and existing_accounts:
            df_new_only = df_distressed[~df_distressed[account_col].isin(existing_accounts)].copy()
            logger.info("[Dedup] %d existing filtered, %d new remain", initial_count - len(df_new_only), len(df_new_only))
        else:
            if not account_col:
                logger.warning("[Dedup] Account column not found — skipping pre-dedup")
            df_new_only = df_distressed
            logger.info("[Dedup] No existing records — all %d are new", len(df_new_only))

        if df_new_only.empty:
            logger.info("[Dedup] All distressed records already in DB — nothing to do")
            return True

        # PHASE 4: SNIPER
        _parcel_url = _get_county(county_id)["urls"].get("parcel") or PARCEL_LOOKUP_URL
        df_enriched = await _sniper_enrich_accounts(df_new_only, max_accounts=max_sniper_accounts, parcel_lookup_url=_parcel_url)

        # PHASE 5: Save to county-scoped output directory
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        out_dir = RAW_TAX_DELINQUENCIES_DIR / county_id / "new"
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"tax_delinquencies_{county_id}_{today}.csv"
        df_enriched.to_csv(csv_path, index=False)
        logger.info("[Save] Saved %d records → %s", len(df_enriched), csv_path)

        if load_to_db:
            _load_to_database(csv_path, county_id)

        return True

    except Exception as e:
        logger.error("[Pipeline] Execution failed: %s", e)
        logger.debug(traceback.format_exc())
        return False


# ---------------------------------------------------------------------------
# DB load
# ---------------------------------------------------------------------------

def _load_to_database(csv_path: Path, county_id: str) -> None:
    """Load scraped CSV into the tax_delinquencies table.

    Delegates to the shared `load_scraped_data_to_db` helper so this engine uses
    the same loader path as the admin upload endpoint (preserving fallback).
    """
    from src.utils.scraper_db_helper import load_scraped_data_to_db

    try:
        load_scraped_data_to_db(
            'tax',
            csv_path,
            destination_dir=RAW_TAX_DELINQUENCIES_DIR / county_id,
        )
    except Exception as e:
        logger.error("[DB] Load failed: %s", e)
        logger.debug(traceback.format_exc())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from src.utils.scraper_db_helper import record_scraper_stats

    parser = argparse.ArgumentParser(description="Tax Delinquent RADAR + SNIPER Pipeline")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough",
                        help="County identifier (default: hillsborough)")
    parser.add_argument("--mode", choices=["download-only", "full-pipeline"], default="full-pipeline",
                        help="Run mode (default: full-pipeline)")
    parser.add_argument("--tax-year", type=int, default=DEFAULT_TAX_YEAR,
                        help=f"Tax year to query (default: {DEFAULT_TAX_YEAR})")
    parser.add_argument("--min-years", type=int, default=MIN_YEARS_DELINQUENT,
                        help=f"Minimum years delinquent (default: {MIN_YEARS_DELINQUENT})")
    parser.add_argument("--max-sniper", type=int, default=100,
                        help="Max accounts to enrich in SNIPER phase (default: 100)")
    parser.add_argument("--status", default=DEFAULT_ACCOUNT_STATUS,
                        help=f"Account status filter (default: {DEFAULT_ACCOUNT_STATUS})")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip RADAR phase and use existing CSV")
    parser.add_argument("--load-to-db", action="store_true",
                        help="Load scraped records into database after pipeline completes")
    parser.add_argument("--headful", action="store_true",
                        help="Open a real browser window (useful for debugging)")
    parser.add_argument("--cf-bypass", dest="cf_bypass", action="store_true", default=None,
                        help="Force Cloudflare-bypass mode (Edge + warmed persistent profile, no proxy, no stealth). "
                             "Otherwise auto-enabled when source has cf_bypass_required=true.")
    parser.add_argument("--no-cf-bypass", dest="cf_bypass", action="store_false",
                        help="Force-disable CF-bypass even if source has cf_bypass_required=true.")
    args = parser.parse_args()

    _t0 = time.monotonic()
    try:
        logger.info("Starting Tax Delinquent Engine (%s mode, county=%s)", args.mode, args.county_id)

        if args.mode == "download-only":
            asyncio.run(download_tax_delinquent_report(
                tax_year=args.tax_year,
                account_status=args.status,
                county_id=args.county_id,
                headful=args.headful,
                cf_bypass=args.cf_bypass,
            ))
        else:
            asyncio.run(run_radar_sniper_pipeline(
                tax_year=args.tax_year,
                account_status=args.status,
                min_years_delinquent=args.min_years,
                max_sniper_accounts=args.max_sniper,
                skip_download=args.skip_download,
                county_id=args.county_id,
                headful=args.headful,
                load_to_db=args.load_to_db,
                cf_bypass=args.cf_bypass,
            ))

        logger.info("Tax Delinquent Engine completed successfully")

        try:
            record_scraper_stats(
                source_type="tax_delinquencies",
                total_scraped=0, matched=0, unmatched=0, skipped=0,
                run_success=True,
                duration_seconds=round(time.monotonic() - _t0, 2),
                county_id=args.county_id,
            )
        except Exception as _se:
            logger.warning("Could not record scraper stats: %s", _se)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        logger.debug(traceback.format_exc())
        try:
            record_scraper_stats(
                source_type="tax_delinquencies",
                total_scraped=0, matched=0, unmatched=0, skipped=0,
                run_success=False,
                error_message=str(e)[:500],
                duration_seconds=round(time.monotonic() - _t0, 2),
                county_id=args.county_id,
            )
        except Exception:
            pass
        sys.exit(1)
