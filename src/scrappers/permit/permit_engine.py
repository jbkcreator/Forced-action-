"""
Building Permit Data Collection Pipeline — county-agnostic, browser-use only.

Navigates the county Accela permit portal, applies date filters, triggers the
"Export to Spreadsheet" button to download all results in one shot, then loads
the file into a DataFrame and saves it to the raw data directory.

Usage:
    python -m src.scrappers.permit.permit_engine --county-id hillsborough
    python -m src.scrappers.permit.permit_engine --county-id hillsborough --start-date 2026-05-01 --end-date 2026-05-07
    python -m src.scrappers.permit.permit_engine --county-id hillsborough --load-to-db --headful
    python -m src.scrappers.permit.permit_engine --county-id pinellas --load-to-db
"""

import asyncio
import argparse
import json
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from config.constants import (
    RAW_PERMIT_DIR,
    PERMIT_SEARCH_URL,
    DOWNLOAD_FILE_PATTERNS,
    TEMP_DOWNLOADS_DIR,
    BROWSER_DOWNLOAD_TEMP_PATTERN,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.utils.county_config import get_county_config
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

RAW_PERMIT_DIR.mkdir(parents=True, exist_ok=True)

# Canonical column aliases — Accela portals vary slightly by county
COLUMN_ALIASES: dict[str, list[str]] = {
    "Date": ["Date", "Filed Date", "Opened Date", "Application Date", "Open Date"],
    "Record Number": ["Record Number", "Application Number", "Record #", "Permit Number"],
    "Record Type": ["Record Type", "Application Type", "Type", "Permit Type"],
    "Description": ["Description", "Desc", "Permit Description"],
    "Project Name": ["Project Name", "Project"],
    "Related Records": ["Related Records", "Related"],
    "Status": ["Status", "Application Status", "Permit Status"],
    "Short Notes": ["Short Notes", "Notes", "Short Note"],
    "Address": ["Address", "Location", "Parcel Address", "Property Address"],
}


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


def build_agent_task(source: dict, start_str: str, end_str: str) -> str:
    """
    Generate a browser-use agent task from county source metadata + date range.
    Uses Claude to produce step-by-step instructions tailored to the source's
    navigation_hint and description. Falls back to a template on LLM failure.
    """
    import anthropic
    from config.settings import get_settings

    portal_url = source.get("url", PERMIT_SEARCH_URL)
    description = source.get("description", "")
    nav_hint = source.get("navigation_hint", "") or ""

    meta = {
        "portal_url": portal_url,
        "start_date": start_str,
        "end_date": end_str,
        "description": description,
        "navigation_hint": nav_hint,
    }

    system_prompt = (
        "You generate browser-automation task instructions for a browser-use Agent. "
        "The agent controls a Chromium browser and must trigger a file download (CSV or Excel export). "
        "Write concise, numbered steps in plain English. "
        "Do NOT add any explanation outside the task text."
    )

    user_prompt = f"""Generate a browser-use agent task to download building permit records from a county Accela portal.

Source metadata:
{json.dumps(meta, indent=2)}

Requirements:
- Navigate to portal_url.
- If the search form is collapsed, expand it (look for "Search Applications" link).
- Fill the start date field with {start_str} and the end date field with {end_str} (MM/DD/YYYY format).
- Submit the search and wait for the results table to appear.
- Find and click the "Export to Spreadsheet" or "Export Results" or "Download" button to download all records.
- Wait at least 20 seconds for the file download to complete before finishing.
- Do not navigate away or open new tabs during the download.
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
        return _template_task(source, start_str, end_str)


def _template_task(source: dict, start_str: str, end_str: str) -> str:
    """Template fallback when LLM is unavailable."""
    portal_url = source.get("url", PERMIT_SEARCH_URL)
    nav_hint = source.get("navigation_hint", "") or ""
    return (
        f"Go to {portal_url}.\n"
        "Wait 5 seconds for the page to fully load.\n"
        f"{nav_hint}\n"
        "If you see a 'Search Applications' link, click it to expand the search form.\n"
        f"Fill the start date field with {start_str} and the end date field with {end_str}.\n"
        "Click the Search button and wait for the results table to load.\n"
        "Find the 'Export to Spreadsheet' or 'Export Results' or 'Download' button and click it.\n"
        "Wait 20 seconds for the file download to complete.\n"
        "Do not navigate away or open new tabs."
    )


# ---------------------------------------------------------------------------
# Browser-use agent (file download mode)
# ---------------------------------------------------------------------------

async def run_browser_agent(
    task: str,
    download_dir: Path,
    headful: bool = False,
) -> tuple:
    """
    Run a browser-use Agent that triggers a file download (export button).

    Returns (history, start_time). Caller uses start_time with _locate_download()
    to find the file the agent downloaded.
    """
    from browser_use import Agent, Browser
    from src.utils.http_helpers import get_browser_use_proxy

    llm = _make_llm()

    browser = Browser(
        headless=not headful,
        disable_security=True,
        proxy=get_browser_use_proxy(),
        downloads_path=str(download_dir),
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
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
        max_steps=60,
        use_judge=False,
    )

    start_time = time.time()
    logger.info("[Agent] Starting browser-use agent (download mode)...")
    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[Agent] Agent did not complete within step budget")
        return history, start_time
    except Exception as e:
        logger.error("[Agent] Run failed: %s", e)
        logger.debug(traceback.format_exc())
        return None, start_time


# ---------------------------------------------------------------------------
# Download file detection
# ---------------------------------------------------------------------------

def _locate_download(download_dir: Path, start_time: float) -> Optional[Path]:
    """Find a file downloaded after start_time in download_dir and temp dirs."""
    def recent_candidates(folder: Path):
        if not folder.exists():
            return []
        paths = []
        for pattern in DOWNLOAD_FILE_PATTERNS:
            for f in folder.glob(pattern):
                try:
                    if f.stat().st_mtime >= start_time:
                        paths.append(f)
                except FileNotFoundError:
                    continue
        return paths

    candidates = recent_candidates(download_dir)

    temp_base = TEMP_DOWNLOADS_DIR
    if temp_base.exists():
        for temp_dir in temp_base.glob(BROWSER_DOWNLOAD_TEMP_PATTERN):
            candidates.extend(recent_candidates(temp_dir))

    if not candidates:
        logger.warning("[Locate] No downloaded files found after agent run")
        return None

    most_recent = max(candidates, key=lambda p: p.stat().st_mtime)
    logger.info("[Locate] Found download: %s", most_recent)
    return most_recent


# ---------------------------------------------------------------------------
# Data processing
# ---------------------------------------------------------------------------

def _load_file(file_path: Path) -> pd.DataFrame:
    """Load the downloaded CSV or Excel file into a DataFrame."""
    logger.info("[Process] Loading: %s", file_path)
    if file_path.suffix.lower() in (".xls", ".xlsx"):
        df = pd.read_excel(file_path)
        logger.info("[Process] Loaded %d records from Excel", len(df))
        return df
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            df = pd.read_csv(file_path, encoding=enc)
            logger.info("[Process] Loaded %d records (enc=%s)", len(df), enc)
            return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    raise ValueError(f"Could not read file with any known encoding: {file_path}")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map raw column names to canonical names using COLUMN_ALIASES."""
    rename_map = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in df.columns and alias != canonical:
                rename_map[alias] = canonical
                break
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


# ---------------------------------------------------------------------------
# Stats helper
# ---------------------------------------------------------------------------

def _record_stats(source_type: str, **kwargs):
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(source_type=source_type, **kwargs)
    except Exception as e:
        logger.warning("[Stats] Could not record scraper stats (non-critical): %s", e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(args):
    county_id = args.county_id
    county_cfg = get_county_config(county_id)

    source = county_cfg.get("sources", {}).get("permits")
    if source is None:
        portal_url = county_cfg.get("urls", {}).get("permit") or PERMIT_SEARCH_URL
        source = {"url": portal_url, "signal_type": "permits"}

    if args.end_date:
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        end_dt = datetime.now()
    if args.start_date:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_dt = end_dt - timedelta(days=1)

    start_str = start_dt.strftime("%m/%d/%Y")
    end_str = end_dt.strftime("%m/%d/%Y")

    logger.info("=" * 60)
    logger.info("%s BUILDING PERMITS — DATA COLLECTION", county_cfg["display_name"].upper())
    logger.info("Date range: %s → %s", start_str, end_str)
    logger.info("=" * 60)

    download_dir = RAW_PERMIT_DIR / county_id / "downloads"
    download_dir.mkdir(parents=True, exist_ok=True)

    task = build_agent_task(source, start_str, end_str)
    history, start_time = await run_browser_agent(task, download_dir, headful=args.headful)

    if history is None:
        logger.error("[Main] Agent run returned no history — aborting")
        return

    # Give the browser a moment to finish writing
    await asyncio.sleep(5)

    downloaded_file = _locate_download(download_dir, start_time)
    if downloaded_file is None:
        logger.error("[Main] No downloaded file found — aborting")
        return

    df = _load_file(downloaded_file)
    df = _normalize_columns(df)
    logger.info("[Main] %d total records after normalization", len(df))

    today_str = datetime.now().strftime("%Y%m%d")
    out_dir = RAW_PERMIT_DIR / county_id / "new"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"permits_{county_id}_{today_str}.csv"
    df.to_csv(csv_path, index=False)
    logger.info("[Main] Saved %d records → %s", len(df), csv_path)

    # Clean up the raw download
    try:
        downloaded_file.unlink()
    except Exception:
        pass

    if not args.load_to_db:
        logger.info("[Main] Skipping DB load (pass --load-to-db to enable)")
        return

    logger.info("[Main] Loading permits into database...")
    from src.core.database import get_db_context
    from src.loaders.permits import PermitLoader

    try:
        with get_db_context() as session:
            loader = PermitLoader(session, county_id)
            matched, unmatched, skipped = loader.load_from_csv(
                str(csv_path),
                skip_duplicates=True,
            )
            session.commit()

        total = matched + unmatched + skipped
        match_rate = (matched / total * 100) if total > 0 else 0
        logger.info("=" * 60)
        logger.info("DATABASE LOAD SUMMARY")
        logger.info("  Matched:    %6d", matched)
        logger.info("  Unmatched:  %6d", unmatched)
        logger.info("  Skipped:    %6d", skipped)
        logger.info("  Match Rate: %5.1f%%", match_rate)
        logger.info("=" * 60)

        # Rescore affected properties
        try:
            affected_ids = loader.get_affected_property_ids() if hasattr(loader, "get_affected_property_ids") else []
            if affected_ids:
                logger.info("[Rescore] Triggering CDS rescore for %d properties...", len(affected_ids))
                from src.services.cds_engine import MultiVerticalScorer
                with get_db_context() as score_session:
                    scorer = MultiVerticalScorer(score_session)
                    scorer.score_properties_by_ids(affected_ids, save_to_db=True, county_id=county_id)
                    score_session.commit()
                logger.info("[Rescore] CDS rescore completed")
        except Exception as score_err:
            logger.warning("[Rescore] CDS rescore failed (non-critical): %s", score_err)

        _record_stats(
            source_type="permits",
            county_id=county_id,
            total_scraped=total,
            matched=matched,
            unmatched=unmatched,
            skipped=skipped,
        )

        try:
            csv_path.unlink()
            logger.info("[Main] CSV deleted after successful DB insertion")
        except Exception:
            pass

    except Exception as e:
        logger.error("[Main] DB load failed: %s", e)
        logger.debug(traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape building permits from county Accela portal")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough",
                        help="County identifier (default: hillsborough)")
    parser.add_argument("--start-date", type=str,
                        help="Start date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--end-date", type=str,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--load-to-db", action="store_true",
                        help="Load scraped data into database after scraping")
    parser.add_argument("--headful", action="store_true",
                        help="Open a real browser window (requires xvfb-run on server)")
    args = parser.parse_args()
    asyncio.run(main(args))
