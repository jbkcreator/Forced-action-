"""
Building Permit Data Collection Pipeline — county-agnostic, 4-mode scraping.

Scrape modes (set via source special_flags.scrape_mode):
  download_direct  Direct HTTP GET/POST — fastest, no browser, needs download_url +
                   download_params in source config.
  selector         Playwright fills form fields from selectors config, clicks export,
                   captures file download. Falls back to browser_use automatically
                   when selectors fail (stale DOM, portal update).
  extract          browser-use reads table rows, paginates, returns JSON — no file
                   download needed.
  download         browser-use clicks the export button and waits for the file
                   download (default / legacy mode).

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


def _get_scrape_mode(source: dict) -> str:
    """
    Return the effective permit-engine sub-mode for main()'s dispatch.

    The top-level `scrape_mode` enum on county_sources is:
      playwright_only / playwright_then_ai → "selector"  (cached code path)
      ai_only / unset                      → look up permit-specific sub-mode

    The AI sub-mode (download_direct / extract / download) is stored in
    special_flags.permit_ai_strategy. Sources that don't set it default to
    "download" (browser-use clicks export, waits for file).
    """
    top_level = source.get("scrape_mode")
    if top_level in ("playwright_only", "playwright_then_ai"):
        return "selector"
    return source.get("permit_ai_strategy") or "download"


def build_agent_task(source: dict, start_str: str, end_str: str) -> str:
    """
    Generate a browser-use agent task from county source metadata + date range.
    Routes to extract task or download task based on source special_flags.scrape_mode.
    Falls back to template on LLM failure.
    """
    if _get_scrape_mode(source) == "extract":
        return build_extract_task(source, start_str, end_str)
    return build_download_task(source, start_str, end_str)


def build_download_task(source: dict, start_str: str, end_str: str) -> str:
    """Generate a task that clicks the export button and waits for a file download."""
    import anthropic
    from config.settings import get_settings

    portal_url = source.get("url", PERMIT_SEARCH_URL)
    nav_hint = source.get("navigation_hint", "") or ""

    meta = {
        "portal_url": portal_url,
        "start_date": start_str,
        "end_date": end_str,
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
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key.get_secret_value())
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            temperature=0,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        task = response.content[0].text.strip()
        logger.info("[LLM] Generated download task:\n%s", task)
        return task
    except Exception as e:
        logger.warning("[LLM] Task generation failed (%s) — using template fallback", e)
        return _template_download_task(source, start_str, end_str)


def _template_download_task(source: dict, start_str: str, end_str: str) -> str:
    """Template fallback for download mode."""
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


def build_extract_task(source: dict, start_str: str, end_str: str) -> str:
    """
    Generate a task that reads records directly from the results table,
    paginates through all pages, and returns structured JSON.
    Used when the portal export is unreliable (e.g. Pinellas Accela).
    """
    portal_url = source.get("url", PERMIT_SEARCH_URL)
    nav_hint = source.get("navigation_hint", "") or ""

    return (
        f"Go to {portal_url}.\n"
        "Wait 5 seconds for the page to fully load.\n"
        f"{nav_hint}\n"
        "Navigate to the Building module if not already there.\n"
        "Search for permits by date range: set the application date start field to "
        f"{start_str} and the end date field to {end_str} (MM/DD/YYYY format).\n"
        "Click Search and wait for the results table to load.\n"
        "Extract ALL records without losing any information. For each record capture these exact fields:\n"
        "  - Date\n"
        "  - Record Type\n"
        "  - Record Number\n"
        "  - Status\n"
        "  - Action\n"
        "  - Address\n"
        "  - Project Name\n"
        "  - Expiration Date\n"
        "  - Description\n"
        "If a field is not present in the table, set it to an empty string.\n"
        "After extracting all records on the current page, check for a 'Next' pagination button.\n"
        "If it exists, click it and repeat extraction. Continue until there are no more pages.\n"
        "Return ALL collected records as a single JSON object in this exact format:\n"
        '{"permits": [{"Date": "", "Record Type": "", "Record Number": "", "Status": "", '
        '"Action": "", "Address": "", "Project Name": "", "Expiration Date": "", "Description": ""}]}\n'
        'If no records are found return: {"permits": []}\n'
        "Do not save any files. Return only the JSON as your final result."
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

    llm = _make_llm()

    browser = Browser(
        headless=not headful,
        disable_security=True,
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
# Direct HTTP download (no browser)
# ---------------------------------------------------------------------------

async def _scrape_download_direct(
    source: dict,
    start_dt: datetime,
    end_dt: datetime,
    download_dir: Path,
) -> Optional[Path]:
    """
    Download permit data via a plain HTTP request — no browser launched.

    Requires source config keys (spread from special_flags):
      download_url          Full URL to request.
      download_method       "GET" or "POST" (default "GET").
      download_params       Dict of query/form params; use {start_date} and
                            {end_date} as placeholders (formatted per
                            download_date_format).
      download_date_format  strftime format for param substitution
                            (default "%Y-%m-%d").
      download_headers      Optional dict of extra HTTP headers.

    Returns the saved Path on success, None on failure.
    """
    import requests

    download_url = source.get("download_url")
    if not download_url:
        logger.error("[DirectDL] scrape_mode=download_direct requires source.download_url")
        return None

    method = source.get("download_method", "GET").upper()
    raw_params = source.get("download_params") or {}
    date_fmt = source.get("download_date_format", "%Y-%m-%d")
    headers = source.get("download_headers") or {}

    start_formatted = start_dt.strftime(date_fmt)
    end_formatted = end_dt.strftime(date_fmt)

    params = {
        k: v.replace("{start_date}", start_formatted).replace("{end_date}", end_formatted)
           if isinstance(v, str) else v
        for k, v in raw_params.items()
    }

    logger.info("[DirectDL] %s %s  params=%s", method, download_url, params)

    try:
        if method == "POST":
            resp = requests.post(download_url, data=params, headers=headers, timeout=60)
        else:
            resp = requests.get(download_url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
    except Exception as exc:
        logger.error("[DirectDL] Request failed: %s", exc)
        return None

    content_type = resp.headers.get("Content-Type", "")
    if "spreadsheetml" in content_type or "excel" in content_type or download_url.endswith(".xlsx"):
        ext = ".xlsx"
    elif download_url.endswith(".xls"):
        ext = ".xls"
    else:
        ext = ".csv"

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = download_dir / f"direct_{stamp}{ext}"
    dest.write_bytes(resp.content)
    logger.info("[DirectDL] Saved %d bytes → %s", len(resp.content), dest)
    return dest


# ---------------------------------------------------------------------------
# Playwright selector scraping — dynamic action sequence
# ---------------------------------------------------------------------------

_browser_args = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
    "--window-size=1920,1080",
]


async def _scrape_selector(
    source: dict,
    start_str: str,
    end_str: str,
    download_dir: Path,
    headful: bool = False,
    county_id: str = None,
) -> pd.DataFrame:
    """
    Playwright selector-based scraping — LLM-generated Python code approach.

    First run: LLM generates an async def run_scrape(...) function, validates it
    against a forbidden-name allowlist, caches code in CountySource.special_flags.playwright_code.

    Subsequent runs: cached code exec'd directly — zero LLM cost.

    On execution failure: code cleared from DB; next run regenerates (self-heal).
    Raises on any failure so the caller falls back to browser_use download mode.
    """
    from playwright.async_api import async_playwright
    from src.utils.action_sequence import (
        PlaywrightCodeError,
        generate_playwright_code,
        execute_playwright_code,
        persist_playwright_code,
        clear_playwright_code,
    )

    source_id = source.get("source_id")
    code: Optional[str] = source.get("playwright_code")
    is_new_code = False

    if not code:
        logger.info("[Selector] No cached playwright_code — generating via LLM")
        code = generate_playwright_code(source)
        is_new_code = True
    else:
        logger.info("[Selector] Using cached playwright_code (%d chars)", len(code))
        if source.get("playwright_code_approved") is False:
            logger.warning(
                "[Selector] Running UNAPPROVED playwright_code for source_id=%s — "
                "admin should review and approve via /admin/sources",
                source_id,
            )

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=not headful, args=_browser_args)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            placeholders = {
                "url": source.get("url", ""),
                "start_date": start_str,
                "end_date": end_str,
            }
            result = await execute_playwright_code(
                code, page, download_dir, placeholders, county_id
            )
        except PlaywrightCodeError as exc:
            logger.warning("[Selector] playwright_code execution failed (%s) — clearing cache", exc)
            if not is_new_code and source_id and county_id:
                # Only clear cache for code that was previously persisted —
                # newly-generated code that failed its smoke test was never persisted.
                clear_playwright_code(county_id, source_id)
            raise
        finally:
            await browser.close()

    # Smoke test passed (execute returned a DataFrame). Only now persist newly-
    # generated code — broken LLM output never reaches the DB.
    if is_new_code and source_id and county_id:
        persist_playwright_code(
            county_id, source_id, code, is_approved=False,
        )
        logger.info(
            "[Selector] playwright_code persisted after successful smoke test "
            "(source_id=%s, %d chars, awaiting admin approval)",
            source_id, len(code),
        )
    return result


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
# Extract mode result parser
# ---------------------------------------------------------------------------

def _parse_extract_result(history, county_id: str) -> Optional[pd.DataFrame]:
    """
    Parse structured JSON returned by the extract-mode agent.
    Returns a DataFrame with canonical permit columns, or None on failure.
    """
    if history is None:
        logger.error("[Extract] No agent history — agent failed to run")
        return None

    final = history.final_result()
    if not final:
        contents = history.extracted_content()
        final = contents[-1] if contents else None
    if not final:
        logger.error("[Extract] Agent returned no result")
        return None

    json_start = final.find("{")
    json_end = final.rfind("}") + 1
    if json_start == -1 or json_end == 0:
        logger.error("[Extract] No JSON found in agent result: %r", final[:300])
        return None

    try:
        data = json.loads(final[json_start:json_end])
    except json.JSONDecodeError as e:
        logger.error("[Extract] JSON decode failed: %s | snippet: %r", e, final[json_start:json_end][:300])
        return None

    permits = data.get("permits", [])
    if not permits:
        logger.info("[Extract] Agent returned 0 permit records")
        return pd.DataFrame()

    logger.info("[Extract] Agent extracted %d permit records", len(permits))
    df = pd.DataFrame(permits)

    # Ensure all canonical columns are present
    canonical = ["Date", "Record Type", "Record Number", "Status", "Action",
                 "Address", "Project Name", "Expiration Date", "Description"]
    for col in canonical:
        if col not in df.columns:
            df[col] = ""

    df["county_id"] = county_id
    return df[canonical + ["county_id"]]


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

    scrape_mode = _get_scrape_mode(source)
    logger.info("[Main] scrape_mode=%s", scrape_mode)

    today_str = datetime.now().strftime("%Y%m%d")
    out_dir = RAW_PERMIT_DIR / county_id / "new"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"permits_{county_id}_{today_str}.csv"

    temp_file: Optional[Path] = None  # tracks any temp download for cleanup

    if scrape_mode == "download_direct":
        temp_file = await _scrape_download_direct(source, start_dt, end_dt, download_dir)
        if temp_file is None:
            logger.error("[Main] Direct download failed — aborting")
            return
        df = _load_file(temp_file)
        if df.empty:
            logger.warning("[Main] Direct download returned 0 rows — nothing to load")
            temp_file.unlink(missing_ok=True)
            return

    elif scrape_mode == "selector":
        try:
            df = await _scrape_selector(
                source, start_str, end_str, download_dir,
                headful=args.headful, county_id=county_id,
            )
        except Exception as sel_err:
            logger.warning(
                "[Selector] Playwright selectors failed (%s) — falling back to browser_use download",
                sel_err,
            )
            task = build_download_task(source, start_str, end_str)
            history, start_time = await run_browser_agent(task, download_dir, headful=args.headful)
            if history is None:
                logger.error("[Main] Browser-use fallback also failed — aborting")
                return
            await asyncio.sleep(5)
            temp_file = _locate_download(download_dir, start_time)
            if temp_file is None:
                logger.error("[Main] No file found after browser-use fallback — aborting")
                return
            df = _load_file(temp_file)
        if df.empty:
            logger.warning("[Main] Selector mode returned 0 rows — nothing to load")
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            return

    elif scrape_mode == "extract":
        # browser-use reads table rows directly and returns JSON — no file download
        task = build_agent_task(source, start_str, end_str)
        history, _ = await run_browser_agent(task, download_dir, headful=args.headful)
        df = _parse_extract_result(history, county_id)
        if df is None:
            logger.error("[Main] Extract failed — agent returned no parseable result")
            return
        if df.empty:
            logger.info("[Main] Extract returned 0 records — nothing to load")
            return

    else:  # "download" — browser-use clicks export button, waits for file
        task = build_agent_task(source, start_str, end_str)
        history, start_time = await run_browser_agent(task, download_dir, headful=args.headful)
        if history is None:
            logger.error("[Main] Agent run returned no history — aborting")
            return
        await asyncio.sleep(5)
        temp_file = _locate_download(download_dir, start_time)
        if temp_file is None:
            logger.error("[Main] No downloaded file found — aborting")
            return
        df = _load_file(temp_file)
        if df.empty:
            logger.error(
                "[Main] Downloaded CSV has 0 rows — Accela export session likely expired. "
                "Try a shorter date range or set scrape_mode=extract in county source special_flags."
            )
            temp_file.unlink(missing_ok=True)
            return

    # Clean up temp download before saving canonical CSV
    if temp_file and temp_file.exists():
        try:
            temp_file.unlink()
        except Exception:
            pass

    logger.info("[Main] %d total records", len(df))
    df.to_csv(csv_path, index=False)
    logger.info("[Main] Saved → %s", csv_path)

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
