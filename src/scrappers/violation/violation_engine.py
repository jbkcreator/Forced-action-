"""
Code Enforcement Violations Data Collection Pipeline — county-agnostic, browser-use only.

Navigates the county code enforcement portal, applies a date-range filter, extracts
all violation records from every page, and returns them as structured JSON.  Python
normalises the column names and saves a CSV.

Usage:
    python -m src.scrappers.violation.violation_engine --county-id hillsborough
    python -m src.scrappers.violation.violation_engine --county-id hillsborough --start-date 2026-05-01 --end-date 2026-05-07
    python -m src.scrappers.violation.violation_engine --county-id hillsborough --load-to-db --headful
    python -m src.scrappers.violation.violation_engine --county-id pinellas --load-to-db
"""

import asyncio
import argparse
import json
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from config.constants import (
    RAW_VIOLATIONS_DIR,
    VIOLATION_SEARCH_URL,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.utils.county_config import get_county_config
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

RAW_VIOLATIONS_DIR.mkdir(parents=True, exist_ok=True)

# Accela portal column aliases — maps any variant name to the canonical key
COLUMN_ALIASES: dict[str, list[str]] = {
    "Date": ["Date", "Filed Date", "Opened Date", "Application Date", "Open Date"],
    "Record Number": ["Record Number", "Application Number", "Record #", "Case Number"],
    "Record Type": ["Record Type", "Application Type", "Type", "Violation Type"],
    "Description": ["Description", "Desc", "Violation Description"],
    "Project Name": ["Project Name", "Project"],
    "Related Records": ["Related Records", "Related"],
    "Status": ["Status", "Application Status"],
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

    portal_url = source.get("url", VIOLATION_SEARCH_URL)
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
        "The agent controls a Chromium browser and must return extracted data as JSON. "
        "Write concise, numbered steps in plain English. "
        "Do NOT add any explanation outside the task text."
    )

    user_prompt = f"""Generate a browser-use agent task to extract code enforcement violation records from a county portal.

Source metadata:
{json.dumps(meta, indent=2)}

Requirements:
- Navigate to portal_url.
- Fill the start date field with {start_str} and the end date field with {end_str} (MM/DD/YYYY format).
- Submit the search and wait for the results table to appear.
- Scrape ALL rows from ALL pages: for each page, extract every row's fields, then click Next (if available) and repeat.
- Return the collected records as a single JSON array where each element is an object with keys matching the column headers.
- Include: Record Number, Date, Record Type, Description, Status, Address, Short Notes (use empty string for missing fields).
- The JSON array must be the ONLY content in the final result — no explanatory text around it.
- Keep the task under 400 words.
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
    portal_url = source.get("url", VIOLATION_SEARCH_URL)
    nav_hint = source.get("navigation_hint", "") or ""
    return (
        f"Go to {portal_url}.\n"
        "Wait 5 seconds for the page to fully load.\n"
        f"{nav_hint}\n"
        f"Fill the start date field with {start_str} and the end date field with {end_str}.\n"
        "Submit the search and wait for the results table to appear.\n"
        "Scrape ALL rows from ALL pages:\n"
        "  - Extract every row from the current page.\n"
        "  - Click the Next button to advance.\n"
        "  - Repeat until Next is disabled or absent.\n"
        "Return the results as a pure JSON array. Each element must have these keys:\n"
        "  Record Number, Date, Record Type, Description, Status, Address, Short Notes.\n"
        "Use empty string for any missing field. Return ONLY the JSON array."
    )


# ---------------------------------------------------------------------------
# Browser-use agent (JSON extraction mode)
# ---------------------------------------------------------------------------

async def run_browser_agent(task: str, headful: bool = False) -> tuple:
    """
    Run a browser-use Agent that navigates the portal and returns extracted data.

    Returns (history, None). Caller reads history.final_result() for the JSON payload.
    """
    from browser_use import Agent, Browser

    llm = _make_llm()

    browser = Browser(
        headless=not headful,
        disable_security=True,
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
        max_steps=120,
        use_judge=False,
    )

    logger.info("[Agent] Starting browser-use agent (extraction mode)...")
    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[Agent] Agent did not complete within step budget")
        return history, None
    except Exception as e:
        logger.error("[Agent] Run failed: %s", e)
        logger.debug(traceback.format_exc())
        return None, None


# ---------------------------------------------------------------------------
# Result parsing
# ---------------------------------------------------------------------------

def _parse_agent_result(result_str: str) -> list[dict]:
    """
    Parse the agent's final result into a list of row dicts.

    Accepts:
      - Pure JSON array
      - JSON array wrapped in markdown code fences
      - Pipe-delimited table (legacy fallback)
    """
    if not result_str:
        return []

    text = result_str.strip()

    # Strip markdown fences
    if text.startswith("```"):
        lines = text.split("\n")
        inner = [l for l in lines if not l.startswith("```")]
        text = "\n".join(inner).strip()

    # Try JSON first
    try:
        data = json.loads(text)
        if isinstance(data, list):
            logger.info("[Parse] Parsed %d records from JSON", len(data))
            return data
        if isinstance(data, dict) and any(isinstance(v, list) for v in data.values()):
            # Wrapped: {"records": [...]}
            for v in data.values():
                if isinstance(v, list):
                    logger.info("[Parse] Parsed %d records from wrapped JSON", len(v))
                    return v
    except (json.JSONDecodeError, ValueError):
        pass

    # Fallback: pipe-delimited
    rows = []
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    header = None
    for line in lines:
        if "|" not in line:
            continue
        if line.replace("|", "").replace("-", "").strip() == "":
            continue
        parts = [p.strip() for p in line.split("|")]
        if header is None:
            header = parts
            continue
        if len(parts) >= len(header):
            rows.append(dict(zip(header, parts)))

    if rows:
        logger.info("[Parse] Parsed %d records from pipe-delimited fallback", len(rows))
    else:
        logger.warning("[Parse] Could not parse any records from agent result")

    return rows


def _normalize_columns(rows: list[dict], source: dict) -> pd.DataFrame:
    """
    Map raw column names to canonical names.

    Order:
      1. Apply admin-configured renames from CountyColumnMapping.mapping (was
         previously source["ori_column_map"]; now read from the dedicated
         ColumnMapper table).
      2. Apply the engine-internal COLUMN_ALIASES fallback for Accela variants
         that don't yet have an admin mapping.
    """
    source_id = source.get("source_id")
    col_rename: dict = {}
    if source_id:
        try:
            from src.loaders.column_mapper import ColumnMapper
            # Pass the actual scraped columns so an approved mapping built against
            # a different column shape (stale Accela variant) gets skipped in
            # favour of any matching pending row.
            raw_cols = list(rows[0].keys()) if rows else None
            row = ColumnMapper.fetch_mapping_row(source_id, raw_cols=raw_cols)
            if row is not None:
                col_rename = dict(row.mapping or {})
        except Exception as exc:  # noqa: BLE001 — non-critical lookup, fallback path is fine
            logger.warning("[Normalize] ColumnMapper lookup failed (%s) — relying on aliases", exc)

    normalized = []
    for row in rows:
        if col_rename:
            row = {col_rename.get(k, k): v for k, v in row.items()}
        norm_row = {}
        for target_col, aliases in COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in row:
                    norm_row[target_col] = row[alias]
                    break
            if target_col not in norm_row:
                norm_row[target_col] = row.get(target_col, "")
        normalized.append(norm_row)
    return pd.DataFrame(normalized)


# ---------------------------------------------------------------------------
# Playwright scrape (bypasses browser-use timeout issues)
# ---------------------------------------------------------------------------

async def _scrape_with_playwright(
    playwright_code: str,
    source: dict,
    start_str: str,
    end_str: str,
    download_dir: Path,
    headful: bool = False,
    county_id: str = "",
) -> Optional[pd.DataFrame]:
    from playwright.async_api import async_playwright
    from src.utils.action_sequence import execute_playwright_code, PlaywrightCodeError

    url = source.get("url") or VIOLATION_SEARCH_URL

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not headful,
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
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            df = await execute_playwright_code(
                playwright_code,
                page,
                download_dir,
                placeholders={"url": url, "start_date": start_str, "end_date": end_str},
                county_id=county_id,
            )
            return df if not df.empty else None
        except PlaywrightCodeError as e:
            logger.error("[Playwright] Scrape failed: %s", e)
            return None
        finally:
            try:
                await browser.close()
            except Exception:
                pass


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

    # Find the violations source from county config (sources is a dict keyed by signal_type)
    source = county_cfg.get("sources", {}).get("violations")
    if source is None:
        portal_url = county_cfg.get("urls", {}).get("violation") or VIOLATION_SEARCH_URL
        source = {"url": portal_url, "signal_type": "violations"}

    # PRR-only counties (e.g. Pinellas) require a manual public-records request —
    # automated scraping is not possible; skip with a clear log.
    if source.get("prr_only"):
        logger.info(
            "[Main] %s violations source is PRR-only (manual public-records request required). "
            "Skipping automated scrape for county '%s'.",
            county_cfg.get("display_name", county_id), county_id,
        )
        return

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

    download_dir = RAW_VIOLATIONS_DIR / county_id
    download_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("%s CODE VIOLATIONS — DATA COLLECTION", county_cfg["display_name"].upper())
    logger.info("Date range: %s → %s", start_str, end_str)
    logger.info("=" * 60)

    playwright_code = source.get("playwright_code")
    if playwright_code:
        logger.info("[Main] Using Playwright scrape mode")
        raw_df = await _scrape_with_playwright(
            playwright_code, source, start_str, end_str, download_dir,
            headful=args.headful, county_id=county_id,
        )
        if raw_df is None or raw_df.empty:
            logger.info("[Main] 0 violation records from Playwright — nothing to load")
            return
        df = _normalize_columns(raw_df.to_dict("records"), source)
    else:
        task = build_agent_task(source, start_str, end_str)
        history, _ = await run_browser_agent(task, headful=args.headful)

        if history is None:
            logger.error("[Main] Agent run returned no history — aborting")
            return

        final_result = history.final_result()
        if not final_result:
            logger.error("[Main] Agent returned empty result — aborting")
            return

        logger.info("[Main] Agent result length: %d chars", len(str(final_result)))
        rows = _parse_agent_result(str(final_result))

        if not rows:
            logger.info("[Main] 0 violation records extracted for this date range")
            return

        df = _normalize_columns(rows, source)

    logger.info("[Main] %d total records after normalization", len(df))

    today_str = datetime.now().strftime("%Y%m%d")
    out_dir = RAW_VIOLATIONS_DIR / county_id / "new"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"violations_{county_id}_{today_str}.csv"
    df.to_csv(csv_path, index=False)
    logger.info("[Main] Saved %d records → %s", len(df), csv_path)

    if not args.load_to_db:
        logger.info("[Main] Skipping DB load (pass --load-to-db to enable)")
        return

    logger.info("[Main] Loading violations into database...")
    from src.core.database import get_db_context
    from src.loaders.violations import ViolationLoader

    try:
        with get_db_context() as session:
            loader = ViolationLoader(session, county_id)
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
            with get_db_context() as session:
                loader2 = ViolationLoader(session, county_id)
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
            source_type="violations",
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
    parser = argparse.ArgumentParser(description="Scrape code enforcement violations from county portal")
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
