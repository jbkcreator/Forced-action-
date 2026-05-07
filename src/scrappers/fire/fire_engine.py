"""
Fire / Calls-for-Service Scraper — county-agnostic, browser-use only.

Downloads the current "Calls for Service" CSV from the county Sheriff's Office
public GIS portal. Fire-related rows are filtered after download using
FIRE_INCIDENT_TYPES keywords.

Portal: https://gis.hcso.tampa.fl.us/publicgis/callsforservice/
Export: Dojo Select → CSV → "Export Mapped Calls Only" button

The browser-use AI agent reads the page and adapts to Dojo widget ID changes —
no hardcoded selectors needed.

Entry point:
    scrape_fire_incidents(county_id, date_range)
"""

import asyncio
import logging
import os
import shutil
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple

from config.constants import (
    RAW_FIRE_DIR,
    TEMP_DOWNLOADS_DIR,
    BROWSER_DOWNLOAD_TEMP_PATTERN,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.core.database import get_db_context
from src.core.models import Property, Incident
from src.utils.county_config import get_county
from src.utils.csv_deduplicator import deduplicate_csv, rotate_csv_archives
from src.utils.prompt_loader import get_prompt
from sqlalchemy import select, and_

logger = logging.getLogger(__name__)

_FIRE_PORTAL_URL = "https://gis.hcso.tampa.fl.us/publicgis/callsforservice/"
_FIRE_DEDUP_KEY = ["Report Number"]

FIRE_INCIDENT_TYPES = [
    "structure fire",
    "fire",
    "smoke",
    "explosion",
    "arson",
    "wildland fire",
    "vehicle fire",
]


def _is_headless() -> bool:
    return os.environ.get("FIRE_HEADLESS", "true").lower() not in ("false", "0", "no")


# ---------------------------------------------------------------------------
# LLM helper
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


# ---------------------------------------------------------------------------
# Download detection
# ---------------------------------------------------------------------------

def _locate_recent_download(start_time: float) -> Optional[Path]:
    """
    Search all known browser-use download locations for a .csv that appeared
    after start_time. Browser-use creates per-session subdirs under the OS temp
    directory, whose exact path varies by platform and environment.
    """
    import tempfile

    parent_roots: list[Path] = [
        Path(tempfile.gettempdir()),
        Path(r"C:\tmp"),
        Path("/tmp"),
    ]
    if TEMP_DOWNLOADS_DIR.exists():
        parent_roots.append(TEMP_DOWNLOADS_DIR.parent)

    seen: set = set()
    unique_roots: list[Path] = []
    for root in parent_roots:
        try:
            resolved = root.resolve()
        except (OSError, RuntimeError):
            continue
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        unique_roots.append(resolved)

    search_dirs: list[Path] = [RAW_FIRE_DIR / "temp"]
    for root in unique_roots:
        try:
            search_dirs.extend(root.glob(BROWSER_DOWNLOAD_TEMP_PATTERN))
        except OSError:
            continue

    candidates: list[Path] = []
    for folder in search_dirs:
        if not folder.exists():
            continue
        for path in folder.glob("*.csv"):
            try:
                if path.stat().st_mtime >= start_time:
                    candidates.append(path)
            except FileNotFoundError:
                continue

    if not candidates:
        logger.warning("[fire] No CSV found post-download. Searched: %s",
                       [str(d) for d in search_dirs])
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


# ---------------------------------------------------------------------------
# Browser-use download
# ---------------------------------------------------------------------------

async def _download_calls_csv(portal_url: str = _FIRE_PORTAL_URL) -> Optional[Path]:
    """
    Browser-use agent that navigates the HCSO portal, picks CSV from the
    export dropdown, and clicks the export button. Resilient to Dojo widget
    ID changes — the LLM reads the page and adapts.
    """
    from browser_use import Agent, Browser
    from src.utils.http_helpers import get_browser_use_proxy

    temp_dir = RAW_FIRE_DIR / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    start_time = time.time()

    # Try YAML prompt first; fall back to inline template
    try:
        task = get_prompt(
            "fire_prompts.yaml",
            "fire_calls_download.task_template",
            portal_url=portal_url,
        )
    except Exception:
        task = (
            f"Go to {portal_url}.\n"
            "Wait for the page to fully load (networkidle).\n"
            "Find the export format selector (a dropdown or select element).\n"
            "Set the export format to 'CSV'.\n"
            "Find and click the 'Export Mapped Calls Only' or 'Export' button.\n"
            "Wait 20 seconds for the file download to complete.\n"
            "Do not navigate away or open new tabs."
        )

    proxy = get_browser_use_proxy()
    logger.warning("[fire] Launching browser-use agent — proxy: %s",
                   "Oxylabs enabled" if proxy else "NO PROXY — direct")

    browser = Browser(
        headless=_is_headless(),
        disable_security=True,
        proxy=proxy,
        downloads_path=str(temp_dir),
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-first-run",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1920,1080",
        ],
    )

    llm = _make_llm()
    agent = Agent(task=task, llm=llm, browser=browser, max_steps=60, use_judge=False)

    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[fire] Agent could not finish within step limit")
            return None
        logger.warning("[fire] Agent completed, waiting 15s for download to finalize")
        await asyncio.sleep(15)
    except Exception as e:
        logger.error("[fire] Agent execution failed: %s", e, exc_info=True)
        return None

    downloaded = _locate_recent_download(start_time)
    if not downloaded:
        return None

    final_path = temp_dir / downloaded.name
    if downloaded.parent != temp_dir:
        try:
            shutil.move(str(downloaded), str(final_path))
        except Exception as e:
            logger.warning("[fire] Could not move downloaded file: %s", e)
            final_path = downloaded

    logger.warning("[fire] Download captured: %s (%d bytes)", final_path, final_path.stat().st_size)
    return final_path


# ---------------------------------------------------------------------------
# Data processing
# ---------------------------------------------------------------------------

def _filter_fire_rows(raw_csv: Path) -> Path:
    """Keep only fire-related rows and write a filtered CSV to temp/."""
    import pandas as pd

    df = pd.read_csv(raw_csv, dtype=str).fillna("")
    logger.info("[fire] Raw CSV: %d rows, columns: %s", len(df), list(df.columns))

    mask = df["Category"].str.lower().str.contains("fire", na=False)
    df_fire = df[mask].copy()
    logger.info("[fire] Fire rows: %d / %d total", len(df_fire), len(df))

    filtered_path = raw_csv.parent / f"{raw_csv.stem}_fire_filtered.csv"
    df_fire.to_csv(filtered_path, index=False)

    if raw_csv != filtered_path:
        try:
            raw_csv.unlink()
        except Exception:
            pass

    return filtered_path


async def _run_download_pipeline(
    start_date: date,
    end_date: date,
    county_id: str,
) -> Optional[Path]:
    """Download → filter → deduplicate. Returns deduplicated CSV path or None."""
    config = get_county(county_id)
    portal_url = config.get("portals", {}).get("fire_incidents_url", _FIRE_PORTAL_URL)

    raw_csv = await _download_calls_csv(portal_url)
    if not raw_csv:
        logger.error("[fire] Download failed — aborting")
        return None

    fire_csv = _filter_fire_rows(raw_csv)

    today = date.today().strftime("%Y%m%d")
    return deduplicate_csv(
        new_csv_path=fire_csv,
        destination_dir=RAW_FIRE_DIR,
        unique_key_columns=_FIRE_DEDUP_KEY,
        output_filename=f"fire_calls_{today}.csv",
        keep_original=False,
    )


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _parse_date(date_str: str) -> date:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y %H:%M:%S"):
        try:
            from datetime import datetime
            return datetime.strptime(date_str.split()[0], fmt.split()[0]).date()
        except ValueError:
            continue
    return date.today()


def _match_property(db, address: str, county_id: str) -> Optional[int]:
    from sqlalchemy import func
    return db.execute(
        select(Property.id).where(
            and_(
                Property.county_id == county_id,
                func.lower(Property.address).contains(address.lower()[:30]),
            )
        ).limit(1)
    ).scalar_one_or_none()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def scrape_fire_incidents(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,
) -> int:
    """
    Scrape fire incidents from the HCSO Calls-for-Service portal and create
    Incident records. Returns the number of new Incident records created.
    """
    if date_range is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=7)
    else:
        start_date, end_date = date_range

    deduped_csv = asyncio.run(_run_download_pipeline(start_date, end_date, county_id))

    if not deduped_csv or not deduped_csv.exists():
        logger.warning("[fire] No deduplicated CSV produced — nothing to load")
        return 0

    import pandas as pd
    df = pd.read_csv(deduped_csv, dtype=str).fillna("")
    logger.info("[fire] Loading %d new fire records to DB", len(df))

    created = 0
    skipped_no_match = 0

    with get_db_context() as db:
        for _, row in df.iterrows():
            address = row.get("Address", "").strip()
            date_str = row.get("Incident Start Date", "").strip()
            incident_date = _parse_date(date_str) if date_str else date.today()

            property_id = _match_property(db, address, county_id)
            if not property_id:
                skipped_no_match += 1
                continue

            incident = Incident(
                property_id=property_id,
                incident_type="Fire",
                incident_date=incident_date,
                county_id=county_id,
            )
            db.add(incident)
            created += 1

        db.commit()

    logger.info("[fire] %s: created=%d no_match=%d", county_id, created, skipped_no_match)

    rotate_csv_archives(RAW_FIRE_DIR)

    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(
            source_type="fire_incidents",
            total_scraped=created + skipped_no_match,
            matched=created,
            unmatched=skipped_no_match,
            skipped=0,
        )
    except Exception as stats_err:
        logger.warning("[fire] Could not record scraper stats (non-critical): %s", stats_err)

    return created


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Scrape fire incidents")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    args = parser.parse_args()
    n = scrape_fire_incidents(county_id=args.county_id)
    print(f"Done — {n} fire incidents created")
