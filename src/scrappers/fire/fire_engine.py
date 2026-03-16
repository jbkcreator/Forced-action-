"""
Fire / Calls-for-Service Scraper — HCSO GIS Portal

Downloads the current "Calls for Service" CSV from the Hillsborough County
Sheriff's Office public GIS portal.  Fire-related rows are filtered after
download using FIRE_INCIDENT_TYPES keywords.

Portal: https://gis.hcso.tampa.fl.us/publicgis/callsforservice/
Export: Dojo Select → CSV  →  "Export Mapped Calls Only" button

Entry point:
    scrape_fire_incidents(county_id, date_range)
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple, List, Dict

from config.constants import RAW_FIRE_DIR
from src.core.database import get_db_context
from src.core.models import Property, Incident
from src.utils.county_config import get_county
from src.utils.csv_deduplicator import deduplicate_csv, rotate_csv_archives
from sqlalchemy import select, and_

logger = logging.getLogger(__name__)

# HCSO Calls-for-Service public GIS portal
_FIRE_PORTAL_URL = "https://gis.hcso.tampa.fl.us/publicgis/callsforservice/"

# Dedup unique key — Report Number uniquely identifies each call
_FIRE_DEDUP_KEY = ["Report Number"]

# Fire incident type keywords to include (exclude medical, traffic)
FIRE_INCIDENT_TYPES = [
    "structure fire",
    "fire",
    "smoke",
    "explosion",
    "arson",
    "wildland fire",
    "vehicle fire",
]


async def _download_calls_csv(portal_url: str = _FIRE_PORTAL_URL) -> Optional[Path]:
    """
    Opens the HCSO Calls-for-Service portal, sets the export dropdown to CSV,
    clicks "Export Mapped Calls Only", and saves the raw file to a temp location
    inside RAW_FIRE_DIR before deduplication.

    Returns the Path to the raw downloaded CSV, or None on failure.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("[fire] playwright not installed — run: pip install playwright && playwright install chromium")
        return None

    # Save raw download to temp/ inside the fire data dir
    temp_dir = RAW_FIRE_DIR / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    from src.utils.http_helpers import get_playwright_proxy

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                proxy=get_playwright_proxy(),
            )
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()

            logger.info("[fire] Navigating to %s", portal_url)
            await page.goto(portal_url, timeout=60000, wait_until="networkidle")

            # ── Step 1: set the Dojo Select widget value to "csv" ──────────────
            logger.info("[fire] Setting export type to CSV via Dojo widget")
            await page.evaluate("""
                () => {
                    const widget = dijit.byId('dijit_form_Select_2');
                    if (widget) {
                        widget.set('value', 'csv');
                    } else {
                        const node = document.querySelector('#dijit_form_Select_2 input[type=hidden]');
                        if (node) node.value = 'csv';
                        const label = document.querySelector('#dijit_form_Select_2 .dijitSelectLabel');
                        if (label) label.textContent = 'CSV';
                    }
                }
            """)

            await page.wait_for_timeout(500)

            # ── Step 2: click "Export Mapped Calls Only" and capture download ──
            logger.info("[fire] Clicking 'Export Mapped Calls Only'")
            async with page.expect_download(timeout=120000) as dl_info:
                await page.click('#dijit_form_Button_3_label')

            download = await dl_info.value
            save_path = temp_dir / (download.suggested_filename or "calls_for_service_temp.csv")
            await download.save_as(str(save_path))
            logger.info("[fire] Downloaded CSV → %s (%d bytes)", save_path, save_path.stat().st_size)

            await browser.close()
            return save_path

    except Exception as e:
        logger.error("[fire] CSV download failed: %s", e, exc_info=True)
        return None


def _filter_fire_rows(raw_csv: Path) -> Path:
    """
    Read the raw downloaded CSV, keep only fire-related rows, and write a
    filtered CSV back to temp/ for dedup processing.

    Returns path to the filtered CSV.
    """
    import pandas as pd

    df = pd.read_csv(raw_csv, dtype=str).fillna("")
    logger.info("[fire] Raw CSV: %d rows, columns: %s", len(df), list(df.columns))

    mask = df["Category"].str.lower().str.contains("fire", na=False)
    df_fire = df[mask].copy()
    logger.info("[fire] Fire rows: %d / %d total", len(df_fire), len(df))

    filtered_path = raw_csv.parent / f"{raw_csv.stem}_fire_filtered.csv"
    df_fire.to_csv(filtered_path, index=False)

    # Remove the unfiltered raw file (only if it's a different path)
    if raw_csv != filtered_path:
        try:
            raw_csv.unlink()
        except Exception:
            pass

    return filtered_path


async def _scrape_fire_portal_playwright(
    start_date: date,
    end_date: date,
    county_id: str,
) -> Optional[Path]:
    """
    Download → filter → deduplicate the Calls-for-Service CSV.
    Returns the deduplicated CSV path in RAW_FIRE_DIR/new/, or None on failure.
    """
    config = get_county(county_id)
    portal_url = config.get("portals", {}).get("fire_incidents_url", _FIRE_PORTAL_URL)

    raw_csv = await _download_calls_csv(portal_url)
    if not raw_csv:
        return None

    fire_csv = _filter_fire_rows(raw_csv)

    today = date.today().strftime("%Y%m%d")
    deduped_path = deduplicate_csv(
        new_csv_path=fire_csv,
        destination_dir=RAW_FIRE_DIR,
        unique_key_columns=_FIRE_DEDUP_KEY,
        output_filename=f"fire_calls_{today}.csv",
        keep_original=False,
    )
    return deduped_path


def _parse_date(date_str: str) -> Optional[date]:
    """Parse common date formats from portal."""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y %H:%M:%S"):
        try:
            from datetime import datetime
            return datetime.strptime(date_str.split()[0], fmt.split()[0]).date()
        except ValueError:
            continue
    return date.today()


def _match_property(db, address: str, county_id: str) -> Optional[int]:
    """Find property_id by address string (case-insensitive partial match)."""
    from sqlalchemy import func
    result = db.execute(
        select(Property.id).where(
            and_(
                Property.county_id == county_id,
                func.lower(Property.address).contains(address.lower()[:30]),
            )
        ).limit(1)
    ).scalar_one_or_none()
    return result


def scrape_fire_incidents(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,
) -> int:
    """
    Scrape fire incidents from the HCSO Calls-for-Service portal and create
    Incident records.  Follows the standard CSV dedup/rotation pattern.

    Args:
        county_id:  County to process.
        date_range: Unused (portal exports current mapped calls only); kept for
                    interface compatibility.

    Returns:
        Number of new Incident records created.
    """
    import asyncio

    if date_range is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=7)
    else:
        start_date, end_date = date_range

    deduped_csv = asyncio.run(
        _scrape_fire_portal_playwright(start_date, end_date, county_id)
    )

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
            incident_type = row.get("Incident Type", "Fire").strip()
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

    logger.info(
        "[fire] %s: created=%d no_match=%d",
        county_id, created, skipped_no_match,
    )

    # Rotate CSV archives after successful DB load
    rotate_csv_archives(RAW_FIRE_DIR)

    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(
            source_type='fire_incidents',
            total_scraped=created + skipped_no_match,
            matched=created,
            unmatched=skipped_no_match,
            skipped=0,
        )
    except Exception as stats_err:
        logger.warning("⚠ Could not record scraper stats (non-critical): %s", stats_err)
    return created


if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Scrape fire incidents")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    args = parser.parse_args()
    n = scrape_fire_incidents(county_id=args.county_id)
    print(f"Done — {n} fire incidents created")
