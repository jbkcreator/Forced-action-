"""
Vacant Land Scraper — multi-county, architecture-compliant.

Dispatches on county_sources.scrape_mode (no county-specific branches):
  static_download  → PCPAO JSON API  (Pinellas)
  playwright_only  → HCPA Playwright intercept (Hillsborough — activate when site stable)

Usage:
    python -m src.scrappers.vacant_land.vacant_land_engine --county-id pinellas
    python -m src.scrappers.vacant_land.vacant_land_engine --county-id pinellas --load-to-db
    python -m src.scrappers.vacant_land.vacant_land_engine --county-id hillsborough --load-to-db
"""

import argparse
import sys
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import setup_logging, get_logger
from src.utils.county_config import get_county_config
from src.utils.scraper_db_helper import record_scraper_stats
from src.utils.scraper_outcome_classifier import classify_exception
from config.scraper_outcomes import ScraperOutcome
from config.constants import RAW_VACANT_LAND_DIR

setup_logging()
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# PCPAO API (Pinellas) — static_download mode
# ---------------------------------------------------------------------------

_PCPAO_BASE = "https://www.pcpao.gov"
_PCPAO_VACANT_CODES = (
    "0000,0030,0033,0040,0060,0061,0062,0090,"
    "1000,1035,1090,4000,4090,7000,8012,8013,8014,8052"
)
_PCPAO_COLS = "1,2,36,37,38,6,8,28,31,41,54,60,61,75,136"


def _scrape_pcpao(source: dict) -> pd.DataFrame:
    logger.info("[pcpao] Starting PCPAO vacant land export")
    referer = source.get("special_flags", {}).get("referer", _PCPAO_BASE + "/content/advanced-search")
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": referer,
        "Origin": _PCPAO_BASE,
    })

    logger.info("[pcpao] Warming session")
    session.get(referer, timeout=30)
    session.post(_PCPAO_BASE + "/dal/searchapi/create", data={}, timeout=60)

    form = {
        "rdoProductType": "include_use_code",
        "txtPropertyUseCodes": _PCPAO_VACANT_CODES,
        "result_columns": _PCPAO_COLS,
        "hdnOutputTableCount": "8",
        "is_property_info_fields": "1",
        "is_site_address_fields": "1",
        "is_land_fields": "1",
        "is_building_fields": "0",
        "is_structural_elements_field": "0",
        "is_extra_features_fields": "0",
        "is_exemptions_fields": "0",
        "is_sale_fields": "0",
        "is_sale_history_fields": "0",
    }

    logger.info("[pcpao] Getting record count")
    cnt_resp = session.post(
        _PCPAO_BASE + "/dal/searchapi/advancedSearch?is_count=1",
        data=form,
        timeout=120,
    )
    cnt_resp.raise_for_status()
    cnt_data = cnt_resp.json()
    total_rows = cnt_data["result"][0]["Total Records"]
    fetch_size = cnt_data["result"][0]["Fetch Size"]
    logger.info("[pcpao] Total land-lines: %d  fetch_size: %d", total_rows, fetch_size)

    exp_form = {
        **form,
        "ddlExportType": "json",
        "hdnOrderByColumn": "PARCEL_NUMBER",
        "hdnSortBy": "ASC",
        "fetch_size": str(fetch_size),
    }
    logger.info("[pcpao] Exporting JSON (this may take 60-120s)")
    resp = session.post(
        _PCPAO_BASE + "/dal/searchapi/advancedSearch",
        data=exp_form,
        timeout=300,
    )
    resp.raise_for_status()
    rows = resp.json()
    logger.info("[pcpao] Received %d land-line rows", len(rows))

    by_parcel: dict[str, dict] = {}
    for r in rows:
        pid = r.get("PARCEL_NUMBER", "")
        if pid and pid not in by_parcel:
            by_parcel[pid] = r

    logger.info("[pcpao] Unique parcels after dedup: %d", len(by_parcel))

    today = date.today()
    records = []
    for pid, r in by_parcel.items():
        records.append({
            "county_id":     "pinellas",
            "parcel_id":     pid,
            "use_code":      str(r.get("LAND_USE_CD", "") or "").strip() or None,
            "property_use":  str(r.get("PROPERTY_USE", "") or "").strip() or None,
            "dor_code":      str(r.get("LAND_USE_CD", "") or "").strip() or None,
            "source_name":   "pcpao",
            "last_verified": today.isoformat(),
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# HCPA API (Hillsborough) — playwright_only mode
# ---------------------------------------------------------------------------

_HCPA_SEARCH_BASE = "https://gis.hcpafl.org/CommonServices/property/search/"
_HCPA_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
_HCPA_PAGE_SIZE = 100


def _hcpa_api_get(page, path: str, timeout_ms: int = 90_000) -> dict:
    return page.evaluate(
        f"""async () => {{
            try {{
                const ctrl = new AbortController();
                const tid = setTimeout(() => ctrl.abort(), {timeout_ms});
                const r = await fetch("{path}", {{
                    signal: ctrl.signal,
                    headers: {{'Accept': 'application/json',
                               'Referer': 'https://gis.hcpafl.org/PropertySearch/'}}
                }});
                clearTimeout(tid);
                return {{status: r.status, body: await r.text()}};
            }} catch(e) {{
                return {{status: 0, body: e.toString()}};
            }}
        }}"""
    )


def _scrape_hcpa(source: dict) -> pd.DataFrame:
    import json as _json
    from playwright.sync_api import sync_playwright

    logger.info("[hcpa] Starting HCPA vacant land scrape")
    today = date.today()
    records = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=_HCPA_UA)
        page = ctx.new_page()

        logger.info("[hcpa] Warming browser session")
        page.goto(
            "https://gis.hcpafl.org/PropertySearch/",
            wait_until="domcontentloaded",
            timeout=60_000,
        )
        page.wait_for_timeout(5_000)

        logger.info("[hcpa] Fetching DropDowns to discover vacant codes")
        dd_resp = _hcpa_api_get(page, _HCPA_SEARCH_BASE + "DropDowns")
        if dd_resp["status"] != 200:
            browser.close()
            raise RuntimeError(f"HCPA DropDowns returned HTTP {dd_resp['status']}: {dd_resp['body'][:300]}")

        dd = _json.loads(dd_resp["body"])
        vacant_codes = []
        for grp in dd.get("propertyType", []):
            for sub in grp.get("subtypes", []):
                if "vacant" in sub.get("label", "").lower():
                    vacant_codes.append(sub["code"])

        logger.info("[hcpa] Vacant codes: %s", vacant_codes)
        if not vacant_codes:
            browser.close()
            raise RuntimeError("No vacant property codes found in HCPA DropDowns")

        prop_param = ",".join(vacant_codes)
        page_num = 1
        total_count: Optional[int] = None

        while True:
            search_url = (
                f"{_HCPA_SEARCH_BASE}AdvancedSearch"
                f"?prop={prop_param}&page={page_num}&pagesize={_HCPA_PAGE_SIZE}"
            )
            resp = _hcpa_api_get(page, search_url)
            if resp["status"] != 200:
                logger.error("[hcpa] Page %d returned HTTP %d — stopping", page_num, resp["status"])
                break

            data = _json.loads(resp["body"])
            if not data:
                break

            if page_num == 1:
                total_count = data[0].get("totalCount", 0)
                logger.info("[hcpa] Total vacant properties: %d", total_count)

            for row in data:
                land_use = row.get("landUse") or {}
                use_code = land_use.get("code", "") if isinstance(land_use, dict) else str(land_use)
                property_use = land_use.get("description", "") if isinstance(land_use, dict) else ""
                records.append({
                    "county_id":     "hillsborough",
                    "parcel_id":     str(row.get("displayFolio", "") or "").strip(),
                    "use_code":      str(use_code or "").strip() or None,
                    "property_use":  str(property_use or "").strip() or None,
                    "dor_code":      str(use_code or "").strip() or None,
                    "source_name":   "hcpa",
                    "last_verified": today.isoformat(),
                })

            if total_count is not None and len(records) >= total_count:
                break
            page_num += 1
            page.wait_for_timeout(500)

        browser.close()

    logger.info("[hcpa] Scraped %d vacant parcels", len(records))
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_vacant_land_pipeline(
    county_id: str = "pinellas",
    load_to_db: bool = False,
) -> Optional[Path]:
    cfg = get_county_config(county_id)
    source = cfg.get("sources", {}).get("vacant_land")
    if not source:
        logger.error("[%s] No vacant_land source in county_sources — add a DB row first", county_id)
        return None

    scrape_mode = source.get("scrape_mode", "")
    t0 = time.monotonic()

    try:
        if scrape_mode == "static_download":
            df = _scrape_pcpao(source)
        elif scrape_mode == "playwright_only":
            df = _scrape_hcpa(source)
        else:
            logger.error("[%s] Unsupported scrape_mode: %r", county_id, scrape_mode)
            return None
    except Exception as exc:
        logger.error("[%s] Vacant land scrape failed: %s", county_id, exc)
        logger.debug(traceback.format_exc())
        record_scraper_stats(
            source_type="vacant_land",
            total_scraped=0, matched=0, unmatched=0, skipped=0,
            error_type="scraper_error", outcome=classify_exception(exc),
            error_message=str(exc)[:500],
            duration_seconds=round(time.monotonic() - t0, 2),
            county_id=county_id,
        )
        return None

    duration_s = time.monotonic() - t0

    if df.empty:
        logger.info("[%s] No vacant parcels found", county_id)
        record_scraper_stats(
            source_type="vacant_land",
            total_scraped=0, matched=0, unmatched=0, skipped=0,
            error_type="no_data", outcome=ScraperOutcome.NO_DATA.value,
            duration_seconds=round(duration_s, 2),
            county_id=county_id,
        )
        return None

    today_str = datetime.now().strftime("%Y%m%d")
    out_dir = RAW_VACANT_LAND_DIR / county_id / "new"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"vacant_land_{county_id}_{today_str}.csv"
    df.to_csv(csv_path, index=False)
    logger.info("[%s] Saved %d rows → %s", county_id, len(df), csv_path)

    if load_to_db:
        from src.utils.scraper_db_helper import load_scraped_data_to_db
        load_scraped_data_to_db(
            "vacant_land",
            csv_path,
            destination_dir=out_dir.parent,
            county_id=county_id,
        )
    else:
        record_scraper_stats(
            source_type="vacant_land",
            total_scraped=len(df), matched=0, unmatched=0, skipped=0,
            run_success=True,
            duration_seconds=round(duration_s, 2),
            county_id=county_id,
        )

    return csv_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Vacant Land Scraper")
    parser.add_argument("--county-id", default="pinellas", help="County slug")
    parser.add_argument("--load-to-db", action="store_true", help="Load scraped data into DB")
    args = parser.parse_args()

    result = run_vacant_land_pipeline(county_id=args.county_id, load_to_db=args.load_to_db)
    if result:
        logger.info("Done — output: %s", result)
    else:
        logger.info("Done — no output file (no items or error)")


if __name__ == "__main__":
    main()
