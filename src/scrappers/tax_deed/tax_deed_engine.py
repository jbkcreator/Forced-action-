"""
Tax Deed Sale Auction Scraper — multi-county, architecture-compliant.

Both Hillsborough and Pinellas use the same realtaxdeed.com ColdFusion/jQuery
platform. The county URL comes from county_sources; scraping logic is identical
for both.

Usage:
    python -m src.scrappers.tax_deed.tax_deed_engine --county-id hillsborough --date 06/25/2026
    python -m src.scrappers.tax_deed.tax_deed_engine --county-id pinellas --date 04/15/2026 --walk-dates
    python -m src.scrappers.tax_deed.tax_deed_engine --county-id hillsborough --walk-dates --load-to-db
"""

import argparse
import json
import re
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import setup_logging, get_logger
from src.utils.county_config import get_county_config
from src.utils.scraper_db_helper import record_scraper_stats
from src.utils.http_helpers import get_playwright_proxy
from config.constants import RAW_TAX_DEED_DIR

setup_logging()
logger = get_logger(__name__)

PAGE_LOAD_TIMEOUT = 45_000
ITEM_WAIT_TIMEOUT = 10_000
BETWEEN_PAGE_DELAY = 1.2


# ---------------------------------------------------------------------------
# Page extraction helpers (sync Playwright)
# ---------------------------------------------------------------------------

def _parse_money(text: str) -> Optional[float]:
    cleaned = re.sub(r"[^0-9.]", "", text)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _parse_certificate(text: str) -> tuple[str, str]:
    parts = [p.strip() for p in text.split("/")]
    if len(parts) == 2:
        return parts[0], parts[1]
    return "", text.strip()


def _extract_items_from_page(page, county_id: str, auction_date: str) -> list[dict]:
    items = []
    containers = page.query_selector_all("div.AUCTION_ITEM.PREVIEW")
    for container in containers:
        aid = container.get_attribute("aid") or ""

        status_el = container.query_selector(".ASTAT_MSGB")
        status = status_el.inner_text().strip() if status_el else ""

        sold_to_el = container.query_selector(".ASTAT_MSG_SOLDTO_MSG")
        sold_to = sold_to_el.inner_text().strip() if sold_to_el else ""

        sold_amount_el = container.query_selector(".ASTAT_MSGD")
        sold_amount = _parse_money(sold_amount_el.inner_text()) if sold_amount_el else None

        raw: dict[str, str] = {}
        for row in container.query_selector_all("table.ad_tab .AD_LBL"):
            label = row.inner_text().strip().rstrip(":")
            val_el = row.evaluate_handle("el => el.nextElementSibling")
            val = val_el.as_element().inner_text().strip() if val_el else ""
            raw[label] = val

        cert_year, cert_num = _parse_certificate(raw.get("Certificate #", ""))

        items.append({
            "county_id":          county_id,
            "auction_date":       auction_date,
            "aid":                aid,
            "status":             status,
            "auction_type":       raw.get("Auction Type", "").strip(),
            "case_number":        raw.get("Case #", "").strip(),
            "certificate_number": cert_num or None,
            "certificate_year":   int(cert_year) if cert_year.isdigit() else None,
            "opening_bid":        _parse_money(raw.get("Opening Bid", "")),
            "parcel_id":          raw.get("Parcel ID", "").strip() or None,
            "sold_to":            sold_to or None,
            "sold_amount":        sold_amount,
            "raw_fields":         json.dumps({k: v for k, v in raw.items() if k}),
        })
    return items


def _get_total_pages(page) -> int:
    for selector in ("#maxCA", "#maxCB"):
        el = page.query_selector(selector)
        if el:
            try:
                return int(el.inner_text().strip())
            except ValueError:
                pass
    return 1


def _click_next_page(page) -> bool:
    frame = page.query_selector("div.PageFrame[area='C']")
    if not frame:
        return False
    btn = frame.query_selector("span.PageRight")
    if not btn:
        return False
    btn.click()
    time.sleep(BETWEEN_PAGE_DELAY)
    return True


def _next_auction_date(page) -> Optional[str]:
    el = page.query_selector(".BLHeaderNext a")
    if not el:
        return None
    href = el.get_attribute("href") or ""
    m = re.search(r"AuctionDate=(\d{2}/\d{2}/\d{4})", href, re.IGNORECASE)
    return m.group(1) if m else None


def _scrape_auction_date(
    url_base: str, county_id: str, auction_date: str, browser
) -> tuple[list[dict], Optional[str]]:
    url = f"{url_base}{auction_date}"
    logger.info("[%s] Fetching %s", county_id, auction_date)

    ctx = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        )
    )
    page = ctx.new_page()
    try:
        page.goto(url, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT)
        try:
            page.wait_for_selector(".AUCTION_ITEM", timeout=ITEM_WAIT_TIMEOUT)
        except Exception:
            logger.info("[%s] No auction items for %s", county_id, auction_date)
            next_date = _next_auction_date(page)
            return [], next_date

        total_pages = _get_total_pages(page)
        logger.info("[%s] Found %d page(s)", county_id, total_pages)

        all_items = list(_extract_items_from_page(page, county_id, auction_date))
        for pg in range(2, total_pages + 1):
            logger.info("[%s] Fetching page %d/%d", county_id, pg, total_pages)
            if not _click_next_page(page):
                logger.warning("[%s] Could not click next-page at page %d", county_id, pg)
                break
            try:
                page.wait_for_function(
                    "() => document.querySelectorAll('.AUCTION_ITEM').length > 0",
                    timeout=ITEM_WAIT_TIMEOUT,
                )
            except Exception:
                logger.warning("[%s] Timed out waiting for items on page %d", county_id, pg)
                break
            all_items.extend(_extract_items_from_page(page, county_id, auction_date))

        next_date = _next_auction_date(page)
        logger.info("[%s] %d items extracted (next: %s)", county_id, len(all_items), next_date or "—")
        return all_items, next_date
    finally:
        ctx.close()


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_tax_deed_pipeline(
    county_id: str = "hillsborough",
    start_date: Optional[str] = None,
    walk_dates: bool = False,
    max_dates: int = 12,
    load_to_db: bool = False,
) -> Optional[Path]:
    from playwright.sync_api import sync_playwright

    cfg = get_county_config(county_id)
    source = cfg.get("sources", {}).get("tax_deed_auction")
    if not source:
        logger.error("[%s] No tax_deed_auction source in county_sources — add a DB row first", county_id)
        return None
    if not source.get("is_active", True) is not False and not source.get("url"):
        logger.error("[%s] tax_deed_auction source has no URL configured", county_id)
        return None

    url_base = source["url"]
    if not start_date:
        # Default: next Thursday (auction day)
        d = date.today()
        days_ahead = (3 - d.weekday()) % 7 or 7
        start_date = (d + timedelta(days=days_ahead)).strftime("%m/%d/%Y")

    t0 = time.monotonic()
    all_rows: list[dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, proxy=get_playwright_proxy())
        try:
            current_date = start_date
            dates_visited = 0
            while current_date and dates_visited < max_dates:
                rows, next_date = _scrape_auction_date(url_base, county_id, current_date, browser)
                all_rows.extend(rows)
                dates_visited += 1
                if not walk_dates or not next_date:
                    break
                current_date = next_date
                time.sleep(BETWEEN_PAGE_DELAY)
        except Exception as exc:
            logger.error("[%s] Scrape failed: %s", county_id, exc)
            logger.debug(traceback.format_exc())
        finally:
            browser.close()

    duration_s = time.monotonic() - t0

    if not all_rows:
        logger.info("[%s] No auction items found", county_id)
        record_scraper_stats(
            source_type="tax_deed_auction",
            total_scraped=0, matched=0, unmatched=0, skipped=0,
            run_success=True, error_type="no_data",
            duration_seconds=round(duration_s, 2),
            county_id=county_id,
        )
        return None

    df = pd.DataFrame(all_rows)
    today_str = datetime.now().strftime("%Y%m%d")
    out_dir = RAW_TAX_DEED_DIR / county_id / "new"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"tax_deed_auctions_{county_id}_{today_str}.csv"
    df.to_csv(csv_path, index=False)
    logger.info("[%s] Saved %d rows → %s", county_id, len(df), csv_path)

    if load_to_db:
        from src.utils.scraper_db_helper import load_scraped_data_to_db
        load_scraped_data_to_db(
            "tax_deed_auction",
            csv_path,
            destination_dir=out_dir.parent,
            county_id=county_id,
        )
    else:
        record_scraper_stats(
            source_type="tax_deed_auction",
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
    parser = argparse.ArgumentParser(description="Tax Deed Auction Scraper")
    parser.add_argument("--county-id", default="hillsborough", help="County slug")
    parser.add_argument("--date", default=None, help="Auction date MM/DD/YYYY (default: next Thursday)")
    parser.add_argument("--walk-dates", action="store_true", help="Follow next-auction links")
    parser.add_argument("--max-dates", type=int, default=12, help="Max dates to walk (default: 12)")
    parser.add_argument("--load-to-db", action="store_true", help="Load scraped data into DB")
    args = parser.parse_args()

    result = run_tax_deed_pipeline(
        county_id=args.county_id,
        start_date=args.date,
        walk_dates=args.walk_dates,
        max_dates=args.max_dates,
        load_to_db=args.load_to_db,
    )
    if result:
        logger.info("Done — output: %s", result)
    else:
        logger.info("Done — no output file (no items or error)")


if __name__ == "__main__":
    main()
