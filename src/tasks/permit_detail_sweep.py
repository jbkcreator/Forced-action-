"""
Post-load permit detail enrichment sweep.

Queries building_permits WHERE contractor_name IS NULL (up to --limit rows),
fetches Accela CapDetail pages, parses contractor/applicant/owner fields,
and persists them back to the same row.

Resolution strategy by county:
  Pasco:            capIDs = permit_number.split('-')  — 3-segment format REC26-00000-01VUT
  Hillsborough /
  Pinellas:         single Playwright session; search by record number to get CapDetail href

Usage:
    PYTHONPATH=. python -m src.tasks.permit_detail_sweep
    PYTHONPATH=. python -m src.tasks.permit_detail_sweep --county pasco --limit 100 --delay 1.5
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import httpx
from sqlalchemy import create_engine, text

# Make sure project root is on path when invoked as __main__
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.settings import get_settings
from src.scrappers.permit.detail_parse import parse_permit_detail, PermitDetail
from src.scrappers.permit.detail_url import build_detail_url

logger = logging.getLogger(__name__)

# --- agency code lookup -------------------------------------------------------

_AGENCY_CODE: dict[str, str] = {
    "hillsborough": "HCFL",
    "pinellas": "PINELLAS",
    "pasco": "PASCO",
}

_SEARCH_URL: dict[str, str] = {
    "hillsborough": "https://aca-prod.accela.com/HCFL/Cap/CapHome.aspx?module=Building&TabName=Building",
    "pinellas": "https://aca-prod.accela.com/PINELLAS/Cap/CapHome.aspx?module=Building&TabName=Building",
}

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# --- URL resolution -----------------------------------------------------------

def _pasco_detail_url(permit_number: str) -> Optional[str]:
    """Pasco permit format REC26-00000-01VUT encodes capIDs directly."""
    parts = permit_number.split("-")
    if len(parts) != 3:
        return None
    return build_detail_url("PASCO", parts[0], parts[1], parts[2])


def _fetch_html_httpx(url: str, timeout: float = 30.0) -> Optional[str]:
    """Plain HTTP GET for counties where capIDs are known (Pasco)."""
    try:
        with httpx.Client(headers={"User-Agent": _UA}, follow_redirects=True, timeout=timeout) as client:
            r = client.get(url)
            r.raise_for_status()
            return r.text
    except Exception as exc:
        logger.warning("httpx GET failed for %s: %s", url, exc)
        return None


def _playwright_fetch_html(permit_number: str, county_id: str) -> Optional[str]:
    """
    Open one Playwright page, search by record number, navigate to CapDetail,
    return the HTML.  Called once per H/P permit.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    search_url = _SEARCH_URL.get(county_id)
    if not search_url:
        logger.error("No search URL for county %s", county_id)
        return None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=_UA, viewport={"width": 1400, "height": 900})
        page = ctx.new_page()
        try:
            page.goto(search_url, timeout=60_000, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=30_000)
            page.wait_for_timeout(2000)

            # Fill record number in "General Search" text field
            filled = False
            for sel in [
                "input[id*='txtSearchCondition']",
                "input[id*='txtGSPermit']",
                "input[id*='txtGSRecordNumber']",
                "input[id*='searchCondition']",
            ]:
                el = page.query_selector(sel)
                if el:
                    el.click(click_count=3)
                    page.keyboard.type(permit_number, delay=40)
                    filled = True
                    break

            if not filled:
                logger.warning("[%s] Could not find search input for %s", county_id, permit_number)
                browser.close()
                return None

            # Submit search
            for sel in [
                "#ctl00_PlaceHolderMain_btnNewSearch",
                "a[id*='btnNewSearch']",
                "input[id*='btnNewSearch']",
                "input[id*='btnSearch']",
            ]:
                el = page.query_selector(sel)
                if el:
                    el.click()
                    break

            page.wait_for_load_state("networkidle", timeout=40_000)
            page.wait_for_timeout(3000)

            # Find first CapDetail link in results
            href = None
            for a in page.query_selector_all("a[href*='CapDetail']"):
                h = a.get_attribute("href") or ""
                if "CapDetail" in h:
                    href = h
                    break

            if not href:
                logger.warning("[%s] No CapDetail link found for %s", county_id, permit_number)
                browser.close()
                return None

            if href.startswith("/"):
                href = "https://aca-prod.accela.com" + href

            page.goto(href, timeout=60_000, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=30_000)
            page.wait_for_timeout(2500)

            html = page.content()
            browser.close()
            return html

        except PWTimeout as exc:
            logger.warning("[%s] Playwright timeout for %s: %s", county_id, permit_number, exc)
            browser.close()
            return None
        except Exception as exc:
            logger.error("[%s] Playwright error for %s: %s", county_id, permit_number, exc)
            try:
                browser.close()
            except Exception:
                pass
            return None


# --- DB persistence -----------------------------------------------------------

def _update_permit(conn, permit_id: int, detail: PermitDetail) -> None:
    conn.execute(
        text("""
            UPDATE building_permits SET
                contractor_name         = COALESCE(contractor_name, :contractor_name),
                holder_name             = COALESCE(holder_name, :holder_name),
                completion_status       = COALESCE(completion_status, :completion_status),
                contractor_license      = COALESCE(contractor_license, :contractor_license),
                contractor_license_type = COALESCE(contractor_license_type, :contractor_license_type),
                contractor_phone        = COALESCE(contractor_phone, :contractor_phone),
                contractor_email        = COALESCE(contractor_email, :contractor_email),
                applicant_name          = COALESCE(applicant_name, :applicant_name),
                owner_name              = COALESCE(owner_name, :owner_name)
            WHERE id = :id
        """),
        {
            "contractor_name": detail.licensed_professional_name,
            "holder_name": detail.applicant_name,
            "completion_status": detail.completion_status,
            "contractor_license": detail.contractor_license,
            "contractor_license_type": detail.contractor_license_type,
            "contractor_phone": detail.contractor_phone,
            "contractor_email": detail.contractor_email,
            "applicant_name": detail.applicant_name,
            "owner_name": detail.owner_name,
            "id": permit_id,
        },
    )


# --- Main sweep ---------------------------------------------------------------

def run_sweep(
    county_filter: Optional[str] = None,
    limit: int = 50,
    delay: float = 2.0,
) -> dict[str, int]:
    """
    Fetch and persist detail data for up to `limit` permits missing contractor_name.

    Returns stats dict: enriched / skipped / errors.
    """
    engine = create_engine(get_settings().database_url)
    stats = {"enriched": 0, "skipped": 0, "errors": 0}

    county_clause = "AND c.county_id = :county" if county_filter else ""

    with engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT bp.id, bp.permit_number, c.county_id
                FROM building_permits bp
                JOIN counties c ON c.id = bp.county_id
                WHERE bp.contractor_name IS NULL
                  {county_clause}
                ORDER BY bp.id
                LIMIT :limit
            """),
            {"limit": limit, **({"county": county_filter} if county_filter else {})},
        ).fetchall()

    logger.info("Sweep: %d permits to enrich (county=%s, limit=%d)", len(rows), county_filter or "all", limit)

    for row in rows:
        permit_id, permit_number, county_id = row.id, row.permit_number, row.county_id

        html: Optional[str] = None
        if county_id == "pasco":
            url = _pasco_detail_url(permit_number)
            if url:
                html = _fetch_html_httpx(url)
            else:
                logger.warning("Pasco: cannot derive URL for permit %s", permit_number)
                stats["skipped"] += 1
                continue
        elif county_id in ("hillsborough", "pinellas"):
            html = _playwright_fetch_html(permit_number, county_id)
        else:
            logger.warning("Unknown county %s for permit %s — skipping", county_id, permit_number)
            stats["skipped"] += 1
            continue

        if not html:
            stats["errors"] += 1
            time.sleep(delay)
            continue

        try:
            detail = parse_permit_detail(html)
        except Exception as exc:
            logger.error("Parse failed for %s: %s", permit_number, exc)
            stats["errors"] += 1
            time.sleep(delay)
            continue

        try:
            with engine.begin() as conn:
                _update_permit(conn, permit_id, detail)
            stats["enriched"] += 1
            logger.info(
                "Enriched %s (%s): contractor=%r license=%r",
                permit_number, county_id,
                detail.licensed_professional_name, detail.contractor_license,
            )
        except Exception as exc:
            logger.error("DB update failed for %s: %s", permit_number, exc)
            stats["errors"] += 1

        time.sleep(delay)

    logger.info("Sweep done: %s", stats)
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Permit detail enrichment sweep")
    parser.add_argument("--county", choices=["hillsborough", "pinellas", "pasco"], default=None)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between requests")
    args = parser.parse_args()

    stats = run_sweep(county_filter=args.county, limit=args.limit, delay=args.delay)
    print(f"Done: {stats}")


if __name__ == "__main__":
    main()
