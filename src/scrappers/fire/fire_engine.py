"""
Fire Incident Reports — M1-F Scraper #3

Scrapes fire incident data from the Hillsborough County Fire Rescue
public portal using Playwright. Creates Incident records (type='Fire')
on matched properties.

Portal: https://www.hcflgov.net/fire/incidents  (public, no login)
Fallback: NFIRS public data download if portal is unavailable.

Entry point:
    scrape_fire_incidents(county_id, date_range)
"""

import logging
import re
from datetime import date, timedelta
from typing import Optional, Tuple, List, Dict

from src.core.database import get_db_context
from src.core.models import Property, Incident
from src.utils.county_config import get_county
from sqlalchemy import select, and_

logger = logging.getLogger(__name__)

# Hillsborough Fire Rescue public incident search portal
_FIRE_PORTAL_URL = "https://www.hcflgov.net/fire/incidents"

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


async def _scrape_fire_portal_playwright(
    start_date: date,
    end_date: date,
    county_id: str,
) -> List[Dict]:
    """
    Playwright scraper for Hillsborough Fire Rescue incident portal.
    Returns list of dicts with keys: address, incident_type, incident_date.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("[fire] playwright not installed — run: pip install playwright")
        return []

    records = []
    config = get_county(county_id)
    portal_url = config.get("portals", {}).get("fire_incidents_url", _FIRE_PORTAL_URL)

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            resp = await page.goto(portal_url, timeout=30000)
            if resp and resp.status >= 400:
                logger.warning("[fire] Portal returned HTTP %d — %s", resp.status, portal_url)
                await browser.close()
                return []

            try:
                await page.fill('input[name*="start"], input[id*="start"]',
                                start_date.strftime("%m/%d/%Y"))
                await page.fill('input[name*="end"], input[id*="end"]',
                                end_date.strftime("%m/%d/%Y"))
                await page.click('button[type="submit"], input[type="submit"]')
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass  # portal may not have date filters; scrape current page

            rows = await page.query_selector_all("table tr")
            for row in rows[1:]:  # skip header
                cells = await row.query_selector_all("td")
                if len(cells) < 3:
                    continue
                texts = [await c.inner_text() for c in cells]
                incident_type = texts[1].strip().lower() if len(texts) > 1 else ""
                if not any(kw in incident_type for kw in FIRE_INCIDENT_TYPES):
                    continue
                records.append({
                    "address": texts[0].strip(),
                    "incident_type": texts[1].strip(),
                    "incident_date": _parse_date(texts[2].strip()),
                })

            await browser.close()
    except Exception as e:
        logger.warning("[fire] Playwright scrape failed: %s", e, exc_info=True)

    return records


def _parse_date(date_str: str) -> Optional[date]:
    """Parse common date formats from portal."""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            from datetime import datetime
            return datetime.strptime(date_str, fmt).date()
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
    Scrape fire incidents from the county portal and create Incident records.

    Args:
        county_id:  County to process.
        date_range: (start_date, end_date). Defaults to last 7 days.

    Returns:
        Number of new Incident records created.
    """
    import asyncio

    if date_range is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=7)
    else:
        start_date, end_date = date_range

    raw_records = asyncio.run(
        _scrape_fire_portal_playwright(start_date, end_date, county_id)
    )

    created = 0
    skipped_no_match = 0
    skipped_duplicate = 0

    with get_db_context() as db:
        for rec in raw_records:
            property_id = _match_property(db, rec["address"], county_id)
            if not property_id:
                skipped_no_match += 1
                continue

            incident_date = rec.get("incident_date") or date.today()

            existing = db.execute(
                select(Incident).where(
                    and_(
                        Incident.property_id == property_id,
                        Incident.incident_type == "Fire",
                        Incident.incident_date == incident_date,
                    )
                )
            ).scalar_one_or_none()

            if existing:
                skipped_duplicate += 1
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
        "[fire] %s %s→%s: created=%d no_match=%d duplicate=%d",
        county_id, start_date, end_date, created, skipped_no_match, skipped_duplicate,
    )
    return created


if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Scrape fire incidents")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough", help="County identifier (default: hillsborough)")
    args = parser.parse_args()
    n = scrape_fire_incidents(county_id=args.county_id)
    print(f"Done — {n} fire incidents created")
