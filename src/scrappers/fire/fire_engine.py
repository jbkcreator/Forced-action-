"""
Fire Weather Alerts — NWS API (replaces HCSO browser-use scraper)

Fetches active NWS fire weather alerts for the county's configured NWS forecast
zones and creates Incident records (incident_type='Fire') on matched properties.

When a fire weather alert is active, it covers the entire forecast zone — so
incidents are created for ALL properties in the county (county-wide strategy),
matching the flood engine's FEMA-declaration path. If the alert description
mentions specific ZIP codes, incidents are scoped to those ZIPs instead.

NWS fire alert event types monitored:
    Red Flag Warning, Fire Weather Watch, Fire Warning, Extreme Fire Danger,
    Red Flag Warning, Special Weather Statement (fire context)

NWS zones used (read from counties DB → county_config):
    Hillsborough: FLZ151 (Coastal) + FLZ251 (Inland)
    Pinellas:     FLZ050

Entry point:
    scrape_fire_incidents(county_id, date_range)
"""

import logging
import re
from datetime import date
from typing import Optional, Tuple, List, Dict

import requests

from src.core.database import get_db_context
from src.core.models import Property, Incident
from src.utils.county_config import get_county
from sqlalchemy import select, and_

logger = logging.getLogger(__name__)

_NWS_ZONE_URL = "https://api.weather.gov/alerts/active/zone/{zone_id}"
_NWS_HEADERS = {
    "User-Agent": "ForcedAction/1.0 (distressed-property-intelligence)",
    "Accept": "application/geo+json",
}

FIRE_NWS_EVENTS = [
    "Red Flag Warning",
    "Fire Weather Watch",
    "Fire Warning",
    "Extreme Fire Danger",
]

# FL ZIP code pattern (33xxx–34xxx)
_FL_ZIP_RE = re.compile(r"\b(3[3-4]\d{3})\b")


def _fetch_nws_fire_alerts(zone_ids: List[str]) -> List[Dict]:
    """Fetch active NWS fire weather alerts for the given zone IDs."""
    alerts = []
    for zone_id in zone_ids:
        try:
            resp = requests.get(
                _NWS_ZONE_URL.format(zone_id=zone_id),
                headers=_NWS_HEADERS,
                timeout=15,
            )
            resp.raise_for_status()
            for feature in resp.json().get("features", []):
                props = feature.get("properties", {})
                event = props.get("event", "")
                if any(e in event for e in FIRE_NWS_EVENTS):
                    alerts.append(props)
        except Exception as e:
            logger.warning("[fire] NWS zone fetch failed for %s: %s", zone_id, e)
    return alerts


def _extract_zips(alert: Dict) -> List[str]:
    """Extract FL ZIP codes mentioned in the alert description."""
    description = (alert.get("description") or "") + " " + (alert.get("areaDesc") or "")
    return _FL_ZIP_RE.findall(description)


def scrape_fire_incidents(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,  # noqa: ARG001 — interface consistency
    headfull: bool = False,  # noqa: ARG001 — kept for backwards-compat with cron args
) -> int:
    """
    Fetch active NWS fire weather alerts and create Incident records for
    all matched properties. Returns the number of new Incident records created.

    Strategy:
      - Alerts covering the full zone → county-wide (all properties in county)
      - Alerts with extractable ZIPs  → ZIP-scoped subset only
    """
    try:
        config = get_county(county_id)
    except KeyError:
        logger.error("[fire] Unknown county_id: %s", county_id)
        return 0

    nws_zones = config.get("nws_zones", [])
    if not nws_zones:
        logger.warning("[fire] %s: no nws_zones configured — skipping", county_id)
        return 0

    logger.info("[fire] %s: fetching alerts via zones %s", county_id, nws_zones)
    alerts = _fetch_nws_fire_alerts(nws_zones)

    if not alerts:
        logger.info("[fire] %s: no active fire weather alerts — 0 incidents", county_id)
        try:
            from src.utils.scraper_db_helper import record_scraper_stats
            record_scraper_stats(
                source_type="fire_incidents",
                total_scraped=0, matched=0, unmatched=0, skipped=0,
                county_id=county_id,
            )
        except Exception:
            pass
        return 0

    # Collect ZIPs from all alerts; empty set → county-wide strategy
    all_zips: set = set()
    for alert in alerts:
        all_zips.update(_extract_zips(alert))
        logger.info(
            "[fire] %s: alert active — event=%r area=%r severity=%r",
            county_id,
            alert.get("event"),
            alert.get("areaDesc"),
            alert.get("severity"),
        )

    county_wide = len(all_zips) == 0

    created = 0
    skipped_duplicate = 0
    fire_date = date.today()

    with get_db_context() as db:
        if county_wide:
            logger.info("[fire] %s: no ZIPs in alert — using county-wide strategy", county_id)
            properties = db.execute(
                select(Property).where(Property.county_id == county_id)
            ).scalars().all()
        else:
            logger.info("[fire] %s: ZIP-scoped strategy — %d ZIPs", county_id, len(all_zips))
            properties = db.execute(
                select(Property).where(
                    and_(
                        Property.county_id == county_id,
                        Property.zip.in_(all_zips),
                    )
                )
            ).scalars().all()

        for prop in properties:
            existing = db.execute(
                select(Incident).where(
                    and_(
                        Incident.property_id == prop.id,
                        Incident.incident_type == "Fire",
                        Incident.incident_date == fire_date,
                    )
                )
            ).scalars().first()

            if existing:
                skipped_duplicate += 1
                continue

            db.add(Incident(
                property_id=prop.id,
                incident_type="Fire",
                incident_date=fire_date,
                county_id=county_id,
            ))
            created += 1

        db.commit()

    logger.info(
        "[fire] %s: created=%d duplicate=%d strategy=%s",
        county_id, created, skipped_duplicate,
        "county_wide" if county_wide else f"zip_scoped({len(all_zips)})",
    )
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(
            source_type="fire_incidents",
            total_scraped=created + skipped_duplicate,
            matched=created,
            unmatched=0,
            skipped=skipped_duplicate,
            county_id=county_id,
        )
    except Exception as stats_err:
        logger.warning("[fire] Could not record scraper stats (non-critical): %s", stats_err)

    return created


if __name__ == "__main__":
    import argparse
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Scrape fire weather incidents via NWS")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    args = parser.parse_args()

    n = scrape_fire_incidents(county_id=args.county_id)
    print(f"Done — {n} fire incidents created")
