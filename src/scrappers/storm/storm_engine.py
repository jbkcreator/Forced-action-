"""
Storm Damage Zones — M1-F Scraper #2

Fetches active storm damage alerts from the NOAA/NWS API and matches
affected ZIP codes against properties in the DB. Creates Incident records
(incident_type='storm_damage') for all matched properties.

Data source: NWS CAP alerts API (public, no key required)
    https://api.weather.gov/alerts/active

Entry point:
    scrape_storm_damage(county_id, date_range)
"""

import logging
from datetime import date
from typing import Optional, Tuple, List, Dict

import requests

from src.core.database import get_db_context
from src.core.models import Property, Incident
from src.utils.county_config import get_county
from sqlalchemy import select, and_

logger = logging.getLogger(__name__)

_NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"

# NWS event types that indicate storm/wind/hail damage relevant to roofing
STORM_EVENT_TYPES = [
    "Tornado Warning",
    "Tornado Watch",
    "Severe Thunderstorm Warning",
    "Severe Thunderstorm Watch",
    "Hurricane Warning",
    "Hurricane Watch",
    "Tropical Storm Warning",
    "Tropical Storm Watch",
    "High Wind Warning",
    "Wind Advisory",
    "Special Weather Statement",
    "Flash Flood Warning",
    "Flood Warning",
]


def _fetch_nws_alerts(state: str = "FL") -> List[Dict]:
    """Fetch active NWS alerts for a state."""
    try:
        resp = requests.get(
            _NWS_ALERTS_URL,
            params={"area": state, "status": "actual", "message_type": "alert"},
            headers={"User-Agent": "ForcedAction/1.0 (distressed-property-intelligence)"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("features", [])
    except Exception as e:
        logger.warning("[storm] NWS API fetch failed: %s", e, exc_info=True)
        return []


def _extract_affected_zips(alert: Dict) -> List[str]:
    """Extract ZIP codes from NWS alert geometry or affected zones description."""
    zips = []
    props = alert.get("properties", {})

    # Try geocode/UGC zones — NWS provides FIPS-level county codes
    # Try to parse ZIPs from the description text
    description = props.get("description", "") or ""
    import re
    found_zips = re.findall(r"\b(3[3-4]\d{3})\b", description)  # FL ZIPs 33xxx-34xxx
    zips.extend(found_zips)

    return list(set(zips))


def scrape_storm_damage(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,  # noqa: ARG001 — interface consistency
) -> int:
    """
    Fetch active NWS storm alerts and create Incident records for all
    properties in affected ZIP codes.

    Args:
        county_id:   County to process.
        date_range:  Unused for live API (always fetches current active alerts).
                     Accepted for interface consistency.

    Returns:
        Number of new Incident records created.
    """
    try:
        config = get_county(county_id)
    except KeyError:
        logger.error("[storm] Unknown county_id: %s", county_id)
        return 0

    state = config.get("state", "FL")
    zip_prefixes = config.get("zip_prefixes", [])

    alerts = _fetch_nws_alerts(state)

    affected_zips: set = set()
    storm_date = date.today()

    for alert in alerts:
        props = alert.get("properties", {})
        event = props.get("event", "")
        if not any(event_type in event for event_type in STORM_EVENT_TYPES):
            continue

        alert_zips = _extract_affected_zips(alert)
        county_zips = [
            z for z in alert_zips
            if any(z.startswith(pfx) for pfx in zip_prefixes)
        ]
        affected_zips.update(county_zips)

    if not affected_zips:
        logger.info("[storm] %s: no active storm alerts — 0 incidents", county_id)
        return 0

    created = 0
    skipped_duplicate = 0
    with get_db_context() as db:
        properties = db.execute(
            select(Property).where(
                and_(
                    Property.county_id == county_id,
                    Property.zip.in_(affected_zips),
                )
            )
        ).scalars().all()

        for prop in properties:
            existing = db.execute(
                select(Incident).where(
                    and_(
                        Incident.property_id == prop.id,
                        Incident.incident_type == "storm_damage",
                        Incident.incident_date == storm_date,
                    )
                )
            ).scalars().first()

            if existing:
                skipped_duplicate += 1
                continue

            incident = Incident(
                property_id=prop.id,
                incident_type="storm_damage",
                incident_date=storm_date,
                county_id=county_id,
            )
            db.add(incident)
            created += 1

        db.commit()

    logger.info(
        "[storm] %s: created=%d duplicate=%d zips_affected=%d",
        county_id, created, skipped_duplicate, len(affected_zips),
    )
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(
            source_type='storm_damage',
            total_scraped=created + skipped_duplicate,
            matched=created,
            unmatched=0,
            skipped=skipped_duplicate,
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
    parser = argparse.ArgumentParser(description="Scrape storm damage incidents")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough", help="County identifier (default: hillsborough)")
    args = parser.parse_args()
    n = scrape_storm_damage(county_id=args.county_id)
    print(f"Done — {n} storm damage incidents created")
