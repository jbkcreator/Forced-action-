"""
Flood & Water Damage Reports — M1-F Scraper #4

Sources:
  1. FEMA Disaster Declarations API (public) — county-level flood declarations
  2. FEMA National Flood Insurance Program (NFIP) claims — ZIP-level flood data
  3. NWS active flood warnings (reuses storm_engine NWS fetch pattern)

Creates Incident records (incident_type='flood_damage') on matched properties.

Entry point:
    scrape_flood_damage(county_id, date_range)
"""

import logging
from datetime import date, timedelta
from typing import Optional, Tuple, List, Dict

import requests

from src.core.database import get_db_context
from src.core.models import Property, Incident
from src.utils.county_config import get_county
from sqlalchemy import select, and_

logger = logging.getLogger(__name__)

_FEMA_DISASTERS_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
_FEMA_NFIP_URL = "https://www.fema.gov/api/open/v1/nfipPolicies"
_NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"
_NWS_ZONE_URL = "https://api.weather.gov/alerts/active/zone/{zone_id}"

FLOOD_NWS_EVENTS = [
    "Flash Flood Warning",
    "Flash Flood Watch",
    "Flood Warning",
    "Flood Watch",
    "Flood Advisory",
    "Coastal Flood Warning",
    "Coastal Flood Advisory",
    "Areal Flood Warning",
]


def _fetch_fema_declarations(state: str, county_fips: str, start_date: date) -> List[Dict]:
    """Fetch FEMA flood disaster declarations for a state/county since start_date."""
    url = (
        f"{_FEMA_DISASTERS_URL}"
        f"?$filter=state eq '{state}'"
        f" and declarationDate ge '{start_date.isoformat()}'"
        f"&$orderby=declarationDate desc&$top=50&$format=json"
    )
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=15)
        resp.raise_for_status()
        return resp.json().get("DisasterDeclarationsSummaries", [])
    except Exception as e:
        logger.warning("[flood] FEMA disasters API failed: %s", e, exc_info=True)
        return []


def _fetch_nws_flood_alerts_by_zones(zone_ids: List[str]) -> List[str]:
    """Fetch active NWS flood alerts for specific zone IDs and return affected ZIP codes."""
    import re
    affected_zips = set()
    for zone_id in zone_ids:
        try:
            resp = requests.get(
                _NWS_ZONE_URL.format(zone_id=zone_id),
                headers={"User-Agent": "ForcedAction/1.0", "Accept": "application/geo+json"},
                timeout=15,
            )
            resp.raise_for_status()
            for feature in resp.json().get("features", []):
                props = feature.get("properties", {})
                event = props.get("event", "")
                if not any(e in event for e in FLOOD_NWS_EVENTS):
                    continue
                description = props.get("description", "") or ""
                zips = re.findall(r"\b(3[3-4]\d{3})\b", description)
                affected_zips.update(zips)
        except Exception as e:
            logger.warning("[flood] NWS zone fetch failed for %s: %s", zone_id, e)
    return list(affected_zips)


def _fetch_nws_flood_alerts(state: str) -> List[str]:
    """Fetch active NWS flood alerts for a state (fallback when no zone IDs configured)."""
    import re
    affected_zips = set()
    try:
        resp = requests.get(
            _NWS_ALERTS_URL,
            params={"area": state, "status": "actual"},
            headers={"User-Agent": "ForcedAction/1.0"},
            timeout=15,
        )
        resp.raise_for_status()
        for feature in resp.json().get("features", []):
            props = feature.get("properties", {})
            event = props.get("event", "")
            if not any(e in event for e in FLOOD_NWS_EVENTS):
                continue
            description = props.get("description", "") or ""
            zips = re.findall(r"\b(3[3-4]\d{3})\b", description)
            affected_zips.update(zips)
    except Exception as e:
        logger.warning("[flood] NWS alerts fetch failed: %s", e, exc_info=True)
    return list(affected_zips)


def scrape_flood_damage(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,
) -> int:
    """
    Fetch flood events from FEMA + NWS and create Incident records for
    all matched properties.

    Args:
        county_id:  County to process.
        date_range: (start_date, end_date). Defaults to last 30 days.

    Returns:
        Number of new Incident records created.
    """
    config = get_county(county_id)
    fips = config.get("fips", "")
    state = config.get("state", "FL")
    zip_prefixes = config.get("zip_prefixes", [])
    nws_zones = config.get("nws_zones", [])

    if date_range is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
    else:
        start_date, end_date = date_range

    # Source 1: FEMA disaster declarations — fetch state-wide then filter by county FIPS
    county_fips = fips[2:] if len(fips) >= 5 else fips
    all_declarations = _fetch_fema_declarations(state, county_fips, start_date)
    _FLOOD_INCIDENT_TYPES = {"Flood", "Hurricane", "Coastal Storm", "Severe Storm", "Typhoon"}
    fema_declarations = [
        d for d in all_declarations
        if str(d.get("fipsCountyCode", "")).zfill(3) == county_fips.zfill(3)
        and d.get("incidentType") in _FLOOD_INCIDENT_TYPES
    ]
    has_fema_flood = len(fema_declarations) > 0
    logger.info("[flood] %s: FEMA state-wide=%d county_flood=%d", county_id, len(all_declarations), len(fema_declarations))

    # Source 2: NWS active flood alerts → affected ZIPs (zone-based or state fallback)
    if nws_zones:
        logger.info("[flood] %s: fetching alerts via %d zone(s): %s", county_id, len(nws_zones), nws_zones)
        nws_zips = _fetch_nws_flood_alerts_by_zones(nws_zones)
        county_zips = nws_zips  # zone fetch is already county-scoped
    else:
        nws_zips = _fetch_nws_flood_alerts(state)
        county_zips = [z for z in nws_zips if any(z.startswith(p) for p in zip_prefixes)]

    if not has_fema_flood and not county_zips:
        logger.info("[flood] %s: no active flood events — 0 incidents", county_id)
        try:
            from src.utils.scraper_db_helper import record_scraper_stats
            record_scraper_stats(source_type='flood_damage', total_scraped=0, matched=0, unmatched=0, skipped=0)
        except Exception:
            pass
        return 0

    created = 0
    skipped_duplicate = 0
    flood_date = date.today()

    with get_db_context() as db:
        if has_fema_flood:
            properties = db.execute(
                select(Property).where(Property.county_id == county_id)
            ).scalars().all()
        else:
            properties = db.execute(
                select(Property).where(
                    and_(
                        Property.county_id == county_id,
                        Property.zip.in_(county_zips),
                    )
                )
            ).scalars().all()

        for prop in properties:
            existing = db.execute(
                select(Incident).where(
                    and_(
                        Incident.property_id == prop.id,
                        Incident.incident_type == "flood_damage",
                        Incident.incident_date == flood_date,
                    )
                )
            ).scalars().first()

            if existing:
                skipped_duplicate += 1
                continue

            incident = Incident(
                property_id=prop.id,
                incident_type="flood_damage",
                incident_date=flood_date,
                county_id=county_id,
            )
            db.add(incident)
            created += 1

        db.commit()

    logger.info(
        "[flood] %s: created=%d duplicate=%d strategy=%s",
        county_id, created, skipped_duplicate,
        "fema_county_wide" if has_fema_flood else f"nws_zips({len(county_zips)})",
    )
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(
            source_type='flood_damage',
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
    parser = argparse.ArgumentParser(description="Scrape flood damage incidents")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough", help="County identifier (default: hillsborough)")
    args = parser.parse_args()
    n = scrape_flood_damage(county_id=args.county_id)
    print(f"Done — {n} flood damage incidents created")
