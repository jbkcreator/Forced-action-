"""
Fire Incident Scraper — Tampa Fire Rescue Calls-for-Service API

Source: https://ncapps.tampagov.net/callsforservice/TFR/GetTFRCallsForService
  - JSON API, no auth required, returns ~7 days of rolling incident data
  - Updated every 30 minutes
  - Coverage: City of Tampa (urban core of Hillsborough County)

Creates Incident records (incident_type='Fire') for matched properties.

Entry point:
    scrape_fire_incidents(county_id, date_range)
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import requests

from src.utils.http_helpers import requests_get_with_retry
from src.core.database import get_db_context
from src.core.models import Property, Incident
from src.utils.county_config import get_county
from sqlalchemy import select, and_, func

logger = logging.getLogger(__name__)

_TFR_URL = "https://ncapps.tampagov.net/callsforservice/TFR/GetTFRCallsForService"
_HEADERS = {
    "User-Agent": "ForcedAction/1.0 (distressed-property-intelligence)",
    "Accept": "application/json",
    "Referer": "https://ncapps.tampagov.net/callsforservice/tfr",
}

# Case descriptions that indicate a fire at or near a structure.
# AUTOMATIC FIRE ALARM included — alarms at real properties are valid distress signals.
# TRASH/DUMPSTER excluded — no property damage implied.
FIRE_INCIDENT_TYPES = {
    "BUILDING FIRE",
    "STRUCTURE FIRE",
    "VEHICLE FIRE",
    "AUTOMATIC FIRE ALARM",
    "FIRE OUT INVESTIGATION",
    "TRANSFORMER FIRE",
    "BRUSH FIRE",
    "GRASS FIRE",
    "WILDLAND FIRE",
    "ARSON",
    "EXPLOSION",
}

# Partial matches for descriptions not in the exact set above
FIRE_KEYWORDS = ["fire", "explosion", "arson", "smoke investigation"]

def _normalise_address(raw: str) -> str:
    """Strip and uppercase only — rapidfuzz handles abbreviation differences."""
    return raw.strip().upper()


def _is_fire_incident(description: str) -> bool:
    desc = description.strip().upper()
    if desc in FIRE_INCIDENT_TYPES:
        return True
    desc_lower = description.lower()
    return any(kw in desc_lower for kw in FIRE_KEYWORDS)


def _fetch_incidents() -> List[Dict]:
    """Fetch all incidents from Tampa Fire Rescue API via proxy."""
    try:
        resp = requests_get_with_retry(
            _TFR_URL,
            headers=_HEADERS,
            timeout=20,
            use_proxy=True,
        )
        return resp.json().get("data", [])
    except Exception as exc:
        logger.error("[fire] Tampa Fire Rescue API fetch failed: %s", exc)
        return []


def _filter_fire_incidents(
    all_incidents: List[Dict],
    since: datetime,
) -> List[Dict]:
    """
    Keep only fire-related incidents with a real address dispatched after `since`.
    Excludes 'GRID ONLY' address records — no property to match against.
    """
    results = []
    for inc in all_incidents:
        addr = inc.get("address", "").strip()
        if not addr or addr.upper() == "GRID ONLY":
            continue

        desc = inc.get("case_description", "")
        if not _is_fire_incident(desc):
            continue

        dispatch_str = inc.get("dispatch_time", "")
        try:
            dispatch_dt = datetime.fromisoformat(dispatch_str).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        if dispatch_dt < since:
            continue

        results.append({
            "address": _normalise_address(addr),
            "description": desc.strip(),
            "dispatch_time": dispatch_dt,
            "incident_number": inc.get("incident_number"),
        })
    return results


_MATCH_THRESHOLD = 75  # rapidfuzz token_sort_ratio minimum


def _match_property(db, address: str, county_id: str) -> Optional[int]:
    """
    Match a fire incident address against the properties table.

    Strategy:
      1. Filter candidates by house number prefix (fast index scan).
      2. Rank candidates with rapidfuzz token_sort_ratio — handles abbreviated
         vs expanded street types (ST/STREET, AV/AVENUE) and directional
         prefix/suffix differences (E CHELSEA ST vs CHELSEA STREET EAST).
      3. Accept if best score >= _MATCH_THRESHOLD.
    """
    from rapidfuzz import fuzz

    house = address.split()[0] if address else ""
    if not house:
        return None

    candidates = db.execute(
        select(Property.id, Property.address).where(
            and_(
                Property.county_id == county_id,
                func.upper(Property.address).like(f"{house} %"),
            )
        ).limit(50)
    ).all()

    if not candidates:
        return None

    best_id, best_score = None, 0
    addr_upper = address.upper()
    for prop_id, prop_addr in candidates:
        score = fuzz.token_sort_ratio(addr_upper, (prop_addr or "").upper())
        if score > best_score:
            best_score, best_id = score, prop_id

    return best_id if best_score >= _MATCH_THRESHOLD else None


def scrape_fire_incidents(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,
    headfull: bool = False,  # kept for backwards-compat with cron args
) -> int:
    """
    Fetch fire incidents from Tampa Fire Rescue API and create Incident records
    for matched properties. Returns number of new Incident records created.

    Args:
        county_id:   County to process (currently Tampa Fire covers hillsborough urban core).
        date_range:  (start_date, end_date). Defaults to last 1 day.
    """
    try:
        get_county(county_id)  # validate county exists
    except KeyError:
        logger.error("[fire] Unknown county_id: %s", county_id)
        return 0

    if date_range is None:
        lookback_days = 1
    else:
        start_date, end_date = date_range
        lookback_days = (end_date - start_date).days + 1

    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    logger.info("[fire] %s: fetching Tampa Fire Rescue incidents since %s", county_id, since.date())
    all_incidents = _fetch_incidents()

    if not all_incidents:
        logger.warning("[fire] %s: no data from API", county_id)
        _record_stats(county_id, 0, 0, 0, 0)
        return 0

    fire_incidents = _filter_fire_incidents(all_incidents, since)
    logger.info(
        "[fire] %s: %d fire incidents with address (from %d total API records)",
        county_id, len(fire_incidents), len(all_incidents),
    )

    if not fire_incidents:
        logger.info("[fire] %s: no fire incidents in lookback window — 0 incidents", county_id)
        _record_stats(county_id, 0, 0, 0, 0)
        return 0

    created = skipped_duplicate = skipped_no_match = 0
    fire_date = date.today()

    with get_db_context() as db:
        for inc in fire_incidents:
            property_id = _match_property(db, inc["address"], county_id)

            if not property_id:
                skipped_no_match += 1
                logger.debug("[fire] no property match: %s", inc["address"])
                continue

            existing = db.execute(
                select(Incident).where(
                    and_(
                        Incident.property_id == property_id,
                        Incident.incident_type == "Fire",
                        Incident.incident_date == fire_date,
                    )
                )
            ).scalars().first()

            if existing:
                skipped_duplicate += 1
                continue

            db.add(Incident(
                property_id=property_id,
                incident_type="Fire",
                incident_date=fire_date,
                county_id=county_id,
                source_meta={
                    "incident_number": inc["incident_number"],
                    "description": inc["description"],
                },
            ))
            created += 1

        db.commit()

    logger.info(
        "[fire] %s: created=%d duplicate=%d no_match=%d",
        county_id, created, skipped_duplicate, skipped_no_match,
    )
    _record_stats(county_id, created + skipped_duplicate + skipped_no_match, created, skipped_no_match, skipped_duplicate)
    return created


def _record_stats(county_id: str, total: int, matched: int, unmatched: int, skipped: int) -> None:
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(
            source_type="fire_incidents",
            total_scraped=total,
            matched=matched,
            unmatched=unmatched,
            skipped=skipped,
            county_id=county_id,
        )
    except Exception as exc:
        logger.warning("[fire] Could not record scraper stats: %s", exc)


if __name__ == "__main__":
    import argparse
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Scrape fire incidents from Tampa Fire Rescue API")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--lookback", type=int, default=1, help="Days to look back (default: 1)")
    args = parser.parse_args()

    end = date.today()
    start = end - timedelta(days=args.lookback - 1)
    n = scrape_fire_incidents(county_id=args.county_id, date_range=(start, end))
    print(f"Done — {n} fire incidents created")
