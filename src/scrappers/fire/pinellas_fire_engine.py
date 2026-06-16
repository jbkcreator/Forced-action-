"""
Fire Incident Scraper — Pinellas County 911 CAD Feed

Source: https://911.pinellas.gov/files/Activity.json
  - Public JSON snapshot of currently-ACTIVE 911 calls, regenerated continuously
  - No auth required; covers the entire county (St. Petersburg, Clearwater,
    Largo, Dunedin, unincorporated Pinellas) in one feed
  - IMPORTANT: unlike Tampa TFR this is a live snapshot, NOT rolling history.
    The file only shows calls that are open right now (~10-30 at a time).
    Callers must poll frequently (every 5-10 min) — once-daily will miss most.
    The (property_id, incident_type, incident_date) dedup prevents duplicate
    Incident rows across successive poll cycles.

Matching cascade (in order):
  1. Rapidfuzz address match — for real street addresses (e.g. "1234 MAIN ST").
  2. Pinellas GIS reverse geocode — for intersection addresses ("26 AV S/MLK ST")
     and grid cells ("GRID 643B") where lat/lon from CAD is used to look up
     the containing parcel via egis.pinellas.gov, returning PARCELID_DSP1
     which is then matched against properties.parcel_id.

Entry point:
    scrape_pinellas_fire_incidents(county_id, date_range)
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

from src.utils.http_helpers import requests_get_with_retry
from src.core.database import get_db_context
from src.core.models import Property, Incident
from src.utils.county_config import get_county
from sqlalchemy import text as sa_text, and_, select, func

logger = logging.getLogger(__name__)

_PINELLAS_911_URL = "https://911.pinellas.gov/files/Activity.json"
_PINELLAS_GIS_URL = (
    "https://egis.pinellas.gov/gis/rest/services/PublicWebGIS/Parcels/MapServer/1/query"
)
_CAD_HEADERS = {
    "User-Agent": "ForcedAction/1.0 (distressed-property-intelligence)",
    "Accept": "application/json",
    "Referer": "https://911.pinellas.gov/actcallspub.htm",
}
_GIS_HEADERS = {
    "User-Agent": "ForcedAction/1.0 (distressed-property-intelligence)",
}

_FIRE_CODE_PREFIXES = ("F", "H")  # F = Fire dept, H = Hazmat
_FIRE_TYPE_KEYWORDS = [
    "fire",        # building fire, structure fire, vehicle fire, brush fire, etc.
    "explosion",
    "arson",
    "smoke",       # smoke investigation, smoke in structure
    "hazmat",
    "haz mat",
    "gas leak",
    "fuel spill",
    "chemical",
]
_MATCH_THRESHOLD = 75  # rapidfuzz token_sort_ratio minimum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalise_address(raw: str) -> str:
    return raw.strip().upper()


def _is_grid_cell(location: str) -> bool:
    return location.startswith("GRID")


def _is_intersection(location: str) -> bool:
    return "/" in location


def _is_fire_call(call: Dict) -> bool:
    code = call.get("Code", "")
    call_type = call.get("Type", "").lower()
    return (
        code.startswith(_FIRE_CODE_PREFIXES)
        and any(kw in call_type for kw in _FIRE_TYPE_KEYWORDS)
    )


def _stamp_received(received_time: str) -> Optional[datetime]:
    """
    CAD gives time-only HH:MM:SS.  Stamps today's date, with midnight-rollover
    guard: if it is before 02:00 UTC and received time is after 20:00, the call
    was dispatched yesterday.
    """
    try:
        now_utc = datetime.now(timezone.utc)
        today = now_utc.date()
        if now_utc.hour < 2 and int(received_time[:2]) >= 20:
            today = today - timedelta(days=1)
        dt = datetime.strptime(f"{today} {received_time}", "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def _fetch_active_calls() -> List[Dict]:
    try:
        resp = requests_get_with_retry(
            _PINELLAS_911_URL,
            headers=_CAD_HEADERS,
            timeout=20,
            use_proxy=False,
        )
        return resp.json().get("CallInfo", [])
    except Exception as exc:
        logger.error("[pinellas_fire] CAD fetch failed: %s", exc)
        return []


def _filter_fire_calls(all_calls: List[Dict]) -> List[Dict]:
    results = []
    for call in all_calls:
        if not _is_fire_call(call):
            continue
        location = call.get("Location", "").strip()
        if not location:
            continue
        try:
            lat = float(call["Lat"])
            lon = float(call["Lon"])
        except (KeyError, ValueError, TypeError):
            lat = lon = None
        norm = _normalise_address(location)
        results.append({
            "incident_number": call.get("IncidentNo"),
            "type": call.get("Type", "").strip(),
            "code": call.get("Code", ""),
            "location": norm,
            "is_grid": _is_grid_cell(norm),
            "is_intersection": _is_intersection(norm),
            "dispatched_at": _stamp_received(call.get("Received", "")),
            "lat": lat,
            "lon": lon,
        })
    return results


# ---------------------------------------------------------------------------
# Matching — cascade: address → GIS reverse geocode
# ---------------------------------------------------------------------------

def _match_by_address(db, address: str, county_id: str) -> Optional[int]:
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
    for prop_id, prop_addr in candidates:
        score = fuzz.token_sort_ratio(address, (prop_addr or "").upper())
        if score > best_score:
            best_score, best_id = score, prop_id

    return best_id if best_score >= _MATCH_THRESHOLD else None


def _reverse_geocode_parcel(lat: float, lon: float) -> Optional[str]:
    """
    Call Pinellas County GIS to find the parcel containing (lat, lon).
    Returns PARCELID_DSP1 (dashed format e.g. "19-31-17-90995-001-0020")
    which matches properties.parcel_id, or None if the point falls on a road
    right-of-way or outside any parcel.

    Uses a 30-foot buffer so intersection/grid-cell coordinates that land on
    road ROW still resolve to an adjacent parcel.
    """
    params = {
        "geometry": f"{lon},{lat}",  # ArcGIS expects X,Y → lon,lat
        "geometryType": "esriGeometryPoint",
        "inSR": 4326,
        "spatialRel": "esriSpatialRelIntersects",
        "distance": 30,
        "units": "esriSRUnit_Foot",
        "outFields": "PARCELID_DSP1",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        resp = requests_get_with_retry(
            _PINELLAS_GIS_URL,
            headers=_GIS_HEADERS,
            params=params,
            timeout=15,
            use_proxy=False,
        )
        features = resp.json().get("features", [])
        if not features:
            return None
        parcel_id = features[0].get("attributes", {}).get("PARCELID_DSP1")
        return parcel_id.replace("-", "") if parcel_id else None
    except Exception as exc:
        logger.warning("[pinellas_fire] GIS reverse geocode failed (lat=%s lon=%s): %s", lat, lon, exc)
        return None


def _match_by_parcel_id(db, parcel_id: str, county_id: str) -> Optional[int]:
    row = db.execute(
        sa_text(
            "SELECT id FROM properties WHERE parcel_id = :pid AND county_id = :cid LIMIT 1"
        ),
        {"pid": parcel_id, "cid": county_id},
    ).first()
    return row[0] if row else None


def _match_property(db, call: Dict, county_id: str) -> Optional[int]:
    # Step 1: rapidfuzz address match (only for real street addresses)
    if not call["is_grid"] and not call["is_intersection"]:
        prop_id = _match_by_address(db, call["location"], county_id)
        if prop_id:
            return prop_id

    # Step 2: GIS reverse geocode — works for intersections, grid cells, and
    # real addresses whose string match fell below the threshold
    if call["lat"] is not None and call["lon"] is not None:
        parcel_id = _reverse_geocode_parcel(call["lat"], call["lon"])
        if parcel_id:
            return _match_by_parcel_id(db, parcel_id, county_id)

    return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def scrape_pinellas_fire_incidents(
    county_id: str = "pinellas",
    date_range: Optional[Tuple[date, date]] = None,  # noqa: ARG001 — kept for cron interface parity
    headfull: bool = False,
) -> int:
    """
    Fetch active fire calls from Pinellas 911 CAD and create Incident records
    for matched properties.  Returns the number of new Incident records created.

    Because the feed is a live snapshot (not rolling history), call this
    function every 5-10 minutes via cron rather than once daily.
    """
    try:
        get_county(county_id)
    except KeyError:
        logger.error("[pinellas_fire] Unknown county_id: %s", county_id)
        return 0

    logger.info("[pinellas_fire] %s: fetching Pinellas 911 CAD active calls", county_id)
    all_calls = _fetch_active_calls()

    if not all_calls:
        logger.warning("[pinellas_fire] %s: no data from CAD feed", county_id)
        _record_stats(county_id, 0, 0, 0, 0)
        return 0

    fire_calls = _filter_fire_calls(all_calls)
    logger.info(
        "[pinellas_fire] %s: %d fire calls active (from %d total CAD records)",
        county_id, len(fire_calls), len(all_calls),
    )

    if not fire_calls:
        _record_stats(county_id, 0, 0, 0, 0)
        return 0

    created = skipped_duplicate = skipped_no_match = 0
    fire_date = date.today()

    with get_db_context() as db:
        for call in fire_calls:
            property_id = _match_property(db, call, county_id)

            if not property_id:
                skipped_no_match += 1
                logger.warning(
                    "[pinellas_fire] no property match: location=%r grid=%s intersection=%s",
                    call["location"], call["is_grid"], call["is_intersection"],
                )
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
                    "cad_incident_number": call["incident_number"],
                    "cad_code": call["code"],
                    "cad_type": call["type"],
                },
            ))
            created += 1

        db.commit()

    logger.info(
        "[pinellas_fire] %s: created=%d duplicate=%d no_match=%d",
        county_id, created, skipped_duplicate, skipped_no_match,
    )
    _record_stats(
        county_id,
        created + skipped_duplicate + skipped_no_match,
        created,
        skipped_no_match,
        skipped_duplicate,
    )
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
        logger.warning("[pinellas_fire] Could not record scraper stats: %s", exc)


if __name__ == "__main__":
    import argparse
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Scrape fire incidents from Pinellas County 911 CAD")
    parser.add_argument("--county-id", dest="county_id", default="pinellas")
    args = parser.parse_args()

    n = scrape_pinellas_fire_incidents(county_id=args.county_id)
    print(f"Done — {n} fire incidents created")
