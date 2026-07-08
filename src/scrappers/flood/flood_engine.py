"""
Flood & Water Damage Reports — M1-F Scraper #4

Sources:
  1. FEMA Disaster Declarations API (public) — county-level flood declarations
     (informational only — no ZIP resolution, so no incidents; the 2026-03-18
     county-wide blanket created 523k incidents that had to be purged)
  2. FEMA National Flood Insurance Program (NFIP) claims — ZIP-level flood data,
     stored as synthetic NWSAlert rows (event='NFIP Claim') + targeted tagging
  3. NWS active flood warnings — routed through nws_webhook.process_alert()
     (idempotent backstop for the 5-min nws_poll)

Incidents (incident_type='flood_damage') are created ONLY for properties that
already cross the CDS Silver floor (storm_signal_tagger) — flood is a
stacking-only signal, so blanketing clean properties has zero score effect.

Entry point:
    scrape_flood_damage(county_id, date_range)
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import requests

from src.core.database import get_db_context
from src.utils.county_config import get_county

logger = logging.getLogger(__name__)

_FEMA_DISASTERS_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
_FEMA_NFIP_CLAIMS_URL = "https://www.fema.gov/api/open/v2/FimaNfipClaims"
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


def _fetch_nfip_claims(state: str, county_fips: str, start_date: date) -> List[Tuple[str, date]]:
    """
    Fetch FEMA NFIP paid flood-claim records for a state/county since start_date.
    Returns distinct (zip, date_of_loss) pairs.

    county_fips must be the FULL 5-digit state+county FIPS ('12057') — the
    NFIP countyCode field is 5-digit; the 3-digit form matches nothing.

    NFIP claims data is historical (paid claims, often months behind real-time).
    Useful for catching flood events that didn't trigger an active NWS alert
    or a federal disaster declaration but still produced insurable damage.
    """
    url = (
        f"{_FEMA_NFIP_CLAIMS_URL}"
        f"?$filter=state eq '{state}'"
        f" and countyCode eq '{county_fips.zfill(5)}'"
        f" and dateOfLoss ge '{start_date.isoformat()}'"
        f"&$select=reportedZipCode,dateOfLoss"
        f"&$top=1000&$format=json"
    )
    pairs = set()
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=20)
        resp.raise_for_status()
        for claim in resp.json().get("FimaNfipClaims", []):
            zip_code = claim.get("reportedZipCode")
            loss_raw = claim.get("dateOfLoss") or ""
            if not zip_code or not loss_raw:
                continue
            try:
                loss_date = datetime.fromisoformat(loss_raw.replace("Z", "+00:00")).date()
            except ValueError:
                continue
            pairs.add((str(zip_code)[:5], loss_date))
    except Exception as e:
        logger.warning("[flood] FEMA NFIP claims API failed: %s", e)
    return sorted(pairs)


def _fetch_nws_flood_alerts_by_zones(zone_ids: List[str]) -> List[Dict]:
    """Fetch active NWS flood-event alert features for specific zone IDs."""
    features = []
    for zone_id in zone_ids:
        try:
            resp = requests.get(
                _NWS_ZONE_URL.format(zone_id=zone_id),
                headers={"User-Agent": "ForcedAction/1.0", "Accept": "application/geo+json"},
                timeout=15,
            )
            resp.raise_for_status()
            for feature in resp.json().get("features", []):
                event = feature.get("properties", {}).get("event", "")
                if any(e in event for e in FLOOD_NWS_EVENTS):
                    features.append(feature)
        except Exception as e:
            logger.warning("[flood] NWS zone fetch failed for %s: %s", zone_id, e)
    return features


def _process_nfip_claims(db, county_id: str, claims: List[Tuple[str, date]]) -> Tuple[int, int, int]:
    """
    Store each (zip, date_of_loss) claim as a synthetic NWSAlert row
    (event='NFIP Claim') and tag distressed properties in that ZIP with a
    flood_damage incident. Idempotent via the synthetic alert_id.

    Returns (new_alerts, duplicates, properties_tagged).
    """
    from sqlalchemy import select
    from src.core.models import NWSAlert
    from src.services.storm_signal_tagger import tag_affected_properties

    new_alerts = duplicates = tagged = 0
    for zip_code, loss_date in claims:
        alert_id = f"nfip-{county_id}-{zip_code}-{loss_date.isoformat()}"
        exists = db.execute(
            select(NWSAlert.id).where(NWSAlert.alert_id == alert_id)
        ).scalar_one_or_none()
        if exists:
            duplicates += 1
            continue

        loss_dt = datetime(loss_date.year, loss_date.month, loss_date.day, tzinfo=timezone.utc)
        db.add(NWSAlert(
            alert_id=alert_id,
            event="NFIP Claim",
            severity="Unknown",
            headline=f"NFIP flood claim reported in ZIP {zip_code}",
            affected_zips=[zip_code],
            effective=loss_dt,
            onset=loss_dt,
            county_id=county_id,
        ))
        db.flush()
        new_alerts += 1

        tagged += len(tag_affected_properties(
            [zip_code], alert_id, loss_dt, db, incident_type="flood_damage",
        ))

    db.commit()
    return new_alerts, duplicates, tagged


def scrape_flood_damage(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,
) -> int:
    """
    Fetch flood events from FEMA + NWS; store area-level alert rows and tag
    distressed properties with flood_damage incidents.

    Args:
        county_id:  County to process.
        date_range: (start_date, end_date). Defaults to last 30 days.

    Returns:
        Number of properties newly tagged with flood_damage incidents.
    """
    config = get_county(county_id)
    fips = config.get("fips", "")
    state = config.get("state", "FL")
    nws_zones = config.get("nws_zones", [])

    if date_range is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
    else:
        start_date, end_date = date_range

    # Source 1: FEMA disaster declarations — informational only (county-level,
    # no ZIP resolution → no incidents; see module docstring).
    county_fips3 = (fips[2:] if len(fips) >= 5 else fips).zfill(3)
    all_declarations = _fetch_fema_declarations(state, county_fips3, start_date)
    _FLOOD_INCIDENT_TYPES = {"Flood", "Hurricane", "Coastal Storm", "Severe Storm", "Typhoon"}
    fema_declarations = [
        d for d in all_declarations
        if str(d.get("fipsCountyCode", "")).zfill(3) == county_fips3
        and d.get("incidentType") in _FLOOD_INCIDENT_TYPES
    ]
    if fema_declarations:
        logger.warning(
            "[flood] %s: %d active FEMA flood declaration(s) — county-level only, "
            "incidents come from NWS/NFIP ZIP evidence", county_id, len(fema_declarations),
        )

    tagged = 0
    new_alerts = 0
    duplicates = 0

    # Source 2: NWS active flood alerts — idempotent backstop for nws_poll.
    flood_features = _fetch_nws_flood_alerts_by_zones(nws_zones) if nws_zones else []
    if flood_features:
        from src.services.nws_webhook import process_alert
        with get_db_context() as db:
            for feature in flood_features:
                props = {**(feature.get("properties") or {}), "id": feature.get("id", "")}
                try:
                    result = process_alert(props, db)
                except Exception as e:
                    logger.error("[flood] process_alert failed: %s", e)
                    continue
                status = result.get("status")
                if status == "processed":
                    new_alerts += 1
                    tagged += result.get("tagged_count", 0)
                elif status == "duplicate":
                    duplicates += 1

    # Source 3: FEMA NFIP paid claims → synthetic alerts + targeted tagging.
    nfip_claims = _fetch_nfip_claims(state, fips, start_date)
    if nfip_claims:
        with get_db_context() as db:
            n_new, n_dup, n_tagged = _process_nfip_claims(db, county_id, nfip_claims)
        new_alerts += n_new
        duplicates += n_dup
        tagged += n_tagged
        logger.info(
            "[flood] %s: NFIP claims=%d new_alerts=%d duplicate=%d",
            county_id, len(nfip_claims), n_new, n_dup,
        )

    logger.info(
        "[flood] %s %s→%s: nws_alerts=%d nfip_claims=%d new=%d duplicate=%d props_tagged=%d",
        county_id, start_date, end_date, len(flood_features), len(nfip_claims),
        new_alerts, duplicates, tagged,
    )
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(
            source_type='flood_damage',
            total_scraped=len(flood_features) + len(nfip_claims),
            matched=tagged,
            unmatched=0,
            skipped=duplicates,
            county_id=county_id,
        )
    except Exception as stats_err:
        logger.warning("⚠ Could not record scraper stats (non-critical): %s", stats_err)
    return tagged


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
    print(f"Done — {n} properties tagged with flood damage")
