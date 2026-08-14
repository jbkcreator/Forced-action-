"""
Storm Damage Zones — M1-F Scraper #2

Hourly backstop for the primary NWS pipeline (src/tasks/nws_poll.py, every
5 min): fetches active NWS alerts for the county's forecast zones and routes
each qualifying alert through nws_webhook.process_alert(), which owns
idempotency (alert_id), NWSAlert storage, ZIP resolution via the SAME/UGC
crosswalk, and targeted incident tagging (storm_signal_tagger — only
properties with CDS >= Silver floor get incidents; no county-wide blankets).

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
from src.utils.county_config import get_county

logger = logging.getLogger(__name__)

_NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"
_NWS_ZONE_URL = "https://api.weather.gov/alerts/active/zone/{zone_id}"

# NWS event types that indicate storm/wind/hail damage relevant to roofing.
# Every entry must pass process_alert's _is_qualifying() gate against the
# default nws_relevant_events config — enforced by
# tests/test_weather_zip_mapping.py::test_engine_events_pass_qualifying_gate.
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


def _fetch_nws_alerts_by_zones(zone_ids: List[str]) -> Tuple[List[Dict], List[str]]:
    """Fetch active NWS alerts for a list of NWS zone IDs (preferred — precise).

    Returns (features, failed_zone_ids). A zone that failed never checked for
    alerts, so it must be surfaced even when other zones succeeded — a partial
    outage is not the same as a clean run.
    """
    features = []
    failed_zones = []
    for zone_id in zone_ids:
        try:
            resp = requests.get(
                _NWS_ZONE_URL.format(zone_id=zone_id),
                headers={"User-Agent": "ForcedAction/1.0 (distressed-property-intelligence)",
                         "Accept": "application/geo+json"},
                timeout=15,
            )
            resp.raise_for_status()
            features.extend(resp.json().get("features", []))
        except Exception as e:
            logger.warning("[storm] NWS zone fetch failed for %s: %s", zone_id, e)
            failed_zones.append(f"{zone_id}: {e}")
    return features, failed_zones


def _fetch_nws_alerts(state: str = "FL") -> Tuple[List[Dict], Optional[str]]:
    """Fetch active NWS alerts for a state (fallback when no zone IDs configured).

    Returns (features, error) — error is set if the API call itself failed.
    """
    try:
        resp = requests.get(
            _NWS_ALERTS_URL,
            params={"area": state, "status": "actual", "message_type": "alert"},
            headers={"User-Agent": "ForcedAction/1.0 (distressed-property-intelligence)"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("features", []), None
    except Exception as e:
        logger.warning("[storm] NWS API fetch failed: %s", e, exc_info=True)
        return [], str(e)


def scrape_storm_damage(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,  # noqa: ARG001 — interface consistency
) -> int:
    """
    Fetch active NWS storm alerts and route them through process_alert()
    (idempotent — alerts already handled by the 5-min nws_poll are skipped).

    Returns:
        Number of properties newly tagged with weather incidents.
    """
    try:
        config = get_county(county_id)
    except KeyError:
        logger.error("[storm] Unknown county_id: %s", county_id)
        return 0

    state = config.get("state", "FL")
    nws_zones = config.get("nws_zones", [])

    if nws_zones:
        logger.info("[storm] %s: fetching alerts via %d zone(s): %s", county_id, len(nws_zones), nws_zones)
        alerts, failed_zones = _fetch_nws_alerts_by_zones(nws_zones)
        fetch_error = "; ".join(failed_zones) if failed_zones else None
        total_fetch_failure = len(failed_zones) == len(nws_zones)
    else:
        logger.info("[storm] %s: no nws_zones configured, falling back to state-level fetch", county_id)
        alerts, fetch_error = _fetch_nws_alerts(state)
        total_fetch_failure = bool(fetch_error)

    qualifying = [
        a for a in alerts
        if any(t in (a.get("properties", {}).get("event") or "") for t in STORM_EVENT_TYPES)
    ]

    tagged = 0
    new_alerts = 0
    duplicates = 0
    non_qualifying = 0
    if qualifying:
        from src.services.nws_webhook import process_alert
        with get_db_context() as db:
            for alert in qualifying:
                props = {**(alert.get("properties") or {}), "id": alert.get("id", "")}
                try:
                    result = process_alert(props, db)
                except Exception as e:
                    logger.error("[storm] process_alert failed: %s", e)
                    continue
                status = result.get("status")
                if status == "processed":
                    new_alerts += 1
                    tagged += result.get("tagged_count", 0)
                elif status == "duplicate":
                    duplicates += 1
                elif status == "skipped":
                    non_qualifying += 1
                    logger.warning(
                        "[storm] alert dropped by process_alert gate: event=%r reason=%s",
                        props.get("event"), result.get("reason"),
                    )

    logger.info(
        "[storm] %s: alerts=%d qualifying=%d new=%d duplicate=%d non_qualifying=%d props_tagged=%d",
        county_id, len(alerts), len(qualifying), new_alerts, duplicates, non_qualifying, tagged,
    )
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        if fetch_error:
            # A zone (or the state-level fetch) failed — real signals may
            # have been missed, so this can never be reported as a clean
            # run or a confirmed no-data day, even if other zones succeeded.
            record_scraper_stats(
                source_type='storm_damage',
                total_scraped=len(qualifying),
                matched=tagged,
                unmatched=0,
                skipped=duplicates,
                county_id=county_id,
                run_success=not total_fetch_failure,
                error_type="scraper_error",
                error_message=fetch_error[:500],
            )
        elif qualifying:
            record_scraper_stats(
                source_type='storm_damage',
                total_scraped=len(qualifying),
                matched=tagged,
                unmatched=0,
                skipped=duplicates,
                county_id=county_id,
                error_type="none",
            )
        else:
            record_scraper_stats(
                source_type='storm_damage',
                total_scraped=0,
                matched=0,
                unmatched=0,
                skipped=0,
                county_id=county_id,
                error_type="no_data",
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
    parser = argparse.ArgumentParser(description="Scrape storm damage incidents")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough", help="County identifier (default: hillsborough)")
    args = parser.parse_args()
    n = scrape_storm_damage(county_id=args.county_id)
    print(f"Done — {n} properties tagged with storm damage")
