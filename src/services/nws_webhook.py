"""
NWS webhook processor — National Weather Service CAP alert handler.

Qualifying events activate storm packs in affected ZIPs and notify
subscribers with locked territories in those ZIPs.
"""

import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

QUALIFYING_EVENTS = {
    "Hurricane",
    "Tropical Storm",
    "Severe Thunderstorm",
    "Tornado",
    "Hail",
}

STORM_ACTIVE_TTL = 72 * 3600  # 72 hours


def process_alert(alert_payload: dict, db: Session) -> dict:
    properties = alert_payload.get("properties", alert_payload)
    event_type = properties.get("event", "")
    affected_areas = _extract_zip_codes(properties)

    if not is_qualifying(event_type):
        logger.info("NWS alert: non-qualifying event %r — skipping", event_type)
        return {"activated": 0, "zips": [], "event": event_type}

    logger.info("NWS qualifying event: %s affecting %d areas", event_type, len(affected_areas))

    activated = activate_storm_packs(affected_areas, db)
    queue_storm_scraper(affected_areas)

    return {"activated": activated, "zips": affected_areas, "event": event_type}


def is_qualifying(event_type: str) -> bool:
    return any(q.lower() in event_type.lower() for q in QUALIFYING_EVENTS)


def activate_storm_packs(zip_codes: list[str], db: Session) -> int:
    from src.core.models import Subscriber, ZipTerritory
    from src.core.redis_client import redis_available, rset
    from src.services.sms_compliance import can_send, send_sms

    notified = 0
    for zip_code in zip_codes:
        if redis_available():
            rset(f"storm_active:{zip_code}", "1", ttl_seconds=STORM_ACTIVE_TTL)

    # Find subscribers with locked territories in affected ZIPs
    if not zip_codes:
        return 0

    territories = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code.in_(zip_codes),
            ZipTerritory.status == "locked",
        )
    ).scalars().all()

    sub_ids = list({t.subscriber_id for t in territories if t.subscriber_id})
    if not sub_ids:
        return 0

    subscribers = db.execute(
        select(Subscriber).where(Subscriber.id.in_(sub_ids))
    ).scalars().all()

    for sub in subscribers:
        sub_zips = [t.zip_code for t in territories if t.subscriber_id == sub.id]
        zip_list = ", ".join(sub_zips[:3])
        msg = (
            f"STORM ALERT: High-value roofing & restoration leads now available in {zip_list}. "
            f"Reply BOOST to unlock your Storm Pack."
        )
        phone = getattr(sub, "phone", None)
        if phone and can_send(phone, db):
            send_sms(phone, msg, db, subscriber_id=sub.id, task_type="sms_copy")
            notified += 1

    logger.info("Storm packs activated for %d ZIPs, notified %d subscribers", len(zip_codes), notified)
    return notified


def queue_storm_scraper(zip_codes: list[str]) -> None:
    logger.info("Storm scraper queued for ZIPs: %s (full impl in 2B-2)", zip_codes)


def deactivate(zip_code: str) -> None:
    from src.core.redis_client import rdelete
    rdelete(f"storm_active:{zip_code}")


def _extract_zip_codes(properties: dict) -> list[str]:
    zip_codes = []
    # Try geocode field (FIPS codes → convert to ZIP is out of scope here)
    geocode = properties.get("geocode", {})
    same = geocode.get("SAME", [])
    ugc = geocode.get("UGC", [])

    # Look for explicit zip codes in affected area descriptions
    area_desc = properties.get("areaDesc", "")
    import re
    found = re.findall(r"\b\d{5}\b", area_desc)
    zip_codes.extend(found)

    # Fallback: check parameters for zip list
    params = properties.get("parameters", {})
    if "affectedZips" in params:
        zips = params["affectedZips"]
        if isinstance(zips, list):
            zip_codes.extend(zips)

    return list(set(zip_codes))
