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
    """Resolve a CAP alert payload to the affected ZIP set.

    Three ingest paths:
      1. `geocode.SAME` / `geocode.UGC` — FIPS / UGC codes. Expanded via the
         static crosswalk in `nws_same_to_zip` for our active counties.
         (fa008+, 2026-05-04 — previously ignored.)
      2. `areaDesc` — regex for any 5-digit run.
      3. `parameters.affectedZips` — explicit ZIP list when an upstream relay
         pre-computes it.
    """
    zip_codes: list[str] = []

    # 1. SAME / UGC code expansion
    geocode = properties.get("geocode", {}) or {}
    same = geocode.get("SAME", []) or []
    ugc = geocode.get("UGC", []) or []
    if same or ugc:
        from src.services.nws_same_to_zip import expand_codes
        zip_codes.extend(expand_codes(same, ugc))

    # 2. Explicit ZIPs in the prose
    area_desc = properties.get("areaDesc", "") or ""
    import re
    zip_codes.extend(re.findall(r"\b\d{5}\b", area_desc))

    # 3. Pre-computed affected-ZIP list from a relay
    params = properties.get("parameters", {}) or {}
    explicit = params.get("affectedZips")
    if isinstance(explicit, list):
        zip_codes.extend(str(z) for z in explicit)

    return sorted(set(zip_codes))
