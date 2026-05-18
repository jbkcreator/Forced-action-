"""
NWS webhook processor — single source of truth for all NWS CAP alert handling.

Handles alerts arriving via:
  - POST /webhooks/nws/alert  (inbound push from a relay)
  - src/tasks/nws_poll.py     (autonomous polling every 5 minutes)

Responsibilities (in order):
  1. ID extraction + idempotency check against nws_alerts table
  2. Qualifying event filter (settings.nws_relevant_events)
  3. ZIP resolution via SAME/UGC crosswalk + areaDesc regex + explicit relay list
  4. NWSAlert row insert (raw_payload preserved for audit)
  5. Redis storm_active:{zip} flags (72-hour TTL)
  6. Subscriber notification SMS (gated on settings.storm_pack_enabled)
  7. Internal event logging: NWS_ALERT_RECEIVED, NWS_ALERT_MATCHED_ZIPS,
     STORM_PACK_ELIGIBLE, STORM_PACK_OFFER_SENT
  8. NWSAlert row update: storm_pack_triggered, subscriber_count

Note: STORM_PACK_PURCHASED is logged in the Stripe checkout.session.completed
handler only — never from here.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import get_settings

logger = logging.getLogger(__name__)

STORM_ACTIVE_TTL = 72 * 3600  # 72 hours in seconds


def process_alert(alert_payload: dict, db: Session) -> dict:
    """
    Canonical entry point for all NWS CAP alert processing.

    alert_payload should be the CAP `properties` dict with the top-level
    feature `id` merged in as `alert_payload["id"]` by the caller.

    Returns a dict with keys: status, alert_id, affected_zips, subscriber_count.
    status values: "processed" | "duplicate" | "skipped" | "error"
    """
    settings = get_settings()

    if not settings.nws_weather_enabled:
        logger.info("[NWS] nws_weather_enabled=False — skipping alert")
        return {"status": "skipped", "reason": "feature_disabled"}

    # ── 1. ID extraction ──────────────────────────────────────────────────────
    alert_id = (
        alert_payload.get("id")
        or alert_payload.get("@id")
        or alert_payload.get("identifier")
    )
    if not alert_id:
        logger.warning("[NWS] Alert has no id field — cannot process idempotently")
        return {"status": "error", "reason": "missing_alert_id"}

    # ── 2. Idempotency check ──────────────────────────────────────────────────
    from src.core.models import NWSAlert
    existing = db.execute(
        select(NWSAlert).where(NWSAlert.alert_id == alert_id)
    ).scalar_one_or_none()

    if existing:
        logger.debug("[NWS] Duplicate alert %s — already processed", alert_id)
        return {
            "status": "duplicate",
            "alert_id": alert_id,
            "affected_zips": existing.affected_zips or [],
            "subscriber_count": existing.subscriber_count,
        }

    # ── 3. Qualifying event check ─────────────────────────────────────────────
    properties = alert_payload.get("properties", alert_payload)
    event_type = properties.get("event", "")

    if not _is_qualifying(event_type, settings.nws_relevant_events):
        logger.info("[NWS] Non-qualifying event %r — skipping", event_type)
        return {"status": "skipped", "reason": "non_qualifying_event", "event": event_type}

    logger.info("[NWS] Qualifying event: %s (alert_id=%s)", event_type, alert_id[:60])

    # ── 4. ZIP resolution ─────────────────────────────────────────────────────
    affected_zips = _extract_zip_codes(properties)
    logger.info("[NWS] Resolved %d ZIPs for alert %s", len(affected_zips), alert_id[:40])

    # ── 5. Parse time fields ──────────────────────────────────────────────────
    def _parse_dt(key: str) -> Optional[datetime]:
        val = properties.get(key)
        if not val:
            return None
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None

    geocode = properties.get("geocode", {}) or {}

    # ── 6. Insert NWSAlert row ────────────────────────────────────────────────
    nws_alert = NWSAlert(
        alert_id=alert_id,
        event=event_type,
        severity=properties.get("severity"),
        urgency=properties.get("urgency"),
        certainty=properties.get("certainty"),
        headline=properties.get("headline"),
        description=properties.get("description"),
        instruction=properties.get("instruction"),
        area_desc=properties.get("areaDesc"),
        same_codes=geocode.get("SAME") or [],
        ugc_codes=geocode.get("UGC") or [],
        affected_zips=affected_zips,
        effective=_parse_dt("effective"),
        onset=_parse_dt("onset"),
        expires=_parse_dt("expires"),
        ends=_parse_dt("ends"),
        raw_payload=alert_payload,
    )
    db.add(nws_alert)
    db.flush()  # get ID without committing — lets us update it after activation

    # ── 7. Log NWS_ALERT_RECEIVED ─────────────────────────────────────────────
    _log_event(db, "NWS_ALERT_RECEIVED", {
        "alert_id": alert_id,
        "event": event_type,
        "severity": properties.get("severity"),
        "area_desc": properties.get("areaDesc", "")[:200],
    })

    if affected_zips:
        _log_event(db, "NWS_ALERT_MATCHED_ZIPS", {
            "alert_id": alert_id,
            "zip_count": len(affected_zips),
            "sample_zips": affected_zips[:5],
        })

    # ── 8. Redis storm flags + subscriber notification ────────────────────────
    notified = 0
    if settings.storm_pack_enabled:
        notified = _activate_storm_packs(affected_zips, db, alert_id)
    else:
        logger.info("[NWS] storm_pack_enabled=False — skipping storm pack activation")

    # ── 9. Storm signal tagging (storm_damage incidents on distressed props) ──
    tagged_ids: list = []
    if settings.storm_signal_tagging_enabled and affected_zips:
        try:
            from src.services.storm_signal_tagger import tag_affected_properties
            tagged_ids = tag_affected_properties(
                affected_zips, alert_id, nws_alert.effective, db
            )
            _log_event(db, "STORM_SIGNAL_TAGGED", {
                "alert_id": alert_id,
                "tagged_count": len(tagged_ids),
                "zip_count": len(affected_zips),
            })
        except Exception:
            logger.exception(
                "[NWS] storm signal tagging failed for alert %s", alert_id[:40]
            )
            tagged_ids = []

    # ── 10. Update NWSAlert with activation results + commit ──────────────────
    nws_alert.storm_pack_triggered = notified > 0
    nws_alert.subscriber_count = notified
    db.commit()

    # ── 11. CDS rescore for tagged properties (post-commit, isolated) ─────────
    if tagged_ids and settings.storm_signal_rescore_enabled:
        try:
            from src.services.cds_engine import MultiVerticalScorer
            scorer = MultiVerticalScorer(db)
            scorer.score_properties_by_ids(tagged_ids, save_to_db=True)
            _log_event(db, "STORM_RESCORE_COMPLETE", {
                "alert_id": alert_id,
                "rescored_count": len(tagged_ids),
            })
        except Exception:
            logger.exception(
                "[NWS] storm rescore failed for alert %s (%d properties)",
                alert_id[:40], len(tagged_ids),
            )

    logger.info(
        "[NWS] Alert %s processed: %d ZIPs, %d subscribers notified, %d props tagged",
        alert_id[:40], len(affected_zips), notified, len(tagged_ids),
    )

    return {
        "status": "processed",
        "alert_id": alert_id,
        "affected_zips": affected_zips,
        "subscriber_count": notified,
        "tagged_count": len(tagged_ids),
        "event": event_type,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _is_qualifying(event_type: str, relevant_events: list) -> bool:
    """Check if event_type matches any entry in the configured relevant events list."""
    et_lower = event_type.lower()
    return any(rel.lower() in et_lower or et_lower in rel.lower() for rel in relevant_events)


def _extract_zip_codes(properties: dict) -> list:
    """Resolve a CAP alert payload to the affected ZIP set.

    Three ingest paths:
      1. geocode.SAME / geocode.UGC — FIPS/UGC codes expanded via static crosswalk
      2. areaDesc — regex for any 5-digit FL-range ZIP
      3. parameters.affectedZips — explicit ZIP list from an upstream relay
    """
    zip_codes: list = []

    geocode = properties.get("geocode", {}) or {}
    same = geocode.get("SAME", []) or []
    ugc = geocode.get("UGC", []) or []
    if same or ugc:
        from src.services.nws_same_to_zip import expand_codes
        zip_codes.extend(expand_codes(same, ugc))

    area_desc = properties.get("areaDesc", "") or ""
    zip_codes.extend(re.findall(r"\b\d{5}\b", area_desc))

    params = properties.get("parameters", {}) or {}
    explicit = params.get("affectedZips")
    if isinstance(explicit, list):
        zip_codes.extend(str(z) for z in explicit)

    return sorted(set(zip_codes))


def _activate_storm_packs(zip_codes: list, db: Session, alert_id: str) -> int:
    """Set Redis storm_active flags and send subscriber notification SMS.

    Returns the number of subscribers notified.
    """
    from src.core.models import Subscriber, ZipTerritory
    from src.core.redis_client import redis_available, rset
    from src.services.sms_compliance import can_send, send_sms

    for zip_code in zip_codes:
        if redis_available():
            rset(f"storm_active:{zip_code}", "1", ttl_seconds=STORM_ACTIVE_TTL)

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
        _log_event(db, "STORM_PACK_ELIGIBLE", {"alert_id": alert_id, "subscriber_count": 0})
        return 0

    _log_event(db, "STORM_PACK_ELIGIBLE", {
        "alert_id": alert_id,
        "subscriber_count": len(sub_ids),
        "zip_count": len(zip_codes),
    })

    subscribers = db.execute(
        select(Subscriber).where(Subscriber.id.in_(sub_ids))
    ).scalars().all()

    notified = 0
    for sub in subscribers:
        sub_zips = [t.zip_code for t in territories if t.subscriber_id == sub.id]
        zip_list = ", ".join(sub_zips[:3])
        msg = (
            f"STORM ALERT: High-value roofing & restoration leads now available in {zip_list}. "
            f"Reply BOOST to unlock your Storm Pack."
        )
        phone = getattr(sub, "phone", None)
        if phone and can_send(phone, db):
            send_sms(phone, msg, db, message_type="transactional", subscriber_id=sub.id, task_type="sms_copy")
            notified += 1
            _log_event(db, "STORM_PACK_OFFER_SENT", {
                "alert_id": alert_id,
                "subscriber_id": sub.id,
                "zip": sub_zips[0] if sub_zips else None,
            })

    logger.info(
        "[NWS] Storm packs: %d ZIPs flagged, %d subscribers notified",
        len(zip_codes), notified,
    )
    return notified


def _log_event(db: Session, event_type: str, payload: dict) -> None:
    """Log an internal business event via the webhook_events table."""
    try:
        from src.services.webhook_log import log_webhook_event
        log_webhook_event(
            source="nws",
            event_type=event_type,
            direction="inbound",
            status="processed",
            payload=payload,
            payload_kind="nws",
        )
    except Exception as exc:
        logger.warning("[NWS] Failed to log event %s: %s", event_type, exc)


# ──────────────────────────────────────────────────────────────────────────────
# Legacy helpers kept for backward compatibility with existing call sites
# ──────────────────────────────────────────────────────────────────────────────

def is_qualifying(event_type: str) -> bool:
    """Backward-compat wrapper — prefers settings.nws_relevant_events."""
    try:
        return _is_qualifying(event_type, get_settings().nws_relevant_events)
    except Exception:
        QUALIFYING_EVENTS = {"Hurricane", "Tropical Storm", "Severe Thunderstorm", "Tornado", "Hail"}
        return any(q.lower() in event_type.lower() for q in QUALIFYING_EVENTS)


def deactivate(zip_code: str) -> None:
    from src.core.redis_client import rdelete
    rdelete(f"storm_active:{zip_code}")
