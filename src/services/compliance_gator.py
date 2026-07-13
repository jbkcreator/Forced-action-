"""
Unified outbound compliance gate — SMS and voice.

Call validate_outbound() before every automated outbound dispatch.

DNC: Tracerfy monthly batch writes flagged phones to sms_opt_outs
     (source='tracerfy_dnc_refresh'). sms_opt_outs is the universal
     enforcement table for all channels — not SMS-specific despite the name.
IVR opt-out: record_ivr_opt_out() writes to sms_opt_outs with
             source='synthflow_ivr_optout', blocking future calls.
Timezone: derived from ZIP centroid (FL ZIPs); area-code fallback for unknown.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

# Area code → IANA timezone. Mirrors sms_compliance._AREA_CODE_TZ.
# 850 (panhandle) maps to CST — conservative (over-suppresses, avoids TCPA violation).
_AREA_CODE_TZ: dict[str, str] = {
    "850": "America/Chicago",
    **{ac: "America/New_York" for ac in [
        "239", "305", "321", "352", "386", "407", "561", "727",
        "754", "772", "786", "813", "863", "904", "941", "954",
    ]},
}


@dataclass
class ComplianceResult:
    allowed: bool
    reason: Optional[str] = None


def validate_outbound(
    phone: str,
    channel: str,               # 'sms' | 'voice'
    db: Session,
    zip_code: Optional[str] = None,
) -> ComplianceResult:
    """
    Pre-dispatch compliance check for SMS and voice channels.

    Gate order:
      1. sms_opt_outs — DNC (Tracerfy) + inbound STOP + IVR opt-out for all channels
      2. Timezone — 8am–9pm local time (legal requirement, always enforced)
    """
    normalized = normalize_phone(phone)
    if not normalized:
        return ComplianceResult(allowed=False, reason="invalid_phone")

    # 1. Universal DNC / opt-out check (covers all channels)
    row = db.execute(
        text("SELECT 1 FROM sms_opt_outs WHERE phone = :phone LIMIT 1"),
        {"phone": normalized},
    ).fetchone()
    if row:
        return ComplianceResult(allowed=False, reason="dnc_or_opted_out")

    # 2. Universal DNC freshness check. Positive checks should already be in
    # sms_opt_outs, but we still block if the latest result is positive, absent,
    # or stale. Owner.phone_metadata is a homeowner-only fallback for legacy rows.
    settings = get_settings()
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.dnc_recheck_days)
    dnc_status = _latest_dnc_status(db, normalized)
    if dnc_status is None:
        dnc_status = _owner_metadata_dnc_status(db, normalized)

    if dnc_status is None:
        return ComplianceResult(allowed=False, reason="dnc_check_required")
    if dnc_status["national_dnc"] or dnc_status["litigator"]:
        return ComplianceResult(allowed=False, reason="dnc_or_opted_out")
    checked_at = dnc_status.get("checked_at")
    if checked_at is None or checked_at < cutoff:
        return ComplianceResult(allowed=False, reason="dnc_check_required")

    # 3. Quiet hours - 8am to 9pm local time (TCPA requirement)
    tz = _resolve_timezone(normalized, zip_code)
    hour = datetime.now(tz).hour
    if hour < 8 or hour >= 21:
        return ComplianceResult(allowed=False, reason="quiet_hours")

    return ComplianceResult(allowed=True)


def _latest_dnc_status(db: Session, phone: str) -> Optional[dict]:
    row = db.execute(
        text("""
            SELECT national_dnc, litigator, checked_at
            FROM dnc_phone_checks
            WHERE phone = :phone
            LIMIT 1
        """),
        {"phone": phone},
    ).fetchone()
    if not row:
        return None
    mapping = getattr(row, "_mapping", row)
    return {
        "national_dnc": bool(mapping["national_dnc"]),
        "litigator": bool(mapping["litigator"]),
        "checked_at": _as_aware_utc(mapping["checked_at"]),
    }


def _owner_metadata_dnc_status(db: Session, phone: str) -> Optional[dict]:
    row = db.execute(
        text("""
            SELECT
                (o.phone_metadata->'phone_1'->>'dnc')::boolean AS national_dnc,
                (o.phone_metadata->'phone_1'->>'litigator')::boolean AS litigator,
                (o.phone_metadata->'phone_1'->>'dnc_checked_at')::timestamptz AS checked_at
            FROM owners o
            WHERE o.phone_1 = :phone
              AND o.phone_metadata->'phone_1'->>'dnc_checked_at' IS NOT NULL
            ORDER BY o.id
            LIMIT 1
        """),
        {"phone": phone},
    ).fetchone()
    if not row:
        return None
    mapping = getattr(row, "_mapping", row)
    return {
        "national_dnc": bool(mapping["national_dnc"]),
        "litigator": bool(mapping["litigator"]),
        "checked_at": _as_aware_utc(mapping["checked_at"]),
    }


def _as_aware_utc(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def has_voice_consent(subscriber_id: int, db: Session) -> bool:
    """
    B0-06: PEWC gate for automated AI voice calls (47 CFR 64.1200(f)(9) +
    FCC Feb-2024 AI-voice ruling — see docs/adr/0030). Distinct from the
    generic marketing consent; a subscriber without a stored voice_consent_at
    row must never receive an automated call.
    """
    try:
        row = db.execute(
            text("""
                SELECT 1 FROM consent_acceptances
                WHERE subscriber_id = :sid AND voice_consent_at IS NOT NULL
                LIMIT 1
            """),
            {"sid": subscriber_id},
        ).fetchone()
    except Exception:
        # Fail closed — a lookup error must never let an unconsented call fire.
        logger.error(
            "has_voice_consent lookup failed for subscriber=%s — blocking call",
            subscriber_id, exc_info=True,
        )
        return False
    return row is not None


def record_ivr_opt_out(phone: str, db: Session) -> None:
    """
    Write an IVR opt-out to sms_opt_outs.
    Called when a DBPR contact opts out during a Synthflow call.
    Delegates to sms_compliance.record_opt_out() to reuse existing logic:
    idempotency check, revenue signal score update, proper audit trail.
    Local import avoids circular dependency (sms_compliance imports compliance_gator).
    """
    normalized = normalize_phone(phone)
    if not normalized:
        return
    from src.services.sms_compliance import record_opt_out  # local import — avoids circular
    record_opt_out(normalized, keyword="OPT_OUT", source="synthflow_ivr_optout", db=db)


def _resolve_timezone(phone: str, zip_code: Optional[str]) -> ZoneInfo:
    """Derive recipient timezone. ZIP centroid first, area-code fallback."""
    if zip_code:
        from src.utils.zip_centroids import get_zip_centroid
        # Try both FL counties — all FL ZIPs are Eastern time
        centroid = get_zip_centroid(zip_code, "hillsborough") or get_zip_centroid(zip_code, "pinellas")
        if centroid:
            return ZoneInfo("America/New_York")
    # Fall back to phone area code
    digits = "".join(c for c in phone if c.isdigit())
    if digits.startswith("1"):
        digits = digits[1:]
    return ZoneInfo(_AREA_CODE_TZ.get(digits[:3], "America/New_York"))
