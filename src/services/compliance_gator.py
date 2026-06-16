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

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.phone_utils import normalize as normalize_phone

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

    # 2. Quiet hours — 8am to 9pm local time (TCPA requirement)
    tz = _resolve_timezone(normalized, zip_code)
    hour = datetime.now(tz).hour
    if hour < 8 or hour >= 21:
        return ComplianceResult(allowed=False, reason="quiet_hours")

    return ComplianceResult(allowed=True)


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
