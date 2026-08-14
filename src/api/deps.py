"""
Shared FastAPI dependencies and primitives for src/api/ routers.

Import from here instead of defining in main.py or individual routers:
    from src.api.deps import (
        get_db,
        VALID_TIERS, VALID_VERTICALS,
        ZIP_RE, FLORIDA_PREFIXES,
        ConsentAcceptanceRequest,
        resolve_phone_with_quality,
        estimate_lead_job_value,
        visible_tier_fields,
    )
"""

import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from config.scoring import VERTICAL_WEIGHTS, for_county
from src.core.database import get_db_context


# ---------------------------------------------------------------------------
# DB dependency
# ---------------------------------------------------------------------------

def get_db():
    with get_db_context() as db:
        yield db


# ---------------------------------------------------------------------------
# Shared query-param parsing
# Used by: revenue metrics, funnel analytics, and other from/to date-range routes
# ---------------------------------------------------------------------------

def parse_iso_date_param(value: Optional[str], field: str) -> Optional[datetime]:
    """Parse an optional ISO date/datetime query param into a tz-aware datetime.

    Raises HTTPException(400) if `value` is non-empty but not valid ISO 8601.
    Naive datetimes are assumed UTC.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field} date: {value!r}")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------

VALID_TIERS: frozenset[str] = frozenset({"starter", "pro", "annual_lock", "founder"})
VALID_VERTICALS: frozenset[str] = frozenset(VERTICAL_WEIGHTS.keys())
SIGNUP_VERTICALS: frozenset[str] = VALID_VERTICALS | frozenset({"investor"})

ZIP_RE = re.compile(r"^\d{5}$")
FLORIDA_PREFIXES = ("33", "34")


# ---------------------------------------------------------------------------
# Shared Pydantic schema
# Used by: checkout, waitlist, free-signup
# ---------------------------------------------------------------------------

class ConsentAcceptanceRequest(BaseModel):
    """Incoming consent acceptance payload from the frontend TermsConsentGate."""
    terms_accepted: bool = Field(..., alias="terms_accepted")
    terms_version: Optional[str] = None
    privacy_version: Optional[str] = None
    accepted_text_hash: Optional[str] = None
    modal_opened_at: Optional[str] = None
    modal_scrolled_to_end_at: Optional[str] = None
    tcpa_accepted: Optional[bool] = Field(default=False, alias="tcpa_accepted")
    tcpa_consent_text: Optional[str] = None
    tcpa_consent_version: Optional[str] = None
    # B0-06: PEWC voice-call consent — distinct from tcpa_accepted (marketing).
    # Never a condition of purchase; defaults unchecked (47 CFR 64.1200(f)(9)).
    voice_consent_accepted: Optional[bool] = Field(default=False, alias="voice_consent_accepted")
    voice_consent_text: Optional[str] = None
    voice_consent_version: Optional[str] = None
    user_agent: Optional[str] = None


# B0-06: server-owned PEWC voice-call disclosure text, keyed by version. The
# client only asserts which version it displayed — never trust client-supplied
# disclosure text, or the audit record can't prove what was actually disclosed.
VOICE_CONSENT_DISCLOSURES: dict[str, str] = {
    "2026.06": (
        "By checking this box, you agree that ForcedAction and its partners may "
        "contact you using an automated telephone dialing system and/or "
        "artificial or prerecorded voice at the phone number provided, "
        "including calls placed by an AI voice assistant, to discuss your "
        "account and available leads. Consent is not required to purchase. "
        "Message/data rates may apply."
    ),
    "2026.07": (
        "I agree to receive recurring automated marketing calls and text messages, "
        "including calls that use an automated or AI-generated voice, from Forced Action "
        "at the phone number provided. Consent is not a condition of purchase. "
        "Msg & data rates may apply. Reply STOP to opt out."
    ),
}


def resolve_voice_consent(consent: Optional[ConsentAcceptanceRequest]) -> Optional[tuple[str, str]]:
    """Validate a client's voice-consent claim against the server-owned registry.

    Returns (disclosure_text, version) if the client asserted acceptance of a
    recognized version, else None. Callers must treat None as "no voice
    consent" — never a reason to block the surrounding signup/checkout flow.
    """
    if not consent or not consent.voice_consent_accepted:
        return None
    version = consent.voice_consent_version
    text = VOICE_CONSENT_DISCLOSURES.get(version) if version else None
    if not text:
        return None
    return text, version


# ---------------------------------------------------------------------------
# Lead display helpers
# Used by: feed, sample-leads, lead-pack detail
# ---------------------------------------------------------------------------

def resolve_phone_with_quality(owner) -> tuple[Optional[str], Optional[dict]]:
    """
    Pick the best phone number to display for an owner and return its
    skip-trace metadata alongside it.

    Iterates phone_1 → phone_2 → phone_3, picking the first non-empty number.
    Returns (number, metadata) where metadata is the matching slot from
    owner.phone_metadata if present, else None.
    """
    if not owner:
        return (None, None)
    meta_map = owner.phone_metadata or {}
    for slot in ("phone_1", "phone_2", "phone_3"):
        number = getattr(owner, slot, None)
        if number:
            return (number, meta_map.get(slot))
    return (None, None)


def estimate_lead_job_value(prop, score) -> dict:
    """Compute job value estimate for a feed lead."""
    try:
        from src.services.job_estimator import estimate_job_value
        distress_types = score.distress_types or []
        return estimate_job_value(prop, distress_types)
    except Exception:
        return {"low": 0, "high": 0, "display": "N/A", "method": "error"}


def visible_tier_fields(prop, score) -> tuple:
    """Return (lead_tier, urgency) honoring the county's tier_visibility flag.

    Counties set to tier_visibility='internal' return (None, None) so
    subscribers don't see misleading labels while calibration is in flight.
    """
    try:
        cfg = for_county(getattr(prop, "county_id", None))
        if cfg.tier_visibility == "internal":
            return (None, None)
    except Exception:
        pass
    return (score.lead_tier, score.urgency_level)
