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
from typing import Optional

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
# Validation constants
# ---------------------------------------------------------------------------

VALID_TIERS: frozenset[str] = frozenset({"starter", "pro", "dominator"})
VALID_VERTICALS: frozenset[str] = frozenset(VERTICAL_WEIGHTS.keys())

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
    user_agent: Optional[str] = None


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
