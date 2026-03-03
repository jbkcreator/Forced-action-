"""
GoHighLevel (GHL) CRM integration.

Fires when a lead scores >= ROUTING_THRESHOLDS["daily"] (60) after scoring.
Creates or updates a GHL Contact, then upserts an Opportunity in the
configured pipeline with the urgency-mapped stage.

Configuration (all via environment variables, see config/settings.py):
    GHL_API_KEY           — GHL private API key (v2)
    GHL_LOCATION_ID       — Location/sub-account ID
    GHL_PIPELINE_ID       — Pipeline ID for distressed-property leads
    GHL_STAGE_IMMEDIATE   — Stage ID for Immediate (SMS) tier (score ≥ 80)
    GHL_STAGE_HIGH        — Stage ID for High (daily email) tier (score 60–79)
    GHL_STAGE_MEDIUM      — Stage ID for Medium (weekly digest) tier (score 40–59)

If GHL_API_KEY or GHL_LOCATION_ID is not set the module is a no-op —
no exception is raised, a debug log line is emitted instead.

GHL API v2 reference:
    https://highlevel.stoplight.io/docs/integrations/
"""

import logging
from typing import Dict, List, Optional

import requests

from config.settings import settings
from config.scoring import ROUTING_THRESHOLDS

logger = logging.getLogger(__name__)

_GHL_BASE = "https://services.leadconnectorhq.com"
_GHL_HEADERS_BASE = {
    "Version": "2021-07-28",
    "Content-Type": "application/json",
}

# Minimum score to push a lead to GHL (anything below this is Low — not routed)
_MIN_SCORE_TO_PUSH = ROUTING_THRESHOLDS["daily"]  # 60


def _is_configured() -> bool:
    """Return True only if the minimum required GHL env vars are present."""
    return bool(
        settings.ghl_api_key is not None
        and settings.ghl_location_id
    )


def _headers() -> Dict[str, str]:
    return {
        **_GHL_BASE_HEADERS,
        "Authorization": f"Bearer {settings.ghl_api_key.get_secret_value()}",
    }


# Fix forward reference — define after _GHL_HEADERS_BASE
_GHL_BASE_HEADERS = _GHL_HEADERS_BASE


def _stage_id_for_urgency(urgency: str) -> Optional[str]:
    """Map urgency level to configured GHL pipeline stage ID."""
    mapping = {
        "Immediate": settings.ghl_stage_immediate,
        "High":      settings.ghl_stage_high,
        "Medium":    settings.ghl_stage_medium,
    }
    return mapping.get(urgency)


def _top_signals(score_data: Dict, n: int = 2) -> List[str]:
    """Return the top N signal types by vertical weight contribution."""
    return score_data.get("distress_types", [])[:n]


def _best_vertical(score_data: Dict) -> str:
    """Return the vertical name with the highest score."""
    vs = score_data.get("vertical_scores", {})
    if not vs:
        return "unknown"
    return max(vs, key=vs.get)


def _find_contact_by_phone(phone: str) -> Optional[str]:
    """
    Search GHL for an existing contact by phone number.
    Returns the contact ID if found, None otherwise.
    """
    try:
        resp = requests.get(
            f"{_GHL_BASE}/contacts/search",
            headers=_headers(),
            params={
                "locationId": settings.ghl_location_id,
                "query": phone,
            },
            timeout=10,
        )
        resp.raise_for_status()
        contacts = resp.json().get("contacts", [])
        if contacts:
            return contacts[0]["id"]
    except Exception as e:
        logger.debug(f"[GHL] contact search failed: {e}")
    return None


def _upsert_contact(score_data: Dict) -> Optional[str]:
    """
    Create or update a GHL contact from score_data.
    Returns the GHL contact ID, or None on failure.
    """
    phone = score_data.get("owner_phone")
    email = score_data.get("owner_email")
    owner_name = score_data.get("owner_name") or ""
    address = score_data.get("address") or ""

    name_parts = owner_name.strip().split(None, 1)
    first_name = name_parts[0] if name_parts else "Unknown"
    last_name = name_parts[1] if len(name_parts) > 1 else "Owner"

    payload = {
        "locationId":  settings.ghl_location_id,
        "firstName":   first_name,
        "lastName":    last_name,
        "address1":    address,
        "customFields": [
            {"id": "parcel_id",       "value": str(score_data.get("parcel_id") or "")},
            {"id": "cds_score",       "value": str(score_data.get("final_cds_score") or 0)},
            {"id": "lead_tier",       "value": score_data.get("lead_tier") or ""},
            {"id": "urgency_level",   "value": score_data.get("urgency_level") or ""},
            {"id": "best_vertical",   "value": _best_vertical(score_data)},
            {"id": "top_signals",     "value": ", ".join(_top_signals(score_data))},
            {"id": "signal_count",    "value": str(score_data.get("signal_count") or 0)},
        ],
        "tags": [
            f"cds-{score_data.get('lead_tier', '').lower().replace(' ', '-')}",
            f"vertical-{_best_vertical(score_data)}",
            "distressed-property",
        ],
    }
    if phone:
        payload["phone"] = phone
    if email:
        payload["email"] = email

    # Check if contact already exists
    existing_id = _find_contact_by_phone(phone) if phone else None

    try:
        if existing_id:
            resp = requests.put(
                f"{_GHL_BASE}/contacts/{existing_id}",
                headers=_headers(),
                json=payload,
                timeout=10,
            )
        else:
            resp = requests.post(
                f"{_GHL_BASE}/contacts/",
                headers=_headers(),
                json=payload,
                timeout=10,
            )
        resp.raise_for_status()
        contact = resp.json().get("contact", {})
        return contact.get("id") or existing_id
    except Exception as e:
        logger.warning(f"[GHL] contact upsert failed for {score_data.get('parcel_id')}: {e}")
        return None


def _upsert_opportunity(contact_id: str, score_data: Dict) -> bool:
    """
    Create a GHL pipeline opportunity linked to the contact.
    Returns True on success.
    """
    if not settings.ghl_pipeline_id:
        logger.debug("[GHL] GHL_PIPELINE_ID not set — skipping opportunity creation")
        return False

    stage_id = _stage_id_for_urgency(score_data.get("urgency_level", ""))
    if not stage_id:
        logger.debug(
            f"[GHL] No stage configured for urgency '{score_data.get('urgency_level')}'"
            " — skipping opportunity"
        )
        return False

    parcel_id = score_data.get("parcel_id") or score_data.get("property_id")
    best_v = _best_vertical(score_data)
    title = (
        f"{score_data.get('address', 'Unknown Address')} "
        f"| {score_data.get('lead_tier')} "
        f"| {best_v.title()}"
    )

    payload = {
        "pipelineId":  settings.ghl_pipeline_id,
        "locationId":  settings.ghl_location_id,
        "name":        title,
        "pipelineStageId": stage_id,
        "contactId":   contact_id,
        "monetaryValue": score_data.get("final_cds_score") or 0,
        "customFields": [
            {"id": "parcel_id",     "value": str(parcel_id or "")},
            {"id": "cds_score",     "value": str(score_data.get("final_cds_score") or 0)},
            {"id": "top_signals",   "value": ", ".join(_top_signals(score_data))},
            {"id": "signal_count",  "value": str(score_data.get("signal_count") or 0)},
        ],
    }

    try:
        resp = requests.post(
            f"{_GHL_BASE}/opportunities/",
            headers=_headers(),
            json=payload,
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.warning(f"[GHL] opportunity creation failed for {parcel_id}: {e}")
        return False


def push_lead_to_ghl(score_data: Dict) -> bool:
    """
    Push a scored lead to GHL CRM.

    Called automatically from cds_engine.save_score_to_database() for every
    lead that scores at or above the daily routing threshold (60).

    Args:
        score_data: The dict returned by MultiVerticalScorer.score_property().
                    Must contain: property_id, parcel_id, address, owner_name,
                    final_cds_score, lead_tier, urgency_level, vertical_scores,
                    distress_types, signal_count.
                    Optional: owner_phone, owner_email (enriched from Owner model).

    Returns:
        True if both contact upsert and opportunity creation succeeded.
        False (silently) if GHL is not configured or any step fails.
    """
    if not _is_configured():
        logger.debug("[GHL] Not configured — skipping CRM push")
        return False

    final_score = score_data.get("final_cds_score", 0)
    if final_score < _MIN_SCORE_TO_PUSH:
        return False

    parcel_id = score_data.get("parcel_id")
    logger.info(
        f"[GHL] Pushing lead: {parcel_id} | score={final_score} "
        f"| {score_data.get('lead_tier')} | {score_data.get('urgency_level')}"
    )

    contact_id = _upsert_contact(score_data)
    if not contact_id:
        logger.warning(f"[GHL] Failed to upsert contact for {parcel_id} — opportunity skipped")
        return False

    ok = _upsert_opportunity(contact_id, score_data)
    if ok:
        logger.info(f"[GHL] Lead pushed successfully: {parcel_id}")
    return ok
