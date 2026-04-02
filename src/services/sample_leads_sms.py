"""
Sample leads SMS service.

When a prospect says YES to sample leads during an outbound call, the Synthflow
agent tags them sample_leads_requested in GHL. A GHL workflow fires a webhook
to POST /webhooks/ghl/sample-leads, which calls this service to:
  1. Query top 3 Gold+ leads for the prospect's ZIP and vertical.
  2. Format them as an SMS-friendly message.
  3. Send the SMS via GHL conversations API.

Owner phone numbers are intentionally withheld — prospects must subscribe to unlock.
"""

import logging
from typing import Optional

from sqlalchemy import select, desc, and_

from src.core.database import get_db_context
from src.core.models import DistressScore, Owner, Property
from config.settings import get_settings
from src.services.ghl_webhook import _ghl_request, _headers, _is_configured, _GHL_BASE

logger = logging.getLogger(__name__)

GOLD_PLUS_TIERS = {"Ultra Platinum", "Platinum", "Gold"}

_SIGNAL_LABELS = {
    "foreclosure":        "Foreclosure",
    "tax_lien":           "Tax Lien",
    "tax_delinquency":    "Tax Delinquency",
    "code_violation":     "Code Violation",
    "lis_pendens":        "Lis Pendens",
    "lien":               "Lien",
    "mechanic_lien":      "Mechanic's Lien",
    "hoa_lien":           "HOA Lien",
    "judgment":           "Judgment",
    "eviction":           "Eviction",
    "probate":            "Probate",
    "bankruptcy":         "Bankruptcy",
    "flood_damage":       "Flood Damage",
    "fire_incident":      "Fire Incident",
    "storm_damage":       "Storm Damage",
    "insurance_claim":    "Insurance Claim",
    "enforcement_permit": "Enforcement Permit",
}

_VERTICAL_LABELS = {
    "roofing":          "Roofing",
    "restoration":      "Restoration",
    "wholesalers":      "Wholesale",
    "fix_flip":         "Fix & Flip",
    "public_adjusters": "Public Adjusters",
    "attorneys":        "Attorneys",
}


def get_sample_leads(zip_code: str, vertical: str, count: int = 3) -> list[dict]:
    """
    Return top `count` Gold+ leads for a ZIP and vertical.
    Owner phone is intentionally excluded — prospects must subscribe to unlock.
    """
    with get_db_context() as db:
        try:
            score_col = DistressScore.vertical_scores[vertical].as_float()
        except KeyError:
            logger.error("[SampleLeads] Unknown vertical '%s'", vertical)
            return []

        rows = db.execute(
            select(Property, DistressScore)
            .join(DistressScore, DistressScore.property_id == Property.id)
            .where(and_(
                Property.zip == zip_code,
                DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
                DistressScore.qualified == True,
                score_col > 0,
            ))
            .order_by(desc(score_col))
            .limit(count)
        ).all()

        leads = []
        for prop, score in rows:
            signals = score.distress_types or []
            signal_labels = [
                _SIGNAL_LABELS.get(s, s.replace("_", " ").title())
                for s in signals[:2]  # max 2 signals per lead in SMS
            ]
            leads.append({
                "address":        prop.address or "Address unavailable",
                "city":           prop.city or "Tampa",
                "zip":            prop.zip or zip_code,
                "signals":        signal_labels,
                "vertical_score": int(score.vertical_scores.get(vertical, 0)) if score.vertical_scores else 0,
                "lead_tier":      score.lead_tier or "Gold",
            })

    return leads


def format_sms_body(leads: list[dict], zip_code: str, vertical: str) -> str:
    """
    Format top leads as an SMS message.
    Withholds owner contact — subscriber unlock required.
    """
    vertical_label = _VERTICAL_LABELS.get(vertical, vertical.title())

    if not leads:
        return (
            f"Forced Action — No {vertical_label} leads found in {zip_code} right now. "
            f"New leads arrive daily. Check forcedactionleads.com to subscribe.\n\n"
            f"Reply STOP to opt out."
        )

    lines = [f"Forced Action — {vertical_label} leads in {zip_code}:\n"]
    for i, lead in enumerate(leads, 1):
        signal_str = " + ".join(lead["signals"]) if lead["signals"] else "Distressed Property"
        lines.append(f"{i}. {lead['address']} — {signal_str}")

    lines.append(
        f"\nOwner contact unlocked with subscription.\n"
        f"forcedactionleads.com\n\n"
        f"Reply STOP to opt out."
    )

    return "\n".join(lines)


def send_ghl_sms(contact_id: str, message: str) -> bool:
    """
    Send an outbound SMS to a GHL contact via the conversations API.
    Returns True on success.
    """
    if not _is_configured():
        logger.warning("[SampleLeads] GHL not configured — cannot send SMS")
        return False

    try:
        settings = get_settings()
        resp = _ghl_request(
            "POST",
            f"{_GHL_BASE}/conversations/messages",
            headers=_headers(),
            json={
                "type":       "SMS",
                "contactId":  contact_id,
                "locationId": settings.ghl_location_id,
                "message":    message,
            },
        )
        if not resp.ok:
            logger.warning(
                "[SampleLeads] GHL SMS failed HTTP %d for contact %s: %s",
                resp.status_code, contact_id, resp.text[:500],
            )
            return False

        logger.info("[SampleLeads] SMS sent to contact %s", contact_id)
        return True

    except Exception as exc:
        logger.error("[SampleLeads] SMS send error for contact %s: %s", contact_id, exc)
        return False


def send_sample_leads(
    contact_id: str,
    zip_code: str,
    vertical: str,
    prospect_name: str = "",
) -> dict:
    """
    Full pipeline: query leads → format SMS → send via GHL.

    Returns:
        {"sent": bool, "lead_count": int, "contact_id": str}
    """
    leads = get_sample_leads(zip_code=zip_code, vertical=vertical)
    message = format_sms_body(leads, zip_code=zip_code, vertical=vertical)

    sent = send_ghl_sms(contact_id=contact_id, message=message)

    logger.info(
        "[SampleLeads] contact=%s zip=%s vertical=%s leads=%d sent=%s",
        contact_id, zip_code, vertical, len(leads), sent,
    )

    return {
        "sent":       sent,
        "lead_count": len(leads),
        "contact_id": contact_id,
    }
