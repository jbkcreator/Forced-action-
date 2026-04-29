"""
Synthflow / Finetuner.ai integration service.

Handles two responsibilities:
  1. Live lead-count lookup by ZIP + vertical — called by the AI agent during
     a call to fill in the [X] placeholder in the outbound script.
  2. Post-call webhook processing — receives outcome from Synthflow after
     each call, looks up the prospect in GHL by phone, and applies tags that
     trigger downstream GHL automations (sample-lead SMS, demo Calendly link).

GHL tag contract (must match GHL workflow triggers):
  synthflow-called          — applied on every completed call
  sample_leads_requested    — prospect said YES to sample leads → GHL sends SMS
  demo_requested            — prospect asked for a demo → GHL sends Calendly SMS
  not_interested            — DNC / remove from active sequences
  synthflow-voicemail       — left voicemail
  synthflow-no_answer       — no answer / hung up
"""

import logging
from typing import Optional

from src.services.ghl_webhook import _ghl_request, _headers, _is_configured
from config.settings import get_settings

logger = logging.getLogger(__name__)

_GHL_BASE = "https://services.leadconnectorhq.com"

# ── Outcome → GHL tag mapping ─────────────────────────────────────────────────
_OUTCOME_TAGS: dict[str, list[str]] = {
    "sample_requested":  ["synthflow-called", "sample_leads_requested"],
    "demo_requested":    ["synthflow-called", "demo_requested"],
    "not_interested":    ["synthflow-called", "not_interested"],
    "voicemail":         ["synthflow-called", "synthflow-voicemail"],
    "no_answer":         ["synthflow-called", "synthflow-no_answer"],
    "completed":         ["synthflow-called"],   # talked but no clear outcome
}


def get_live_lead_count(zip_code: str, vertical: str, county_id: str = "hillsborough") -> dict:
    """
    Return the count of qualified leads in a ZIP for a given vertical,
    plus the most common signal type — used by the Synthflow agent to fill
    the [X] placeholder in the outbound script.

    Returns:
        {
          "count": int,
          "top_signal": str | None,   # e.g. "insurance_claims"
          "zip_available": bool,       # whether territory is still open
        }
    """
    from src.core.database import get_db_context
    from src.core.models import Property, DistressScore, ZipTerritory, FoundingSubscriberCount
    from src.services.stripe_service import _founding_limit
    from sqlalchemy import select, func, and_, desc

    with get_db_context() as session:
        # Count qualified leads in this ZIP for this vertical
        score_col = DistressScore.vertical_scores[vertical].as_float()
        count = session.execute(
            select(func.count()).select_from(Property)
            .join(DistressScore, DistressScore.property_id == Property.id)
            .where(
                and_(
                    Property.zip == zip_code,
                    Property.county_id == county_id,
                    DistressScore.qualified == True,
                )
            )
        ).scalar() or 0

        # Find the most common distress type in this ZIP
        top_signal = None
        try:
            rows = session.execute(
                select(Property, DistressScore)
                .join(DistressScore, DistressScore.property_id == Property.id)
                .where(
                    and_(
                        Property.zip == zip_code,
                        Property.county_id == county_id,
                        DistressScore.qualified == True,
                    )
                )
                .order_by(desc(score_col))
                .limit(20)
            ).all()

            signal_freq: dict = {}
            for prop, score in rows:
                for sig in (score.distress_types or []):
                    signal_freq[sig] = signal_freq.get(sig, 0) + 1
            if signal_freq:
                top_signal = max(signal_freq, key=signal_freq.get)
        except Exception:
            pass

        # Check ZIP territory availability
        territory = session.execute(
            select(ZipTerritory).where(
                ZipTerritory.zip_code == zip_code,
                ZipTerritory.vertical == vertical,
                ZipTerritory.county_id == county_id,
            )
        ).scalar_one_or_none()
        zip_available = (territory is None or territory.status == "available")

        # Founding spots remaining — total across all tiers for this vertical/county
        founding_rows = session.execute(
            select(func.sum(FoundingSubscriberCount.count)).where(
                FoundingSubscriberCount.vertical == vertical,
                FoundingSubscriberCount.county_id == county_id,
            )
        ).scalar() or 0
        tiers = ["starter", "pro", "dominator"]
        total_founding_cap = _founding_limit() * len(tiers)
        founding_spots_remaining = max(0, total_founding_cap - founding_rows)

    return {
        "count": count,
        "top_signal": top_signal,
        "zip_available": zip_available,
        "founding_spots_remaining": founding_spots_remaining,
    }


def _find_ghl_contact_by_phone(phone: str) -> Optional[str]:
    """
    Search GHL for a contact by phone number.
    Normalises the number before querying (strips +1, spaces, dashes).
    Returns GHL contact ID or None.
    """
    if not _is_configured():
        return None

    settings = get_settings()
    # Normalise — strip leading +1 and non-digits for the query
    digits = "".join(c for c in phone if c.isdigit())
    if digits.startswith("1") and len(digits) == 11:
        digits = digits[1:]

    try:
        resp = _ghl_request(
            "GET",
            f"{_GHL_BASE}/contacts/search",
            headers=_headers(),
            params={"locationId": settings.ghl_location_id, "query": digits},
        )
        resp.raise_for_status()
        contacts = resp.json().get("contacts", [])
        if contacts:
            return contacts[0].get("id")
    except Exception as exc:
        logger.warning("[Synthflow] GHL phone lookup failed for %s: %s", phone, exc)
    return None


def _apply_tags_to_contact(contact_id: str, tags: list[str]) -> bool:
    """
    Add tags to an existing GHL contact.
    Uses PUT /contacts/{id} with tag merge (GHL appends, does not replace).
    Returns True on success.
    """
    if not _is_configured():
        return False
    try:
        resp = _ghl_request(
            "PUT",
            f"{_GHL_BASE}/contacts/{contact_id}",
            headers=_headers(),
            json={"tags": tags},
        )
        resp.raise_for_status()
        return True
    except Exception as exc:
        logger.warning("[Synthflow] tag update failed for contact %s: %s", contact_id, exc)
        return False


def _create_prospect_contact(phone: str, name: str, vertical: str, zip_code: str) -> Optional[str]:
    """
    Create a new GHL contact for an outbound prospect who isn't in the DB yet
    (contractor, not a property owner).
    Returns GHL contact ID or None.
    """
    if not _is_configured():
        return None

    settings = get_settings()
    name_parts = name.strip().split(None, 1)
    payload = {
        "locationId": settings.ghl_location_id,
        "firstName":  name_parts[0] if name_parts else "Prospect",
        "lastName":   name_parts[1] if len(name_parts) > 1 else "",
        "phone":      phone,
        "postalCode": zip_code,
        "tags":       [f"vertical-{vertical}", "synthflow-prospect", "synthflow-called"],
    }
    try:
        resp = _ghl_request(
            "POST",
            f"{_GHL_BASE}/contacts/",
            headers=_headers(),
            json=payload,
        )
        # Handle duplicate — GHL 400 with meta.contactId
        if resp.status_code == 400:
            dup_id = resp.json().get("meta", {}).get("contactId")
            if dup_id:
                return dup_id
        resp.raise_for_status()
        return resp.json().get("contact", {}).get("id")
    except Exception as exc:
        logger.warning("[Synthflow] prospect contact create failed for %s: %s", phone, exc)
        return None


def process_call_outcome(
    prospect_phone: str,
    outcome: str,
    vertical: str,
    zip_code: str,
    prospect_name: str = "",
    notes: str = "",
) -> dict:
    """
    Called by POST /webhooks/synthflow after each Synthflow call completes.

    Looks up the prospect in GHL by phone, applies outcome tags, creates
    a contact if none exists, and — for sample_requested outcomes — sends
    the sample leads SMS directly without needing a GHL workflow.

    Args:
        prospect_phone: The number that was called (E.164 or 10-digit).
        outcome:        One of: sample_requested | demo_requested |
                        not_interested | voicemail | no_answer | completed
        vertical:       roofing | restoration | etc.
        zip_code:       Prospect's ZIP (from campaign list or captured by agent).
        prospect_name:  If available from the campaign list.
        notes:          Short summary / transcript snippet (optional).

    Returns:
        {"contact_id": str | None, "tags_applied": list, "created": bool,
         "sms_sent": bool | None}
    """
    tags = _OUTCOME_TAGS.get(outcome, ["synthflow-called"])
    if vertical:
        tags = tags + [f"vertical-{vertical}"]

    # Try to find an existing GHL contact by phone
    contact_id = _find_ghl_contact_by_phone(prospect_phone)
    created = False

    if contact_id:
        _apply_tags_to_contact(contact_id, tags)
        logger.info(
            "[Synthflow] outcome=%s contact=%s tags=%s",
            outcome, contact_id, tags,
        )
    else:
        # Prospect not in GHL yet — create them
        contact_id = _create_prospect_contact(
            phone=prospect_phone,
            name=prospect_name,
            vertical=vertical,
            zip_code=zip_code,
        )
        created = True
        logger.info(
            "[Synthflow] new contact created: %s outcome=%s tags=%s",
            contact_id, outcome, tags,
        )

    # Fire SMS directly for outcomes that need immediate follow-up — no GHL workflow needed
    sms_sent = None

    if outcome == "sample_requested" and contact_id and zip_code:
        try:
            from src.services.sample_leads_sms import send_sample_leads
            result = send_sample_leads(
                contact_id=contact_id,
                zip_code=zip_code,
                vertical=vertical or "roofing",
                prospect_name=prospect_name,
            )
            sms_sent = result.get("sent", False)
            logger.info(
                "[Synthflow] sample leads SMS sent=%s leads=%d contact=%s",
                sms_sent, result.get("lead_count", 0), contact_id,
            )
        except Exception as exc:
            logger.error("[Synthflow] sample leads SMS failed for %s: %s", contact_id, exc)
            sms_sent = False

    elif outcome == "demo_requested" and contact_id:
        try:
            from config.settings import get_settings
            from src.services.sample_leads_sms import send_ghl_sms
            calendly_url = get_settings().demo_calendly_url
            if calendly_url:
                name = prospect_name.split()[0] if prospect_name else "there"
                message = (
                    f"Hi {name}, here's the link to book your 15-minute demo with Josh:\n\n"
                    f"{calendly_url}\n\n"
                    f"No commitment — he'll pull up live leads in your ZIP while you're on the phone.\n\n"
                    f"Reply STOP to opt out."
                )
                sms_sent = send_ghl_sms(contact_id=contact_id, message=message)
                logger.info("[Synthflow] demo SMS sent=%s contact=%s", sms_sent, contact_id)
            else:
                logger.warning("[Synthflow] DEMO_CALENDLY_URL not set — skipping demo SMS")
                sms_sent = False
        except Exception as exc:
            logger.error("[Synthflow] demo SMS failed for %s: %s", contact_id, exc)
            sms_sent = False

    # Log the call to message_outcomes so the anomaly monitor can query it
    try:
        from src.core.database import get_db_context
        from src.core.models import MessageOutcome
        with get_db_context() as _session:
            _session.add(MessageOutcome(
                message_type="voice",
                channel="synthflow",
                template_id=outcome,
            ))
    except Exception as _exc:
        logger.warning("[Synthflow] failed to log call to message_outcomes: %s", _exc)

    return {
        "contact_id": contact_id,
        "tags_applied": tags,
        "created": created,
        "sms_sent": sms_sent,
    }
