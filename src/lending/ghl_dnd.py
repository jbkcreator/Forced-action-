"""Opt-out sync into GoHighLevel: mark the contact do-not-disturb on every channel.

Uses the Next Deal Lending sub-account only (LENDING_GHL_API_KEY / LENDING_GHL_LOCATION_ID, see
ghl_account). Upsert by phone: an existing contact is updated, and a number GHL has never seen
gets a DND-only contact, so a later form or import with that phone stays blocked.
"""
from __future__ import annotations

import logging
from typing import Callable, Optional

from config.lending_compliance import GHL_DND_CHANNELS, GHL_OPT_OUT_TAG
from src.lending.ghl_account import ghl_headers, lending_ghl_account

logger = logging.getLogger(__name__)

GhlDnd = Callable[[str], bool]


def set_ghl_dnd(phone: str) -> bool:
    """True when GHL accepted the DND update. Never raises; logs the status only.

    The upsert carries no ``tags``: GHL's upsert REPLACES the tag list, so sending the opt-out tag there wiped every
    other tag on the contact (lead source, consent, list tags). The tag is added afterwards with the add-tags call,
    which keeps existing tags. A failed tag call does not undo the DND, which is what the opt-out needs."""
    from src.services import ghl_webhook

    account = lending_ghl_account()
    if account is None:
        return False
    headers = ghl_headers(account.api_key)
    body = {
        "locationId": account.location_id,
        "phone": phone,
        "dnd": True,
        "dndSettings": {channel: {"status": "active", "message": "Lending opt-out"} for channel in GHL_DND_CHANNELS},
    }
    try:
        response = ghl_webhook._ghl_request("POST", f"{ghl_webhook._GHL_BASE}/contacts/upsert", headers=headers, json=body)
    except Exception as exc:
        logger.warning("[lending-ghl] DND upsert failed: %s", type(exc).__name__)
        return False
    if response.status_code >= 400:
        logger.warning("[lending-ghl] DND upsert failed: HTTP %s", response.status_code)
        return False
    _add_opt_out_tag(ghl_webhook, headers, _contact_id(response))
    return True


def _contact_id(response) -> Optional[str]:
    try:
        contact_id = ((response.json() or {}).get("contact") or {}).get("id")
    except Exception:  # an unreadable body only costs the tag
        return None
    return str(contact_id) if contact_id else None


def _add_opt_out_tag(ghl_webhook, headers: dict[str, str], contact_id: Optional[str]) -> None:
    """Add the opt-out tag, keeping the contact's other tags. Best effort: logged, never raised."""
    if not contact_id:
        logger.warning("[lending-ghl] DND set but no contact id returned; opt-out tag not added")
        return
    try:
        response = ghl_webhook._ghl_request("POST", f"{ghl_webhook._GHL_BASE}/contacts/{contact_id}/tags",
                                            headers=headers, json={"tags": [GHL_OPT_OUT_TAG]})
    except Exception as exc:
        logger.warning("[lending-ghl] opt-out tag not added: %s", type(exc).__name__)
        return
    if response.status_code >= 400:
        logger.warning("[lending-ghl] opt-out tag not added: HTTP %s", response.status_code)


def get_ghl_dnd() -> Optional[GhlDnd]:
    """The GHL DND writer, or None when the Next Deal Lending account is not configured
    (the sync then waits)."""
    return set_ghl_dnd if lending_ghl_account() is not None else None
