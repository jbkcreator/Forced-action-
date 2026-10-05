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
    """True when GHL accepted the DND update. Never raises; logs the status only."""
    from src.services import ghl_webhook

    account = lending_ghl_account()
    if account is None:
        return False
    body = {
        "locationId": account.location_id,
        "phone": phone,
        "dnd": True,
        "dndSettings": {channel: {"status": "active", "message": "Lending opt-out"} for channel in GHL_DND_CHANNELS},
        "tags": [GHL_OPT_OUT_TAG],
    }
    try:
        response = ghl_webhook._ghl_request(
            "POST", f"{ghl_webhook._GHL_BASE}/contacts/upsert", headers=ghl_headers(account.api_key), json=body,
        )
    except Exception as exc:
        logger.warning("[lending-ghl] DND upsert failed: %s", type(exc).__name__)
        return False
    if response.status_code >= 400:
        logger.warning("[lending-ghl] DND upsert failed: HTTP %s", response.status_code)
        return False
    return True


def get_ghl_dnd() -> Optional[GhlDnd]:
    """The GHL DND writer, or None when the Next Deal Lending account is not configured
    (the sync then waits)."""
    return set_ghl_dnd if lending_ghl_account() is not None else None
