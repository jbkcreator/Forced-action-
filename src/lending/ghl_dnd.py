"""Opt-out sync into GoHighLevel: mark the contact do-not-disturb on every channel.

Uses the configured GHL sub-account (GHL_API_KEY / GHL_LOCATION_ID). Upsert by phone:
an existing contact is updated, and a number GHL has never seen gets a DND-only
contact, so a later form or import with that phone stays blocked.
"""
from __future__ import annotations

import logging
from typing import Callable, Optional

from config.lending_compliance import GHL_DND_CHANNELS, GHL_OPT_OUT_TAG
from config.settings import get_settings

logger = logging.getLogger(__name__)

GhlDnd = Callable[[str], bool]


def set_ghl_dnd(phone: str) -> bool:
    """True when GHL accepted the DND update. Never raises; logs the status only."""
    from src.services import ghl_webhook

    settings = get_settings()
    body = {
        "locationId": settings.ghl_location_id,
        "phone": phone,
        "dnd": True,
        "dndSettings": {channel: {"status": "active", "message": "Lending opt-out"} for channel in GHL_DND_CHANNELS},
        "tags": [GHL_OPT_OUT_TAG],
    }
    try:
        response = ghl_webhook._ghl_request(
            "POST", f"{ghl_webhook._GHL_BASE}/contacts/upsert", headers=ghl_webhook._headers(), json=body,
        )
    except Exception as exc:
        logger.warning("[lending-ghl] DND upsert failed: %s", type(exc).__name__)
        return False
    if response.status_code >= 400:
        logger.warning("[lending-ghl] DND upsert failed: HTTP %s", response.status_code)
        return False
    return True


def get_ghl_dnd() -> Optional[GhlDnd]:
    """The GHL DND writer, or None when GHL is not configured (the sync then waits)."""
    settings = get_settings()
    if settings.ghl_api_key is None or not settings.ghl_location_id:
        return None
    return set_ghl_dnd
