"""The Next Deal Lending GoHighLevel sub-account: the only GHL account lending code may touch.

Credentials are LENDING_GHL_API_KEY + LENDING_GHL_LOCATION_ID. There is deliberately no
fallback to the platform's GHL_API_KEY / GHL_LOCATION_ID: that is a different account, and
lending must never read from it or write to it. With either value missing, no account is
returned and every caller does nothing.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)

_warned_partial = False


@dataclass(frozen=True)
class GhlAccount:
    api_key: str
    location_id: str


def lending_ghl_account() -> Optional[GhlAccount]:
    """The Next Deal Lending credentials, or None unless both are set."""
    global _warned_partial
    settings = get_settings()
    api_key = settings.lending_ghl_api_key.get_secret_value() if settings.lending_ghl_api_key else ""
    location_id = settings.lending_ghl_location_id or ""
    if api_key and location_id:
        return GhlAccount(api_key, location_id)
    if (api_key or location_id) and not _warned_partial:
        missing = "LENDING_GHL_LOCATION_ID" if api_key else "LENDING_GHL_API_KEY"
        logger.error("[lending-ghl] LENDING_GHL_* is only partially set (%s is missing): "
                     "no GHL account will be used until both are set", missing)
        _warned_partial = True
    return None


def ghl_headers(api_key: str, version: str = "2021-07-28") -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Version": version,
            "Content-Type": "application/json", "Accept": "application/json"}
