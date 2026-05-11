"""
Telnyx Number Lookup wrapper.

Single public function:  lookup_phone(phone_e164) -> dict

Returns a dict normalized to match the shape we already store in
Owner.phone_metadata so the sampler / loaders can persist results
without translation:

    {
        "type":      "mobile" | "voip" | "landline" | "unknown",
        "carrier":   "T-Mobile" | None,
        "ported":    bool,
        "country":   "US" | None,
        "raw_type":  str,            # what Telnyx actually returned
        "source":    "telnyx",
        "fetched_at": "ISO-8601",
    }

Errors raise TelnyxLookupError so the caller can decide whether to retry,
fall back, or just log and continue.

Pricing reference (2026-05): ~$0.004 per carrier-type lookup.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import requests

from config.settings import get_settings

logger = logging.getLogger(__name__)

_API_BASE = "https://api.telnyx.com/v2/number_lookup"
_TIMEOUT_SECONDS = 10


class TelnyxLookupError(Exception):
    """Raised when Telnyx returns an error or the request fails."""


def _e164(phone: str) -> Optional[str]:
    """Return a phone string in +1XXXXXXXXXX (E.164) format, or None if unparseable."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    if not digits:
        return None
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if digits.startswith("+"):
        return digits
    return f"+{digits}"


def _normalize_type(raw: Optional[str]) -> str:
    if not raw:
        return "unknown"
    t = raw.lower()
    # Telnyx returns: "fixed_line", "mobile", "voip", "toll_free", "premium", "pager"
    if "mobile" in t or "wireless" in t or "cell" in t:
        return "mobile"
    if "voip" in t:
        return "voip"
    if "fixed" in t or "land" in t or "geographic" in t:
        return "landline"
    return "unknown"


def lookup_phone(phone: str) -> dict:
    """
    Look up carrier + line type for a phone number via Telnyx.

    Raises TelnyxLookupError on any failure. Caller decides whether to retry.
    """
    settings = get_settings()
    if settings.telnyx_api_key is None:
        raise TelnyxLookupError("TELNYX_API_KEY is not configured")

    e164 = _e164(phone)
    if not e164:
        raise TelnyxLookupError(f"Phone '{phone}' is not parseable to E.164")

    url = f"{_API_BASE}/{e164}"
    params = {"type": settings.telnyx_lookup_type or "carrier"}
    headers = {
        "Authorization": f"Bearer {settings.telnyx_api_key.get_secret_value()}",
        "Accept":        "application/json",
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise TelnyxLookupError(f"network error contacting Telnyx: {exc}") from exc

    if resp.status_code == 404:
        # Telnyx returns 404 for invalid / unallocated numbers
        return {
            "type":       "unknown",
            "carrier":    None,
            "ported":     False,
            "country":    None,
            "raw_type":   "not_found",
            "source":     "telnyx",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    if resp.status_code >= 400:
        raise TelnyxLookupError(
            f"Telnyx returned {resp.status_code}: {resp.text[:200]}"
        )

    try:
        payload = resp.json().get("data") or {}
    except ValueError as exc:
        raise TelnyxLookupError(f"non-JSON response from Telnyx: {exc}") from exc

    carrier_obj = payload.get("carrier") or {}
    portability = payload.get("portability") or {}
    raw_type = carrier_obj.get("type") or ""

    return {
        "type":       _normalize_type(raw_type),
        "carrier":    carrier_obj.get("name"),
        "ported":     bool(portability.get("ported", False)),
        "country":    payload.get("country_code"),
        "raw_type":   raw_type or "unknown",
        "source":     "telnyx",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
