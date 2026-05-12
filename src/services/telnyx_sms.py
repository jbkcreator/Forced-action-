"""
Telnyx Messaging API wrapper — outbound SMS.

Single public function: `send_message()`. Mirrors the auth + error-handling
pattern of `src/services/telnyx_lookup.py` so anyone reading one understands
the other.

The compliance gate (`src/services/sms_compliance.py`) calls this AFTER its
opt-out and quiet-hours checks. This wrapper only handles the vendor
mechanics — it does not consult the SMS suppression list, does not write
to the dead-letter queue, and does not check sandbox mode. Those concerns
live one layer up.

Pricing: ~$0.004 per US SMS segment (May 2026).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import requests

from config.settings import get_settings

logger = logging.getLogger(__name__)

_API_BASE = "https://api.telnyx.com/v2/messages"
_TIMEOUT_SECONDS = 15


class TelnyxSMSError(Exception):
    """Raised on any non-2xx response or network failure from Telnyx."""


def send_message(
    to: str,
    body: str,
    message_type: str = "marketing",
) -> dict:
    """
    Send a single SMS via the Telnyx Messaging API.

    Returns a normalized dict:
        {
            "message_id":   str | None,    # Telnyx message UUID
            "status":       str,           # queued | sent | delivered | delivery_failed
            "vendor":       "telnyx",
            "cost_cents":   int,           # estimated per-segment cost in cents
            "sent_at":      ISO-8601 str,
        }

    Raises TelnyxSMSError on auth, validation, or network failure so the
    caller can route to the dead-letter queue with a sensible reason.

    `message_type` is currently informational only (we tag it on logs);
    Telnyx routes based on the messaging profile, not the request.
    """
    settings = get_settings()
    if settings.telnyx_sms_api_key is None:
        raise TelnyxSMSError("TELNYX_SMS_API_KEY is not configured")
    if not settings.telnyx_messaging_profile_id:
        raise TelnyxSMSError("TELNYX_MESSAGING_PROFILE_ID is not configured")
    if not settings.telnyx_from_number:
        raise TelnyxSMSError("TELNYX_FROM_NUMBER is not configured")

    payload = {
        "from": settings.telnyx_from_number,
        "to":   to,
        "text": body,
        "messaging_profile_id": settings.telnyx_messaging_profile_id,
    }
    headers = {
        "Authorization": f"Bearer {settings.telnyx_sms_api_key.get_secret_value()}",
        "Accept":        "application/json",
        "Content-Type":  "application/json",
    }

    try:
        resp = requests.post(_API_BASE, json=payload, headers=headers, timeout=_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise TelnyxSMSError(f"network error contacting Telnyx Messaging: {exc}") from exc

    if resp.status_code in (401, 403):
        raise TelnyxSMSError(f"Telnyx auth error ({resp.status_code}): {resp.text[:300]}")

    if resp.status_code >= 400:
        # Telnyx returns errors as { "errors": [{ "title": "...", "detail": "...", "code": "..." }] }
        try:
            errors = resp.json().get("errors", [])
            detail = "; ".join(e.get("detail") or e.get("title", "") for e in errors)
        except ValueError:
            detail = resp.text[:300]
        raise TelnyxSMSError(f"Telnyx returned {resp.status_code}: {detail}")

    try:
        data = resp.json().get("data") or {}
    except ValueError as exc:
        raise TelnyxSMSError(f"non-JSON response from Telnyx: {exc}") from exc

    cost = data.get("cost") or {}
    try:
        cost_cents = int(round(float(cost.get("amount") or 0.4) * 100))
    except (TypeError, ValueError):
        cost_cents = 0

    return {
        "message_id": data.get("id"),
        "status":     data.get("status", "queued"),
        "vendor":     "telnyx",
        "cost_cents": cost_cents,
        "sent_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
