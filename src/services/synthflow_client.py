"""Synthflow outbound API client — initiates AI voice drop calls."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def initiate_call(
    phone: str,
    agent_id: str,
    context: Dict[str, Any],
) -> Optional[str]:
    """
    POST /calls to Synthflow to trigger an outbound AI voice drop.

    Returns the call_id string on success, None on failure.
    phone must be E.164 format.
    """
    import httpx
    from config.settings import get_settings as _get_settings

    settings = _get_settings()

    if not settings.synthflow_api_key:
        logger.warning("synthflow_api_key not configured — voice drop skipped")
        return None

    api_key = settings.synthflow_api_key.get_secret_value()
    base = settings.synthflow_api_base.rstrip("/")

    # custom_variables is the Synthflow v2 format; metadata kept for backwards compat
    custom_variables = [
        f"{k}: {v}" for k, v in context.items() if v not in (None, "")
    ]
    payload = {
        "phone": phone,
        "agent_id": agent_id,
        "metadata": context,
        "custom_variables": custom_variables,
    }

    try:
        resp = httpx.post(
            f"{base}/calls",
            json=payload,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        call_id = data.get("call_id") or data.get("id")
        logger.info("synthflow call initiated call_id=%s phone=%s", call_id, phone[-4:])
        return call_id
    except Exception as exc:
        logger.error("synthflow initiate_call failed: %s", exc)
        return None
