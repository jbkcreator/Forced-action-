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
    from config.settings import get_settings as _get_settings

    settings = _get_settings()

    if not settings.synthflow_api_key:
        logger.warning("synthflow_api_key not configured — voice drop skipped")
        return None

    api_key = settings.synthflow_api_key.get_secret_value()
    base = settings.synthflow_api_base.rstrip("/")

    # Synthflow v2 POST /calls schema: the agent is identified by `model_id`
    # (not `agent_id`) and `name` (callee name) is required. custom_variables
    # carries the per-call template context as a list of {name,value} pairs.
    custom_variables = [
        {"name": str(k), "value": str(v)}
        for k, v in context.items() if v not in (None, "")
    ]
    callee_name = context.get("subscriber_name") or "there"
    # Set the post-call webhook per-call so transcript/recording/outcome always
    # route back to our unified /webhooks/synthflow handler — independent of
    # each agent's dashboard config (which is invisible and easy to lose).
    # APP_BASE_URL must be the public prod domain (e.g. https://forcedactionleads.com).
    webhook_url = f"{settings.app_base_url.rstrip('/')}/webhooks/synthflow"
    payload = {
        "model_id": agent_id,
        "phone": phone,
        "name": callee_name,
        "custom_variables": custom_variables,
        "external_webhook_url": webhook_url,
    }

    try:
        import requests as _requests  # noqa: PLC0415 (lazy import — avoids circular at module level)
        resp = _requests.post(
            f"{base}/calls",
            json=payload,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        # v2 wraps the id under response.call_id; tolerate flat shapes too.
        resp_obj = data.get("response") if isinstance(data.get("response"), dict) else data
        call_id = resp_obj.get("call_id") or resp_obj.get("id") or data.get("call_id") or data.get("id")
        logger.info("synthflow call initiated call_id=%s phone=%s", call_id, phone[-4:])
        return call_id
    except Exception as exc:
        # Include the response body — a bare HTTPError hides the field-level reason.
        body = getattr(getattr(exc, "response", None), "text", "")
        logger.error("synthflow initiate_call failed: %s | body=%s", exc, body[:500])
        return None
