"""
Aircall REST API client (Closer Cockpit, Sprint S1b).

Thin read-side wrapper over the Aircall Public API:
  base    https://api.aircall.io/v1
  auth    HTTP Basic (api_id : api_token)
  limit   120 req/min/company (well within closer-call volume)

Used by the /webhooks/aircall handler and the tagging consumer to pull the
transcript, sentiment, topics, and a fresh (10-min) recording URL for a call.

Response JSON shapes are parsed defensively — confirm exact shapes against
developer.aircall.io on first live integration. All calls are wrapped and never
log credentials.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from config.settings import get_settings
from src.services.synthflow_transcript import transcript_to_text
from src.utils.http_helpers import requests_get_with_retry

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.aircall.io/v1"
# Aircall expires recording links after 10 minutes (security measure).
_RECORDING_URL_TTL_SEC = 600


class AircallNotConfigured(RuntimeError):
    """Raised when Aircall credentials are absent."""


def _auth() -> tuple[str, str]:
    s = get_settings()
    if not s.aircall_api_id or not s.aircall_api_token:
        raise AircallNotConfigured("AIRCALL_API_ID / AIRCALL_API_TOKEN not set")
    return s.aircall_api_id.get_secret_value(), s.aircall_api_token.get_secret_value()


def _get(path: str) -> dict[str, Any]:
    """GET {base}{path} with Basic auth; returns parsed JSON or raises."""
    url = f"{_BASE_URL}{path}"
    resp = requests_get_with_retry(url, auth=_auth(), timeout=20)
    resp.raise_for_status()
    return resp.json()


def get_call(call_id: str | int) -> Optional[dict]:
    """Full call object (includes `recording` URL, duration, user, number)."""
    try:
        data = _get(f"/calls/{call_id}")
        return data.get("call", data)
    except Exception as exc:
        logger.error("[aircall] get_call failed call_id=%s: %s", call_id, exc)
        return None


def get_transcription(call_id: str | int) -> Optional[str]:
    """Flattened transcript text for a call, or None if unavailable.

    Reuses synthflow_transcript.transcript_to_text, which accepts a string or a
    list of turn dicts ({text|content|message}). Aircall's exact shape is
    confirmed at integration time; this handles the common variants.
    """
    try:
        data = _get(f"/calls/{call_id}/transcription")
        node = data.get("transcription", data)
        content = node.get("content", node) if isinstance(node, dict) else node
        # Common shapes: {"utterances": [...]} or a raw string/list.
        if isinstance(content, dict) and "utterances" in content:
            return transcript_to_text(content["utterances"]) or None
        return transcript_to_text(content) or None
    except Exception as exc:
        logger.error("[aircall] get_transcription failed call_id=%s: %s", call_id, exc)
        return None


def get_sentiment(call_id: str | int) -> Optional[str]:
    """Aircall native sentiment for a call ('positive'|'neutral'|'negative'|'mixed')."""
    try:
        data = _get(f"/calls/{call_id}/sentiments")
        node = data.get("sentiments", data)
        if isinstance(node, list):
            node = node[0] if node else {}
        value = (node or {}).get("value") if isinstance(node, dict) else None
        return value.lower() if isinstance(value, str) else None
    except Exception as exc:
        logger.error("[aircall] get_sentiment failed call_id=%s: %s", call_id, exc)
        return None


def get_topics(call_id: str | int) -> Optional[list]:
    """Aircall native key topics for a call."""
    try:
        data = _get(f"/calls/{call_id}/topics")
        topics = data.get("topics", data)
        if isinstance(topics, list):
            # Normalise to a flat list of strings where possible.
            return [t.get("name", t) if isinstance(t, dict) else t for t in topics]
        return None
    except Exception as exc:
        logger.error("[aircall] get_topics failed call_id=%s: %s", call_id, exc)
        return None


def fresh_recording_url(call_id: str | int) -> tuple[Optional[str], int]:
    """A freshly-issued recording URL (valid ~10 min) for on-demand playback.

    Returns (url, ttl_seconds). url is None when the call has no recording.
    """
    call = get_call(call_id)
    if not call:
        return None, 0
    url = call.get("recording") or call.get("recording_short_url")
    return (url, _RECORDING_URL_TTL_SEC) if url else (None, 0)
