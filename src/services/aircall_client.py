"""
Aircall REST API client (Closer Cockpit, Sprint S1b).

Thin read-side wrapper over the Aircall Public API:
  base    https://api.aircall.io/v1
  auth    HTTP Basic (api_id : api_token)
  limit   120 req/min/company (well within closer-call volume)

Used by the /webhooks/aircall handler and the tagging consumer to pull the
transcript, sentiment, topics, and a fresh (10-min) recording URL for a call.

Contact writes (lending dialer load): find by phone, create, update, delete
and upsert. Writes raise on failure instead of returning None, because the
load step must know exactly which contacts reached the dialer. Every request
is paced to the company rate limit; 429 is always retried, 5xx only for
repeatable requests, so a contact create that may have succeeded is never
repeated.

Response JSON shapes are parsed defensively — confirm exact shapes against
developer.aircall.io on first live integration. All calls are wrapped and never
log credentials.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

import requests

from config.lending_dialer import (
    AIRCALL_MAX_ATTEMPTS,
    AIRCALL_MAX_RETRY_WAIT_SECONDS,
    AIRCALL_REQUEST_TIMEOUT_SECONDS,
    AIRCALL_REQUESTS_PER_MINUTE,
    AIRCALL_RETRY_BASE_SECONDS,
)
from config.settings import get_settings
from src.services.synthflow_transcript import transcript_to_text
from src.utils.http_helpers import requests_get_with_retry

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.aircall.io/v1"
# Aircall expires recording links after 10 minutes (security measure).
_RECORDING_URL_TTL_SEC = 600


class AircallNotConfigured(RuntimeError):
    """Raised when Aircall credentials are absent."""


class AircallRequestError(RuntimeError):
    """An Aircall write or lookup failed; carries the HTTP status (0 = network)."""

    def __init__(self, method: str, path: str, status: int):
        self.status = status
        super().__init__(f"Aircall {method} {path} failed with status {status}")


class AircallAmbiguousContact(RuntimeError):
    """More than one Aircall contact holds the phone; the load must not guess."""


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


# ── Contact writes (lending dialer load) ─────────────────────────────────────

_IDEMPOTENT_METHODS = frozenset({"GET", "DELETE"})


class _RateLimiter:
    """Spaces requests evenly so a process never exceeds the per-minute limit."""

    def __init__(self, per_minute: int) -> None:
        self._interval = 60.0 / per_minute
        self._next_slot = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_slot - now
            self._next_slot = max(now, self._next_slot) + self._interval
        if wait > 0:
            time.sleep(wait)


_rate_limiter = _RateLimiter(AIRCALL_REQUESTS_PER_MINUTE)


def _retry_wait(attempt: int, retry_after: Optional[str]) -> float:
    if retry_after:
        try:
            return min(float(retry_after), AIRCALL_MAX_RETRY_WAIT_SECONDS)
        except ValueError:
            pass
    return min(AIRCALL_RETRY_BASE_SECONDS * (2 ** (attempt - 1)), AIRCALL_MAX_RETRY_WAIT_SECONDS)


def _request(method: str, path: str, *, params: Optional[dict] = None,
             body: Optional[dict] = None, idempotent: Optional[bool] = None) -> dict[str, Any]:
    """One paced Aircall request with retry; returns parsed JSON ({} when empty).

    ``idempotent`` overrides the method default: Aircall updates a contact with
    POST, which is safe to repeat, unlike the POST that creates one.
    """
    url = f"{_BASE_URL}{path}"
    auth = _auth()
    repeatable = method in _IDEMPOTENT_METHODS if idempotent is None else idempotent
    for attempt in range(1, AIRCALL_MAX_ATTEMPTS + 1):
        _rate_limiter.acquire()
        try:
            resp = requests.request(method, url, auth=auth, params=params, json=body,
                                    timeout=AIRCALL_REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            if repeatable and attempt < AIRCALL_MAX_ATTEMPTS:
                logger.warning("[aircall] %s %s network error (%s), retrying", method, path, type(exc).__name__)
                time.sleep(_retry_wait(attempt, None))
                continue
            raise AircallRequestError(method, path, 0) from exc

        status = resp.status_code
        retryable = status == 429 or (status >= 500 and repeatable)
        if retryable and attempt < AIRCALL_MAX_ATTEMPTS:
            wait = _retry_wait(attempt, resp.headers.get("Retry-After"))
            logger.warning("[aircall] %s %s got %d, retrying in %.1fs", method, path, status, wait)
            time.sleep(wait)
            continue
        if status >= 400:
            raise AircallRequestError(method, path, status)
        return resp.json() if resp.content else {}
    raise AircallRequestError(method, path, 429)


@dataclass(frozen=True)
class AircallContactFields:
    """What the caller sees for a contact; None fields are sent as empty."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    information: Optional[str] = None
    email: Optional[str] = None

    def update_body(self) -> dict[str, str]:
        return {
            "first_name": self.first_name or "",
            "last_name": self.last_name or "",
            "company_name": self.company_name or "",
            "information": self.information or "",
        }

    def create_body(self, phone: str) -> dict[str, Any]:
        body: dict[str, Any] = {**self.update_body(), "phone_numbers": [{"label": "Work", "value": phone}]}
        if self.email:
            body["emails"] = [{"label": "Work", "value": self.email}]
        return body


@dataclass(frozen=True)
class ContactUpsertResult:
    contact_id: int
    created: bool


def _mask_phone(phone: str) -> str:
    return f"...{phone[-4:]}" if len(phone) > 4 else "***"


def find_contacts_by_phone(phone: str) -> list[dict]:
    """Every Aircall contact holding this E.164 phone."""
    data = _request("GET", "/contacts/search", params={"phone_number": phone})
    return data.get("contacts", [])


def get_contact(contact_id: int) -> dict:
    """Read one contact by id. Authoritative: search results can lag an update."""
    data = _request("GET", f"/contacts/{contact_id}")
    return data.get("contact", data)


def create_contact(phone: str, fields: AircallContactFields) -> dict:
    data = _request("POST", "/contacts", body=fields.create_body(phone))
    return data.get("contact", data)


def update_contact(contact_id: int, fields: AircallContactFields) -> dict:
    data = _request("POST", f"/contacts/{contact_id}", body=fields.update_body(), idempotent=True)
    return data.get("contact", data)


def delete_contact(contact_id: int) -> None:
    _request("DELETE", f"/contacts/{contact_id}")


def upsert_contact(phone: str, fields: AircallContactFields) -> ContactUpsertResult:
    """Update the contact holding this phone, or create one: re-loads never duplicate.

    Raises AircallAmbiguousContact when several contacts already hold the
    phone, so the caller reports it instead of the load picking one.
    """
    existing = find_contacts_by_phone(phone)
    if len(existing) > 1:
        raise AircallAmbiguousContact(f"{len(existing)} Aircall contacts hold phone {_mask_phone(phone)}")
    if existing:
        contact_id = int(existing[0]["id"])
        update_contact(contact_id, fields)
        logger.info("[aircall] updated contact id=%s phone=%s", contact_id, _mask_phone(phone))
        return ContactUpsertResult(contact_id=contact_id, created=False)
    contact = create_contact(phone, fields)
    contact_id = int(contact["id"])
    logger.info("[aircall] created contact id=%s phone=%s", contact_id, _mask_phone(phone))
    return ContactUpsertResult(contact_id=contact_id, created=True)


def remove_contact_from_pool(phone: str) -> None:
    """Dialer remover used by lending stop-propagation; see src/lending/dialer_removal.py."""
    from src.lending.dialer_removal import remove_contact_from_pool as remove

    remove(phone)
