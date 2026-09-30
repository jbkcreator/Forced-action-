"""Dialer-neutral interface for the lending rules code (Go Live G1).

Rules code calls ``remove`` / ``restore`` / ``upsert_contact`` and never names a
vendor. ``BatchDialerAdapter`` maps them onto BatchDialer endpoints listed in
``config.lending_dialer.BATCHDIALER_ENDPOINTS``; an endpoint that is still ``None``
(capability unconfirmed with the vendor) raises ``UnconfirmedCapability``, which the
opt-out path already treats as "removal stays pending".
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Protocol, Union

import requests

from config.lending_compliance import RemovalReason
from config.lending_dialer import (
    BATCHDIALER_BASE_URL,
    BATCHDIALER_ENDPOINTS,
    BATCHDIALER_TIMEOUT_SECONDS,
)
from config.settings import get_settings
from src.utils.http_helpers import requests_get_with_retry, requests_post_with_retry
from src.lending.dialer_removal import DialerRemovalUndecided

logger = logging.getLogger(__name__)

Endpoint = Optional[tuple[str, str]]
Http = Callable[..., Mapping[str, Any]]


@dataclass(frozen=True)
class DialerContactFields:
    """What the caller sees for a contact; None fields are sent as empty."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    information: Optional[str] = None
    email: Optional[str] = None


@dataclass(frozen=True)
class ContactUpsertResult:
    contact_id: Any
    created: bool


class UnconfirmedCapability(DialerRemovalUndecided):
    """The dialer endpoint for this action is not confirmed with the vendor yet."""


class DialerRequestError(RuntimeError):
    """A dialer request failed; ``status`` is the HTTP status when there was one."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


class Dialer(Protocol):
    def upsert_contact(self, record: Mapping[str, Any]) -> Optional[str]: ...
    def remove(self, phone: str, *, reason: str) -> None: ...
    def restore(self, phone: str) -> None: ...


class InMemoryDialer:
    """Records calls; used in tests and dry runs."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str, Optional[str]]] = []

    def upsert_contact(self, record: Mapping[str, Any]) -> Optional[str]:
        self.events.append(("upsert", record["phone"], None))
        return None

    def remove(self, phone: str, *, reason: str) -> None:
        self.events.append(("remove", phone, reason))

    def restore(self, phone: str) -> None:
        self.events.append(("restore", phone, None))


class BatchDialerAdapter:
    def __init__(self, *, http: Http, endpoints: Mapping[str, Endpoint] = BATCHDIALER_ENDPOINTS) -> None:
        self._http = http
        self._endpoints = endpoints
        self._campaign_ids: Optional[dict[str, Any]] = None

    def _call(self, action: str, payload: dict) -> Mapping[str, Any]:
        endpoint = self._endpoints.get(action)
        if endpoint is None:
            raise UnconfirmedCapability(f"BatchDialer endpoint '{action}' is not confirmed")
        method, path = endpoint
        return self._http(method, path, json=payload)

    def upsert_contact(
        self,
        record_or_phone: Union[Mapping[str, Any], str],
        fields: Optional[DialerContactFields] = None,
        *,
        campaign: Optional[str] = None,
    ) -> Union[Optional[str], ContactUpsertResult]:
        """Two call shapes: ``upsert_contact(record)`` (rules code) returns the contact id;
        ``upsert_contact(phone, fields, campaign=)`` (dialer load) returns a ContactUpsertResult."""
        if isinstance(record_or_phone, Mapping):
            body = self._call("contact_upsert", dict(record_or_phone))
            contact_id = body.get("id")
            return str(contact_id) if contact_id is not None else None
        payload = _contact_body(record_or_phone, fields or DialerContactFields())
        if campaign is not None:
            payload["campaignId"] = self._campaign_id(campaign)
        body = self._call("contact_upsert", payload)
        if body.get("id") is None:
            raise DialerRequestError("dialer returned no contact id")
        return ContactUpsertResult(contact_id=body["id"], created=True)

    def update_contact(self, contact_id: Any, fields: DialerContactFields) -> dict:
        return dict(self._call("contact_update", {"id": contact_id, **_contact_body(None, fields)}))

    def _campaign_id(self, name: str) -> Any:
        if self._campaign_ids is None:
            rows = self._http("GET", "/campaigns", json=None)
            items = rows.get("items", []) if isinstance(rows, Mapping) else rows
            self._campaign_ids = {str(r.get("name")): r.get("id") for r in items}
        if name not in self._campaign_ids:
            raise DialerRequestError(f"no dialer campaign named '{name}'")
        return self._campaign_ids[name]

    def remove(self, phone: str, *, reason: str) -> None:
        # Opt-outs are permanent (DNC list); window/cap holds only leave the campaign and
        # never touch the DNC list, so restore() can never undo a real DNC entry.
        action = "dnc_add" if reason == RemovalReason.OPT_OUT.value else "campaign_remove"
        self._call(action, {"phone": phone})

    def restore(self, phone: str) -> None:
        self._call("campaign_restore", {"phone": phone})


def _contact_body(phone: Optional[str], fields: DialerContactFields) -> dict:
    """Field names follow BatchDialer's contact shape; confirmed by the first write test."""
    body = {"firstName": fields.first_name or "", "lastName": fields.last_name or "",
            "company": fields.company_name or "", "notes": fields.information or "",
            "email": fields.email or ""}
    if phone is not None:
        body["phone"] = phone
    return body


def _requests_http(api_key: str) -> Http:
    """GET/POST go through the repo retry helpers (network errors, 429, 5xx); a 4xx is
    final. Failures log method, path and status only: bodies carry phone numbers."""

    def call(method: str, path: str, *, json: Optional[dict] = None) -> Any:
        url = f"{BATCHDIALER_BASE_URL}{path}"
        kwargs = {"headers": {"X-ApiKey": api_key}, "timeout": BATCHDIALER_TIMEOUT_SECONDS}
        try:
            if method == "GET":
                response = requests_get_with_retry(url, max_retries=3, retry_delay=2, **kwargs)
            elif method == "POST":
                response = requests_post_with_retry(url, json=json, **kwargs)
            else:
                response = requests.request(method, url, json=json, **kwargs)
                response.raise_for_status()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            logger.warning("[dialer] BatchDialer %s %s failed: HTTP %s", method, path, status)
            raise DialerRequestError(f"BatchDialer {method} {path} HTTP {status}", status=status) from exc
        except requests.RequestException as exc:
            logger.warning("[dialer] BatchDialer %s %s failed: %s", method, path, type(exc).__name__)
            raise DialerRequestError(f"BatchDialer {method} {path}: {type(exc).__name__}") from exc
        return response.json() if response.content else {}

    return call


def get_dialer() -> Optional[Dialer]:
    """The configured dialer, or None when no BatchDialer key / base URL is set."""
    key = get_settings().batchdialer_api_key
    if key is None or not BATCHDIALER_BASE_URL:
        return None
    return BatchDialerAdapter(http=_requests_http(key.get_secret_value()))
