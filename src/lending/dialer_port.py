"""Dialer-neutral interface for the lending rules code (Go Live G1).

Rules code calls ``remove`` / ``restore`` / ``upsert_contact`` and never names a
vendor. ``BatchDialerAdapter`` maps them onto BatchDialer endpoints listed in
``config.lending_dialer.BATCHDIALER_ENDPOINTS``; an endpoint that is still ``None``
(capability unconfirmed with the vendor) raises ``UnconfirmedCapability``, which the
opt-out path already treats as "removal stays pending".
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Mapping, Optional, Protocol

import requests

from config.lending_compliance import RemovalReason
from config.lending_dialer import (
    BATCHDIALER_BASE_URL,
    BATCHDIALER_ENDPOINTS,
    BATCHDIALER_TIMEOUT_SECONDS,
)
from config.settings import get_settings
from src.lending.dialer_removal import DialerRemovalUndecided

logger = logging.getLogger(__name__)

Endpoint = Optional[tuple[str, str]]
Http = Callable[..., Mapping[str, Any]]


class UnconfirmedCapability(DialerRemovalUndecided):
    """The dialer endpoint for this action is not confirmed with the vendor yet."""


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

    def _call(self, action: str, payload: dict) -> Mapping[str, Any]:
        endpoint = self._endpoints.get(action)
        if endpoint is None:
            raise UnconfirmedCapability(f"BatchDialer endpoint '{action}' is not confirmed")
        method, path = endpoint
        return self._http(method, path, json=payload)

    def upsert_contact(self, record: Mapping[str, Any]) -> Optional[str]:
        body = self._call("contact_upsert", dict(record))
        contact_id = body.get("id")
        return str(contact_id) if contact_id is not None else None

    def remove(self, phone: str, *, reason: str) -> None:
        # Opt-outs are permanent (DNC list); window/cap holds only leave the campaign.
        action = "dnc_add" if reason == RemovalReason.OPT_OUT.value else "campaign_remove"
        self._call(action, {"phone": phone})

    def restore(self, phone: str) -> None:
        self._call("campaign_restore", {"phone": phone})


def _requests_http(api_key: str) -> Http:
    def call(method: str, path: str, *, json: Optional[dict] = None) -> Mapping[str, Any]:
        response = requests.request(
            method, f"{BATCHDIALER_BASE_URL}{path}", json=json,
            headers={"X-ApiKey": api_key}, timeout=BATCHDIALER_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    return call


def get_dialer() -> Optional[Dialer]:
    """The configured dialer, or None when no BatchDialer key / base URL is set."""
    key = get_settings().batchdialer_api_key
    if key is None or not BATCHDIALER_BASE_URL:
        return None
    return BatchDialerAdapter(http=_requests_http(key.get_secret_value()))
