"""Dialer-neutral interface for the lending rules code (Go Live G1).

Rules code calls ``remove`` / ``restore`` / ``upsert_contact`` and never names a
vendor. ``BatchDialerAdapter`` maps them onto BatchDialer endpoints listed in
``config.lending_dialer.BATCHDIALER_ENDPOINTS``; an endpoint that is still ``None``
(capability unconfirmed with the vendor) raises ``UnconfirmedCapability``, which the
opt-out path already treats as "removal stays pending".
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Protocol, Union

import requests
from sqlalchemy import text

from config.lending_compliance import RemovalReason
from config.lending_dialer import (
    BATCHDIALER_BASE_URL,
    BATCHDIALER_QUICK_TIMEOUT_SECONDS,
    BATCHDIALER_ENDPOINTS,
    BATCHDIALER_TIMEOUT_SECONDS,
)
from config.settings import get_settings
from src.utils.http_helpers import requests_get_with_retry, requests_post_with_retry
from src.lending.dialer_removal import DialerRemovalUndecided
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

Endpoint = Optional[tuple[str, str]]
# A retried create after a server-side success would create a second, untracked contact.
_NO_RETRY_POSTS = frozenset({"/contacts"})
LOAD_ENDPOINTS = ("contacts_add_to_campaign", "contact_update")
Http = Callable[..., Mapping[str, Any]]


@dataclass(frozen=True)
class DialerContactFields:
    """What the caller sees for a contact; None fields are sent as empty."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    information: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None       # property street: BatchDialer's standard Address field,
    city: Optional[str] = None          # which (unlike custom fields) the agent script can show
    state: Optional[str] = None
    postal_code: Optional[str] = None
    customfields: Optional[Mapping[str, str]] = None  # merged over what the dialer already holds
    # Context-card values (src/lending/context_card.py), sent as extra custom fields.
    card: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ContactUpsertResult:
    contact_id: Any
    created: bool


class UnconfirmedCapability(DialerRemovalUndecided):
    """The dialer endpoint for this action is not confirmed with the vendor yet."""


class DialerRequestError(RuntimeError):
    """A dialer request failed; ``status`` is the HTTP status when there was one.

    ``maybe_created`` is set only on a create whose outcome is unknown (timeout, 5xx, a 2xx
    with no contact id): the contact may exist even though the call reported failure."""

    def __init__(self, message: str, status: Optional[int] = None, *, maybe_created: bool = False) -> None:
        super().__init__(message)
        self.status = status
        self.maybe_created = maybe_created


class UnreconciledContact(RuntimeError):
    """An earlier create for this phone failed ambiguously, so a dialer contact may exist that we
    hold no id for. Raised on opt-out so it stays pending (and alarms) instead of completing."""


class ContactFieldsNotSet(DialerRequestError):
    """The contact was created in the campaign but the follow-up field update failed.

    ``contact_id`` is live in the dialer, so the caller must record it: an opt-out can only
    delete contacts we have an id for, and a re-run should update it, not create another."""

    def __init__(self, contact_id: Any, cause: Exception) -> None:
        super().__init__(f"dialer contact {contact_id} created but its fields were not set",
                         status=getattr(cause, "status", None))
        self.contact_id = contact_id


class Dialer(Protocol):
    def upsert_contact(self, record: Mapping[str, Any]) -> Optional[str]: ...
    def remove(self, phone: str, *, reason: str) -> None: ...
    def restore(self, phone: str) -> None: ...
    def get_contact_customfields(self, contact_id: Any, *, quick: bool = False) -> dict: ...


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

    def get_contact_customfields(self, contact_id: Any, *, quick: bool = False) -> dict:
        return {}


class BatchDialerAdapter:
    def __init__(self, *, http: Http, endpoints: Mapping[str, Endpoint] = BATCHDIALER_ENDPOINTS,
                 contact_ids: Optional[Callable[[str], list[str]]] = None,
                 has_unconfirmed_create: Optional[Callable[[str], bool]] = None) -> None:
        self._http = http
        self._has_unconfirmed_create = has_unconfirmed_create
        self._endpoints = endpoints
        self._contact_ids = contact_ids
        self._campaign_ids: Optional[dict[str, Any]] = None

    def _call(self, action: str, payload: dict, **path_params: Any) -> Mapping[str, Any]:
        endpoint = self._endpoints.get(action)
        if endpoint is None:
            raise UnconfirmedCapability(f"BatchDialer endpoint '{action}' is not confirmed")
        method, path = endpoint
        return self._http(method, path.format(**path_params), json=None if method == "GET" else payload)

    def missing_for_load(self) -> list[str]:
        """Load endpoints still unconfirmed; a live load refuses while any is missing."""
        return [name for name in LOAD_ENDPOINTS if self._endpoints.get(name) is None]

    def upsert_contact(
        self,
        record_or_phone: Union[Mapping[str, Any], str],
        fields: Optional[DialerContactFields] = None,
        *,
        campaign: Optional[str] = None,
        vendor_contact_id: Optional[str] = None,
    ) -> Union[Optional[str], ContactUpsertResult]:
        """Two call shapes: ``upsert_contact(record)`` (rules code) returns the contact id;
        ``upsert_contact(phone, fields, campaign=)`` (dialer load) adds the contact straight
        into the campaign (``POST /contacts`` with ``campaignids``), then sets the context-card
        custom fields with a full update, which the bulk import does not carry."""
        if isinstance(record_or_phone, Mapping):
            body = self._call("contact_upsert", dict(record_or_phone))
            contact_id = _contact_id(body)
            return str(contact_id) if contact_id is not None else None
        phone, fields = record_or_phone, fields or DialerContactFields()
        if campaign is None:
            body = self._call("contact_upsert", _contact_body(phone, fields))
            contact_id = _contact_id(body)
        else:
            if self._endpoints.get("contacts_add_to_campaign") is None:
                raise UnconfirmedCapability("BatchDialer endpoint 'contacts_add_to_campaign' is not confirmed")
            campaign_id = self._campaign_id(campaign)
            try:
                body = self._call("contacts_add_to_campaign", {
                    "campaignids": [campaign_id],
                    "contacts": [_import_contact(phone, fields, vendor_contact_id)],
                })
            except DialerRequestError as exc:
                exc.maybe_created = exc.status is None or exc.status >= 500 or exc.status == 408
                raise
            if body.get("success") is False:
                raise DialerRequestError("BatchDialer contact import failed")
            ids = body.get("ids") or []
            contact_id = ids[0] if ids else None
        if contact_id is None:
            raise DialerRequestError("dialer returned no contact id", maybe_created=campaign is not None)
        if campaign is not None:
            try:
                self.update_contact(contact_id, fields, phone=phone, vendor_contact_id=vendor_contact_id)
            except Exception as exc:
                raise ContactFieldsNotSet(contact_id, exc) from exc
        return ContactUpsertResult(contact_id=contact_id, created=True)

    def update_contact(self, contact_id: Any, fields: DialerContactFields, *, phone: Optional[str] = None,
                       vendor_contact_id: Optional[str] = None) -> dict:
        """Full update (PUT): BatchDialer replaces every field it is not sent, so the phone and our
        vendor contact id are sent again, and the stored custom fields (text_consent, anything a
        caller set) are read first and merged. If that read fails nothing is sent."""
        if self._endpoints.get("contact_update") is None:
            raise UnconfirmedCapability("BatchDialer endpoint 'contact_update' is not confirmed")
        existing = self.get_contact_customfields(contact_id)
        body = _contact_body(phone, fields)
        if vendor_contact_id:
            body["vendorcontactid"] = vendor_contact_id
        body["customfields"] = {**existing, **body.get("customfields", {}), **(fields.customfields or {})}
        return dict(self._call("contact_update", body, id=contact_id))

    def get_contact_customfields(self, contact_id: Any, *, quick: bool = False) -> dict:
        """``quick``: one short attempt, no retries (the on-call consent read must not stall ingestion)."""
        endpoint = self._endpoints.get("contact_get")
        if endpoint is None:
            raise UnconfirmedCapability("BatchDialer endpoint 'contact_get' is not confirmed")
        path = endpoint[1].format(id=contact_id)
        body = self._http("GET", path, json=None, quick=True) if quick else self._http("GET", path, json=None)
        return dict((body or {}).get("customfields") or {})

    def _campaign_id(self, name: str) -> Any:
        if self._campaign_ids is None:
            rows = self._http("GET", "/campaigns", json=None)
            items = rows.get("items", []) if isinstance(rows, Mapping) else rows
            self._campaign_ids = {str(r.get("name")): r.get("id") for r in items}
        if name not in self._campaign_ids:
            raise DialerRequestError(f"no dialer campaign named '{name}'")
        return self._campaign_ids[name]

    def remove(self, phone: str, *, reason: str) -> None:
        # The public API has no DNC endpoint, so a permanent opt-out deletes every contact we
        # loaded for the number (our suppression list blocks any reload). Window/cap holds only
        # leave the campaign and never delete, so restore() can never undo an opt-out.
        if reason != RemovalReason.OPT_OUT.value:
            self._call("campaign_remove", {"phone": phone})
            return
        if self._contact_ids is None:
            raise UnconfirmedCapability("no dialer contact lookup configured for opt-out deletes")
        for contact_id in self._contact_ids(phone):
            try:
                self._call("contact_delete", {}, id=contact_id)
            except DialerRequestError as exc:
                if exc.status != 404:  # already gone: the opt-out is done for this contact
                    raise
        if self._has_unconfirmed_create is not None and self._has_unconfirmed_create(phone):
            raise UnreconciledContact("an earlier dialer create for this phone is unconfirmed")

    def restore(self, phone: str) -> None:
        self._call("campaign_restore", {"phone": phone})

    def call_transcript(self, call_record_id: Any) -> Optional[str]:
        """The call's transcript as "role: text" lines, or None when there is none.

        Reads ``GET /cdrs/{id}/transcription`` (public API docs, "Get Transcription
        (JSON)"), which returns timed segments of ``{time, role, text}``.
        """
        try:
            segments = self._call("call_transcription", {}, id=call_record_id)
        except DialerRequestError as exc:
            if exc.status == 404:
                return None
            raise
        items = segments.get("items", []) if isinstance(segments, Mapping) else segments
        lines = [
            f"{segment.get('role') or 'unknown'}: {' '.join(str(segment.get('text') or '').split())}"
            for segment in items or []
            if isinstance(segment, Mapping) and str(segment.get("text") or "").strip()
        ]
        return "\n".join(lines) or None


def _contact_id(body: Mapping[str, Any]) -> Any:
    return body.get("id") if body.get("id") is not None else body.get("contactId")


def _ten_digit(phone: str) -> str:
    """BatchDialer stores US numbers as 10 digits; contact update rejects E.164 ("+1...")."""
    digits = "".join(ch for ch in phone if ch.isdigit())
    return digits[1:] if len(digits) == 11 and digits.startswith("1") else digits


def _import_contact(phone: str, fields: DialerContactFields, vendor_contact_id: Optional[str]) -> dict:
    """One contact in the ``POST /contacts`` import shape (public API docs, "Add contacts")."""
    contact = {"firstname": fields.first_name or "", "lastname": fields.last_name or "",
               "email": fields.email or "", "phonenumber1": _ten_digit(phone),
               "addressline1": fields.address or "", "city": fields.city or "",
               "state": fields.state or "", "postalcode": fields.postal_code or ""}
    if vendor_contact_id:
        contact["vendorcontactid"] = vendor_contact_id
    return contact


def _contact_body(phone: Optional[str], fields: DialerContactFields) -> dict:
    """BatchDialer contact shape (public API docs, "Add single contact"; create/update/delete
    confirmed live 2026-09-30). Custom fields are free-form and hold what the caller sees."""
    body: dict[str, Any] = {
        "firstname": fields.first_name or "",
        "lastname": fields.last_name or "",
        "address": fields.address or "",
        "city": fields.city or "",
        "state": fields.state or "",
        "postalcode": fields.postal_code or "",
        "customfields": {
            **{f"card_{name}": value for name, value in fields.card.items()},
            "entity_name": fields.company_name or "",
            "details": fields.information or "",
            "email": fields.email or "",
        },
    }
    if phone is not None:
        body["phonenumbers"] = [{"phonenumber": _ten_digit(phone)}]
    return body


def _requests_http(api_key: str) -> Http:
    """GET/POST go through the repo retry helpers (network errors, 429, 5xx); a 4xx is
    final. Failures log method, path and status only: bodies carry phone numbers."""

    def call(method: str, path: str, *, json: Optional[dict] = None, quick: bool = False) -> Any:
        url = f"{BATCHDIALER_BASE_URL}{path}"
        kwargs = {"headers": {"X-ApiKey": api_key}, "timeout": BATCHDIALER_TIMEOUT_SECONDS}
        try:
            if quick:
                response = requests.get(url, headers=kwargs["headers"], timeout=BATCHDIALER_QUICK_TIMEOUT_SECONDS)
                response.raise_for_status()
            elif method == "GET":
                response = requests_get_with_retry(url, max_retries=3, retry_delay=2, **kwargs)
            elif method == "POST" and path not in _NO_RETRY_POSTS:
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
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            # A 2xx that is not JSON (proxy error page, truncated body) is a failed call,
            # not a crash: callers only handle DialerRequestError.
            logger.warning("[dialer] BatchDialer %s %s returned a non-JSON body", method, path)
            raise DialerRequestError(f"BatchDialer {method} {path}: non-JSON response") from exc

    return call


def get_dialer() -> Optional[Dialer]:
    """The configured dialer, or None when no BatchDialer key / base URL is set."""
    key = get_settings().batchdialer_api_key
    if key is None or not BATCHDIALER_BASE_URL:
        return None
    return BatchDialerAdapter(http=_requests_http(key.get_secret_value()), contact_ids=_loaded_contact_ids,
                              has_unconfirmed_create=_has_unconfirmed_create)


def _loaded_contact_ids(phone: str) -> list[str]:
    """Every dialer contact id ever loaded for the phone (active or not)."""
    from src.core.database import get_db_context

    with get_db_context() as db:
        return [str(r[0]) for r in db.execute(
            text("SELECT DISTINCT dialer_contact_id FROM lending.dialer_load_records "
                 "WHERE phone = :p AND dialer_contact_id IS NOT NULL"),
            {"p": normalize_phone(phone)},
        )]


def _has_unconfirmed_create(phone: str) -> bool:
    from src.core.database import get_db_context

    with get_db_context() as db:
        return db.execute(
            text("SELECT EXISTS (SELECT 1 FROM lending.dialer_unconfirmed_creates "
                 "WHERE phone = :p AND resolved_at IS NULL)"),
            {"p": normalize_phone(phone)},
        ).scalar_one()


def get_http() -> Optional[Http]:
    """The authenticated BatchDialer transport, or None when no key / base URL is set."""
    key = get_settings().batchdialer_api_key
    if key is None or not BATCHDIALER_BASE_URL:
        return None
    return _requests_http(key.get_secret_value())
