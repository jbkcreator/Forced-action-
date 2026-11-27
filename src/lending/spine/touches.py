"""R1 1.1 touches: every call, text, email, note, stage and loan event for one spine contact.

Local touches come from NDL's own tables in one ``UNION ALL``; live touches from GHL (conversation messages and
contact notes) and Instantly (emails per address). Each touch carries a provider ref (``ghl_msg:<id>``,
``ghl_note:<id>``, ``instantly:<id>``, ``call:<dialer_call_id>``) so a touch held in two places is one touch. A
touch never carries message text or a phone number. Gmail or Hunter would be one more ``TouchSource``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Protocol

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from config.lending_contact_spine import BATCHDIALER_CONTACT, EMAIL, GHL_CONTACT, GHL_HISTORY_DAYS, PHONE
from src.lending.ghl_calls import message_time

logger = logging.getLogger(__name__)

_GHL_KINDS = {"TYPE_SMS": "text", "TYPE_EMAIL": "email", "TYPE_CAMPAIGN_CALL": "call", "TYPE_CALL": "call"}
_INSTANTLY_RECEIVED = 2  # Instantly v2 email ue_type: 2 = received from the lead


@dataclass(frozen=True)
class Touch:
    at: Optional[datetime]
    system: str
    kind: str
    direction: Optional[str]
    outcome: Optional[str]
    ref: str


@dataclass(frozen=True)
class ContactKeys:
    phones: tuple[str, ...]
    emails: tuple[str, ...]
    ghl_contact_ids: tuple[str, ...]
    dialer_contact_ids: tuple[str, ...]
    lendingflow_lead_ids: tuple[int, ...]
    loan_file_ids: tuple[int, ...]


class TouchSource(Protocol):
    system: str
    configured: bool

    def touches(self, keys: ContactKeys) -> list[Touch]: ...  # raises on failure


def _as_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, str):  # SQLite (tests) returns text
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def contact_keys(db: Session, contact_id: int) -> ContactKeys:
    ids: dict[str, list[str]] = {}
    for kind, value in db.execute(text("SELECT kind, value FROM lending.contact_identifiers WHERE contact_id = :c "
                                       "ORDER BY kind, value"), {"c": contact_id}):
        ids.setdefault(kind, []).append(value)
    refs: dict[str, list[int]] = {}
    for source, ref in db.execute(text("SELECT source, source_ref FROM lending.contact_sources WHERE contact_id = :c "
                                       "AND source IN ('lendingflow_leads', 'loan_files') ORDER BY source_ref"),
                                  {"c": contact_id}):
        if ref.isdigit():
            refs.setdefault(source, []).append(int(ref))
    return ContactKeys(
        phones=tuple(ids.get(PHONE, ())), emails=tuple(ids.get(EMAIL, ())),
        ghl_contact_ids=tuple(ids.get(GHL_CONTACT, ())), dialer_contact_ids=tuple(ids.get(BATCHDIALER_CONTACT, ())),
        lendingflow_lead_ids=tuple(sorted(refs.get("lendingflow_leads", ()))),
        loan_file_ids=tuple(sorted(refs.get("loan_files", ()))),
    )


# (sql, {param: ContactKeys field}): each branch keeps only the conditions whose key list is non-empty.
_SELECT = "SELECT {at} AS at, {system} AS system, {kind} AS kind, {direction} AS direction, {outcome} AS outcome, " \
          "{ref} AS ref FROM lending.{table} WHERE {where}"
_NULL = "CAST(NULL AS varchar)"
_BRANCHES: tuple[tuple[dict[str, str], tuple[tuple[str, str, str], ...]], ...] = (
    (dict(table="call_dispositions", at="COALESCE(call_started_at, created_at)",
          system="COALESCE(call_source, 'batchdialer')", kind="'call'", direction="direction", outcome="disposition",
          ref="'call:' || dialer_call_id", base="dialer_call_id IS NOT NULL"),
     (("phone IN :phones", "phones", "phones"), ("dialer_contact_id IN :dialer_ids", "dialer_ids", "dialer_contact_ids"))),
    (dict(table="missed_call_events", at="COALESCE(decided_at, created_at)", system="'ghl'", kind="'text'",
          direction="'outbound'", outcome="'missed-call text'",
          ref="CASE WHEN provider_message_id IS NOT NULL THEN 'ghl_msg:' || provider_message_id "
              "ELSE 'missed_call_events:' || id END", base="status = 'sent'"),
     (("phone IN :phones", "phones", "phones"),)),
    (dict(table="pending_actions", at="COALESCE(executed_at, created_at)",
          system="CASE WHEN channel LIKE 'instantly%' THEN 'instantly' WHEN channel LIKE 'ghl%' THEN 'ghl' "
                 "ELSE 'ndl' END",
          kind="CASE WHEN channel = 'ghl_sms' THEN 'text' WHEN channel LIKE '%email' THEN 'email' "
               "WHEN channel = 'ghl_note' THEN 'note' ELSE 'message' END",
          direction="'outbound'", outcome=_NULL,
          ref="CASE WHEN provider_ref IS NULL THEN 'pending_actions:' || id "
              "WHEN channel IN ('ghl_sms', 'ghl_email') THEN 'ghl_msg:' || provider_ref "
              "WHEN channel = 'ghl_note' THEN 'ghl_note:' || provider_ref "
              "WHEN channel LIKE 'instantly%' THEN 'instantly:' || provider_ref "
              "ELSE 'pending_actions:' || id END",
          # a cold email's provider ref is an Instantly lead id (the live email list has it) and a stage move has its
          # own ghl_stage_events row: either would show twice
          base="status = 'sent' AND channel NOT IN ('instantly_cold_email', 'ghl_stage')"),
     (("recipient_phone IN :phones", "phones", "phones"), ("lower(recipient_email) IN :emails", "emails", "emails"))),
    (dict(table="instantly_replies", at="received_at", system="'instantly'", kind="'email'", direction="'inbound'",
          outcome="intent",
          ref="CASE WHEN dedup_key LIKE 'reply:%' THEN 'instantly:' || substr(dedup_key, 7) "
              "ELSE 'instantly_replies:' || id END", base="1 = 1"),
     (("lower(lead_email) IN :emails", "emails", "emails"),)),
    (dict(table="ghl_stage_events", at="event_at", system="'ghl'", kind="'stage'", direction=_NULL,
          outcome="stage_name", ref="'ghl_stage_events:' || id", base="1 = 1"),
     (("phone IN :phones", "phones", "phones"),)),
    (dict(table="file_routing_events", at="created_at", system="'ndl'", kind="'routing'", direction=_NULL,
          outcome="action", ref="'file_routing_events:' || id", base="1 = 1"),
     (("lead_id IN :lead_ids", "lead_ids", "lendingflow_lead_ids"), ("loan_file_id IN :file_ids", "file_ids", "loan_file_ids"))),
    (dict(table="loan_file_status_events", at="at", system="'ndl'", kind="'loan_status'", direction=_NULL,
          outcome="status", ref="'loan_file_status_events:' || id", base="1 = 1"),
     (("loan_file_id IN :file_ids", "file_ids", "loan_file_ids"),)),
)


def local_touches(db: Session, keys: ContactKeys) -> list[Touch]:
    selects: list[str] = []
    params: dict[str, list] = {}
    for parts, conditions in _BRANCHES:
        live = [(sql, param, field) for sql, param, field in conditions if getattr(keys, field)]
        if not live:
            continue
        for _, param, field in live:
            params[param] = list(getattr(keys, field))
        where = f"{parts['base']} AND ({' OR '.join(sql for sql, _, _ in live)})"
        selects.append(_SELECT.format(where=where, **{k: v for k, v in parts.items() if k != "base"}))
    if not selects:
        return []
    statement = text(" UNION ALL ".join(selects)).bindparams(*(bindparam(p, expanding=True) for p in params))
    return [Touch(_as_datetime(r.at), r.system, r.kind, r.direction, r.outcome, r.ref)
            for r in db.execute(statement, params)]


class GhlTouches:
    """GHL conversation messages and contact notes per GHL contact id (Next Deal Lending sub-account only)."""

    system = "ghl"

    def __init__(self, account: Any, *, read_messages: Optional[Callable[[str], list]] = None,
                 read_notes: Optional[Callable[[str], list]] = None, now: Optional[datetime] = None) -> None:
        self.configured = account is not None
        self._now = now
        if account is not None:
            from src.lending.attempt_log import messages_reader

            self._read_messages = read_messages or messages_reader(account)
            self._read_notes = read_notes or _ghl_notes_reader(account)

    def touches(self, keys: ContactKeys) -> list[Touch]:
        since = (self._now or datetime.now(timezone.utc)) - timedelta(days=GHL_HISTORY_DAYS)
        found: list[Touch] = []
        for contact_id in keys.ghl_contact_ids:
            for message in self._read_messages(contact_id):
                touch = _ghl_message_touch(message)
                if touch and touch.at and touch.at >= since:
                    found.append(touch)
            for note in self._read_notes(contact_id):
                if note.get("id"):
                    found.append(Touch(message_time(note), "ghl", "note", None, None, f"ghl_note:{note['id']}"))
        return found


def _ghl_message_touch(message: Mapping[str, Any]) -> Optional[Touch]:
    message_type = message.get("messageType") or ""
    if not message.get("id"):
        return None
    # ghl_call_import stores a GHL call as call_dispositions "ghl:<message id>": the same ref, so it shows once
    ref = f"call:ghl:{message['id']}" if message_type == "TYPE_CALL" else f"ghl_msg:{message['id']}"
    return Touch(message_time(message), "ghl", _GHL_KINDS.get(message_type, "message"), message.get("direction"),
                 message.get("status"), ref)


def _ghl_notes_reader(account: Any) -> Callable[[str], list]:
    """UNVERIFIED: ``GET /contacts/{id}/notes`` returning ``{"notes": [{"id", "dateAdded", ...}]}`` (GHL API v2
    reference; this codebase only POSTs notes so far). Raises on an HTTP error."""
    from config.lending_alarms import GHL_TIMEOUT_SECONDS
    from src.lending.ghl_account import ghl_headers
    from src.services.ghl_webhook import _GHL_BASE
    from src.utils.http_helpers import requests_get_with_retry

    headers = ghl_headers(account.api_key)

    def read(contact_id: str) -> list:
        response = requests_get_with_retry(f"{_GHL_BASE}/contacts/{contact_id}/notes", headers=headers,
                                           max_retries=1, retry_delay=1, timeout=GHL_TIMEOUT_SECONDS)
        response.raise_for_status()
        return (response.json() or {}).get("notes") or []

    return read


class InstantlyTouches:
    """NDL Instantly emails per address (P1.3's ``LENDING_INSTANTLY_API_KEY`` account)."""

    system = "instantly"

    def __init__(self, account: Any, *, read_emails: Optional[Callable[[str], list]] = None) -> None:
        self.configured = account is not None
        if account is not None:
            self._read_emails = read_emails or _instantly_emails_reader(account)

    def touches(self, keys: ContactKeys) -> list[Touch]:
        found: list[Touch] = []
        for email in keys.emails:
            for item in self._read_emails(email):
                if not item.get("id"):
                    continue
                at = _as_datetime(item.get("timestamp_email") or item.get("timestamp_created"))
                direction = "inbound" if item.get("ue_type") == _INSTANTLY_RECEIVED else "outbound"
                found.append(Touch(at, "instantly", "email", direction, None, f"instantly:{item['id']}"))
        return found


def _instantly_emails_reader(account: Any) -> Callable[[str], list]:
    """UNVERIFIED: ``GET /api/v2/emails?lead=<email>`` returning ``{"items": [{"id", "ue_type", "timestamp_email"}]}``
    (Instantly public API v2 reference, not a captured response, as instantly_client.py notes)."""
    from src.utils.http_helpers import requests_get_with_retry

    headers = {"Authorization": f"Bearer {account.api_key}", "Accept": "application/json"}

    def read(email: str) -> list:
        response = requests_get_with_retry(f"{account.base_url}/api/v2/emails", headers=headers,
                                           params={"lead": email, "limit": 100}, max_retries=1, retry_delay=1,
                                           timeout=15)
        response.raise_for_status()
        return (response.json() or {}).get("items") or []

    return read
