"""BatchDialer call results on the GHL contact, so Josh sees the ISA's calls before he calls (one direction only).

When a BatchDialer call ends, the GHL contact on that phone gets the tag ``called-in-batchdialer``, plus ``bd-connected`` (someone picked up), ``bd-callback`` or
``bd-booked`` by result. With LENDING_GHL_CF_LAST_BD_CALL set, the contact's "last BatchDialer call" field is set too,
e.g. "2026-11-02 10:14 ET, no answer, 2 BatchDialer calls". Every lead is covered: the GHL contact id stored on the
newest LendingFlow or website lead is used when there is one; any other loaded number (a cold list lead, pushed to GHL by
the dialer-load sync) is found in GHL by phone (contact search), once per run. A phone with no GHL contact yet counts an
attempt and is retried, so a contact the sync creates a few minutes later still gets the result. With LENDING_GHL_CF_LAST_BD_RESULT set, the GHL dropdown field "Last BatchDialer result" (options = the 10
result codes) holds the latest result code, so Josh's smart lists can filter on it and a contact never shows two
results at once; the tags stay yes/no facts. Tags go through the add-tags call only (GHL's upsert REPLACES tags) and the field through a contact update
that carries no tags; tags are only ever added, never removed.

A call is written once, and again when its result code changes (callers often set the code during wrap-up, after the
call ended). ``call_pipeline.follow_up`` starts the write in a daemon thread so the CDR poller is never held up by
GHL; the LendingFlow sweep retries failed writes up to BD_NOTE_MAX_ATTEMPTS times for BD_NOTE_LOOKBACK_HOURS. Skipped
while the lending emergency halt is on (a tag can start a GHL workflow). Off until LENDING_BD_RESULT_TO_GHL_ENABLED=true.
"""
from __future__ import annotations

import logging
import threading
from contextlib import AbstractContextManager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_dispositions import ANSWERED_CODES, BOOKED_CODE, DISPOSITIONS, LEGACY_DISPOSITION_ALIASES
from config.lending_followup import CALL_SOURCE_BATCHDIALER
from config.lending_lendingflow import (
    BD_NOTE_LOOKBACK_HOURS,
    BD_NOTE_MAX_ATTEMPTS,
    GHL_CONFIG_ERROR_STATUS_CODES,
    GHL_TAG_BD_BOOKED,
    GHL_TAG_BD_CALLBACK,
    GHL_TAG_BD_CONNECTED,
    GHL_TAG_CALLED_IN_BATCHDIALER,
)
from config.settings import get_settings
from src.lending.global_halt import is_globally_halted

logger = logging.getLogger(__name__)

CALLBACK_CODE = "CALLBACK_REQUESTED"
NOTE_TIMEZONE = ZoneInfo("America/New_York")
SWEEP_LIMIT = 50

SessionFactory = Callable[[], AbstractContextManager[Session]]


class GhlNoteWriter(Protocol):
    def add_tags(self, contact_id: str, tags: list[str]) -> None: ...
    def set_field(self, contact_id: str, field_id: str, value: str) -> None: ...
    def find_contact_id(self, phone: str) -> Optional[str]: ...


class GhlWriteError(RuntimeError):
    """A GHL write failed; the message holds the step and HTTP status only."""


class LendingGhlNoteWriter:
    def __init__(self, account: Any) -> None:
        self._account = account

    def _call(self, step: str, method: str, path: str, body: Mapping[str, Any]) -> dict[str, Any]:
        from src.lending.ghl_account import ghl_headers
        from src.services import ghl_webhook

        try:
            response = ghl_webhook._ghl_request(method, f"{ghl_webhook._GHL_BASE}{path}",
                                                headers=ghl_headers(self._account.api_key), json=dict(body))
        except Exception as exc:
            raise GhlWriteError(f"{step}: {type(exc).__name__}") from exc
        if response.status_code >= 400:
            level = logging.ERROR if response.status_code in GHL_CONFIG_ERROR_STATUS_CODES else logging.WARNING
            logger.log(level, "[bd-result-to-ghl] %s refused: HTTP %s", step, response.status_code)
            raise GhlWriteError(f"{step}: HTTP {response.status_code}")
        try:
            return dict(response.json() or {})
        except ValueError:
            return {}

    def find_contact_id(self, phone: str) -> Optional[str]:
        """The GHL contact on this phone in the Next Deal Lending sub-account, or None."""
        found = self._call("search contact", "POST", "/contacts/search", {
            "locationId": self._account.location_id, "page": 1, "pageLimit": 1,
            "filters": [{"field": "phone", "operator": "eq", "value": phone}],
        })
        contacts = found.get("contacts") or []
        return str(contacts[0]["id"]) if contacts and contacts[0].get("id") else None

    def add_tags(self, contact_id: str, tags: list[str]) -> None:
        self._call("add tags", "POST", f"/contacts/{contact_id}/tags", {"tags": tags})

    def set_field(self, contact_id: str, field_id: str, value: str) -> None:
        self._call("set field", "PUT", f"/contacts/{contact_id}", {"customFields": [{"id": field_id, "value": value}]})


def result_tags(disposition: Optional[str]) -> list[str]:
    tags = [GHL_TAG_CALLED_IN_BATCHDIALER]
    if disposition in ANSWERED_CODES:
        tags.append(GHL_TAG_BD_CONNECTED)
    if disposition == CALLBACK_CODE:
        tags.append(GHL_TAG_BD_CALLBACK)
    if disposition == BOOKED_CODE:
        tags.append(GHL_TAG_BD_BOOKED)
    return tags


def result_code(disposition: Optional[str]) -> Optional[str]:
    """The value for the GHL dropdown "Last BatchDialer result": one of the current DISPOSITIONS (a legacy code is
    read through LEGACY_DISPOSITION_ALIASES), or None when there is no result yet or it is not a known code."""
    if not disposition:
        return None
    code = LEGACY_DISPOSITION_ALIASES.get(disposition, disposition)
    return code if code in DISPOSITIONS else None


def last_call_text(ended: datetime, disposition: Optional[str], calls: int) -> str:
    result = disposition.replace("_", " ").lower() if disposition else "no result yet"
    return (f"{ended.astimezone(NOTE_TIMEZONE):%Y-%m-%d %H:%M} ET, {result}, "
            f"{calls} BatchDialer call{'s' if calls != 1 else ''}")


# Due calls, locked so the follow-up thread and the sweep never write the same call at once. A call qualifies when its
# phone has a lead GHL contact id or was loaded into BatchDialer (every lead we dial); anything else is left out before
# the LIMIT so it can never fill a batch. ``ghl_contact_id`` is the newest lead's, else NULL (found by phone).
_DUE = """
    WITH due AS (
        SELECT d.id, d.phone, d.disposition, d.call_ended_at
          FROM lending.call_dispositions d
         WHERE d.call_source = :source AND d.call_ended_at IS NOT NULL AND d.phone IS NOT NULL
           AND d.call_ended_at > :since AND d.ghl_note_attempts < :max_attempts
           AND (d.ghl_note_at IS NULL OR d.disposition IS DISTINCT FROM d.ghl_note_disposition)
           AND (EXISTS (SELECT 1 FROM lending.lendingflow_leads l
                         WHERE l.phone = d.phone AND l.ghl_contact_id IS NOT NULL)
                OR EXISTS (SELECT 1 FROM lending.web_leads w
                            WHERE w.phone = d.phone AND w.ghl_contact_id IS NOT NULL)
                OR EXISTS (SELECT 1 FROM lending.dialer_load_records r WHERE r.phone = d.phone))
           {row_filter}
         ORDER BY d.call_ended_at LIMIT :limit
           FOR UPDATE OF d SKIP LOCKED)
    SELECT due.id, due.phone, due.disposition, due.call_ended_at, c.ghl_contact_id,
           (SELECT count(*) FROM lending.call_dispositions x
             WHERE x.phone = due.phone AND x.call_source = :source
               AND (x.direction = 'outbound' OR x.direction IS NULL)) AS calls
      FROM due
      LEFT JOIN LATERAL (
            SELECT ghl_contact_id FROM (
                SELECT ghl_contact_id, received_at FROM lending.lendingflow_leads
                 WHERE phone = due.phone AND ghl_contact_id IS NOT NULL
                UNION ALL
                SELECT ghl_contact_id, received_at FROM lending.web_leads
                 WHERE phone = due.phone AND ghl_contact_id IS NOT NULL) leads
             ORDER BY received_at DESC LIMIT 1) c ON true
"""


def _contact_by_phone(writer: GhlNoteWriter, phone: str, cache: dict[str, Optional[str]]) -> Optional[str]:
    """One GHL search per phone per run (a phone often has several due calls)."""
    if phone not in cache:
        cache[phone] = writer.find_contact_id(phone)
    return cache[phone]


def write_due_notes(db: Session, writer: GhlNoteWriter, *, row_id: Optional[int] = None,
                    now: Optional[datetime] = None) -> int:
    """Write the due BatchDialer results (one call with ``row_id``, else a sweep batch) to GHL. Each call is
    committed on its own. Returns how many were written. Never raises per call."""
    now = now or datetime.now(timezone.utc)
    params: dict[str, Any] = {"source": CALL_SOURCE_BATCHDIALER, "since": now - timedelta(hours=BD_NOTE_LOOKBACK_HOURS),
                              "max_attempts": BD_NOTE_MAX_ATTEMPTS, "limit": 1 if row_id is not None else SWEEP_LIMIT}
    if row_id is not None:
        params["row_id"] = row_id
    sql = text(_DUE.format(row_filter="AND d.id = :row_id" if row_id is not None else ""))
    rows = db.execute(sql, params).mappings().all()
    settings = get_settings()
    field_id, result_field_id = settings.lending_ghl_cf_last_bd_call, settings.lending_ghl_cf_last_bd_result
    written = 0
    found_by_phone: dict[str, Optional[str]] = {}
    for row in rows:
        result = result_code(row["disposition"])
        try:
            contact_id = row["ghl_contact_id"] or _contact_by_phone(writer, row["phone"], found_by_phone)
            if contact_id is None:
                raise GhlWriteError("no GHL contact on this phone yet")
            writer.add_tags(contact_id, result_tags(row["disposition"]))
            if field_id:
                writer.set_field(contact_id, field_id,
                                 last_call_text(row["call_ended_at"], row["disposition"], int(row["calls"])))
            if result_field_id and result:
                writer.set_field(contact_id, result_field_id, result)
        except Exception as exc:  # class only
            logger.warning("[bd-result-to-ghl] call row %s not written to GHL: %s", row["id"], type(exc).__name__)
            db.execute(text("UPDATE lending.call_dispositions SET ghl_note_attempts = ghl_note_attempts + 1 "
                            "WHERE id = :id"), {"id": row["id"]})
            continue
        db.execute(text("UPDATE lending.call_dispositions SET ghl_note_at = :now, ghl_note_disposition = :code, "
                        "ghl_note_attempts = 0 WHERE id = :id"),
                   {"now": now, "code": row["disposition"], "id": row["id"]})
        written += 1
    db.commit()
    return written


def _live_writer() -> Optional[GhlNoteWriter]:
    from src.lending.ghl_account import lending_ghl_account

    account = lending_ghl_account()
    return LendingGhlNoteWriter(account) if account is not None else None


def _enabled() -> bool:
    return get_settings().lending_bd_result_to_ghl_enabled and not is_globally_halted()


def _default_sessions() -> AbstractContextManager[Session]:
    from src.lending.db import lending_session

    return lending_session()


def write_notes(row_id: Optional[int] = None, *, sessions: SessionFactory = _default_sessions,
                writer_factory: Callable[[], Optional[GhlNoteWriter]] = _live_writer) -> int:
    """Own session, committed. Never raises."""
    if not _enabled():
        return 0
    writer = writer_factory()
    if writer is None:
        return 0
    try:
        with sessions() as db:
            return write_due_notes(db, writer, row_id=row_id)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[bd-result-to-ghl] write failed: %s", type(exc).__name__)
        return 0


def write_note_in_background(row_id: int) -> None:
    """``call_pipeline.follow_up`` task: the GHL write runs in a daemon thread, so the CDR poller never waits on GHL.
    A write lost to a restart is picked up by the sweep."""
    if not _enabled():
        return
    threading.Thread(target=write_notes, args=(row_id,), name=f"bd-result-to-ghl-{row_id}", daemon=True).start()
