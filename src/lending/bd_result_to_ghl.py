"""BatchDialer call results on the GHL contact, so Josh sees the ISA's calls before he calls (one direction only).

When a BatchDialer call ends, the GHL contact of the lead on that phone (the newest LendingFlow or website lead with a
GHL contact id) gets the tag ``called-in-batchdialer``, plus ``bd-connected`` (someone picked up), ``bd-callback`` or
``bd-booked`` by result. With LENDING_GHL_CF_LAST_BD_CALL set, the contact's "last BatchDialer call" field is set too,
e.g. "2026-11-02 10:14 ET, no answer, 2 BatchDialer calls". Phones with no GHL contact id in our lead tables are
skipped. Tags go through the add-tags call only (GHL's upsert REPLACES tags) and the field through a contact update
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

from config.lending_dispositions import ANSWERED_CODES, BOOKED_CODE
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


class GhlWriteError(RuntimeError):
    """A GHL write failed; the message holds the step and HTTP status only."""


class LendingGhlNoteWriter:
    def __init__(self, account: Any) -> None:
        self._account = account

    def _call(self, step: str, method: str, path: str, body: Mapping[str, Any]) -> None:
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


def last_call_text(ended: datetime, disposition: Optional[str], calls: int) -> str:
    result = disposition.replace("_", " ").lower() if disposition else "no result yet"
    return (f"{ended.astimezone(NOTE_TIMEZONE):%Y-%m-%d %H:%M} ET, {result}, "
            f"{calls} BatchDialer call{'s' if calls != 1 else ''}")


# Due calls, locked so the follow-up thread and the sweep never write the same call at once. The GHL contact is the
# newest LendingFlow or website lead on the phone that has one.
_DUE = """
    WITH due AS (
        SELECT d.id, d.phone, d.disposition, d.call_ended_at
          FROM lending.call_dispositions d
         WHERE d.call_source = :source AND d.call_ended_at IS NOT NULL AND d.phone IS NOT NULL
           AND d.call_ended_at > :since AND d.ghl_note_attempts < :max_attempts
           AND (d.ghl_note_at IS NULL OR d.disposition IS DISTINCT FROM d.ghl_note_disposition)
           {row_filter}
         ORDER BY d.call_ended_at LIMIT :limit
           FOR UPDATE OF d SKIP LOCKED)
    SELECT due.id, due.disposition, due.call_ended_at, c.ghl_contact_id,
           (SELECT count(*) FROM lending.call_dispositions x
             WHERE x.phone = due.phone AND x.call_source = :source
               AND (x.direction = 'outbound' OR x.direction IS NULL)) AS calls
      FROM due
      JOIN LATERAL (
            SELECT ghl_contact_id FROM (
                SELECT ghl_contact_id, received_at FROM lending.lendingflow_leads
                 WHERE phone = due.phone AND ghl_contact_id IS NOT NULL
                UNION ALL
                SELECT ghl_contact_id, received_at FROM lending.web_leads
                 WHERE phone = due.phone AND ghl_contact_id IS NOT NULL) leads
             ORDER BY received_at DESC LIMIT 1) c ON true
"""


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
    field_id = get_settings().lending_ghl_cf_last_bd_call
    written = 0
    for row in rows:
        try:
            writer.add_tags(row["ghl_contact_id"], result_tags(row["disposition"]))
            if field_id:
                writer.set_field(row["ghl_contact_id"], field_id,
                                 last_call_text(row["call_ended_at"], row["disposition"], int(row["calls"])))
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
