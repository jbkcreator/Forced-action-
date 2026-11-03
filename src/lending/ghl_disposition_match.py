"""GHL call outcomes (G3): the workflow webhook's event is stored, then matched onto the GHL call it belongs to.

GHL's call API carries no disposition (checked 2026-11-03), so a GHL workflow ("call disposition set" -> Webhook)
posts the outcome Josh picked. The call itself reaches lending.call_dispositions only through the GHL call import
(cron */15), so the outcome usually arrives first: it waits in lending.ghl_disposition_events and
:func:`attach_pending` (run by the webhook and after every import) attaches it once the call is stored.

Matching (:func:`choose_call`):
* the event names the GHL call (message id): only that call, ``ghl:<id>``; never a guess, even when it is not
  imported yet. It may replace an earlier outcome on the same call (the caller changed it)
* otherwise: the latest outbound GHL call to the same phone, with no outcome yet, started within
  GHL_DISPOSITION_MATCH_WINDOW_MINUTES of the event's call time (or of its arrival when GHL sends no time)
* a call already marked DNC_REQUEST is never overwritten (the BatchDialer rule)

A matched call gets the code, the list version and the default unfunded cause, and is then delivered to the Sheet and
Slack like a BatchDialer call (the caller commits first; :func:`deliver_matched`). Acting on the outcome (DNC opt-out,
consent) is Dev C's lane and is not done here. The event's ``gate_failed`` is written onto the call (NURTURE + gate
failed counts as Gated / Nurture sent), and its user name ({{user.name}}) fills the call's caller only when the import
could not name one (no ``userId`` on the call message: caller_name NULL or "GHL (unknown)").

The event table may not exist yet (the code can deploy before migrations/apply_lending_ghl_disposition_events.py
runs): :func:`attach_pending` then logs and matches nothing, inside a savepoint so the caller's transaction is kept.
"""
from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import text

from config.lending_dispositions import (
    GHL_UNKNOWN_CALLER,
    DEFAULT_CAUSE,
    DISPOSITIONS,
    DISPOSITION_LIST_VERSION,
    DNC_CODE,
    GHL_DISPOSITION_FIELD_CANDIDATES,
    GHL_DISPOSITION_LABELS,
    GHL_DISPOSITION_DEDUPE_MINUTES,
    GHL_DISPOSITION_MATCH_BATCH,
    GHL_DISPOSITION_MATCH_WINDOW_MINUTES,
    GHL_DISPOSITION_PENDING_DAYS,
)
from config.lending_followup import CALL_SOURCE_GHL, GHL_CALL_ID_PREFIX
from src.lending.dispositions import _to_dt, normalize_code
from src.lending.ghl_consent_events import InvalidEvent, _sources
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

# What a GHL merge field becomes when it does not resolve: such a value is treated as not sent.
_UNRESOLVED = frozenset({"null", "none", "undefined", "nan"})
_TRUE = frozenset({"true", "yes", "y", "1", "gate_failed"})
_FALSE = frozenset({"false", "no", "n", "0"})

REASON_NO_DISPOSITION = "disposition is required (check the GHL merge field)"
REASON_UNKNOWN_DISPOSITION = "disposition is not one of the call outcomes"
REASON_NO_KEY = "phone or call_id is required"
INVALID_EVENT_REASONS = frozenset({REASON_NO_DISPOSITION, REASON_UNKNOWN_DISPOSITION, REASON_NO_KEY})


class InvalidDispositionEvent(InvalidEvent):
    """An event that can never be used. ``reason`` is a fixed, client-safe message (one of INVALID_EVENT_REASONS)."""

    def __init__(self, message: str, *, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class GhlDispositionEvent:
    contact_id: Optional[str]
    phone: Optional[str]
    disposition_raw: str
    code: str
    ghl_call_id: Optional[str]
    caller_name: Optional[str]
    called_at: Optional[datetime]
    gate_failed: Optional[bool]


def _value(body: Mapping[str, Any], name: str) -> Optional[str]:
    for source in _sources(dict(body)):
        for key in GHL_DISPOSITION_FIELD_CANDIDATES[name]:
            raw = source.get(key)
            if raw in (None, ""):
                continue
            value = str(raw).strip()
            if value and value.lower() not in _UNRESOLVED and not value.startswith("{{"):
                return value
    return None


def ghl_code(raw: Optional[str]) -> Optional[str]:
    """Our code for a GHL label, a ``disp:<CODE>`` tag or a code; None when it is none of the 10 (or a legacy code)."""
    if not raw:
        return None
    key = re.sub(r"[^A-Z0-9]+", "_", raw.upper()).strip("_")
    key = key[len("DISP_"):] if key.startswith("DISP_") else key
    if key in GHL_DISPOSITION_LABELS:
        return GHL_DISPOSITION_LABELS[key]
    code, _ = normalize_code(key)
    return code if code in DISPOSITIONS else None


def _gate_failed(raw: Optional[str]) -> Optional[bool]:
    if raw is None:
        return None
    lowered = raw.lower()
    if lowered in _TRUE or "gate_failed" in lowered:
        return True
    return False if lowered in _FALSE else None


def parse_event(body: Mapping[str, Any]) -> GhlDispositionEvent:
    """The event, or InvalidDispositionEvent when it can never be used (no outcome, an outcome outside the 10, or nothing to
    match it on)."""
    raw = _value(body, "disposition")
    if not raw:
        raise InvalidDispositionEvent(REASON_NO_DISPOSITION, reason=REASON_NO_DISPOSITION)
    code = ghl_code(raw)
    if code is None:
        raise InvalidDispositionEvent(REASON_UNKNOWN_DISPOSITION, reason=REASON_UNKNOWN_DISPOSITION)
    raw_phone = _value(body, "phone")
    phone = normalize(raw_phone) if raw_phone else None
    call_id = _value(body, "call_id")
    if not phone and not call_id:
        raise InvalidDispositionEvent(REASON_NO_KEY, reason=REASON_NO_KEY)
    return GhlDispositionEvent(
        contact_id=_value(body, "contact_id"), phone=phone, disposition_raw=raw[:100], code=code,
        ghl_call_id=call_id[:90] if call_id else None, caller_name=(_value(body, "user") or "")[:200] or None,
        called_at=_to_dt(_value(body, "called_at")), gate_failed=_gate_failed(_value(body, "gate_failed")),
    )


_INSERT = text("""
    INSERT INTO lending.ghl_disposition_events
        (contact_id, phone, disposition_raw, code, gate_failed, ghl_call_id, caller_name, called_at, raw_event)
    VALUES (:contact_id, :phone, :raw, :code, :gate_failed, :call_id, :caller, :called_at, CAST(:raw_event AS jsonb))
    RETURNING id
""")


# A GHL webhook retry repeats the same body: an identical event received within GHL_DISPOSITION_DEDUPE_MINUTES is that
# retry, not a second outcome. The advisory lock (released at commit) makes two concurrent retries wait for each other.
_LOCK_DELIVERY = text("SELECT pg_advisory_xact_lock(hashtext(:key))")
_FIND_REPEAT = text("""
    SELECT id FROM lending.ghl_disposition_events
     WHERE code = :code AND phone IS NOT DISTINCT FROM :phone AND ghl_call_id IS NOT DISTINCT FROM :call_id
       AND called_at IS NOT DISTINCT FROM :called_at AND caller_name IS NOT DISTINCT FROM :caller
       AND received_at > now() - make_interval(mins => :minutes)
     ORDER BY id LIMIT 1
""")


def record_event(db, ev: GhlDispositionEvent, body: Mapping[str, Any]) -> tuple[int, bool]:
    """Store the event once: returns ``(event id, duplicate)``; a repeated delivery returns the stored event's id."""
    key = "|".join(str(v) for v in ("ghl-disposition", ev.phone, ev.code, ev.ghl_call_id, ev.called_at,
                                    ev.caller_name))
    db.execute(_LOCK_DELIVERY, {"key": key})
    existing = db.execute(_FIND_REPEAT, {
        "code": ev.code, "phone": ev.phone, "call_id": ev.ghl_call_id, "called_at": ev.called_at,
        "caller": ev.caller_name, "minutes": GHL_DISPOSITION_DEDUPE_MINUTES,
    }).scalar()
    if existing is not None:
        return existing, True
    event_id = db.execute(_INSERT, {
        "contact_id": ev.contact_id, "phone": ev.phone, "raw": ev.disposition_raw, "code": ev.code,
        "gate_failed": ev.gate_failed, "call_id": ev.ghl_call_id, "caller": ev.caller_name, "called_at": ev.called_at,
        "raw_event": json.dumps({k: body.get(k) for k in ("contact_id", "disposition", "call_id", "called_at")},
                                default=str),
    }).scalar_one()
    return event_id, False


def choose_call(event: Mapping[str, Any], calls: Iterable[Mapping[str, Any]],
                window_minutes: int = GHL_DISPOSITION_MATCH_WINDOW_MINUTES) -> Optional[Mapping[str, Any]]:
    """The call this outcome belongs to, or None (see the module docstring)."""
    if event.get("ghl_call_id"):
        exact = f"{GHL_CALL_ID_PREFIX}{event['ghl_call_id']}"
        call = next((c for c in calls if c["dialer_call_id"] == exact), None)
        if call is None or (call["disposition"] == DNC_CODE and event["code"] != DNC_CODE):
            return None
        return call
    ref = event.get("called_at") or event["received_at"]
    window = timedelta(minutes=window_minutes)
    open_calls = [c for c in calls if c["disposition"] is None and c["call_started_at"] is not None
                  and abs(c["call_started_at"] - ref) <= window]
    return max(open_calls, key=lambda c: c["call_started_at"], default=None)


# Only events that have a call to land on are read, so outcomes that cannot match yet never fill the batch and hold
# back newer ones. Mirrors choose_call: the exact call (unless it is DNC and the event is not), else an open outbound
# GHL call to the phone within the window.
_PENDING = text("""
    SELECT e.id, e.phone, e.code, e.ghl_call_id, e.called_at, e.received_at, e.caller_name, e.gate_failed
      FROM lending.ghl_disposition_events e
     WHERE e.matched_call_id IS NULL AND e.received_at >= :since
       AND EXISTS (SELECT 1 FROM lending.call_dispositions d
                    WHERE d.call_source = :source AND d.direction = 'outbound'
                      AND ((e.ghl_call_id IS NOT NULL AND d.dialer_call_id = :prefix || e.ghl_call_id
                            AND (d.disposition IS DISTINCT FROM :dnc OR e.code = :dnc))
                        OR (e.ghl_call_id IS NULL AND d.phone = e.phone AND d.disposition IS NULL
                            AND d.call_started_at BETWEEN COALESCE(e.called_at, e.received_at) - make_interval(mins => :w)
                                                      AND COALESCE(e.called_at, e.received_at) + make_interval(mins => :w))))
     ORDER BY e.received_at, e.id LIMIT :limit FOR UPDATE OF e SKIP LOCKED
""")
# One read for every pending event: its exact call, or the open outbound GHL calls to its phone around its time.
# Locked in id order (two matchers never deadlock): a concurrent writer waits, and Postgres re-checks the WHERE on the
# latest row version, so the rows read here stay current until commit.
_CANDIDATES = text("""
    SELECT id, dialer_call_id, phone, call_started_at, disposition, unfunded_cause FROM lending.call_dispositions
     WHERE call_source = :source AND direction = 'outbound'
       AND (dialer_call_id = ANY(:call_ids)
            OR (phone = ANY(:phones) AND disposition IS NULL AND call_started_at BETWEEN :lo AND :hi))
     ORDER BY id FOR UPDATE
""")
# The write repeats choose_call's rule: only an open call or the exact call the event names, and a DNC is never
# replaced by another outcome.
_SET_CALL = text("""
    UPDATE lending.call_dispositions SET disposition = :d, disposition_at = clock_timestamp(),
           disposition_list_version = :v, unfunded_cause = COALESCE(unfunded_cause, :cause), gate_failed = :gate,
           caller_name = CASE WHEN caller_name IS NULL OR caller_name = :unknown_caller
                              THEN COALESCE(:caller, caller_name) ELSE caller_name END,
           unfunded_cause_provisional = CASE WHEN unfunded_cause IS NULL AND :cause IS NOT NULL THEN true
                                             ELSE unfunded_cause_provisional END
     WHERE id = :id AND (disposition IS NULL OR dialer_call_id = :exact)
       AND (disposition IS DISTINCT FROM :dnc OR :d = :dnc)
""")
_SET_EVENT = text("UPDATE lending.ghl_disposition_events SET matched_call_id = :call, matched_at = :at WHERE id = :id")


def _candidates(db, events: list[Mapping[str, Any]], window: timedelta) -> list[dict[str, Any]]:
    refs = [e.get("called_at") or e["received_at"] for e in events]
    rows = db.execute(_CANDIDATES, {
        "source": CALL_SOURCE_GHL,
        "call_ids": [f"{GHL_CALL_ID_PREFIX}{e['ghl_call_id']}" for e in events if e.get("ghl_call_id")],
        "phones": sorted({e["phone"] for e in events if e.get("phone") and not e.get("ghl_call_id")}),
        "lo": min(refs) - window, "hi": max(refs) + window,
    }).mappings().all()
    return [dict(r) for r in rows]


def attach_pending(db, now: datetime, *, window_minutes: int = GHL_DISPOSITION_MATCH_WINDOW_MINUTES) -> list[int]:
    """Attach every pending outcome that now has its call; returns the matched call ids (deliver them after the
    caller commits). Does not commit. Never raises: a missing table or a failed query matches nothing."""
    try:
        with db.begin_nested():
            return _attach(db, now, timedelta(minutes=window_minutes), window_minutes)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.warning("[ghl-disposition] matching skipped: %s", type(exc).__name__)
        return []


def _attach(db, now: datetime, window: timedelta, window_minutes: int) -> list[int]:
    events = [dict(e) for e in db.execute(_PENDING, {
        "since": now - timedelta(days=GHL_DISPOSITION_PENDING_DAYS), "limit": GHL_DISPOSITION_MATCH_BATCH,
        "source": CALL_SOURCE_GHL, "prefix": GHL_CALL_ID_PREFIX, "dnc": DNC_CODE, "w": window_minutes,
    }).mappings().all()]
    if not events:
        return []
    calls = _candidates(db, events, window)
    by_phone: dict[Optional[str], list[dict[str, Any]]] = {}
    by_id = {c["dialer_call_id"]: c for c in calls}
    for c in calls:
        by_phone.setdefault(c["phone"], []).append(c)
    matched: list[int] = []
    call_writes: list[dict[str, Any]] = []
    event_writes: list[dict[str, Any]] = []
    for event in events:
        exact = f"{GHL_CALL_ID_PREFIX}{event['ghl_call_id']}" if event.get("ghl_call_id") else None
        pool = ([by_id[exact]] if exact in by_id else []) if exact else by_phone.get(event.get("phone"), [])
        call = choose_call(event, pool, window_minutes)
        if call is None:
            continue
        cause = DEFAULT_CAUSE.get(event["code"]) if not call.get("unfunded_cause") else None
        call_writes.append({"d": event["code"], "v": DISPOSITION_LIST_VERSION, "cause": cause, "id": call["id"],
                            "gate": event.get("gate_failed"), "caller": event.get("caller_name"),
                            "unknown_caller": GHL_UNKNOWN_CALLER, "exact": exact, "dnc": DNC_CODE})
        event_writes.append({"call": call["id"], "at": now, "id": event["id"]})
        call["disposition"] = event["code"]  # taken: a second outcome for the same phone needs another call
        if call["id"] not in matched:
            matched.append(call["id"])
    if call_writes:
        db.execute(_SET_CALL, call_writes)
        db.execute(_SET_EVENT, event_writes)
    if matched:
        logger.info("[ghl-disposition] matched %d outcome(s) onto GHL calls", len(matched))
    return matched


def deliver_matched(call_ids: Iterable[int], deliver: Optional[Callable[[int], None]] = None) -> None:
    """Sheet + Slack for each matched call (each delivery never raises). Call after the matching commit."""
    if deliver is None:
        from src.lending.disposition_delivery import deliver_disposition as deliver
    for call_id in call_ids:
        deliver(call_id)
