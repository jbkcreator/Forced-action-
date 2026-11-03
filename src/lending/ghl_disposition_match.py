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
    """The event, or InvalidEvent when it can never be used (no outcome, an outcome outside the 10, or nothing to
    match it on)."""
    raw = _value(body, "disposition")
    if not raw:
        raise InvalidEvent("disposition is required (check the GHL merge field)")
    code = ghl_code(raw)
    if code is None:
        raise InvalidEvent("disposition is not one of the call outcomes")
    raw_phone = _value(body, "phone")
    phone = normalize(raw_phone) if raw_phone else None
    call_id = _value(body, "call_id")
    if not phone and not call_id:
        raise InvalidEvent("phone or call_id is required")
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


def record_event(db, ev: GhlDispositionEvent, body: Mapping[str, Any]) -> int:
    """Store the event (one row per delivery: a repeat is harmless, the second match finds the call already set)."""
    return db.execute(_INSERT, {
        "contact_id": ev.contact_id, "phone": ev.phone, "raw": ev.disposition_raw, "code": ev.code,
        "gate_failed": ev.gate_failed, "call_id": ev.ghl_call_id, "caller": ev.caller_name, "called_at": ev.called_at,
        "raw_event": json.dumps({k: body.get(k) for k in ("contact_id", "disposition", "call_id", "called_at")},
                                default=str),
    }).scalar_one()


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


_PENDING = text("""
    SELECT id, phone, code, ghl_call_id, called_at, received_at, caller_name, gate_failed
      FROM lending.ghl_disposition_events
     WHERE matched_call_id IS NULL AND received_at >= :since
     ORDER BY received_at, id LIMIT :limit FOR UPDATE SKIP LOCKED
""")
# One read for every pending event: its exact call, or the open outbound GHL calls to its phone around its time.
_CANDIDATES = text("""
    SELECT id, dialer_call_id, phone, call_started_at, disposition, unfunded_cause FROM lending.call_dispositions
     WHERE call_source = :source AND direction = 'outbound'
       AND (dialer_call_id = ANY(:call_ids)
            OR (phone = ANY(:phones) AND disposition IS NULL AND call_started_at BETWEEN :lo AND :hi))
""")
_SET_CALL = text("""
    UPDATE lending.call_dispositions SET disposition = :d, disposition_at = clock_timestamp(),
           disposition_list_version = :v, unfunded_cause = COALESCE(unfunded_cause, :cause), gate_failed = :gate,
           caller_name = CASE WHEN caller_name IS NULL OR caller_name = :unknown_caller
                              THEN COALESCE(:caller, caller_name) ELSE caller_name END,
           unfunded_cause_provisional = CASE WHEN unfunded_cause IS NULL AND :cause IS NOT NULL THEN true
                                             ELSE unfunded_cause_provisional END
     WHERE id = :id
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
    events = [dict(e) for e in db.execute(_PENDING, {"since": now - timedelta(days=GHL_DISPOSITION_PENDING_DAYS),
                                                      "limit": GHL_DISPOSITION_MATCH_BATCH}).mappings().all()]
    if not events:
        return []
    calls = _candidates(db, events, window)
    by_phone: dict[Optional[str], list[dict[str, Any]]] = {}
    by_id = {c["dialer_call_id"]: c for c in calls}
    for c in calls:
        by_phone.setdefault(c["phone"], []).append(c)
    matched: list[int] = []
    for event in events:
        pool = ([by_id[k]] if (k := f"{GHL_CALL_ID_PREFIX}{event['ghl_call_id']}") in by_id else []) \
            if event.get("ghl_call_id") else by_phone.get(event.get("phone"), [])
        call = choose_call(event, pool, window_minutes)
        if call is None:
            continue
        cause = DEFAULT_CAUSE.get(event["code"]) if not call.get("unfunded_cause") else None
        db.execute(_SET_CALL, {"d": event["code"], "v": DISPOSITION_LIST_VERSION, "cause": cause, "id": call["id"],
                             "gate": event.get("gate_failed"), "caller": event.get("caller_name"),
                             "unknown_caller": GHL_UNKNOWN_CALLER})
        db.execute(_SET_EVENT, {"call": call["id"], "at": now, "id": event["id"]})
        call["disposition"] = event["code"]  # taken: a second outcome for the same phone needs another call
        if call["id"] not in matched:
            matched.append(call["id"])
    if matched:
        logger.info("[ghl-disposition] matched %d outcome(s) onto GHL calls", len(matched))
    return matched


def deliver_matched(call_ids: Iterable[int], deliver: Optional[Callable[[int], None]] = None) -> None:
    """Sheet + Slack for each matched call (each delivery never raises). Call after the matching commit."""
    if deliver is None:
        from src.lending.disposition_delivery import deliver_disposition as deliver
    for call_id in call_ids:
        deliver(call_id)
