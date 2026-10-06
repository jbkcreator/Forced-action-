"""Record dialer call events into lending.call_dispositions (spec §4.4).

One row per dialer call, keyed by the dialer's call id, so replays and
out-of-order events (a disposition before the call end) converge on one row.
Never commits: the caller owns the transaction.

``parse_event`` is the only place that knows the dialer's payload shape; the rest
works on the normalized ``DialerCallEvent``.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_compliance import DEFAULT_TZ
from config.lending_dispositions import (
    ABANDONED_RESULT,
    CDR_ANSWERED_STATUSES,
    BOOKED_CODE,
    DEFAULT_CAUSE,
    DISPOSITION_LIST_VERSION,
    DIALER_ORIGIN,
    DIRECTION_ALIASES,
    DISPOSITIONS,
    DNC_CODE,
    EVENT_FIELD_CANDIDATES,
    SYSTEM_DISPOSITION_ALIASES,
    UNANSWERED_CODES,
)
from config.lending_queues import NURTURE
from config.settings import get_settings
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DialerCallEvent:
    call_id: str
    contact_id: Optional[str]
    direction: Optional[str]
    phone: Optional[str]
    seat_id: Optional[str]
    seat_name: Optional[str]
    caller_id_number: Optional[str]
    campaign_id: Optional[str]
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    duration: Optional[int]
    disposition_raw: Optional[str]
    recording_ref: Optional[str]
    disclosure_logged: Optional[bool]
    raw: dict


@dataclass(frozen=True)
class RecordedCall:
    row_id: int
    call_id: str
    phone: Optional[str]
    caller_seat: Optional[str]
    disposition: Optional[str]
    opt_out_propagated: bool
    call_ended: bool
    dnc_requested: bool = False
    unknown_code: Optional[str] = None  # raw code the list does not know (alert once)
    unanswered: bool = False
    booking_blocked: bool = False
    dnc_removal_pending: bool = False  # opt-out recorded but the dialer removal is unconfirmed


def last4(phone: Optional[str]) -> str:
    return f"***{phone[-4:]}" if phone else "none"


def _dig(obj: Any, path: str) -> Any:
    for part in path.split("."):
        if not isinstance(obj, dict) or part not in obj:
            return None
        obj = obj[part]
    return obj


def _pick(data: dict, field: str) -> Any:
    for path in EVENT_FIELD_CANDIDATES[field]:
        value = _dig(data, path)
        if value not in (None, ""):
            return value
    return None


def _str(value: Any) -> Optional[str]:
    return str(value) if value not in (None, "") else None


def _to_dt(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
            number = float(value)
            return datetime.fromtimestamp(number / 1000 if number > 1e11 else number, tz=timezone.utc)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(float(value)) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def _direction(value: Any) -> Optional[str]:
    s = _str(value)
    if not s:
        return None
    direction = DIRECTION_ALIASES.get(s.lower())
    if direction is None:
        logger.warning("[lending] unknown call direction %r", s)
    return direction


def _seat_name(data: dict) -> Optional[str]:
    explicit = _str(_pick(data, "seat_name"))
    if explicit:
        return explicit
    parts = (_str(_dig(data, "agent.firstname")), _str(_dig(data, "agent.lastname")))
    return " ".join(p for p in parts if p) or None


def _recording(value: Any) -> Optional[str]:
    ref = _str(value)
    return DIALER_ORIGIN + ref if ref and ref.startswith("/") else ref


def parse_event(raw: Any) -> Optional[DialerCallEvent]:
    """Normalize one webhook / poll record. None when it carries no call id."""
    if not isinstance(raw, dict):
        return None
    data = raw["data"] if isinstance(raw.get("data"), dict) else raw
    call_id = _str(_pick(data, "call_id"))
    if not call_id:
        return None
    return DialerCallEvent(
        call_id=call_id,
        contact_id=_str(_pick(data, "contact_id")),
        direction=_direction(_pick(data, "direction")),
        phone=_str(_pick(data, "phone")),
        seat_id=_str(_pick(data, "seat_id")),
        seat_name=_seat_name(data),
        caller_id_number=_str(_pick(data, "caller_id_number")),
        campaign_id=_str(_pick(data, "campaign_id")),
        started_at=_to_dt(_pick(data, "started_at")),
        ended_at=_to_dt(_pick(data, "ended_at")),
        duration=_to_int(_pick(data, "duration")),
        disposition_raw=_str(_pick(data, "disposition")),
        recording_ref=_recording(_pick(data, "recording")),
        disclosure_logged=_to_bool(_pick(data, "disclosure")),
        raw=raw,
    )


def normalize_code(raw: Optional[str]) -> tuple[Optional[str], bool]:
    """(our code or None, known). Case and spacing are forgiven; the dialer's built-in
    results map through SYSTEM_DISPOSITION_ALIASES; anything else is unknown."""
    if not raw:
        return None, True
    key = re.sub(r"[^A-Z0-9]+", "_", raw.upper()).strip("_")
    if key in CDR_ANSWERED_STATUSES:
        return None, True
    if key in DISPOSITIONS:
        return key, True
    if key in SYSTEM_DISPOSITION_ALIASES:
        return SYSTEM_DISPOSITION_ALIASES[key], True
    return None, False


def is_abandoned(raw: Optional[str]) -> bool:
    return bool(raw) and re.sub(r"[^A-Z0-9]+", "_", raw.upper()).strip("_") == ABANDONED_RESULT


def seat_group_for(seat_id: Optional[str]) -> Optional[str]:
    if not seat_id:
        return None
    for part in get_settings().lending_seat_groups.split(","):
        seat, _, group = part.partition(":")
        if seat.strip() == seat_id and group.strip():
            return group.strip().upper()
    return None


def lookup_load_record(db, contact_id: Optional[str], phone: Optional[str],
                       started_at: Optional[datetime]) -> Optional[dict]:
    """Queue, campaign and borrower details from the dialer load table.

    Deliberately not filtered on ``active``: a DNC_REQUEST pulls the contact
    from the dialer before its disposition event is processed.
    """
    if not (contact_id or phone):
        return None
    if not db.execute(text("SELECT to_regclass('lending.dialer_load_records') IS NOT NULL")).scalar():
        return None
    cols = "pool, campaign_tag, borrower_name, entity_name, property_address"
    with db.begin_nested():
        if contact_id:
            row = db.execute(
                text(f"SELECT {cols} FROM lending.dialer_load_records "
                     "WHERE dialer_contact_id = :cid ORDER BY loaded_at DESC LIMIT 1"),
                {"cid": contact_id},
            ).mappings().first()
            if row:
                return dict(row)
        if phone:
            row = db.execute(
                text(f"SELECT {cols} FROM lending.dialer_load_records "
                     "WHERE phone = :phone AND loaded_at <= :at ORDER BY loaded_at DESC LIMIT 1"),
                {"phone": phone, "at": started_at or datetime.now(timezone.utc)},
            ).mappings().first()
            return dict(row) if row else None
    return None


def _effective_end(ev: DialerCallEvent) -> Optional[datetime]:
    """The end time that counts as an attempt. A disposition-only event (the dialer's
    push is tied to the disposition) has no end time: derive it, never leave the attempt uncounted."""
    if ev.ended_at:
        return ev.ended_at
    if not ev.disposition_raw:
        return None
    if ev.started_at:
        return ev.started_at + timedelta(seconds=ev.duration or 0)
    return datetime.now(timezone.utc)


def _is_unanswered(ev: DialerCallEvent, code: Optional[str], ended: bool) -> bool:
    if code:
        return code in UNANSWERED_CODES
    return ended and not ev.disposition_raw and (ev.duration or 0) == 0


def record_dialer_event(db, ev: DialerCallEvent) -> RecordedCall:
    """Upsert the call row for one dialer event and derive the follow-on facts."""
    phone = normalize(ev.phone) if ev.phone else None
    if ev.phone and not phone:
        logger.warning("[lending] call %s: dialed number could not be normalized", ev.call_id)
    ended_at = _effective_end(ev)
    code, known = normalize_code(ev.disposition_raw)

    previous = db.execute(
        text("SELECT disposition_raw FROM lending.call_dispositions WHERE dialer_call_id = :id"),
        {"id": ev.call_id},
    ).first()

    row = db.execute(
        text(
            "INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, caller_seat, caller_name, "
            "caller_id_number, dialer_contact_id, seat_group, talk_duration_sec, call_started_at, call_ended_at, "
            "recording_ref, recording_status, dialer_campaign_id, raw_event) "
            "VALUES (:call_id, :direction, :phone, :seat, :name, :did, :contact_id, :group, :duration, :started, "
            ":ended, :recording, CASE WHEN CAST(:recording AS text) IS NOT NULL THEN 'pending' END, "
            ":campaign_id, CAST(:raw AS jsonb)) "
            "ON CONFLICT (dialer_call_id) DO UPDATE SET "
            "direction = COALESCE(EXCLUDED.direction, lending.call_dispositions.direction), "
            "phone = COALESCE(EXCLUDED.phone, lending.call_dispositions.phone), "
            "caller_seat = COALESCE(EXCLUDED.caller_seat, lending.call_dispositions.caller_seat), "
            "caller_name = COALESCE(EXCLUDED.caller_name, lending.call_dispositions.caller_name), "
            "caller_id_number = COALESCE(EXCLUDED.caller_id_number, lending.call_dispositions.caller_id_number), "
            "dialer_contact_id = COALESCE(EXCLUDED.dialer_contact_id, lending.call_dispositions.dialer_contact_id), "
            "seat_group = COALESCE(EXCLUDED.seat_group, lending.call_dispositions.seat_group), "
            "talk_duration_sec = COALESCE(EXCLUDED.talk_duration_sec, lending.call_dispositions.talk_duration_sec), "
            "call_started_at = COALESCE(EXCLUDED.call_started_at, lending.call_dispositions.call_started_at), "
            "call_ended_at = CASE WHEN :real_end THEN EXCLUDED.call_ended_at "
            "ELSE COALESCE(lending.call_dispositions.call_ended_at, EXCLUDED.call_ended_at) END, "
            "recording_ref = COALESCE(EXCLUDED.recording_ref, lending.call_dispositions.recording_ref), "
            "recording_status = CASE WHEN EXCLUDED.recording_ref IS NOT NULL "
            "AND lending.call_dispositions.recording_status IS NULL THEN 'pending' "
            "ELSE lending.call_dispositions.recording_status END, "
            "dialer_campaign_id = COALESCE(EXCLUDED.dialer_campaign_id, lending.call_dispositions.dialer_campaign_id), "
            "raw_event = EXCLUDED.raw_event, updated_at = now() "
            "RETURNING id, phone, caller_seat, disposition, campaign_tag, queue, dialer_contact_id, "
            "call_started_at, call_ended_at, unfunded_cause, opt_out_propagated_at"
        ),
        {
            "call_id": ev.call_id, "direction": ev.direction, "phone": phone,
            "seat": ev.seat_id, "name": ev.seat_name, "did": ev.caller_id_number,
            "contact_id": ev.contact_id, "group": seat_group_for(ev.seat_id),
            "duration": ev.duration, "started": ev.started_at, "ended": ended_at, "real_end": ev.ended_at is not None,
            "recording": ev.recording_ref, "campaign_id": ev.campaign_id, "raw": json.dumps(ev.raw, default=str),
        },
    ).mappings().one()

    if ev.seat_id and not seat_group_for(ev.seat_id):
        logger.warning("[lending] call %s: seat %s has no shift group (LENDING_SEAT_GROUPS)", ev.call_id, ev.seat_id)

    disposition = row["disposition"]
    unknown_code = None
    if ev.disposition_raw:
        if not known:
            if not previous or previous[0] != ev.disposition_raw:
                unknown_code = ev.disposition_raw
                logger.warning("[lending] call %s: unknown disposition code %r", ev.call_id, ev.disposition_raw)
            db.execute(
                text("UPDATE lending.call_dispositions SET disposition_raw = :raw, disposition_at = clock_timestamp() WHERE id = :id"),
                {"raw": ev.disposition_raw, "id": row["id"]},
            )
        elif code is not None and code != disposition and disposition == DNC_CODE and row["opt_out_propagated_at"] is None:
            logger.warning("[lending] call %s: kept DNC_REQUEST over a later %s until the opt-out is propagated",
                           ev.call_id, code)
        elif code is not None and code != disposition:
            cause = DEFAULT_CAUSE.get(code or "") if not row["unfunded_cause"] else None
            db.execute(
                text("UPDATE lending.call_dispositions SET disposition = :d, disposition_raw = :raw, "
                     "disposition_at = clock_timestamp(), disposition_list_version = :v, "
                     "unfunded_cause = COALESCE(unfunded_cause, :cause), "
                     "unfunded_cause_provisional = CASE WHEN :cause IS NOT NULL THEN true ELSE unfunded_cause_provisional END "
                     "WHERE id = :id"),
                {"d": code, "raw": ev.disposition_raw, "v": DISPOSITION_LIST_VERSION, "cause": cause, "id": row["id"]},
            )
            disposition = code

    record = None
    if not row["campaign_tag"] or not row["queue"] or code == BOOKED_CODE or _is_unanswered(ev, code, ended_at is not None):
        record = lookup_load_record(db, row["dialer_contact_id"], row["phone"], row["call_started_at"])
    if record:
        db.execute(
            text("UPDATE lending.call_dispositions SET campaign_tag = COALESCE(campaign_tag, :tag), "
                 "queue = COALESCE(queue, :queue) WHERE id = :id"),
            {"tag": record.get("campaign_tag"), "queue": record.get("pool"), "id": row["id"]},
        )

    if ev.disclosure_logged:
        db.execute(text("UPDATE lending.call_dispositions SET recording_disclosure_logged = true WHERE id = :id"),
                   {"id": row["id"]})

    booking_blocked = code == BOOKED_CODE and bool(record) and record.get("pool") == NURTURE
    if booking_blocked:
        db.execute(text("UPDATE lending.call_dispositions SET booking_blocked = true WHERE id = :id"), {"id": row["id"]})

    unanswered = (row["call_ended_at"] is not None and ev.direction != "inbound"
                  and _is_unanswered(ev, code, True) and row["phone"] is not None
                  and not is_abandoned(ev.disposition_raw))
    if unanswered:
        queue_missed_call(db, ev.call_id, row["phone"], ev.caller_id_number, record, row["call_ended_at"])

    return RecordedCall(
        row_id=row["id"], call_id=ev.call_id, phone=row["phone"], caller_seat=row["caller_seat"],
        disposition=disposition, opt_out_propagated=row["opt_out_propagated_at"] is not None,
        call_ended=row["call_ended_at"] is not None, dnc_requested=(code == DNC_CODE or disposition == DNC_CODE),
        unknown_code=unknown_code, unanswered=unanswered, booking_blocked=booking_blocked,
    )


def _suppressed(db, phone: str) -> bool:
    return bool(db.execute(
        text("SELECT EXISTS (SELECT 1 FROM lending.suppression_list WHERE phone = :p) "
             "OR EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND do_not_contact)"),
        {"p": phone},
    ).scalar())


def queue_missed_call(db, call_id: str, phone: str, caller_id_number: Optional[str],
                      record: Optional[dict], ended_at: datetime) -> str:
    """Queue the missed-call text signal. One sendable event per contact per Eastern day;
    suppressed numbers are recorded as blocked. Returns the stored status. Idempotent per call."""
    day: date = ended_at.astimezone(ZoneInfo(DEFAULT_TZ)).date()
    if _suppressed(db, phone):
        status = "blocked"
    else:
        taken = db.execute(
            text("SELECT 1 FROM lending.missed_call_events WHERE phone = :p AND event_date_et = :d "
                 "AND status <> 'duplicate_day' AND dialer_call_id <> :c"),
            {"p": phone, "d": day, "c": call_id},
        ).first()
        status = "duplicate_day" if taken else "pending"
    record = record or {}
    reason = None if record.get("property_address") else "your recent inquiry"
    db.execute(
        text("INSERT INTO lending.missed_call_events "
             "(dialer_call_id, phone, event_date_et, caller_id_number, property_address, reason, status) "
             "VALUES (:c, :p, :d, :did, :addr, :reason, :status) ON CONFLICT (dialer_call_id) DO NOTHING"),
        {"c": call_id, "p": phone, "d": day, "did": caller_id_number,
         "addr": record.get("property_address"), "reason": reason, "status": status},
    )
    return status
