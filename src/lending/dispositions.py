"""Record Aircall call events into lending.call_dispositions (spec §4.4).

One row per Aircall call, keyed by the Aircall call ID, so replays and
out-of-order events (call.tagged before call.ended) converge on the same row.
Never commits: the caller owns the transaction.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from sqlalchemy import text

from config.lending_dispositions import DISPOSITIONS
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

HANDLED_EVENTS = frozenset({"call.ended", "call.tagged", "call.untagged"})


@dataclass(frozen=True)
class RecordedCall:
    row_id: int
    call_id: str
    etype: str
    phone: Optional[str]
    caller_seat: Optional[str]
    disposition: Optional[str]
    opt_out_propagated: bool
    dnc_tagged: bool = False


def last4(phone: Optional[str]) -> str:
    return f"***{phone[-4:]}" if phone else "none"


def _epoch_to_dt(value: Any) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc) if value else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _str_or_none(value: Any) -> Optional[str]:
    return str(value) if value not in (None, "") else None


def tag_names(tags: Any) -> Optional[list[str]]:
    """Tag names from an Aircall ``tags`` list; None when the event carries no tags key."""
    if tags is None:
        return None
    return [t.get("name") if isinstance(t, dict) else t for t in tags if t]


def resolve_disposition(current: Optional[str], tags: Sequence[str]) -> tuple[Optional[str], bool]:
    """(disposition, multiple) for the result tags now on the call.

    A result tag that wasn't already counted is the latest one and wins; the
    flag marks a call that carries more than one result tag.
    """
    results = list(dict.fromkeys(t for t in tags if t in DISPOSITIONS))
    if not results:
        return None, False
    newer = [t for t in results if t != current]
    return (newer[-1] if newer else current), len(results) > 1


def lookup_load_record(db, contact_id: Optional[str], phone: Optional[str],
                       started_at: Optional[datetime]) -> Optional[dict]:
    """Campaign tag and borrower details from the dialer load table.

    Deliberately not filtered on ``active``: a DNC_REQUEST pulls the contact
    from the dialer before its disposition event is processed.
    """
    if not (contact_id or phone):
        return None
    if not db.execute(text("SELECT to_regclass('lending.dialer_load_records') IS NOT NULL")).scalar():
        return None
    cols = "campaign_tag, borrower_name, entity_name, property_address"
    with db.begin_nested():
        if contact_id:
            row = db.execute(
                text(f"SELECT {cols} FROM lending.dialer_load_records "
                     "WHERE aircall_contact_id = :cid ORDER BY loaded_at DESC LIMIT 1"),
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


def record_aircall_event(db, etype: str, data: dict) -> Optional[RecordedCall]:
    """Upsert the call row for a call.ended / call.tagged / call.untagged event."""
    call_id = _str_or_none(data.get("id") or data.get("call_id"))
    if etype not in HANDLED_EVENTS or not call_id:
        return None

    user = data.get("user") or {}
    number = data.get("number") or {}
    contact = data.get("contact") or {}
    dialed = data.get("raw_digits") or data.get("to")
    phone = normalize(dialed)
    if dialed and not phone:
        logger.warning("[lending] call %s: dialed number could not be normalized", call_id)
    is_ended = etype == "call.ended"

    row = db.execute(
        text(
            "INSERT INTO lending.call_dispositions (aircall_call_id, direction, phone, caller_seat, caller_name, "
            "caller_line, aircall_contact_id, talk_duration_sec, call_started_at, call_ended_at, raw_event) "
            "VALUES (:call_id, :direction, :phone, :seat, :name, :line, :contact_id, :duration, :started, :ended, "
            "CAST(:raw AS jsonb)) "
            "ON CONFLICT (aircall_call_id) DO UPDATE SET "
            "direction = COALESCE(EXCLUDED.direction, lending.call_dispositions.direction), "
            "phone = COALESCE(EXCLUDED.phone, lending.call_dispositions.phone), "
            "caller_seat = COALESCE(EXCLUDED.caller_seat, lending.call_dispositions.caller_seat), "
            "caller_name = COALESCE(EXCLUDED.caller_name, lending.call_dispositions.caller_name), "
            "caller_line = COALESCE(EXCLUDED.caller_line, lending.call_dispositions.caller_line), "
            "aircall_contact_id = COALESCE(EXCLUDED.aircall_contact_id, lending.call_dispositions.aircall_contact_id), "
            "talk_duration_sec = COALESCE(EXCLUDED.talk_duration_sec, lending.call_dispositions.talk_duration_sec), "
            "call_started_at = COALESCE(EXCLUDED.call_started_at, lending.call_dispositions.call_started_at), "
            "call_ended_at = COALESCE(EXCLUDED.call_ended_at, lending.call_dispositions.call_ended_at), "
            "raw_event = EXCLUDED.raw_event, updated_at = now() "
            "RETURNING id, phone, caller_seat, disposition, campaign_tag, "
            "aircall_contact_id, call_started_at, opt_out_propagated_at"
        ),
        {
            "call_id": call_id,
            "direction": _str_or_none(data.get("direction")),
            "phone": phone,
            "seat": _str_or_none(user.get("id")),
            "name": _str_or_none(user.get("name")),
            "line": _str_or_none(number.get("id")),
            "contact_id": _str_or_none(contact.get("id")),
            "duration": data.get("duration") if is_ended else None,
            "started": _epoch_to_dt(data.get("started_at")),
            "ended": _epoch_to_dt(data.get("ended_at")) if is_ended else None,
            "raw": json.dumps({"event": etype, "data": data}, default=str),
        },
    ).mappings().one()

    disposition = row["disposition"]
    names = tag_names(data.get("tags"))
    # A verbal decline is never dropped: any DNC_REQUEST tag seen on the call triggers the
    # opt-out, even if a later or simultaneous result tag becomes the stored disposition.
    dnc_tagged = bool(names) and "DNC_REQUEST" in names
    # Only call.tagged / call.untagged may clear a result: a replayed or late call.ended
    # carries a stale tag list and must not erase what the caller already set.
    if names is not None and (etype != "call.ended" or any(t in DISPOSITIONS for t in names)):
        new, multiple = resolve_disposition(disposition, names)
        if new != disposition:
            db.execute(
                text("UPDATE lending.call_dispositions SET disposition = :d, disposition_tag_raw = :d, "
                     "disposition_at = now(), multiple_dispositions = :m WHERE id = :id"),
                {"d": new, "m": multiple, "id": row["id"]},
            )
            disposition = new
        else:
            db.execute(
                text("UPDATE lending.call_dispositions SET multiple_dispositions = :m WHERE id = :id"),
                {"m": multiple, "id": row["id"]},
            )

    if not row["campaign_tag"]:
        record = lookup_load_record(db, row["aircall_contact_id"], row["phone"], row["call_started_at"])
        if record and record.get("campaign_tag"):
            db.execute(
                text("UPDATE lending.call_dispositions SET campaign_tag = :t WHERE id = :id"),
                {"t": record["campaign_tag"], "id": row["id"]},
            )

    return RecordedCall(
        row_id=row["id"], call_id=call_id, etype=etype, phone=row["phone"],
        caller_seat=row["caller_seat"], disposition=disposition,
        opt_out_propagated=row["opt_out_propagated_at"] is not None,
        dnc_tagged=dnc_tagged,
    )
