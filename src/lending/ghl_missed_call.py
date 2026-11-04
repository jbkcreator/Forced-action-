"""G4: a GHL call nobody picked up gets the missed-call text, decided exactly like a BatchDialer one.

The GHL call import runs every 15 minutes, too late for the 60 s rule, so a GHL workflow ("call status" = no answer,
busy, failed or voicemail -> Webhook) posts the call to ``POST /webhooks/lending/ghl-missed-call`` the moment it
ends. Body, as built in the GHL workflow "NDL - Missed call to backend" (trigger "Call details", status busy /
voicemail / no-answer, direction Outgoing): phone ({{phoneCall.to}}), contact_phone ({{contact.phone}}, the fallback),
contact_id, call_status ({{phoneCall.callStatus}}), user ({{phoneCall.user.name}}), called_at
({{phoneCall.startTime}}), ended_at ({{phoneCall.endTime}}), duration; call_id and direction are optional.

GHL workflows have no call / message id merge field (checked 2026-11-04), so without a call_id the call is keyed by
phone and call time (``ghl-missed:<phone hash>:<epoch>``): a GHL call already imported for the same phone and time is
reused, and otherwise the GHL call import takes the row over later (``adopt_missed_call``), so one call stays one
row and one attempt. A time GHL sends in a format that cannot be read is logged (the raw time text, never a phone)
and the arrival time is used.

The missed-call event is queued for the call (``queue_missed_call``: one text per
number per Eastern day, suppressed numbers recorded as blocked, idempotent per call). text_back then applies every
send gate it applies to BatchDialer calls: the 60 s rule, text consent (LendingFlow leads count as consented),
8am-8pm, MISSED_CALL_TEXT_ENABLED and the emergency stop (in the GHL sender). Nothing here sends: the decision runs
in the CDR poller's text-back step and once more right after the webhook answers.

A call whose webhook never arrived is never texted late: its NO_ANSWER / LEFT_VOICEMAIL outcome, matched after the
import, queues the event and text_back records it skipped_late (ghl_outcome_hooks).
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any, Mapping, Optional

from sqlalchemy import text

from config.lending_dispositions import GHL_UNKNOWN_CALLER
from config.lending_followup import CALL_SOURCE_GHL, GHL_CALL_ID_PREFIX, GHL_CALLER_SEAT, GHL_CAMPAIGN_TAG
from config.lending_missed_call import (
    GHL_MISSED_CALL_KEY_PREFIX,
    GHL_MISSED_CALL_MATCH_SECONDS,
    GHL_MISSED_CALL_STATUSES,
    GHL_OUTBOUND_DIRECTIONS,
)
from src.lending.compliance import phone_hash
from src.lending.dispositions import _to_dt, queue_missed_call
from src.lending.ghl_consent_events import InvalidEvent
from src.lending.ghl_disposition_match import _value
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

REASON_NO_PHONE = "phone is required"
_SAFE_TIME_TEXT = re.compile(r"^[0-9A-Za-z :,/.+-]{1,40}$")

_INSERT_CALL = text("""
    INSERT INTO lending.call_dispositions
        (dialer_call_id, call_source, direction, phone, caller_seat, caller_name, campaign_tag, disposition_raw,
         talk_duration_sec, call_started_at, call_ended_at, raw_event)
    VALUES (:call_id, :source, 'outbound', :phone, :seat, :caller_name, :tag, :raw_outcome,
            0, :started, :ended, CAST(:raw_event AS jsonb))
    ON CONFLICT (dialer_call_id) DO NOTHING
""")
# A GHL call the import already stored for this phone, started near this call (the webhook came after the import).
_FIND_IMPORTED = text("""
    SELECT dialer_call_id FROM lending.call_dispositions
     WHERE call_source = :source AND direction = 'outbound' AND phone = :phone AND dialer_call_id LIKE :prefix
       AND call_started_at BETWEEN :lo AND :hi
     ORDER BY abs(extract(epoch FROM call_started_at - CAST(:at AS timestamptz))) LIMIT 1
""")
# The import stores a call: the missed-call row for the same phone and time takes its real key, with its event.
_ADOPT = text("""
    WITH pick AS (
        SELECT id, dialer_call_id FROM lending.call_dispositions
         WHERE dialer_call_id LIKE :missed_prefix AND call_source = :source AND direction = 'outbound'
           AND phone = :phone AND call_started_at BETWEEN :lo AND :hi
           AND NOT EXISTS (SELECT 1 FROM lending.call_dispositions WHERE dialer_call_id = :call_id)
         ORDER BY abs(extract(epoch FROM call_started_at - CAST(:at AS timestamptz))), id
         LIMIT 1 FOR UPDATE
    ), moved AS (
        UPDATE lending.missed_call_events e SET dialer_call_id = :call_id FROM pick
         WHERE e.dialer_call_id = pick.dialer_call_id
    )
    UPDATE lending.call_dispositions d SET dialer_call_id = :call_id FROM pick WHERE d.id = pick.id
    RETURNING d.id
""")
_EVENT_STATUS = text("SELECT status FROM lending.missed_call_events WHERE dialer_call_id = :call_id")
_STORED_CALL = text("SELECT call_source, direction, phone, call_ended_at FROM lending.call_dispositions "
                    "WHERE dialer_call_id = :call_id")


class InvalidMissedCall(InvalidEvent):
    """A missed-call post that can never be used. ``reason`` is a fixed, client-safe message."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class GhlMissedCall:
    dialer_call_id: str
    phone: str
    status: str
    started_at: datetime
    ended_at: datetime
    contact_id: Optional[str]
    caller_name: Optional[str]


def _status(raw: Optional[str]) -> str:
    return (raw or "").strip().lower().replace("_", "-").replace(" ", "-")


def _time(body: Mapping[str, Any], name: str) -> Optional[datetime]:
    """A call time GHL sent, or None; a value sent in a format that cannot be read is logged so it can be added."""
    raw = _value(body, name)
    parsed = _to_dt(raw)
    if raw and parsed is None:
        shown = raw if _SAFE_TIME_TEXT.match(raw) else f"<{len(raw)} chars>"
        logger.warning("[ghl-missed-call] %s not readable (%r); the arrival time is used", name, shown)
    return parsed


def _phone(body: Mapping[str, Any]) -> Optional[str]:
    """``phone`` ({{phoneCall.to}}), else ``contact_phone`` when it did not resolve (both are phone candidates)."""
    raw = _value(body, "phone")
    return normalize(raw) if raw else None


def missed_call_key(phone: str, at: datetime) -> str:
    """The call's key when GHL sends no call id: phone hash + call time to the second, so a retried post with the
    same times lands on the same row."""
    return f"{GHL_MISSED_CALL_KEY_PREFIX}{phone_hash(phone)[:16]}:{int(at.timestamp())}"


def parse_missed_call(body: Mapping[str, Any], received_at: datetime) -> Optional[GhlMissedCall]:
    """The missed outbound call, None when the post is not one (answered, inbound), InvalidMissedCall when unusable.

    The call's end is the time GHL sent (never later than arrival), else its start, else the arrival time."""
    status = _status(_value(body, "call_status"))
    direction = (_value(body, "direction") or "outbound").strip().lower()
    if status not in GHL_MISSED_CALL_STATUSES or direction not in GHL_OUTBOUND_DIRECTIONS:
        return None
    phone = _phone(body)
    if not phone:
        raise InvalidMissedCall(REASON_NO_PHONE)
    started_sent, ended_sent = _time(body, "called_at"), _time(body, "ended_at")
    ended = min(ended_sent or started_sent or received_at, received_at)
    started = min(started_sent or ended, ended)
    call_id = _value(body, "call_id")
    if call_id:
        key = f"{GHL_CALL_ID_PREFIX}{call_id}"[:100]
    else:
        key = missed_call_key(phone, started if (started_sent or ended_sent)
                              else received_at.replace(second=0, microsecond=0))
    return GhlMissedCall(
        dialer_call_id=key, phone=phone, status=status, started_at=started, ended_at=ended,
        contact_id=_value(body, "contact_id"), caller_name=(_value(body, "user") or "")[:200] or None,
    )


def _window(at: datetime) -> dict[str, Any]:
    span = timedelta(seconds=GHL_MISSED_CALL_MATCH_SECONDS)
    return {"lo": at - span, "hi": at + span, "at": at}


def adopt_missed_call(db, call_id: str, phone: str, started_at: datetime) -> bool:
    """The GHL call import is storing ``call_id`` (``ghl:<message id>``): a missed-call row posted for the same phone
    and time takes that key, with its missed-call event, so the import updates it instead of adding a second row.
    Returns True when a row was taken over. Does not commit."""
    return db.execute(_ADOPT, {"missed_prefix": f"{GHL_MISSED_CALL_KEY_PREFIX}%", "source": CALL_SOURCE_GHL,
                               "phone": phone, "call_id": call_id, **_window(started_at)}).first() is not None


def record_missed_call(db, call: GhlMissedCall) -> Optional[str]:
    """Store the call (the import updates the same row later) and queue its missed-call event. Returns the call's
    stored event status (pending, blocked, duplicate_day; a repeated post gets the first post's status, e.g. sent),
    or None when the stored call is not an outbound GHL call to this number. Does not commit."""
    if call.dialer_call_id.startswith(GHL_MISSED_CALL_KEY_PREFIX):
        imported = db.execute(_FIND_IMPORTED, {"source": CALL_SOURCE_GHL, "phone": call.phone,
                                               "prefix": f"{GHL_CALL_ID_PREFIX}%", **_window(call.started_at)}).scalar()
        if imported:
            call = replace(call, dialer_call_id=imported)
    db.execute(_INSERT_CALL, {
        "call_id": call.dialer_call_id, "source": CALL_SOURCE_GHL, "phone": call.phone, "seat": GHL_CALLER_SEAT,
        "caller_name": call.caller_name or GHL_UNKNOWN_CALLER, "tag": GHL_CAMPAIGN_TAG,
        "raw_outcome": f"ghl:{call.status}"[:100], "started": call.started_at, "ended": call.ended_at,
        "raw_event": json.dumps({"source": CALL_SOURCE_GHL, "via": "missed_call_webhook",
                                 "message_id": (call.dialer_call_id[len(GHL_CALL_ID_PREFIX):]
                                                if call.dialer_call_id.startswith(GHL_CALL_ID_PREFIX) else None),
                                 "contact_id": call.contact_id, "call_status": call.status}),
    })
    stored = db.execute(_STORED_CALL, {"call_id": call.dialer_call_id}).first()
    if stored is None or stored[0] != CALL_SOURCE_GHL or stored[1] != "outbound" or stored[2] != call.phone:
        logger.warning("[ghl-missed-call] call %s already stored as another call; not queued", call.dialer_call_id)
        return None
    queue_missed_call(db, call.dialer_call_id, call.phone, None, None, stored[3] or call.ended_at)
    return db.execute(_EVENT_STATUS, {"call_id": call.dialer_call_id}).scalar()


def run_text_back_now() -> None:
    """Decide the queued text right away instead of waiting for the CDR poller's next step. Never raises."""
    try:
        from src.lending.text_back import run_text_back_cycle

        counts = run_text_back_cycle()
        if counts:
            logger.info("[ghl-missed-call] text-back %s", counts)
    except Exception as exc:  # class only: bodies carry phone numbers
        logger.error("[ghl-missed-call] text-back step failed (%s); the CDR poller retries", type(exc).__name__)
