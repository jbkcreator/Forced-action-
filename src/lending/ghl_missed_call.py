"""G4: a GHL call nobody picked up gets the missed-call text, decided exactly like a BatchDialer one.

The GHL call import runs every 15 minutes, too late for the 60 s rule, so a GHL workflow ("call status" = no answer,
busy, failed or voicemail -> Webhook) posts the call to ``POST /webhooks/lending/ghl-missed-call`` the moment it
ends. UNVERIFIED body (the GHL workflow is built by Dev A): call_id (the GHL call message id, required), phone
(required), call_status, contact_id, direction, user, called_at, ended_at.

The call is stored in lending.call_dispositions under the import's own key (``ghl:<message id>``), so the import
later updates the same row in place, and the missed-call event is queued for it (``queue_missed_call``: one text per
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
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional

from sqlalchemy import text

from config.lending_dispositions import GHL_UNKNOWN_CALLER
from config.lending_followup import CALL_SOURCE_GHL, GHL_CALL_ID_PREFIX, GHL_CALLER_SEAT, GHL_CAMPAIGN_TAG
from config.lending_missed_call import GHL_MISSED_CALL_STATUSES, OUTBOUND_DIRECTIONS
from src.lending.dispositions import _to_dt, queue_missed_call
from src.lending.ghl_consent_events import InvalidEvent
from src.lending.ghl_disposition_match import _value
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

REASON_NO_CALL_ID = "call_id is required (the GHL call message id)"
REASON_NO_PHONE = "phone is required"

_INSERT_CALL = text("""
    INSERT INTO lending.call_dispositions
        (dialer_call_id, call_source, direction, phone, caller_seat, caller_name, campaign_tag, disposition_raw,
         talk_duration_sec, call_started_at, call_ended_at, raw_event)
    VALUES (:call_id, :source, 'outbound', :phone, :seat, :caller_name, :tag, :raw_outcome,
            0, :started, :ended, CAST(:raw_event AS jsonb))
    ON CONFLICT (dialer_call_id) DO NOTHING
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


def parse_missed_call(body: Mapping[str, Any], received_at: datetime) -> Optional[GhlMissedCall]:
    """The missed outbound call, None when the post is not one (answered, inbound), InvalidMissedCall when unusable.

    The call's end is the time GHL sent (never later than arrival), else its start, else the arrival time."""
    status = _status(_value(body, "call_status"))
    direction = (_value(body, "direction") or "outbound").strip().lower()
    if status not in GHL_MISSED_CALL_STATUSES or direction not in OUTBOUND_DIRECTIONS:
        return None
    call_id = _value(body, "call_id")
    if not call_id:
        raise InvalidMissedCall(REASON_NO_CALL_ID)
    raw_phone = _value(body, "phone")
    phone = normalize(raw_phone) if raw_phone else None
    if not phone:
        raise InvalidMissedCall(REASON_NO_PHONE)
    started = _to_dt(_value(body, "called_at"))
    ended = _to_dt(_value(body, "ended_at")) or started or received_at
    ended = min(ended, received_at)
    return GhlMissedCall(
        dialer_call_id=f"{GHL_CALL_ID_PREFIX}{call_id}"[:100], phone=phone, status=status,
        started_at=min(started or ended, ended), ended_at=ended, contact_id=_value(body, "contact_id"),
        caller_name=(_value(body, "user") or "")[:200] or None,
    )


def record_missed_call(db, call: GhlMissedCall) -> Optional[str]:
    """Store the call (the import updates the same row later) and queue its missed-call event. Returns the call's
    stored event status (pending, blocked, duplicate_day; a repeated post gets the first post's status, e.g. sent),
    or None when the stored call is not an outbound GHL call to this number. Does not commit."""
    db.execute(_INSERT_CALL, {
        "call_id": call.dialer_call_id, "source": CALL_SOURCE_GHL, "phone": call.phone, "seat": GHL_CALLER_SEAT,
        "caller_name": call.caller_name or GHL_UNKNOWN_CALLER, "tag": GHL_CAMPAIGN_TAG,
        "raw_outcome": f"ghl:{call.status}"[:100], "started": call.started_at, "ended": call.ended_at,
        "raw_event": json.dumps({"source": CALL_SOURCE_GHL, "via": "missed_call_webhook",
                                 "message_id": call.dialer_call_id[len(GHL_CALL_ID_PREFIX):],
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
