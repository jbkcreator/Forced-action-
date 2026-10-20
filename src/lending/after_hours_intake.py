"""After-hours voicemail and answering-service intake (split T-06, Runbook §5.3 Layers 2 and 3).

A GHL workflow (Transcript Generated / Call Status = voicemail -> Webhook) or, later, an outside answering service
posts the caller's message to ``POST /webhooks/lending/after-hours-intake``. :func:`record_intake` then, in the
request's transaction, finds or adds the caller in ``lending.contacts``, logs the call on
``lending.call_dispositions`` (inbound, ``disposition_raw`` = ``after_hours_voicemail``, the body kept as
``raw_event``) and pins the contact. A redelivered event (same source and call id, or same phone and time) changes
nothing.

At 08:55 ET (``src.tasks.lending_pinned_callbacks``) :func:`draft_morning_replies` takes every contact still pinned
(:func:`pins_to_call`): one with text consent gets a reply drafted from the transcript (fixed text when there is none
or the model's draft is unusable), frozen in ``lending.pending_actions`` for Approve; Cora's minute sweep posts the
card. One without consent is listed as call-only. Nothing is sent here; GHL's instant missed-call text (WP-GL-9) is
untouched.

Body fields (top level or under ``contact``): ``phone`` (required), ``first_name`` / ``name``, ``message`` /
``transcription`` / ``transcript``, ``received_at`` (ISO 8601, default now), ``source`` (default ``ghl_voicemail``),
``call_id`` / ``message_id``.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional, Protocol

from sqlalchemy import text

from config.lending_after_hours import (DEFAULT_SOURCE, DRAFT_MAX_CHARS, DRAFT_PROMPT, DRAFT_TOOL_NAME,
                                        FALLBACK_REPLY, INTAKE_MARKER, PIN_DURATION)
from src.lending.consent import has_text_consent
from src.lending.cora_tools import borrower_draft_problem
from src.lending.ghl_consent_events import InvalidEvent
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

_PHONE_FIELDS = ("phone", "from", "from_number")
_NAME_FIELDS = ("first_name", "firstName", "name", "full_name")
_MESSAGE_FIELDS = ("message", "transcription", "transcript", "voicemail_transcript")
_TIME_FIELDS = ("received_at", "date_added", "dateAdded")
_REF_FIELDS = ("call_id", "message_id", "messageId", "callId")


@dataclass(frozen=True)
class IntakeEvent:
    phone: str
    first_name: Optional[str]
    message: Optional[str]
    received_at: datetime
    source: str
    ref: str

    @property
    def call_key(self) -> str:
        """The ``dialer_call_id`` of the logged call; also the draft's idempotency key."""
        return f"after_hours:{self.source}:{self.ref}"


@dataclass(frozen=True)
class PinnedCall:
    """A contact still pinned, with the voicemail that pinned it."""
    contact_id: int
    event: IntakeEvent

    @property
    def lead_ref(self) -> str:
        return f"contact:{self.contact_id}"


@dataclass(frozen=True)
class MorningReply:
    pin: PinnedCall
    action_id: Optional[int]  # None: no text consent, call only


class DraftQueue(Protocol):
    def enqueue(self, *, tool_name: str, channel: str, payload: Mapping[str, Any], summary: str,
                recipient_phone: str | None = None, contact_ref: str | None = None,
                idempotency_key: str | None = None) -> int: ...


Drafter = Callable[[Optional[str], Optional[str]], Optional[str]]


def _field_value(body: Mapping[str, Any], names: tuple[str, ...]) -> Optional[str]:
    contact = body.get("contact") if isinstance(body.get("contact"), dict) else {}
    for name in names:
        value = body.get(name) or contact.get(name)
        if isinstance(value, (str, int)) and str(value).strip():
            return str(value).strip()
    return None


def _received_at(raw: Optional[str], now: datetime) -> datetime:
    if not raw:
        return now
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise InvalidEvent("received_at is not an ISO 8601 time") from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_intake(body: Mapping[str, Any], *, now: datetime) -> IntakeEvent:
    phone = normalize(_field_value(body, _PHONE_FIELDS))
    if not phone:
        raise InvalidEvent("a valid US phone is required")
    received_at = _received_at(_field_value(body, _TIME_FIELDS), now)
    name = _field_value(body, _NAME_FIELDS)
    ref = (_field_value(body, _REF_FIELDS)
           or hashlib.sha256(f"{phone}|{received_at.isoformat()}".encode()).hexdigest()[:24])
    return IntakeEvent(phone=phone, first_name=name.split()[0] if name else None,
                       message=_field_value(body, _MESSAGE_FIELDS), received_at=received_at,
                       source=(_field_value(body, ("source",)) or DEFAULT_SOURCE)[:40], ref=ref)


_CONTACT_ID = text("""
    WITH added AS (INSERT INTO lending.contacts (phone) VALUES (:phone) ON CONFLICT (phone) DO NOTHING RETURNING id)
    SELECT id FROM added UNION ALL SELECT id FROM lending.contacts WHERE phone = :phone LIMIT 1
""")
_LOG_CALL = text("""
    INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, disposition_raw, call_ended_at,
                                           raw_event, created_at)
    VALUES (:key, 'inbound', :phone, :marker, :ended, CAST(:raw AS jsonb), :now)
    ON CONFLICT (dialer_call_id) DO NOTHING
""")
_PIN = text("UPDATE lending.contacts SET pinned_at = :now, pinned_until = :until WHERE id = :id")


def record_intake(db, body: Mapping[str, Any], *, now: Optional[datetime] = None) -> bool:
    """Find or add the contact, log the call and pin it. True when the event is new. Does not commit; raises
    :class:`InvalidEvent`."""
    now = now or datetime.now(timezone.utc)
    event = parse_intake(body, now=now)
    contact_id = db.execute(_CONTACT_ID, {"phone": event.phone}).scalar_one()
    logged = db.execute(_LOG_CALL, {"key": event.call_key, "phone": event.phone, "marker": INTAKE_MARKER,
                                    "ended": event.received_at, "raw": json.dumps(dict(body), default=str),
                                    "now": now}).rowcount
    if logged:
        db.execute(_PIN, {"id": contact_id, "now": now, "until": now + PIN_DURATION})
    return bool(logged)


_PINS = text("""
    SELECT DISTINCT ON (c.id) c.id, c.pinned_at, d.raw_event
      FROM lending.contacts c
      JOIN lending.call_dispositions d ON d.phone = c.phone AND d.disposition_raw = :marker
                                      AND d.created_at = c.pinned_at
     WHERE c.pinned_until > :now AND NOT c.do_not_contact
       AND NOT EXISTS (SELECT 1 FROM lending.call_dispositions o
                        WHERE o.phone = c.phone AND o.direction = 'outbound' AND o.call_ended_at >= c.pinned_at)
     ORDER BY c.id, d.call_ended_at DESC
""")


def pins_to_call(db, now: datetime) -> list[PinnedCall]:
    """Contacts still pinned (within ``pinned_until``, not called back since), oldest pin first."""
    rows = sorted(db.execute(_PINS, {"marker": INTAKE_MARKER, "now": now}).mappings(), key=lambda r: r["pinned_at"])
    return [PinnedCall(row["id"], parse_intake(row["raw_event"], now=row["pinned_at"])) for row in rows]


def fallback_reply(first_name: Optional[str]) -> str:
    return FALLBACK_REPLY.format(name=first_name or "there")


def model_drafter(messages_client: Any, model: str) -> Drafter:
    """A :data:`Drafter` that asks the model for the reply; None when there is no transcript or the call fails."""

    def draft(first_name: Optional[str], transcript: Optional[str]) -> Optional[str]:
        if not transcript:
            return None
        try:
            response = messages_client.create(
                model=model, max_tokens=300, system=DRAFT_PROMPT.format(max_chars=DRAFT_MAX_CHARS),
                messages=[{"role": "user", "content": f"First name: {first_name or 'unknown'}\n"
                                                      f"Voicemail transcript:\n{transcript}"}])
            return "".join(block.text for block in response.content if getattr(block, "type", "") == "text").strip()
        except Exception as exc:
            logger.warning("[after-hours] reply draft model call failed (%s); using the fixed reply",
                           type(exc).__name__)
            return None

    return draft


def reply_body(event: IntakeEvent, drafter: Drafter) -> str:
    """The model's draft when it is usable (non-empty, short enough, quotes no numbers), else the fixed reply."""
    drafted = drafter(event.first_name, event.message)
    if drafted and len(drafted) <= DRAFT_MAX_CHARS and borrower_draft_problem({"body": drafted}) is None:
        return drafted
    return fallback_reply(event.first_name)


def _queue_reply(queue: DraftQueue, pin: PinnedCall, drafter: Drafter) -> int:
    event = pin.event
    return queue.enqueue(
        tool_name=DRAFT_TOOL_NAME, channel="ghl_sms",
        payload={"lead_ref": pin.lead_ref, "to_phone": event.phone, "body": reply_body(event, drafter)},
        summary=f"After-hours voicemail reply to {event.first_name or 'caller'} (…{event.phone[-4:]})",
        recipient_phone=event.phone, contact_ref=pin.lead_ref, idempotency_key=event.call_key)


def draft_morning_replies(db, pins: list[PinnedCall], queue: DraftQueue, drafter: Drafter) -> list[MorningReply]:
    """Queue a reply for Approve for each pin with text consent (idempotent per voicemail); the rest are call-only."""
    return [MorningReply(pin, _queue_reply(queue, pin, drafter) if has_text_consent(db, pin.event.phone) else None)
            for pin in pins]
