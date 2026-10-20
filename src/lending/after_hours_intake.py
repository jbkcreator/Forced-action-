"""After-hours voicemail and answering-service intake.

A GHL workflow (Call Status = voicemail -> Webhook) or, later, an outside answering service posts the caller's
message to ``POST /webhooks/lending/after-hours-intake``. :func:`record_intake` then, in the request's transaction,
finds or adds the caller in ``lending.contacts``, logs the call on ``lending.call_dispositions`` (inbound,
``disposition_raw`` = ``after_hours_voicemail``, the body kept as ``raw_event``) and pins the contact until the end of
the next business-day shift. A redelivered event changes nothing: it is keyed on the call id, else on the phone and
time, else on the phone, transcript and ET date.

At 08:55 ET on business days (``src.tasks.lending_pinned_callbacks``) :func:`draft_morning_replies` takes every
contact still pinned (:func:`pins_to_call`): one with text consent gets a reply drafted from the transcript (fixed
text when there is none or the model's draft is unusable), frozen in ``lending.pending_actions`` for Approve; Cora's
minute sweep posts the card. A voicemail that already has a draft is not drafted again. One without consent is listed
as call-only. Nothing is sent here; GHL's instant missed-call text is untouched.

Body fields (top level or under ``contact``): ``phone`` (required), ``first_name`` / ``name``, ``message`` /
``transcription`` / ``transcript``, ``received_at`` (ISO 8601), ``source`` (default ``ghl_voicemail``),
``call_id`` / ``message_id``.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Protocol

from sqlalchemy import text

from config.lending_after_hours import (BUSINESS_DAYS, DEFAULT_SOURCE, DRAFT_FORBIDDEN, DRAFT_MAX_CHARS, DRAFT_PROMPT,
                                        DRAFT_TOOL_NAME, FALLBACK_REPLY, INTAKE_MARKER, MORNING_POST_ET, PIN_UNTIL_ET)
from config.lending_reminders import TIMEZONE
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


def _parse_time(raw: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise InvalidEvent("received_at is not an ISO 8601 time") from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _stable_ref(phone: str, raw_time: Optional[str], received_at: datetime, message: Optional[str]) -> str:
    """Dedup key without a call id: the sender's own time when given, else the transcript and the ET date, so a
    redelivery of the same voicemail lands on the same key."""
    basis = (f"{phone}|{received_at.isoformat()}" if raw_time
             else f"{phone}|{message or ''}|{received_at.astimezone(TIMEZONE).date().isoformat()}")
    return hashlib.sha256(basis.encode()).hexdigest()[:24]


def parse_intake(body: Mapping[str, Any], *, now: datetime) -> IntakeEvent:
    phone = normalize(_field_value(body, _PHONE_FIELDS))
    if not phone:
        raise InvalidEvent("a valid US phone is required")
    raw_time = _field_value(body, _TIME_FIELDS)
    received_at = _parse_time(raw_time) if raw_time else now
    name = _field_value(body, _NAME_FIELDS)
    message = _field_value(body, _MESSAGE_FIELDS)
    ref = _field_value(body, _REF_FIELDS) or _stable_ref(phone, raw_time, received_at, message)
    return IntakeEvent(phone=phone, first_name=name.split()[0] if name else None, message=message,
                       received_at=received_at, source=(_field_value(body, ("source",)) or DEFAULT_SOURCE)[:40],
                       ref=ref)


def pin_until(now: datetime) -> datetime:
    """End of the shift on the next business day whose 08:55 morning list is still to come: the same day before the
    post, else the next business day, so every pin is on one morning list."""
    local = now.astimezone(TIMEZONE)
    day = local.date()
    if local.weekday() not in BUSINESS_DAYS or local.time() >= MORNING_POST_ET:
        day += timedelta(days=1)
    while day.weekday() not in BUSINESS_DAYS:
        day += timedelta(days=1)
    return datetime.combine(day, PIN_UNTIL_ET, tzinfo=TIMEZONE)


# DO UPDATE (not DO NOTHING) so a concurrent first delivery for the same new phone still returns the id.
_CONTACT_ID = text("""
    INSERT INTO lending.contacts (phone) VALUES (:phone)
    ON CONFLICT (phone) DO UPDATE SET phone = EXCLUDED.phone
    RETURNING id
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
        db.execute(_PIN, {"id": contact_id, "now": now, "until": pin_until(now)})
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
_DRAFTED = text("SELECT idempotency_key, action_id FROM lending.pending_actions WHERE idempotency_key = ANY(:keys)")


def pins_to_call(db, now: datetime) -> list[PinnedCall]:
    """Contacts still pinned (within ``pinned_until``, not called back since), oldest pin first."""
    rows = sorted(db.execute(_PINS, {"marker": INTAKE_MARKER, "now": now}).mappings(), key=lambda r: r["pinned_at"])
    return [PinnedCall(row["id"], parse_intake(row["raw_event"], now=row["pinned_at"])) for row in rows]


def fallback_reply(first_name: Optional[str]) -> str:
    return FALLBACK_REPLY.format(name=first_name or "there")


def model_drafter(messages_client: Any, model: str) -> Drafter:
    """A :data:`Drafter` that asks the model for the reply; None when there is no transcript or the call fails.
    The transcript is caller-controlled, so it goes in as tagged data, never as instructions."""

    def draft(first_name: Optional[str], transcript: Optional[str]) -> Optional[str]:
        if not transcript:
            return None
        cleaned = transcript.replace("<", " ").replace(">", " ")
        try:
            response = messages_client.create(
                model=model, max_tokens=300, system=DRAFT_PROMPT.format(max_chars=DRAFT_MAX_CHARS),
                messages=[{"role": "user", "content": f"First name: {first_name or 'unknown'}\n"
                                                      f"<voicemail>\n{cleaned}\n</voicemail>"}])
            return "".join(block.text for block in response.content if getattr(block, "type", "") == "text").strip()
        except Exception as exc:
            logger.warning("[after-hours] reply draft model call failed (%s); using the fixed reply",
                           type(exc).__name__)
            return None

    return draft


def reply_body(event: IntakeEvent, drafter: Drafter) -> str:
    """The model's draft when it is usable (non-empty, short, no figures, links, rate or approval wording), else the
    fixed reply."""
    drafted = drafter(event.first_name, event.message)
    if (drafted and len(drafted) <= DRAFT_MAX_CHARS and borrower_draft_problem({"body": drafted}) is None
            and not DRAFT_FORBIDDEN.search(drafted)):
        return drafted
    return fallback_reply(event.first_name)


def _queue_reply(queue: DraftQueue, pin: PinnedCall, drafter: Drafter) -> int:
    event = pin.event
    return queue.enqueue(
        tool_name=DRAFT_TOOL_NAME, channel="ghl_sms",
        payload={"lead_ref": pin.lead_ref, "to_phone": event.phone, "body": reply_body(event, drafter)},
        summary=(f"After-hours voicemail reply to {event.first_name or 'caller'} (…{event.phone[-4:]}), drafted from "
                 "the caller's voicemail; sends 8am-8pm borrower time"),
        recipient_phone=event.phone, contact_ref=pin.lead_ref, idempotency_key=event.call_key)


def draft_morning_replies(db, pins: list[PinnedCall], queue: DraftQueue, drafter: Drafter) -> list[MorningReply]:
    """Queue a reply for Approve for each pin with text consent; a voicemail already drafted keeps its draft (no
    second model call). The rest are call-only."""
    drafted = dict(db.execute(_DRAFTED, {"keys": [pin.event.call_key for pin in pins]}).all())
    replies = []
    for pin in pins:
        if pin.event.call_key in drafted:
            replies.append(MorningReply(pin, drafted[pin.event.call_key]))
        elif has_text_consent(db, pin.event.phone):
            replies.append(MorningReply(pin, _queue_reply(queue, pin, drafter)))
        else:
            replies.append(MorningReply(pin, None))
    return replies
