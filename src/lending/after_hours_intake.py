"""After-hours voicemail and answering-service intake (split T-06, Runbook §5.3 Layers 2 and 3).

A GHL workflow (Transcript Generated / Call Status = voicemail -> Webhook) or, later, an outside answering service
posts the caller's message to ``POST /webhooks/lending/after-hours-intake``. :func:`record_intake` then, in the
request's transaction:

1. finds the caller in ``lending.contacts`` by phone, or adds them;
2. logs the call on ``lending.call_dispositions`` (inbound, LEFT_VOICEMAIL, campaign tag ``inbound_voicemail``);
3. pins the contact first for the next morning (see :func:`active_pins`).

After the commit, :func:`queue_reply_draft` has the model draft a personal text from the transcript (fixed text when
there is none or the model fails) and freezes it in ``lending.pending_actions``. Nothing is sent here: Cora's minute
sweep posts the Approve / Revise / Reject card and only an approved draft is ever sent. GHL's instant missed-call
text (WP-GL-9) is untouched; this draft is the next-morning follow-up.

A redelivered event (same source and call id, or same phone and time) logs nothing new and queues no second draft.

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
                                        FALLBACK_REPLY, INTAKE_CAMPAIGN_TAG, INTAKE_DIRECTION, INTAKE_DISPOSITION,
                                        PIN_DURATION)
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
class IntakeResult:
    recorded: bool
    contact_id: Optional[int]
    draft_allowed: bool


class DraftQueue(Protocol):
    def enqueue(self, *, tool_name: str, channel: str, payload: Mapping[str, Any], summary: str,
                recipient_phone: str | None = None, contact_ref: str | None = None,
                idempotency_key: str | None = None) -> int: ...


Drafter = Callable[[Optional[str], Optional[str]], Optional[str]]


def _first(body: Mapping[str, Any], names: tuple[str, ...]) -> Optional[str]:
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
    phone = normalize(_first(body, _PHONE_FIELDS))
    if not phone:
        raise InvalidEvent("a valid US phone is required")
    received_at = _received_at(_first(body, _TIME_FIELDS), now)
    name = _first(body, _NAME_FIELDS)
    ref = _first(body, _REF_FIELDS) or hashlib.sha256(f"{phone}|{received_at.isoformat()}".encode()).hexdigest()[:24]
    return IntakeEvent(phone=phone, first_name=name.split()[0] if name else None,
                       message=_first(body, _MESSAGE_FIELDS), received_at=received_at,
                       source=(_first(body, ("source",)) or DEFAULT_SOURCE)[:40], ref=ref)


_UPSERT_CONTACT = text("""
    INSERT INTO lending.contacts (phone) VALUES (:phone)
    ON CONFLICT (phone) DO UPDATE SET phone = EXCLUDED.phone
    RETURNING id, do_not_contact
""")
_LOG_CALL = text("""
    INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, campaign_tag, disposition,
                                           disposition_raw, call_ended_at, raw_event)
    VALUES (:key, :direction, :phone, :tag, :disposition, :source, :at, CAST(:raw AS jsonb))
    ON CONFLICT (dialer_call_id) DO NOTHING
""")
_PIN = text("UPDATE lending.contacts SET pinned_at = :at, pinned_until = :until WHERE id = :id")
_SUPPRESSED = text("SELECT EXISTS (SELECT 1 FROM lending.suppression_list WHERE phone = :phone)")


def record_intake(db, body: Mapping[str, Any], *, now: Optional[datetime] = None) -> tuple[IntakeEvent, IntakeResult]:
    """Find or add the contact, log the call and pin it. Does not commit; raises :class:`InvalidEvent`."""
    now = now or datetime.now(timezone.utc)
    event = parse_intake(body, now=now)
    contact = db.execute(_UPSERT_CONTACT, {"phone": event.phone}).mappings().one()
    inserted = db.execute(_LOG_CALL, {
        "key": event.call_key, "direction": INTAKE_DIRECTION, "phone": event.phone, "tag": INTAKE_CAMPAIGN_TAG,
        "disposition": INTAKE_DISPOSITION, "source": event.source, "at": event.received_at,
        "raw": json.dumps(dict(body), default=str),
    }).rowcount
    if not inserted:
        return event, IntakeResult(False, contact["id"], False)
    db.execute(_PIN, {"id": contact["id"], "at": now, "until": now + PIN_DURATION})
    suppressed = contact["do_not_contact"] or db.execute(_SUPPRESSED, {"phone": event.phone}).scalar()
    return event, IntakeResult(True, contact["id"], not suppressed)


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
                                                      f"Voicemail transcript:\n{transcript[:4000]}"}])
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


def queue_reply_draft(queue: DraftQueue, event: IntakeEvent, result: IntakeResult,
                      drafter: Drafter) -> Optional[int]:
    """Freeze the reply for approval; the card is posted by Cora's sweep. Returns the action id, or None."""
    if not (result.recorded and result.draft_allowed and result.contact_id):
        return None
    lead_ref = f"contact:{result.contact_id}"
    return queue.enqueue(
        tool_name=DRAFT_TOOL_NAME, channel="ghl_sms",
        payload={"lead_ref": lead_ref, "to_phone": event.phone, "body": reply_body(event, drafter)},
        summary=(f"After-hours voicemail reply to {event.first_name or 'caller'} (…{event.phone[-4:]}), "
                 f"left {event.received_at:%b %d %H:%M} UTC"),
        recipient_phone=event.phone, contact_ref=lead_ref, idempotency_key=event.call_key)


_ACTIVE_PINS = text("""
    SELECT c.id, c.phone, c.pinned_at, c.pinned_until
      FROM lending.contacts c
     WHERE c.pinned_until > :now AND NOT c.do_not_contact
       AND NOT EXISTS (SELECT 1 FROM lending.call_dispositions d
                        WHERE d.phone = c.phone AND d.direction = 'outbound' AND d.call_ended_at >= c.pinned_at)
     ORDER BY c.pinned_at
""")


def active_pins(db, now: datetime) -> list[Mapping[str, Any]]:
    """Contacts still pinned: within ``pinned_until`` and not called back since the pin."""
    return list(db.execute(_ACTIVE_PINS, {"now": now}).mappings())
