"""Night responder steps: the message, or the booking, each after-hours conversation is waiting for.

A conversation has a step due while ``prompted_state`` differs from ``state``
(``src/lending/after_hours_conversation.py``). :func:`process_due` does each due step once. A question, the slot
offer and the booking confirmation go by text or email through the Next Deal Lending GHL account, with no approval
card: the wording is fixed text (``config/lending_after_hours_conversation.py``) and every send is checked first.

After the third answer the lead is offered up to three call slots on the next business morning, read through
``slot_finder``. The slot they pick is booked through ``booker``, and the confirmation follows. Both are passed in:
this module never talks to a calendar itself. With no calendar passed in, or none open, the conversation closes as
``no_slots`` and the lead stays on the morning call list. A slot taken since it was offered brings a fresh offer.
A booking call with no clear result is never made a second time (``booking_unknown``).

Checks before a send, in order: the conversation has not expired; the recipient still has consent (text) or is not
suppressed (email); it is 8am-8pm for the recipient; for a text, the carrier has approved lending texts; and the
message carries no Backflip name, number or domain. A recipient outside the window, or a text before carrier
approval, waits and is tried again on the next pass. The lending emergency halt stops the whole pass.

Each step is claimed first, so two workers never send the same message or book the same slot; a claim with no
recorded result (a crash midway) is recorded as unknown and never repeated. Every send leaves a receipt on the row:
state, template version, provider message id, status and time. Logs carry ids only, never phones, names or message
text.

Runs from Cora's minute sweep and, so the first reply is immediate, right after the webhook that made a step due.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_after_hours_conversation import (BOOKED, CHANNEL_EMAIL, CHANNEL_SMS, CLOSED,
                                                     CLOSED_BLOCKED_LANE, CLOSED_BOOKING_REFUSED,
                                                     CLOSED_BOOKING_UNKNOWN, CLOSED_EXPIRED, CLOSED_NO_CONSENT,
                                                     CLOSED_NO_SLOTS, CLOSED_SEND_FAILED, CLOSED_SEND_REFUSED,
                                                     CONVERSATION_TTL_HOURS, EMAIL_SUBJECT, MAX_SEND_ATTEMPTS, OFFERED_SLOT_COUNT,
                                                     OPEN_STATES, SEND_BATCH_SIZE, SEND_ROUNDS_PER_PASS,
                                                     SENDABLE_STATES, SLOT_CHOICE, SLOT_CHOSEN, SLOT_OFFER,
                                                     SLOT_OFFER_AFTER_TAKEN, SLOT_OFFER_AGAIN, STALE_CLAIM_SECONDS,
                                                     TEMPLATE_VERSIONS)
from config.lending_compliance import CALL_WINDOW_END, CALL_WINDOW_START, DEFAULT_TZ
from src.lending.after_hours_conversation import (booked_message, conversations_enabled, next_morning_window,
                                                  question_message, slot_offer_message)
from src.lending.compliance import text_window_open
from src.lending.consent import has_text_consent
from src.lending.ghl_sms import GhlSendRefused, GhlSmsError
from src.lending.global_halt import is_globally_halted
from src.lending.lane_guard import backflip_hit
from src.services.email_suppression import is_email_suppressed

logger = logging.getLogger(__name__)

SENT = "sent"
SEND_UNKNOWN = "send_unknown"
WAITING_HOURS = "waiting_hours"
WAITING_CARRIER = "waiting_carrier"
NOT_CONFIGURED = "not_configured"
RETRY_LATER = "retry_later"
BOOKED_NOW = "booked"
SLOT_TAKEN = "slot_taken"
# Outcomes that leave the step due: the conversation is not tried again in the same pass.
_STILL_DUE = (WAITING_HOURS, WAITING_CARRIER, NOT_CONFIGURED, RETRY_LATER)

BOOKING_BOOKED = "booked"
BOOKING_SLOT_TAKEN = "slot_taken"
BOOKING_REFUSED = "refused"

# (phone, body, first_name) -> provider message id; the real one is GhlSmsSender.
TextSender = Callable[..., str]
# (to, subject, body, phone=, first_name=) -> provider message id; the real one is GhlEmailSender.
EmailSender = Callable[..., str]
# (window_start, window_end, now) -> the open call slot starts in that window, timezone-aware.
SlotFinder = Callable[[datetime, datetime, datetime], Sequence[datetime]]


@dataclass(frozen=True)
class BookingRequest:
    conversation_id: int
    slot_start: datetime
    channel: str
    phone: Optional[str]
    email: Optional[str]
    first_name: Optional[str]
    ghl_contact_id: Optional[str]
    answers: tuple[str, ...]


@dataclass(frozen=True)
class BookingOutcome:
    status: str  # BOOKING_BOOKED, BOOKING_SLOT_TAKEN or BOOKING_REFUSED
    booking_ref: Optional[str] = None


# Books the slot on the calendar and moves the lead's GHL card to Booked. It returns only what it knows for
# certain; when it raises, the booking may or may not exist, so it is never called again for that conversation.
Booker = Callable[[BookingRequest], BookingOutcome]


@dataclass(frozen=True)
class DueStep:
    conversation_id: int
    channel: str
    phone: Optional[str]
    email: Optional[str]
    first_name: Optional[str]
    state: str
    send_attempts: int
    ghl_contact_id: Optional[str]
    answers: list
    offered_slots: list  # ISO slot starts, in the order they were numbered for the lead
    chosen_slot_start: Optional[datetime]
    offered_before: bool  # a slot offer has already gone out in this conversation

    @property
    def template_version(self) -> str:
        return TEMPLATE_VERSIONS[(self.state, self.channel)]


_CLOSE_EXPIRED = text("""
    UPDATE lending.after_hours_conversations
       SET state = :closed, closed_reason = :reason, closed_at = :now, updated_at = :now, sending_claimed_at = NULL
     WHERE state = ANY(:open_states) AND expires_at <= :now
""")
# The message is treated as delivered (prompted_state = state): it is never sent a second time.
_SETTLE_STALE_SENDS = text("""
    UPDATE lending.after_hours_conversations
       SET prompted_state = state, sending_claimed_at = NULL, updated_at = :now,
           sent_messages = sent_messages || jsonb_build_array(jsonb_build_object(
               'state', state, 'status', CAST(:status AS text), 'at', CAST(:at AS text)))
     WHERE sending_claimed_at < :cutoff AND state = ANY(:sendable_states)
    RETURNING id
""")
# A booking call that never reported back may have reached the calendar: closed for a person to check, not retried.
_SETTLE_STALE_BOOKINGS = text("""
    UPDATE lending.after_hours_conversations
       SET state = :closed, closed_reason = :reason, closed_at = :now, updated_at = :now, sending_claimed_at = NULL
     WHERE sending_claimed_at < :cutoff AND state = :slot_chosen
    RETURNING id
""")
# slot_chosen never has prompted_state = state, so a picked slot is always due for its booking.
_CLAIM = text("""
    WITH picked AS (
        SELECT id FROM lending.after_hours_conversations
         WHERE state = ANY(:due_states) AND prompted_state IS DISTINCT FROM state
           AND sending_claimed_at IS NULL AND id <> ALL(CAST(:still_due AS bigint[]))
         ORDER BY updated_at
         LIMIT :limit
           FOR UPDATE SKIP LOCKED)
    UPDATE lending.after_hours_conversations c
       SET sending_claimed_at = :now
      FROM picked
     WHERE c.id = picked.id
    RETURNING c.id, c.channel, c.phone, c.email, c.first_name, c.state, c.send_attempts, c.ghl_contact_id,
              c.answers, c.offered_slots, c.chosen_slot_start,
              c.sent_messages @> CAST(:slot_offer_receipt AS jsonb)
""")
_RELEASE = text("""
    UPDATE lending.after_hours_conversations
       SET sending_claimed_at = NULL, send_attempts = :send_attempts
     WHERE id = :id
""")
_CLOSE = text("""
    UPDATE lending.after_hours_conversations
       SET state = :closed, closed_reason = :reason, closed_at = :now, updated_at = :now, sending_claimed_at = NULL
     WHERE id = :id
""")
# The time to reply runs from the first message: one held overnight for the 8am window must not arrive already
# close to expiry.
_RECORD_SEND = text("""
    UPDATE lending.after_hours_conversations
       SET prompted_state = :state, sending_claimed_at = NULL, send_attempts = 0, updated_at = :now,
           expires_at = CASE WHEN sent_messages = '[]'::jsonb THEN CAST(:now AS timestamptz) + make_interval(hours => :ttl_hours)
                             ELSE expires_at END,
           offered_slots = COALESCE(CAST(:offered_slots AS jsonb), offered_slots),
           sent_messages = sent_messages || CAST(:receipt AS jsonb)
     WHERE id = :id
""")
_RECORD_BOOKING = text("""
    UPDATE lending.after_hours_conversations
       SET state = :booked, booking_ref = :booking_ref, closed_at = :now, updated_at = :now,
           sending_claimed_at = NULL, send_attempts = 0
     WHERE id = :id
""")
# Back to the slot choice with nothing offered, so the next send reads the calendar again.
_OFFER_FRESH_SLOTS = text("""
    UPDATE lending.after_hours_conversations
       SET state = :slot_choice, prompted_state = NULL, offered_slots = '[]'::jsonb, chosen_slot_start = NULL,
           slot_reprompts = 0, sending_claimed_at = NULL, updated_at = :now
     WHERE id = :id
""")


def close_expired(db, now: datetime) -> int:
    return db.execute(_CLOSE_EXPIRED, {"closed": CLOSED, "reason": CLOSED_EXPIRED, "now": now,
                                       "open_states": list(OPEN_STATES)}).rowcount


def settle_stale_claims(db, now: datetime) -> int:
    cutoff = now - timedelta(seconds=STALE_CLAIM_SECONDS)
    sends = db.execute(_SETTLE_STALE_SENDS, {
        "now": now, "status": SEND_UNKNOWN, "at": now.isoformat(), "sendable_states": list(SENDABLE_STATES),
        "cutoff": cutoff}).scalars().all()
    if sends:
        logger.error("[after-hours-responder] %d send(s) left unfinished recorded as unknown, not resent: ids=%s",
                     len(sends), sends)
    bookings = db.execute(_SETTLE_STALE_BOOKINGS, {
        "now": now, "closed": CLOSED, "reason": CLOSED_BOOKING_UNKNOWN, "slot_chosen": SLOT_CHOSEN,
        "cutoff": cutoff}).scalars().all()
    if bookings:
        logger.error("[after-hours-responder] %d booking(s) left unfinished closed as unknown: check the calendar "
                     "for ids=%s", len(bookings), bookings)
    return len(sends) + len(bookings)


def _claim(db, now: datetime, limit: int, still_due: Sequence[int]) -> list[DueStep]:
    rows = db.execute(_CLAIM, {
        "due_states": [*SENDABLE_STATES, SLOT_CHOSEN], "now": now, "limit": limit, "still_due": list(still_due),
        "slot_offer_receipt": json.dumps([{"state": SLOT_CHOICE}])}).all()
    return [DueStep(*row) for row in rows]


def _window_open(item: DueStep, now: datetime) -> bool:
    """8am-8pm for the recipient: by the phone's own timezones when there is one, else Eastern."""
    if item.phone:
        return text_window_open(item.phone, now=now)
    local = now.astimezone(ZoneInfo(DEFAULT_TZ)).time()
    return CALL_WINDOW_START <= local < CALL_WINDOW_END


def _may_contact(db, item: DueStep) -> bool:
    if item.channel == CHANNEL_SMS:
        return has_text_consent(db, item.phone or "")
    return not is_email_suppressed(db, item.email or "")


def _waiting(item: DueStep, now: datetime, *, carrier_approved: bool, has_sender: bool) -> Optional[str]:
    """Why this message cannot go yet (it is tried again on the next pass), or None."""
    if not _window_open(item, now):
        return WAITING_HOURS
    if item.channel == CHANNEL_SMS and not carrier_approved:
        return WAITING_CARRIER
    return None if has_sender else NOT_CONFIGURED


def _slots_to_offer(item: DueStep, slot_finder: Optional[SlotFinder], now: datetime) -> list[datetime]:
    """The slots this offer lists: the same ones again when the lead's reply named none (the numbers must keep
    their meaning), else up to OFFERED_SLOT_COUNT open slots on the next business morning."""
    if item.offered_slots:
        return [datetime.fromisoformat(slot) for slot in item.offered_slots]
    if slot_finder is None:
        return []
    window_start, window_end = next_morning_window(now)
    found = sorted(slot for slot in slot_finder(window_start, window_end, now)
                   if window_start <= slot < window_end)
    return found[:OFFERED_SLOT_COUNT]


def _message(item: DueStep, slots: Sequence[datetime]) -> str:
    if item.state == BOOKED:
        return booked_message(item.chosen_slot_start)
    if item.state != SLOT_CHOICE:
        return question_message(item.state, item.first_name)
    wording = (SLOT_OFFER_AGAIN if item.offered_slots else
               SLOT_OFFER_AFTER_TAKEN if item.offered_before else SLOT_OFFER)
    return slot_offer_message(slots, wording=wording)


def _receipt(item: DueStep, status: str, now: datetime, message_id: Optional[str] = None) -> str:
    return json.dumps([{"state": item.state, "template_version": item.template_version, "status": status,
                        "provider_message_id": message_id, "at": now.isoformat()}])


def _record_send(db, item: DueStep, status: str, now: datetime, *, message_id: Optional[str] = None,
                 offered_slots: Optional[str] = None) -> None:
    db.execute(_RECORD_SEND, {"id": item.conversation_id, "state": item.state, "now": now,
                              "ttl_hours": CONVERSATION_TTL_HOURS, "offered_slots": offered_slots,
                              "receipt": _receipt(item, status, now, message_id)})


def _stop(db, item: DueStep, reason: str, now: datetime) -> str:
    """This step will never be done. A conversation still in progress closes with the reason; a booking already
    made stands, and the reason its confirmation did not go is kept on the receipt instead."""
    if item.state == BOOKED:
        _record_send(db, item, reason, now)
    else:
        db.execute(_CLOSE, {"id": item.conversation_id, "closed": CLOSED, "reason": reason, "now": now})
    return reason


def _send_one(db, item: DueStep, *, text_sender: Optional[TextSender], email_sender: Optional[EmailSender],
              slot_finder: Optional[SlotFinder], carrier_approved: bool, now: datetime, started: list[bool]) -> str:
    """Check, send and record one due message; returns the outcome (the caller commits). ``started`` is set the
    moment the sender is about to be called: from then on a failure may have reached the recipient."""
    if not _may_contact(db, item):
        return _stop(db, item, CLOSED_NO_CONSENT, now)
    sender = text_sender if item.channel == CHANNEL_SMS else email_sender
    waiting = _waiting(item, now, carrier_approved=carrier_approved, has_sender=sender is not None)
    if waiting:
        db.execute(_RELEASE, {"id": item.conversation_id, "send_attempts": item.send_attempts})
        return waiting
    slots: list[datetime] = []
    if item.state == SLOT_CHOICE:
        slots = _slots_to_offer(item, slot_finder, now)
        if not slots:
            return _stop(db, item, CLOSED_NO_SLOTS, now)
    body = _message(item, slots)
    if backflip_hit(body):
        logger.error("[after-hours-responder] id=%s message held: Backflip lane guard", item.conversation_id)
        return _stop(db, item, CLOSED_BLOCKED_LANE, now)
    offered_slots = json.dumps([slot.isoformat() for slot in slots]) if slots else None
    started[0] = True
    try:
        if item.channel == CHANNEL_SMS:
            message_id = text_sender(item.phone, body, item.first_name)
        else:
            message_id = email_sender(item.email, EMAIL_SUBJECT, body, phone=item.phone, first_name=item.first_name)
    except GhlSendRefused:
        return _stop(db, item, CLOSED_SEND_REFUSED, now)
    except GhlSmsError as exc:
        if exc.ambiguous:
            _record_send(db, item, SEND_UNKNOWN, now, offered_slots=offered_slots)
            return SEND_UNKNOWN
        attempts = item.send_attempts + 1
        if attempts >= MAX_SEND_ATTEMPTS:
            return _stop(db, item, CLOSED_SEND_FAILED, now)
        db.execute(_RELEASE, {"id": item.conversation_id, "send_attempts": attempts})
        return RETRY_LATER
    _record_send(db, item, SENT, now, message_id=message_id, offered_slots=offered_slots)
    return SENT


def _book_one(db, item: DueStep, *, booker: Optional[Booker], now: datetime) -> str:
    """Book the slot the lead picked and record the result; returns the outcome (the caller commits)."""
    if booker is None:
        return _stop(db, item, CLOSED_NO_SLOTS, now)
    try:
        outcome = booker(BookingRequest(
            conversation_id=item.conversation_id, slot_start=item.chosen_slot_start, channel=item.channel,
            phone=item.phone, email=item.email, first_name=item.first_name, ghl_contact_id=item.ghl_contact_id,
            answers=tuple(item.answers)))
    except Exception as exc:  # class only: calendar errors can embed the lead's details
        logger.error("[after-hours-responder] id=%s booking gave no clear result (%s): check the calendar",
                     item.conversation_id, type(exc).__name__)
        return _stop(db, item, CLOSED_BOOKING_UNKNOWN, now)
    if outcome.status == BOOKING_BOOKED:
        db.execute(_RECORD_BOOKING, {"id": item.conversation_id, "booked": BOOKED,
                                     "booking_ref": outcome.booking_ref, "now": now})
        return BOOKED_NOW
    if outcome.status == BOOKING_SLOT_TAKEN:
        db.execute(_OFFER_FRESH_SLOTS, {"id": item.conversation_id, "slot_choice": SLOT_CHOICE, "now": now})
        return SLOT_TAKEN
    return _stop(db, item, CLOSED_BOOKING_REFUSED, now)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def process_due(db, *, text_sender: Optional[TextSender], email_sender: Optional[EmailSender],
                carrier_approved: bool, slot_finder: Optional[SlotFinder] = None, booker: Optional[Booker] = None,
                now: Optional[datetime] = None, limit: int = SEND_BATCH_SIZE,
                clock: Optional[Callable[[], datetime]] = None) -> dict[str, int]:
    """Do every due step once. Commits: the claims first (so a crash cannot make a second worker repeat a step),
    then each step's outcome on its own. A step that makes another due (a booking, then its confirmation) is
    followed up in the same pass; a step left waiting is not tried twice in one pass. The window is checked
    against the time each message is reached: ``clock`` is read per message unless ``now`` is injected. Slots are
    offered only when both ``slot_finder`` and ``booker`` are given."""
    clock = clock or _utcnow
    started_at = now or clock()
    counts: dict[str, int] = {}
    expired = close_expired(db, started_at)
    if expired:
        counts[CLOSED_EXPIRED] = expired
    settle_stale_claims(db, started_at)
    still_due: list[int] = []
    for _ in range(SEND_ROUNDS_PER_PASS):
        items = _claim(db, started_at, limit, still_due)
        db.commit()
        if not items:
            break
        for item in items:
            outcome = _do_step(db, item, text_sender=text_sender, email_sender=email_sender,
                               slot_finder=slot_finder if booker else None, booker=booker,
                               carrier_approved=carrier_approved, now=now or clock())
            if outcome is None:
                continue
            if outcome in _STILL_DUE:
                still_due.append(item.conversation_id)
            counts[outcome] = counts.get(outcome, 0) + 1
            logger.info("[after-hours-responder] id=%s state=%s channel=%s outcome=%s", item.conversation_id,
                        item.state, item.channel, outcome)
    return counts


def _do_step(db, item: DueStep, *, text_sender: Optional[TextSender], email_sender: Optional[EmailSender],
             slot_finder: Optional[SlotFinder], booker: Optional[Booker], carrier_approved: bool,
             now: datetime) -> Optional[str]:
    """One claimed step, committed. None when it crashed with its claim kept (it settles as unknown later)."""
    started = [False]
    try:
        if item.state == SLOT_CHOSEN:
            outcome = _book_one(db, item, booker=booker, now=now)
        else:
            outcome = _send_one(db, item, text_sender=text_sender, email_sender=email_sender,
                                slot_finder=slot_finder, carrier_approved=carrier_approved, now=now,
                                started=started)
        db.commit()
        return outcome
    except Exception as exc:  # class only: SQL and HTTP errors can embed phones
        db.rollback()
        # A booking step raises here only on the write that records the booker's answer: the booking may exist.
        may_have_happened = started[0] or item.state == SLOT_CHOSEN
        logger.error("[after-hours-responder] id=%s crashed (%s)%s", item.conversation_id, type(exc).__name__,
                     "; the claim is kept and settles as unknown" if may_have_happened else "")
        return None if may_have_happened else _release_unsent(db, item)


def _release_unsent(db, item: DueStep) -> Optional[str]:
    """After an error BEFORE the sender was called nothing went out: free the claim so the next pass tries again.
    None when even that write failed (the claim then goes stale and settles as unknown)."""
    try:
        db.execute(_RELEASE, {"id": item.conversation_id, "send_attempts": item.send_attempts})
        db.commit()
        return RETRY_LATER
    except Exception as exc:  # class only
        logger.error("[after-hours-responder] id=%s claim could not be released (%s)", item.conversation_id,
                     type(exc).__name__)
        db.rollback()
        return None


def run_send_cycle(*, now: Optional[datetime] = None) -> dict[str, int]:
    """One pass with the real settings, senders and its own session. Nothing while the feature is off or the
    lending emergency halt is on."""
    if not conversations_enabled():
        return {}
    if is_globally_halted():
        logger.warning("[after-hours-responder] pass skipped: lending emergency halt is on")
        return {}
    from config.settings import get_settings
    from src.lending.db import lending_session
    from src.lending.ghl_email import get_email_sender
    from src.lending.ghl_sms import get_sender

    from src.lending.after_hours_calendar import calendar_hooks

    # With no morning calendar configured both hooks are None: a lead who answers all three questions is closed
    # as no_slots and stays on the morning call list.
    with lending_session() as db:
        slot_finder, booker = calendar_hooks(db)
        return process_due(db, text_sender=get_sender(), email_sender=get_email_sender(),
                           carrier_approved=bool(get_settings().lending_sms_carrier_approved),
                           slot_finder=slot_finder, booker=booker, now=now)


def run_send_cycle_now() -> None:
    """Send what a webhook just made due, without waiting for the next sweep. Never raises."""
    try:
        counts = run_send_cycle()
        if counts:
            logger.info("[after-hours-responder] %s", counts)
    except Exception as exc:  # class only: errors can embed phones
        logger.error("[after-hours-responder] send pass failed (%s); the next sweep retries", type(exc).__name__)
