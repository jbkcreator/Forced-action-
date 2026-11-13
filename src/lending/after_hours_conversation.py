"""Night responder conversation: three qualifying questions, then a call slot for the next business morning.

One row in ``lending.after_hours_conversations`` per after-hours inquiry. :func:`open_conversation` starts it,
:func:`record_reply` applies an inbound message from the reply webhook, and :func:`next_turn` is the whole rule set
as a pure function. Sending the messages and booking the slot are done by the worker that reads the rows: a message
is due while ``prompted_state`` differs from ``state``, and :func:`mark_prompted` records that it went out.

A reply counts only while the lead has been asked the current question (``prompted_state`` = ``state``), so two
quick texts are never taken as answers to two questions. A stop request closes the conversation at once and is
written to the opt-out lists (``propagate_opt_out``), so it stops every channel, whatever wording the lead used.
Nothing here sends, and nothing commits: the caller owns the transaction. Logs carry ids only, never answers or
phones.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_compliance import OptOutChannel
from config.lending_after_hours_conversation import (ANSWER_MAX_CHARS, BOOKED, BOOKED_CONFIRMATION, CHANNEL_EMAIL,
                                                     CHANNEL_SMS, CLOSED, CLOSED_OPTED_OUT, CLOSED_UNCLEAR_SLOT,
                                                     CONVERSATION_TTL_HOURS, GREETING, HANDLED_MESSAGE_IDS_KEPT,
                                                     MAX_SLOT_REPROMPTS, MORNING_END_HOUR, MORNING_START_HOUR,
                                                     OPEN_STATES, QUESTION_STATES, QUESTIONS, SLOT_CHOICE,
                                                     SLOT_CHOSEN, SLOT_OFFER, SLOT_TIMEZONE)
from src.lending.compliance import propagate_opt_out
from src.lending.instantly_replies import is_stop_request
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

OUTCOME_ANSWERED = "answered"
OUTCOME_SLOT_CHOSEN = "slot_chosen"
OUTCOME_REPROMPT = "reprompt"
OUTCOME_OPTED_OUT = "opted_out"
OUTCOME_UNCLEAR_SLOT = "unclear_slot"
OUTCOME_IGNORED = "ignored"
OUTCOME_NO_CONVERSATION = "no_conversation"
OUTCOME_DUPLICATE = "duplicate"
OUTCOME_NOT_AWAITING = "not_awaiting"

# "2", "2.", "#2", "option 2": the whole message must be the pick, so "2 units in Tampa" is never read as slot 2.
_SLOT_PICK = re.compile(r"(?:option\s*|#\s*)?(\d)\s*[.)]?", re.IGNORECASE)


@dataclass(frozen=True)
class Turn:
    """What one inbound message does to a conversation."""
    outcome: str
    state: str
    answers: tuple[str, ...]
    chosen_slot_index: Optional[int] = None
    closed_reason: Optional[str] = None


def conversations_enabled() -> bool:
    from config.settings import get_settings

    return bool(get_settings().lending_after_hours_conversation_enabled)


def slot_pick(message: str, offered_slot_count: int) -> Optional[int]:
    """The zero-based index of the offered slot the message names, or None."""
    match = _SLOT_PICK.fullmatch(message.strip())
    if not match:
        return None
    number = int(match.group(1))
    return number - 1 if 1 <= number <= offered_slot_count else None


def next_turn(state: str, answers: Sequence[str], message: str, *, offered_slot_count: int = 0,
              slot_reprompts: int = 0) -> Turn:
    """Apply one inbound message to a conversation in ``state``. Pure: no clock, no database."""
    kept = tuple(answers)
    body = (message or "").strip()
    if state not in OPEN_STATES or state == SLOT_CHOSEN or not body:
        return Turn(OUTCOME_IGNORED, state, kept)
    if is_stop_request(body):
        return Turn(OUTCOME_OPTED_OUT, CLOSED, kept, closed_reason=CLOSED_OPTED_OUT)
    if state in QUESTION_STATES:
        position = QUESTION_STATES.index(state)
        following = QUESTION_STATES[position + 1] if position + 1 < len(QUESTION_STATES) else SLOT_CHOICE
        return Turn(OUTCOME_ANSWERED, following, (*kept, body[:ANSWER_MAX_CHARS]))
    picked = slot_pick(body, offered_slot_count)
    if picked is not None:
        return Turn(OUTCOME_SLOT_CHOSEN, SLOT_CHOSEN, kept, chosen_slot_index=picked)
    if slot_reprompts >= MAX_SLOT_REPROMPTS:
        return Turn(OUTCOME_UNCLEAR_SLOT, CLOSED, kept, closed_reason=CLOSED_UNCLEAR_SLOT)
    return Turn(OUTCOME_REPROMPT, SLOT_CHOICE, kept)


def question_message(state: str, first_name: Optional[str]) -> str:
    """The message for a question state; the first question carries the greeting."""
    position = QUESTION_STATES.index(state)
    question = QUESTIONS[position]
    return f"{GREETING.format(name=first_name or 'there')} {question}" if position == 0 else question


def next_morning_window(now: datetime) -> tuple[datetime, datetime]:
    """The next business morning still ahead of ``now``: (start, end), never starting in the past. A lead who
    finishes at 9pm Friday gets Monday morning; one who finishes at 10am on a weekday gets the rest of that
    morning."""
    zone = ZoneInfo(SLOT_TIMEZONE)
    day = now.astimezone(zone).date()
    while True:
        closes = datetime.combine(day, time(MORNING_END_HOUR), tzinfo=zone)
        if day.weekday() < 5 and closes > now:
            return max(datetime.combine(day, time(MORNING_START_HOUR), tzinfo=zone), now), closes
        day += timedelta(days=1)


def slot_label(slot_start: datetime) -> str:
    """"Wed Oct 21, 9:30 AM ET": the slot in the calendar's own timezone."""
    local = slot_start.astimezone(ZoneInfo(SLOT_TIMEZONE))
    return f"{local:%a %b} {local.day}, {local.hour % 12 or 12}:{local:%M} {'AM' if local.hour < 12 else 'PM'} ET"


def slot_offer_message(slots: Sequence[datetime], *, wording: str = SLOT_OFFER) -> str:
    return wording.format(slots="\n".join(f"{number}) {slot_label(slot)}"
                                          for number, slot in enumerate(slots, start=1)))


def booked_message(slot_start: datetime) -> str:
    return BOOKED_CONFIRMATION.format(slot=slot_label(slot_start))


_OPEN = text("""
    INSERT INTO lending.after_hours_conversations
           (inquiry_key, source, channel, contact_key, phone, email, ghl_contact_id, first_name, state,
            opened_at, updated_at, expires_at)
    VALUES (:inquiry_key, :source, :channel, :contact_key, :phone, :email, :ghl_contact_id, :first_name, :state,
            :now, :now, :expires_at)
    ON CONFLICT DO NOTHING
    RETURNING id
""")


def open_conversation(db, *, inquiry_key: str, source: str, channel: str, now: datetime,
                      phone: Optional[str] = None, email: Optional[str] = None,
                      ghl_contact_id: Optional[str] = None, first_name: Optional[str] = None) -> Optional[int]:
    """Start a conversation at the first question. Returns its id, or None when this inquiry already has one or
    the contact already has an open one (a second inquiry the same night joins the first). Raises ValueError when
    the channel's own address is missing."""
    norm_phone = normalize(phone) if phone else None
    norm_email = (email or "").strip().lower() or None
    contact_key = norm_phone if channel == CHANNEL_SMS else norm_email if channel == CHANNEL_EMAIL else None
    if not contact_key:
        raise ValueError(f"an after-hours conversation over {channel} needs that channel's address")
    conversation_id = db.execute(_OPEN, {
        "inquiry_key": inquiry_key[:200], "source": source[:40], "channel": channel, "contact_key": contact_key,
        "phone": norm_phone, "email": norm_email, "ghl_contact_id": ghl_contact_id,
        "first_name": (first_name or "")[:100] or None, "state": QUESTION_STATES[0], "now": now,
        "expires_at": now + timedelta(hours=CONVERSATION_TTL_HOURS),
    }).scalar_one_or_none()
    if conversation_id is not None:
        logger.info("[after-hours-conversation] opened id=%s source=%s channel=%s", conversation_id, source, channel)
    return conversation_id


_MARK_PROMPTED = text("""
    UPDATE lending.after_hours_conversations
       SET prompted_state = :state, updated_at = :now,
           offered_slots = COALESCE(CAST(:offered_slots AS jsonb), offered_slots)
     WHERE id = :id AND state = :state
""")


def mark_prompted(db, conversation_id: int, state: str, *, now: datetime,
                  offered_slots: Optional[Sequence[datetime]] = None) -> bool:
    """Record that the message for ``state`` went out (with the slots it offered). False when the conversation has
    moved on since, so a late send never reopens an earlier question."""
    slots = json.dumps([slot.isoformat() for slot in offered_slots]) if offered_slots is not None else None
    return bool(db.execute(_MARK_PROMPTED, {"id": conversation_id, "state": state, "now": now,
                                            "offered_slots": slots}).rowcount)


# The newest open conversation for the sender; locked so two webhook deliveries apply one at a time.
_FIND_OPEN = text("""
    SELECT id, channel, phone, email, state, prompted_state, answers, offered_slots, slot_reprompts,
           handled_message_ids
      FROM lending.after_hours_conversations
     WHERE state = ANY(:open_states) AND expires_at > :now
       AND (ghl_contact_id = CAST(:ghl_contact_id AS text) OR phone = CAST(:phone AS text))
     ORDER BY opened_at DESC
     LIMIT 1
       FOR UPDATE
""")
_APPLY_TURN = text("""
    UPDATE lending.after_hours_conversations
       SET state = :state, answers = CAST(:answers AS jsonb), prompted_state = :prompted_state,
           chosen_slot_start = :chosen_slot_start, slot_reprompts = :slot_reprompts,
           closed_reason = :closed_reason, closed_at = :closed_at,
           handled_message_ids = CAST(:handled_message_ids AS jsonb),
           ghl_contact_id = COALESCE(ghl_contact_id, :ghl_contact_id), updated_at = :now
     WHERE id = :id
""")


def record_reply(db, *, message_id: str, body: str, now: datetime, phone: Optional[str] = None,
                 ghl_contact_id: Optional[str] = None) -> str:
    """Apply an inbound message to the sender's open conversation and return the outcome. Does not commit."""
    norm_phone = normalize(phone) if phone else None
    if not (norm_phone or ghl_contact_id):
        return OUTCOME_NO_CONVERSATION
    row = db.execute(_FIND_OPEN, {"open_states": list(OPEN_STATES), "now": now, "ghl_contact_id": ghl_contact_id,
                                  "phone": norm_phone}).mappings().first()
    if row is None:
        return OUTCOME_NO_CONVERSATION
    handled = list(row["handled_message_ids"])
    if message_id in handled:
        return OUTCOME_DUPLICATE
    if row["prompted_state"] != row["state"]:
        return OUTCOME_NOT_AWAITING
    offered = list(row["offered_slots"])
    turn = next_turn(row["state"], row["answers"], body, offered_slot_count=len(offered),
                     slot_reprompts=row["slot_reprompts"])
    if turn.outcome == OUTCOME_IGNORED:
        return OUTCOME_IGNORED
    reprompted = turn.outcome == OUTCOME_REPROMPT
    chosen = datetime.fromisoformat(offered[turn.chosen_slot_index]) if turn.chosen_slot_index is not None else None
    db.execute(_APPLY_TURN, {
        "id": row["id"], "state": turn.state, "answers": json.dumps(list(turn.answers)),
        # A repeated slot offer is due again: clearing prompted_state is what queues it.
        "prompted_state": None if reprompted else row["prompted_state"],
        "chosen_slot_start": chosen, "slot_reprompts": row["slot_reprompts"] + (1 if reprompted else 0),
        "closed_reason": turn.closed_reason, "closed_at": now if turn.state in (CLOSED, BOOKED) else None,
        "handled_message_ids": json.dumps([*handled, message_id][-HANDLED_MESSAGE_IDS_KEPT:]),
        "ghl_contact_id": ghl_contact_id, "now": now,
    })
    if turn.outcome == OUTCOME_OPTED_OUT:
        propagate_opt_out(db, phone=row["phone"], email=row["email"],
                          source_ref=f"after_hours_conversation:{row['id']}",
                          channel=OptOutChannel.EMAIL if row["channel"] == CHANNEL_EMAIL else OptOutChannel.SMS)
    logger.info("[after-hours-conversation] id=%s reply outcome=%s state=%s", row["id"], turn.outcome, turn.state)
    return turn.outcome
