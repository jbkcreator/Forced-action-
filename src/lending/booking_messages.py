"""WP-GL-10: schedule, cancel and render the booking confirmation and reminders.

Entry point for a confirmed booking: ``handle_booking_confirmed(db, payload)``. It resolves the
contact from ``fa_max_persons`` (via ``person_id``), writes the three ``lending.booking_messages``
rows (idempotent on ``(booking_ref, kind)``) and opens the confirmation-call task. Nothing in this
module sends: ``reminder_worker`` does, behind the consent / suppression / window gates.

The payload is the contract the GL-5 booking event must carry (not yet emitted by #323 — see the
GL-5 owner's answer; the transport, outbox event or otherwise, is theirs to choose)::

    booking_ref        str       fa_max_bookings.booking_ref
    provider_event_id  str|None  fa_max_bookings.provider_event_id (the GHL appointment id)
    person_id          str|None  fa_max_bookings.person_id -> fa_max_persons
    slot_start_utc     datetime  timezone-aware
    property_address   str|None  from the gate answers; None drops the address phrase
    booked_by          str|None  the caller's login (gate.captured_by), or "ai"
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Mapping, Optional

from sqlalchemy import text

from config.lending_reminders import (
    CONFIRMATION_NO_ADDRESS,
    CONFIRMATION_WITH_ADDRESS,
    EMAIL_CONFIRMATION_NO_ADDRESS,
    EMAIL_CONFIRMATION_WITH_ADDRESS,
    EMAIL_NIGHT_BEFORE_NO_ADDRESS,
    EMAIL_NIGHT_BEFORE_WITH_ADDRESS,
    EMAIL_NINETY_MIN_NO_ADDRESS,
    EMAIL_NINETY_MIN_WITH_ADDRESS,
    EMAIL_SUBJECT_CONFIRMATION,
    EMAIL_SUBJECT_NIGHT_BEFORE,
    EMAIL_SUBJECT_NINETY_MIN,
    KIND_CONFIRMATION,
    KIND_NIGHT_BEFORE,
    KIND_NINETY_MIN,
    MAX_TEXT_CHARS,
    NIGHT_BEFORE_HOUR_ET,
    NIGHT_BEFORE_MINUTE_ET,
    NIGHT_BEFORE_NO_ADDRESS,
    NIGHT_BEFORE_WITH_ADDRESS,
    NINETY_MIN_NO_ADDRESS,
    NINETY_MIN_SECONDS,
    NINETY_MIN_WITH_ADDRESS,
    STATUS_CANCELLED,
    STATUS_PENDING,
    STATUS_SKIPPED,
    TEXT_WINDOW_END_HOUR,
    TEXT_WINDOW_START_HOUR,
    TIMEZONE,
)
from src.lending.compliance import phone_hash
from src.lending.text_back import first_name_of
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

_INSERT = text("""
    INSERT INTO lending.booking_messages
        (booking_ref, provider_event_id, person_id, kind, send_at, status, skip_reason, first_name,
         contact_phone, contact_email, property_address, slot_start_utc, booked_by)
    VALUES
        (:booking_ref, :provider_event_id, :person_id, :kind, :send_at, :status, :skip_reason, :first_name,
         :contact_phone, :contact_email, :property_address, :slot_start_utc, :booked_by)
    ON CONFLICT (booking_ref, kind) DO NOTHING
""")

_PERSON = text("""
    SELECT COALESCE(m.full_name, p.full_name) AS full_name,
           COALESCE(m.phone, p.phone)         AS phone,
           COALESCE(m.email, p.email)         AS email
      FROM fa_max_persons p
      LEFT JOIN fa_max_persons m ON m.person_id = p.merged_into_id
     WHERE p.person_id::text = :person_id
""")

_CANCEL_BY_REF = text("""
    UPDATE lending.booking_messages SET status = 'cancelled', cancel_reason = :reason, decided_at = now()
     WHERE booking_ref = :ref AND status = 'pending'
""")
_CANCEL_BY_EVENT = text("""
    UPDATE lending.booking_messages SET status = 'cancelled', cancel_reason = :reason, decided_at = now()
     WHERE provider_event_id = :event_id AND status = 'pending'
""")


@dataclass(frozen=True)
class ScheduleResult:
    inserted: int
    skip_reason: Optional[str]  # set when every row was recorded as skipped (no person / no contact method)
    assignee: Optional[str]     # who owns the confirmation call


# ── scheduling ────────────────────────────────────────────────────────────────

def handle_booking_confirmed(db, payload: Mapping[str, Any], *, now: Optional[datetime] = None) -> ScheduleResult:
    """Schedule the three messages and the confirmation-call task for one confirmed booking.

    Idempotent: a redelivered event inserts nothing and changes nothing. Does not commit.
    A booking with no resolvable person or no phone/email still gets its rows, recorded as skipped
    with the reason, so the gap is visible instead of silent.
    """
    from src.lending.confirmation_tasks import assign_confirmation_task

    now = now or datetime.now(timezone.utc)
    booking_ref = str(payload["booking_ref"])
    slot_start = payload["slot_start_utc"]
    if slot_start.tzinfo is None:
        raise ValueError("slot_start_utc must be timezone-aware")

    person_id = str(payload["person_id"]) if payload.get("person_id") else None
    person = _person(db, person_id) if person_id else None
    phone = normalize_phone(person["phone"]) if person and person["phone"] else None
    email = (person["email"] or "").strip().lower() or None if person else None
    skip_reason = None
    if person_id is None:
        skip_reason = "no_person"
    elif person is None:
        skip_reason = "person_not_found"
    elif not phone and not email:
        skip_reason = "no_contact_method"

    common = {
        "booking_ref": booking_ref,
        "provider_event_id": payload.get("provider_event_id"),
        "person_id": person_id,
        "first_name": first_name_of(person["full_name"]) if person else None,
        "contact_phone": phone,
        "contact_email": email,
        "property_address": (payload.get("property_address") or None),
        "slot_start_utc": slot_start,
        "booked_by": payload.get("booked_by"),
    }
    rows = [
        _row(common, KIND_CONFIRMATION, now, skip_reason),
        _row(common, KIND_NIGHT_BEFORE, night_before_send_at(slot_start), skip_reason, now=now),
        _row(common, KIND_NINETY_MIN, slot_start - timedelta(seconds=NINETY_MIN_SECONDS), skip_reason, now=now),
    ]
    inserted = sum(db.execute(_INSERT, row).rowcount for row in rows)  # 3 rows; per-statement rowcount is exact
    if skip_reason:
        logger.warning("[booking-messages] booking_ref=%s recorded as skipped (%s)", booking_ref, skip_reason)
    else:
        logger.info("[booking-messages] booking_ref=%s scheduled %d rows phone_hash=%s",
                    booking_ref, inserted, phone_hash(phone)[:12] if phone else "-")
    assignee = assign_confirmation_task(db, booking_ref=booking_ref, person_id=person_id,
                                        booked_by=payload.get("booked_by"), slot_start_utc=slot_start)
    return ScheduleResult(inserted=inserted, skip_reason=skip_reason, assignee=assignee)


def _person(db, person_id: str) -> Optional[Mapping[str, Any]]:
    return db.execute(_PERSON, {"person_id": person_id}).mappings().first()


def _row(common: dict, kind: str, send_at: Optional[datetime], skip_reason: Optional[str], *,
         now: Optional[datetime] = None) -> dict:
    """One insert row. A reminder whose time has already passed is recorded as skipped (visible)."""
    status, reason = STATUS_PENDING, skip_reason
    if send_at is None or (now is not None and send_at <= now and kind != KIND_CONFIRMATION):
        status, reason = STATUS_SKIPPED, reason or "too_late"
    elif skip_reason:
        status = STATUS_SKIPPED
    return {**common, "kind": kind, "send_at": send_at or common["slot_start_utc"],
            "status": status, "skip_reason": reason}


def night_before_send_at(slot_start_utc: datetime) -> datetime:
    """NIGHT_BEFORE_HOUR_ET on the Eastern calendar day before the slot (may be in the past)."""
    day_before: date = slot_start_utc.astimezone(TIMEZONE).date() - timedelta(days=1)
    return datetime.combine(day_before, time(NIGHT_BEFORE_HOUR_ET, NIGHT_BEFORE_MINUTE_ET), tzinfo=TIMEZONE)


# ── cancellation ──────────────────────────────────────────────────────────────

def cancel_by_provider_event(db, provider_event_id: str, reason: str) -> int:
    """Cancel pending messages for the booking whose GHL appointment id this is. Does not commit.
    A message already claimed for sending cannot be recalled."""
    n = db.execute(_CANCEL_BY_EVENT, {"event_id": provider_event_id, "reason": reason}).rowcount
    logger.info("[booking-messages] cancelled %d pending rows (event) reason=%s", n, reason)
    return n


def cancel_by_booking_ref(db, booking_ref: str, reason: str) -> int:
    n = db.execute(_CANCEL_BY_REF, {"ref": booking_ref, "reason": reason}).rowcount
    logger.info("[booking-messages] cancelled %d pending rows booking_ref=%s reason=%s", n, booking_ref, reason)
    return n


# ── the text window ───────────────────────────────────────────────────────────

def text_window_open(moment: datetime) -> bool:
    return TEXT_WINDOW_START_HOUR <= moment.astimezone(TIMEZONE).hour < TEXT_WINDOW_END_HOUR


def next_text_window(moment: datetime) -> datetime:
    """The first moment at or after ``moment`` at which a text may go out."""
    local = moment.astimezone(TIMEZONE)
    if text_window_open(moment):
        return moment
    day = local.date() if local.hour < TEXT_WINDOW_START_HOUR else local.date() + timedelta(days=1)
    return datetime.combine(day, time(TEXT_WINDOW_START_HOUR), tzinfo=TIMEZONE)


# ── rendering ─────────────────────────────────────────────────────────────────

def display_number(e164: str) -> str:
    """(813) 555-0100 for a +1XXXXXXXXXX number; anything else is shown unchanged."""
    digits = e164[2:] if e164.startswith("+1") and len(e164) == 12 else ""
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}" if digits.isdigit() else e164


def _fields(first_name: Optional[str], slot_start_utc: datetime, property_address: Optional[str],
            number: Optional[str], kind: str) -> dict:
    if kind == KIND_NINETY_MIN and not number:
        raise ValueError("the 90-minute message needs the number to call")
    local = slot_start_utc.astimezone(TIMEZONE)
    hour = local.hour % 12 or 12
    return {
        "first_name": first_name or "there",
        "date": local.strftime("%A, %B") + f" {local.day}",
        "time": f"{hour}:{local.strftime('%M')} {'am' if local.hour < 12 else 'pm'} ET",
        "property_address": (property_address or "").split(",")[0].strip(),
        "number": display_number(number) if number else "",
    }


def render_text(kind: str, *, first_name: Optional[str], slot_start_utc: datetime,
                property_address: Optional[str] = None, number: Optional[str] = None) -> str:
    """The client-approved text for ``kind``; never longer than MAX_TEXT_CHARS. ``number`` (E.164) is the
    text-back number the 90-minute wording asks the borrower to call; required for that kind."""
    fields = _fields(first_name, slot_start_utc, property_address, number, kind)
    template = _TEXT[kind][bool(fields["property_address"])]
    return template.format(**fields)[:MAX_TEXT_CHARS]


def render_email(kind: str, *, first_name: Optional[str], slot_start_utc: datetime,
                 property_address: Optional[str] = None, number: Optional[str] = None) -> tuple[str, str]:
    fields = _fields(first_name, slot_start_utc, property_address, number, kind)
    body = _EMAIL[kind][bool(fields["property_address"])].format(**fields)
    return _EMAIL_SUBJECT[kind], body


# kind -> (without address, with address)
_TEXT = {
    KIND_CONFIRMATION: (CONFIRMATION_NO_ADDRESS, CONFIRMATION_WITH_ADDRESS),
    KIND_NIGHT_BEFORE: (NIGHT_BEFORE_NO_ADDRESS, NIGHT_BEFORE_WITH_ADDRESS),
    KIND_NINETY_MIN: (NINETY_MIN_NO_ADDRESS, NINETY_MIN_WITH_ADDRESS),
}
_EMAIL = {
    KIND_CONFIRMATION: (EMAIL_CONFIRMATION_NO_ADDRESS, EMAIL_CONFIRMATION_WITH_ADDRESS),
    KIND_NIGHT_BEFORE: (EMAIL_NIGHT_BEFORE_NO_ADDRESS, EMAIL_NIGHT_BEFORE_WITH_ADDRESS),
    KIND_NINETY_MIN: (EMAIL_NINETY_MIN_NO_ADDRESS, EMAIL_NINETY_MIN_WITH_ADDRESS),
}
_EMAIL_SUBJECT = {
    KIND_CONFIRMATION: EMAIL_SUBJECT_CONFIRMATION,
    KIND_NIGHT_BEFORE: EMAIL_SUBJECT_NIGHT_BEFORE,
    KIND_NINETY_MIN: EMAIL_SUBJECT_NINETY_MIN,
}
