"""WP-GL-10: schedule, cancel and render the booking confirmation and reminders.

Entry point for a confirmed booking: ``handle_booking_confirmed(db, payload)``. It resolves the
contact from ``fa_max_persons`` (via ``person_id``), writes the three ``lending.booking_messages``
rows (idempotent on ``(booking_ref, kind)``) and opens the confirmation-call task. Nothing in this
module sends: ``reminder_worker`` does, behind the consent / suppression / window gates.

The payload is the contract the GL-5 booking event must carry (not yet emitted by #323 — see the
GL-5 owner's answer; the transport, outbox event or otherwise, is theirs to choose)::

    booking_ref        str       fa_max_bookings.booking_ref
    provider_event_id  str|None  fa_max_bookings.provider_event_id (the GHL appointment id)
    person_id          str|None  fa_max_bookings.person_id -> fa_max_persons (optional, see below)
    phone              str|None  the lending contact's phone (E.164); wins over the person's phone
    first_name         str|None  the contact's first name; wins over the person's name
    email              str|None  the contact's email, for the no-text-consent fallback
    text_consent       bool      the caller asked "Is it okay if we text you the confirmation?" and the
                                 contact said yes (G6); recorded as ``on_call_yes`` consent. Only an
                                 explicit True counts; AI bookings never carry it (they texted us first).
    slot_start_utc     datetime  timezone-aware
    property_address   str|None  from the gate answers; None drops the address phrase
    booked_by          str|None  the caller's login (gate.captured_by), or "ai"

Lending contacts are identified by phone only and have no ``fa_max_persons`` row (confirmed by the
WP-GL-9 owner), so a lending booking supplies ``phone`` / ``first_name`` itself; ``person_id`` is the
fallback source for contact details.
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
    EMAIL_GATE_FAIL,
    EMAIL_NINETY_MIN_WITH_ADDRESS,
    EMAIL_SUBJECT_GATE_FAIL,
    EMAIL_SUBJECT_CONFIRMATION,
    EMAIL_SUBJECT_NIGHT_BEFORE,
    EMAIL_SUBJECT_NINETY_MIN,
    GATE_FAIL_TEXT,
    KIND_CONFIRMATION,
    KIND_GATE_FAIL,
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
from src.lending.confirmation_tasks import AI_BOOKER
from src.lending.consent import record_consent
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
    ON CONFLICT (booking_ref, kind) DO UPDATE SET
        provider_event_id = EXCLUDED.provider_event_id, person_id = EXCLUDED.person_id,
        send_at = EXCLUDED.send_at, status = EXCLUDED.status, skip_reason = EXCLUDED.skip_reason,
        cancel_reason = NULL, channel = NULL, attempts = 0, first_name = EXCLUDED.first_name,
        contact_phone = EXCLUDED.contact_phone, contact_email = EXCLUDED.contact_email,
        property_address = EXCLUDED.property_address, slot_start_utc = EXCLUDED.slot_start_utc,
        booked_by = EXCLUDED.booked_by, provider_message_id = NULL, decided_at = NULL, sent_at = NULL,
        replanned_at = now()
     WHERE lending.booking_messages.status <> 'sending'
       AND (lending.booking_messages.slot_start_utc IS DISTINCT FROM EXCLUDED.slot_start_utc
            OR (lending.booking_messages.contact_phone IS NULL AND lending.booking_messages.contact_email IS NULL
                AND (EXCLUDED.contact_phone IS NOT NULL OR EXCLUDED.contact_email IS NOT NULL)))
""")

_KNOWN_BOOKING = text("""
    SELECT count(*) AS n,
           count(*) FILTER (WHERE contact_phone IS NULL AND contact_email IS NULL) AS without_contact
      FROM lending.booking_messages WHERE booking_ref = :ref
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
_CANCEL_TASK_BY_REF = text("""
    UPDATE lending.confirmation_tasks SET cancelled_at = now()
     WHERE booking_ref = :ref AND cancelled_at IS NULL AND completed_at IS NULL
""")
_CANCEL_TASK_BY_EVENT = text("""
    UPDATE lending.confirmation_tasks SET cancelled_at = now()
     WHERE booking_ref IN (SELECT booking_ref FROM lending.booking_messages WHERE provider_event_id = :event_id)
       AND cancelled_at IS NULL AND completed_at IS NULL
""")


@dataclass(frozen=True)
class ScheduleResult:
    inserted: int               # rows written: new rows, or rows re-planned for a changed slot
    skip_reason: Optional[str]  # set when every row was recorded as skipped (no person / no contact method)
    assignee: Optional[str]     # who owns the confirmation call


# ── scheduling ────────────────────────────────────────────────────────────────

def handle_booking_confirmed(db, payload: Mapping[str, Any], *, now: Optional[datetime] = None) -> ScheduleResult:
    """Schedule the three messages and the confirmation-call task for one confirmed booking.

    Idempotent: a redelivered event (same slot) writes nothing and changes nothing (the exception: a booking whose
    earlier delivery had no phone or email is completed by a delivery that has one). The same ``booking_ref``
    with a different ``slot_start_utc`` is a reschedule: its rows are re-planned for the new time (a row being
    sent right now is left alone). Consent is recorded on the first delivery only, so a redelivery or a
    reschedule never re-grants consent the contact has since revoked. Does not commit.
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
    phone = normalize_phone(payload.get("phone") or "") or (normalize_phone(person["phone"]) if person and person["phone"] else None)
    email = ((payload.get("email") or (person["email"] if person else None) or "").strip().lower()) or None
    name = (payload.get("first_name") or "").strip() or (first_name_of(person["full_name"]) if person else None)
    skip_reason = None
    if not phone and not email:
        skip_reason = ("no_person" if person_id is None else "person_not_found" if person is None
                       else "no_contact_method")

    common = {
        "booking_ref": booking_ref,
        "provider_event_id": payload.get("provider_event_id"),
        "person_id": person_id,
        "first_name": name,
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
    known = db.execute(_KNOWN_BOOKING, {"ref": booking_ref}).mappings().one()
    first_delivery = known["n"] == 0
    repairing = known["n"] > 0 and known["n"] == known["without_contact"]   # earlier delivery had no phone or email
    inserted = sum(db.execute(_INSERT, row).rowcount for row in rows)  # 3 rows; per-statement rowcount is exact
    if skip_reason:
        logger.warning("[booking-messages] booking_ref=%s recorded as skipped (%s)", booking_ref, skip_reason)
    else:
        logger.info("[booking-messages] booking_ref=%s scheduled %d rows phone_hash=%s",
                    booking_ref, inserted, phone_hash(phone)[:12] if phone else "-")
    if (first_delivery or repairing) and payload.get("text_consent") is True and phone and payload.get("booked_by") not in (None, AI_BOOKER):
        record_consent(db, phone, "on_call_yes", captured_by=payload["booked_by"])
    assignee = assign_confirmation_task(db, booking_ref=booking_ref, person_id=person_id,
                                        booked_by=payload.get("booked_by"), slot_start_utc=slot_start, booked_at=now,
                                        revive=not first_delivery and inserted > 0)
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

_RECENTLY_REPLANNED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.booking_messages
                    WHERE provider_event_id = :event_id AND replanned_at > now() - (:seconds * interval '1 second'))
""")


def cancel_by_provider_event(db, provider_event_id: str, reason: str, *, spare_replanned_seconds: int = 0) -> int:
    """Cancel pending messages for the booking whose GHL appointment id this is. Does not commit.
    A message already claimed for sending cannot be recalled.

    ``spare_replanned_seconds``: GHL's "rescheduled" event and the booking flow's new-slot event come from two
    systems in no guaranteed order. When the new slot was planned first, a late cancel would wipe it, so a cancel
    with a grace window does nothing if the booking was re-planned within that many seconds."""
    if spare_replanned_seconds and db.execute(
            _RECENTLY_REPLANNED, {"event_id": provider_event_id, "seconds": spare_replanned_seconds}).scalar():
        logger.info("[booking-messages] cancel ignored: booking was re-planned within %ds (reason=%s)",
                    spare_replanned_seconds, reason)
        return 0
    n = db.execute(_CANCEL_BY_EVENT, {"event_id": provider_event_id, "reason": reason}).rowcount
    db.execute(_CANCEL_TASK_BY_EVENT, {"event_id": provider_event_id})
    logger.info("[booking-messages] cancelled %d pending rows (event) reason=%s", n, reason)
    return n


_BOOKING_CONTACT = text("""
    SELECT person_id, first_name, contact_phone, contact_email, property_address, slot_start_utc, booked_by,
           provider_event_id
      FROM lending.booking_messages WHERE booking_ref = :ref ORDER BY id LIMIT 1
""")


def handle_booking_gate_failed(db, booking_ref: str, *, now: Optional[datetime] = None) -> str:
    """The caller's check did not pass for an AI-booked call: cancel its pending reminders and queue the one
    "we can't hold the call" message to the contact (text if they consented, email otherwise; the worker applies
    the same suppression / consent / window gates as every other message). Returns "queued", "duplicate",
    "unknown_booking" or "not_ai_booked". Idempotent per booking. Does not commit.

    Releasing the calendar slot and moving the contact to nurture belong to the booking flow (WP-GL-5)."""
    now = now or datetime.now(timezone.utc)
    contact = db.execute(_BOOKING_CONTACT, {"ref": booking_ref}).mappings().first()
    if contact is None:
        logger.warning("[booking-messages] gate failed for unknown booking_ref=%s; nothing sent", booking_ref)
        return "unknown_booking"
    if contact["booked_by"] not in (None, AI_BOOKER):
        logger.warning("[booking-messages] booking_ref=%s was booked by a caller; gate-fail message not sent", booking_ref)
        return "not_ai_booked"
    cancel_by_booking_ref(db, booking_ref, "gate_failed")
    skip = None if contact["contact_phone"] or contact["contact_email"] else "no_contact_method"
    row = {
        "booking_ref": booking_ref, "provider_event_id": contact["provider_event_id"], "person_id": contact["person_id"],
        "kind": KIND_GATE_FAIL, "send_at": now, "status": STATUS_SKIPPED if skip else STATUS_PENDING,
        "skip_reason": skip, "first_name": contact["first_name"], "contact_phone": contact["contact_phone"],
        "contact_email": contact["contact_email"], "property_address": contact["property_address"],
        "slot_start_utc": contact["slot_start_utc"], "booked_by": contact["booked_by"],
    }
    queued = db.execute(_INSERT, row).rowcount
    logger.info("[booking-messages] booking_ref=%s gate failed; gate_fail message %s", booking_ref,
                "queued" if queued else "already queued")
    return "queued" if queued else "duplicate"


_AI_BOOKING_BY_PHONE = text("""
    SELECT booking_ref, provider_event_id FROM lending.booking_messages
     WHERE contact_phone = :phone AND kind = 'confirmation' AND (booked_by IS NULL OR booked_by = 'ai')
       AND slot_start_utc > :now
     ORDER BY slot_start_utc LIMIT 1
""")


def find_ai_booking(db, phone: str, now: Optional[datetime] = None) -> Optional[Mapping[str, Any]]:
    """The upcoming AI-booked call for this phone (``booking_ref`` and its GHL ``provider_event_id``), or None."""
    norm = normalize_phone(phone or "")
    if not norm:
        return None
    return db.execute(_AI_BOOKING_BY_PHONE, {"phone": norm, "now": now or datetime.now(timezone.utc)}).mappings().first()


def handle_nurture_entry(db, phone: str, *, now: Optional[datetime] = None) -> str:
    """A contact entered the GHL Nurture stage. If an AI-booked call for them is still ahead, the caller's check
    did not pass (Josh: a failed check moves the contact to nurture), so run the failed-check handling for that
    booking. Returns the handling outcome, or "no_ai_booking" when there is nothing to do. A call that already
    happened, or one a caller booked, is never touched. Does not commit."""
    now = now or datetime.now(timezone.utc)
    booking = find_ai_booking(db, phone, now)
    if booking is None:
        return "no_ai_booking"
    return handle_booking_gate_failed(db, booking["booking_ref"], now=now)


def cancel_by_booking_ref(db, booking_ref: str, reason: str) -> int:
    n = db.execute(_CANCEL_BY_REF, {"ref": booking_ref, "reason": reason}).rowcount
    db.execute(_CANCEL_TASK_BY_REF, {"ref": booking_ref})
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
    KIND_GATE_FAIL: (GATE_FAIL_TEXT, GATE_FAIL_TEXT),
}
_EMAIL = {
    KIND_CONFIRMATION: (EMAIL_CONFIRMATION_NO_ADDRESS, EMAIL_CONFIRMATION_WITH_ADDRESS),
    KIND_NIGHT_BEFORE: (EMAIL_NIGHT_BEFORE_NO_ADDRESS, EMAIL_NIGHT_BEFORE_WITH_ADDRESS),
    KIND_NINETY_MIN: (EMAIL_NINETY_MIN_NO_ADDRESS, EMAIL_NINETY_MIN_WITH_ADDRESS),
    KIND_GATE_FAIL: (EMAIL_GATE_FAIL, EMAIL_GATE_FAIL),
}
_EMAIL_SUBJECT = {
    KIND_CONFIRMATION: EMAIL_SUBJECT_CONFIRMATION,
    KIND_NIGHT_BEFORE: EMAIL_SUBJECT_NIGHT_BEFORE,
    KIND_NINETY_MIN: EMAIL_SUBJECT_NINETY_MIN,
    KIND_GATE_FAIL: EMAIL_SUBJECT_GATE_FAIL,
}
