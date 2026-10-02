"""WP-GL-10: schedule, cancel and render booking confirmation and reminders.

Public API:
  schedule_booking_messages(db, booking) → int  rows inserted
  cancel_booking_messages(db, booking_ref, reason) → int  rows cancelled
  render_text(kind, **fields) → str
  render_email(kind, **fields) → tuple[subject, body]

All state is in lending.booking_messages. schedule_booking_messages()
uses ON CONFLICT DO NOTHING so retried events from WP-GL-5's booking
trigger are idempotent.

A booking passed to schedule_booking_messages must carry:
  booking_ref       str         stable booking identifier from WP-GL-5
  first_name        str         contact first name (may be blank)
  contact_phone     str|None    normalized E.164; None → email-only
  contact_email     str|None    used for fallback (B4)
  property_address  str|None    None → templates drop the address phrase
  slot_start_utc    datetime    timezone-aware UTC
  booked_by         str|None    caller seat id, or 'ai'
  text_consent      bool        caller asked and logged the yes (G6)
  status            str         'confirmed' or 'pending' (AI bookings start pending)
  ghl_contact_id    str|None    GHL contact id for Live messenger

GL-5 provides this contract via its booking event. Until GL-5 is merged,
schedule_booking_messages() accepts a plain dict with these keys.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_reminders import (
    ALL_KINDS,
    CALLBACK_NUMBER_PLACEHOLDER,
    CONFIRMATION_NO_ADDRESS,
    CONFIRMATION_WITH_ADDRESS,
    NIGHT_BEFORE_NO_ADDRESS,
    NIGHT_BEFORE_WITH_ADDRESS,
    NINETY_MIN_NO_ADDRESS,
    NINETY_MIN_WITH_ADDRESS,
    EMAIL_CONFIRMATION_NO_ADDRESS,
    EMAIL_CONFIRMATION_WITH_ADDRESS,
    EMAIL_FROM,
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
    MIN_LEAD_SECONDS_90MIN,
    NIGHT_BEFORE_HOUR_ET,
    NIGHT_BEFORE_MINUTE_ET,
    NINETY_MIN_SECONDS,
    TIMEZONE,
)

logger = logging.getLogger(__name__)

_INSERT = text("""
    INSERT INTO lending.booking_messages
        (booking_ref, kind, channel, send_at, first_name, contact_phone,
         contact_email, property_address, slot_start_utc, booked_by,
         text_consent)
    VALUES
        (:booking_ref, :kind, :channel, :send_at, :first_name, :contact_phone,
         :contact_email, :property_address, :slot_start_utc, :booked_by,
         :text_consent)
    ON CONFLICT (booking_ref, kind) DO NOTHING
""")

_CANCEL = text("""
    UPDATE lending.booking_messages
       SET status = 'cancelled', cancel_reason = :reason
     WHERE booking_ref = :booking_ref
       AND status = 'pending'
""")


def schedule_booking_messages(db, booking: dict[str, Any]) -> int:
    """Write confirmation and reminder rows for one booking.

    Returns the number of rows inserted (0 if all already existed).
    Does not commit; caller commits.
    """
    booking_ref: str = booking["booking_ref"]
    first_name: str = booking.get("first_name") or ""
    contact_phone: Optional[str] = booking.get("contact_phone")
    contact_email: Optional[str] = booking.get("contact_email")
    property_address: Optional[str] = booking.get("property_address")
    slot_start_utc: datetime = booking["slot_start_utc"]
    booked_by: Optional[str] = booking.get("booked_by")
    text_consent: bool = bool(booking.get("text_consent", False))

    # Determine the outbound channel for this contact.
    # Text is primary when: 10DLC has been approved (LENDING_TEXT_ENABLED)
    # AND the caller logged the consent yes (G6). Email is the fallback (B4).
    channel = _channel(text_consent=text_consent)

    rows: list[dict] = []

    # Confirmation — always scheduled; send_at is at booking time (NOW at the
    # caller side, but we record a one-second future so the worker picks it up).
    rows.append(_row(
        booking_ref=booking_ref,
        kind=KIND_CONFIRMATION,
        channel=channel,
        send_at=_confirmation_send_at(),
        first_name=first_name,
        contact_phone=contact_phone,
        contact_email=contact_email,
        property_address=property_address,
        slot_start_utc=slot_start_utc,
        booked_by=booked_by,
        text_consent=text_consent,
    ))

    # Night-before reminder — skip if booking is made after the send window on
    # the calendar day before the slot (e.g. booked at 7pm for 10am tomorrow;
    # the 6pm window has passed).
    night_before_at = _night_before_send_at(slot_start_utc)
    if night_before_at is not None:
        rows.append(_row(
            booking_ref=booking_ref,
            kind=KIND_NIGHT_BEFORE,
            channel=channel,
            send_at=night_before_at,
            first_name=first_name,
            contact_phone=contact_phone,
            contact_email=contact_email,
            property_address=property_address,
            slot_start_utc=slot_start_utc,
            booked_by=booked_by,
            text_consent=text_consent,
        ))
    else:
        logger.info(
            "[booking-messages] night_before skipped (window passed) booking_ref=%s", booking_ref
        )

    # 90-minute reminder — skip if the slot is fewer than MIN_LEAD_SECONDS_90MIN away.
    ninety_min_at = slot_start_utc - timedelta(seconds=NINETY_MIN_SECONDS)
    now_utc = datetime.now(timezone.utc)
    if (ninety_min_at - now_utc).total_seconds() >= MIN_LEAD_SECONDS_90MIN:
        rows.append(_row(
            booking_ref=booking_ref,
            kind=KIND_NINETY_MIN,
            channel=channel,
            send_at=ninety_min_at,
            first_name=first_name,
            contact_phone=contact_phone,
            contact_email=contact_email,
            property_address=property_address,
            slot_start_utc=slot_start_utc,
            booked_by=booked_by,
            text_consent=text_consent,
        ))
    else:
        logger.info(
            "[booking-messages] ninety_min skipped (too soon) booking_ref=%s", booking_ref
        )

    if not rows:
        return 0

    result = db.execute(_INSERT, rows)
    inserted = result.rowcount
    logger.info(
        "[booking-messages] scheduled %d/%d rows booking_ref=%s channel=%s",
        inserted, len(rows), booking_ref, channel,
    )
    return inserted


def cancel_booking_messages(db, booking_ref: str, reason: str) -> int:
    """Cancel all pending messages for a booking_ref.

    Called when a booking is cancelled or rescheduled (from the GHL webhook
    in lending_ghl_router). Does not commit; caller commits.
    """
    result = db.execute(_CANCEL, {"booking_ref": booking_ref, "reason": reason})
    logger.info(
        "[booking-messages] cancelled %d pending rows booking_ref=%s reason=%s",
        result.rowcount, booking_ref, reason,
    )
    return result.rowcount


# ── Template rendering ────────────────────────────────────────────────────────

def render_text(
    kind: str,
    *,
    first_name: str,
    slot_start_utc: datetime,
    property_address: Optional[str] = None,
    number: str = CALLBACK_NUMBER_PLACEHOLDER,
) -> str:
    """Render the approved text template for a given kind.

    Truncates to MAX_TEXT_CHARS to stay within carrier limits.
    """
    fmt = _text_template(kind, has_address=bool(property_address))
    dt_et = slot_start_utc.astimezone(TIMEZONE)
    hour = dt_et.hour % 12 or 12
    ampm = "am" if dt_et.hour < 12 else "pm"
    time_str = f"{hour}:{dt_et.strftime('%M')} {ampm} ET"
    body = fmt.format(
        first_name=first_name or "there",
        date=dt_et.strftime("%A, %B") + f" {dt_et.day}",
        time=time_str,
        property_address=property_address or "",
        number=number,
    )
    return body[:MAX_TEXT_CHARS]


def render_email(
    kind: str,
    *,
    first_name: str,
    slot_start_utc: datetime,
    property_address: Optional[str] = None,
    number: str = CALLBACK_NUMBER_PLACEHOLDER,
) -> tuple[str, str]:
    """Render the email subject and body for a given kind."""
    subject = _email_subject(kind)
    fmt = _email_template(kind, has_address=bool(property_address))
    dt_et = slot_start_utc.astimezone(TIMEZONE)
    hour = dt_et.hour % 12 or 12
    ampm = "am" if dt_et.hour < 12 else "pm"
    time_str = f"{hour}:{dt_et.strftime('%M')} {ampm} ET"
    body = fmt.format(
        first_name=first_name or "there",
        date=dt_et.strftime("%A, %B") + f" {dt_et.day}",
        time=time_str,
        property_address=property_address or "",
        number=number,
    )
    return subject, body


# ── Internals ─────────────────────────────────────────────────────────────────

def _channel(*, text_consent: bool) -> str:
    """Determine the outbound channel for this contact.

    Text is used only when LENDING_TEXT_ENABLED is true AND the caller logged
    the consent yes. Email is the fallback in all other cases (B4).
    """
    from config.settings import get_settings
    text_enabled = getattr(get_settings(), "lending_text_enabled", False)
    if text_enabled and text_consent:
        return "text"
    return "email"


def _confirmation_send_at() -> datetime:
    """Confirmation goes out immediately — one second from now."""
    return datetime.now(timezone.utc) + timedelta(seconds=1)


def _night_before_send_at(slot_start_utc: datetime) -> Optional[datetime]:
    """The ET evening send time on the calendar day before the slot.

    Returns None if that moment is in the past (booking made too late).
    """
    from datetime import date as date_cls

    slot_et = slot_start_utc.astimezone(TIMEZONE)
    day_before = slot_et.date() - timedelta(days=1)
    if day_before < date_cls.today():
        return None

    # Build an aware datetime at NIGHT_BEFORE_HOUR_ET on the day before.
    send_et = datetime(
        day_before.year, day_before.month, day_before.day,
        NIGHT_BEFORE_HOUR_ET, NIGHT_BEFORE_MINUTE_ET,
        tzinfo=TIMEZONE,
    )
    send_utc = send_et.astimezone(timezone.utc)
    if send_utc <= datetime.now(timezone.utc):
        return None
    return send_utc


def _row(
    *,
    booking_ref: str,
    kind: str,
    channel: str,
    send_at: datetime,
    first_name: str,
    contact_phone: Optional[str],
    contact_email: Optional[str],
    property_address: Optional[str],
    slot_start_utc: datetime,
    booked_by: Optional[str],
    text_consent: bool,
) -> dict:
    return {
        "booking_ref": booking_ref,
        "kind": kind,
        "channel": channel,
        "send_at": send_at,
        "first_name": first_name,
        "contact_phone": contact_phone,
        "contact_email": contact_email,
        "property_address": property_address,
        "slot_start_utc": slot_start_utc,
        "booked_by": booked_by,
        "text_consent": text_consent,
    }


def _text_template(kind: str, *, has_address: bool) -> str:
    if kind == KIND_CONFIRMATION:
        return CONFIRMATION_WITH_ADDRESS if has_address else CONFIRMATION_NO_ADDRESS
    if kind == KIND_NIGHT_BEFORE:
        return NIGHT_BEFORE_WITH_ADDRESS if has_address else NIGHT_BEFORE_NO_ADDRESS
    if kind == KIND_NINETY_MIN:
        return NINETY_MIN_WITH_ADDRESS if has_address else NINETY_MIN_NO_ADDRESS
    raise ValueError(f"unknown reminder kind: {kind!r}")


def _email_template(kind: str, *, has_address: bool) -> str:
    if kind == KIND_CONFIRMATION:
        return EMAIL_CONFIRMATION_WITH_ADDRESS if has_address else EMAIL_CONFIRMATION_NO_ADDRESS
    if kind == KIND_NIGHT_BEFORE:
        return EMAIL_NIGHT_BEFORE_WITH_ADDRESS if has_address else EMAIL_NIGHT_BEFORE_NO_ADDRESS
    if kind == KIND_NINETY_MIN:
        return EMAIL_NINETY_MIN_WITH_ADDRESS if has_address else EMAIL_NINETY_MIN_NO_ADDRESS
    raise ValueError(f"unknown reminder kind: {kind!r}")


def _email_subject(kind: str) -> str:
    return {
        KIND_CONFIRMATION: EMAIL_SUBJECT_CONFIRMATION,
        KIND_NIGHT_BEFORE: EMAIL_SUBJECT_NIGHT_BEFORE,
        KIND_NINETY_MIN: EMAIL_SUBJECT_NINETY_MIN,
    }[kind]
