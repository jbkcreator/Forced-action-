"""The three scheduling operations agents call.

Reads and writes sit on opposite sides of the agent runtime by design.
get_slots() is a plain call: the agent deciding what to offer needs the answer
before it can build anything, and routing that through the work queue would
cost a poll interval per lookup. book() is the side-effecting half and is
registered as a tool so it inherits the audit log.

book() runs the suppression gate itself rather than relying on the send layer.
The calendar provider emails the invitation, so a booking reaches a borrower
without ever passing through the outbound path — the gate has to live here or
it does not run at all for this channel.

Business outcomes are returned; provider failures are raised. A suppressed
contact and an already-taken slot are answers the caller must act on, whereas
a provider rejection is an error and belongs in the agent loop's handler.
"""
from __future__ import annotations

import hashlib
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text as sa_text

from config.calendar import (
    CALENDAR_TIMEZONE,
    CALENDAR_VENTURE_KEY,
    DEFAULT_SLOT_DURATION_MINUTES,
    RESCHEDULE_ALERT_RULE,
)
from src.services.calendar.availability import (
    BusyBlock,
    Slot,
    _require_aware,
    compute_free_slots,
)
from src.services.calendar.client import CalendarClient, CalendarEvent

logger = logging.getLogger(__name__)

RESCHEDULE_ALTERNATIVE_LIMIT = 3

BUSY_CACHE_PREFIX = "calendar:busy:"
# Short enough that a slot taken elsewhere disappears from the page quickly,
# long enough to absorb a link being opened repeatedly.
BUSY_CACHE_TTL_SECONDS = 30

_TZ = ZoneInfo(CALENDAR_TIMEZONE)


@dataclass(frozen=True)
class BookingResult:
    """Outcome of a booking attempt. `reason` is a stable machine-readable code."""

    booked: bool
    event: Optional[CalendarEvent] = None
    booking_ref: Optional[str] = None
    reason: Optional[str] = None
    detail: Optional[str] = None


@dataclass(frozen=True)
class RescheduleRequest:
    """A reschedule surfaced for a human, not an automated rebooking.

    v1 deliberately stops short of moving anything: it gathers the booking and
    some alternatives, pages EXCEPTIONS, and marks the row so the booking is
    visibly in question. Autonomous negotiation is out of scope.
    """

    booking_ref: str
    current: Optional[CalendarEvent]
    alternatives: list[Slot]
    alerted: bool


def get_slots(
    *,
    client: CalendarClient,
    calendar_id: str,
    window_start: datetime,
    window_end: datetime,
    now: datetime,
    duration_minutes: int = DEFAULT_SLOT_DURATION_MINUTES,
    include_weekends: bool = False,
    use_cache: bool = False,
) -> list[Slot]:
    """Bookable slots in the window, honouring every availability rule.

    `use_cache` is for display only. Showing a borrower availability that is
    a few seconds stale costs nothing — they get "that slot just went" and
    pick another. Never enable it for a check that decides a write: book()'s
    re-read must see the calendar as it is now, and a cached answer there
    would reintroduce the double-booking it exists to prevent.
    """
    busy = (
        _cached_busy(client, calendar_id, window_start, window_end)
        if use_cache
        else client.get_busy(calendar_id=calendar_id, start=window_start, end=window_end)
    )
    return compute_free_slots(
        busy=busy,
        window_start=window_start,
        window_end=window_end,
        now=now,
        duration_minutes=duration_minutes,
        include_weekends=include_weekends,
    )


def book(
    *,
    client: CalendarClient,
    session,
    calendar_id: str,
    slot: Slot,
    attendee_email: str,
    topic: str,
    description: str = "",
    person_id: Optional[Any] = None,
    tracked_link_id: Optional[int] = None,
    gate_id: Optional[str] = None,
    phone: Optional[str] = None,
    first_name: Optional[str] = None,
    text_consent: bool = False,
) -> BookingResult:
    """Book a slot for an attendee, refusing if suppressed or already taken.

    gate_id must reference a passed fa_max_booking_gates row. If omitted or
    invalid, the booking is refused — fail closed. This enforces WP-GL-5's
    requirement that no booking reaches Josh's calendar without a caller
    completing the gate. The daily cap (config/booking_gate.py
    CALENDAR_DAILY_CAP) is checked with a Postgres advisory lock to prevent
    concurrent bypass.

    phone/first_name/text_consent are passed straight through to the
    booking-confirmed notification (see _notify_booking_confirmed) — the
    gate table holds enum codes only (no phone, name or consent), so a
    caller that has this contact detail must supply it here. text_consent
    is G6's "is it okay if we text you the confirmation?" yes, asked and
    logged at the booking close, not part of the earlier gate screening.

    Commits. The row claiming the slot must be durable before the calendar
    event is created, or a crash in between leaves a meeting in the
    borrower's inbox that no record points at. A stray row is recoverable;
    a stray meeting is not.
    """
    from src.agents.fa_max.tool_registry import check_suppression
    from src.services.calendar.gate import enforce_daily_cap, get_passed_gate_by_id

    # A naive time would crash the tz-aware busy comparison, or be stored in
    # TIMESTAMPTZ at whatever offset the DB session happens to use.
    _require_aware(slot.start, "slot.start")
    _require_aware(slot.end, "slot.end")

    # Gate re-validation: confirm the gate_id still passes at booking time.
    # Stored as code-only JSONB — never inspect free-text financial fields here.
    if not gate_id:
        logger.info("calendar.book: refused — no gate_id provided")
        return BookingResult(booked=False, reason="gate_required")

    gate_row = get_passed_gate_by_id(session, gate_id)
    if gate_row is None:
        logger.info("calendar.book: refused — gate_id=%s not passed or list blocked", gate_id)
        return BookingResult(booked=False, reason="gate_not_passed")

    suppression = check_suppression(
        recipient=attendee_email, channel="email", session=session
    )
    if suppression["suppressed"]:
        logger.info(
            "calendar.book: refused — recipient suppressed (reason=%s)",
            suppression["reason"],
        )
        return BookingResult(
            booked=False, reason="suppressed", detail=suppression["reason"]
        )

    # A retried request for an already-claimed slot must replay the original
    # outcome, not get refused on the cap — the agent loop re-runs steps by
    # design, and a replay must never look like a fresh failure. Checked
    # before the cap for that reason.
    key = _idempotency_key(calendar_id, slot, attendee_email)
    replay = _existing_booking(session, key)
    if replay is not None:
        return replay

    # Daily cap — advisory lock serialises concurrent requests. Checked
    # against the calendar day slot.start falls on, not the day the request
    # happens to arrive.
    if not enforce_daily_cap(session, slot.start):
        logger.info("calendar.book: refused — daily cap reached")
        return BookingResult(booked=False, reason="daily_cap_reached")

    if _is_taken(client=client, calendar_id=calendar_id, slot=slot):
        logger.info("calendar.book: refused — slot taken since it was offered")
        return BookingResult(booked=False, reason="slot_taken")

    claim = _claim_slot(
        session=session, calendar_id=calendar_id, slot=slot,
        attendee_email=attendee_email, topic=topic, person_id=person_id,
        tracked_link_id=tracked_link_id, idempotency_key=key, gate_id=gate_id,
    )
    if claim is None:
        # Another booking holds this slot or this key. Whoever committed
        # first owns it; the database settled the race, not the re-check.
        logger.info("calendar.book: refused — lost the slot claim")
        replay = _existing_booking(session, key)
        return replay or BookingResult(booked=False, reason="slot_taken")

    booking_ref = claim
    try:
        event = client.create_event(
            calendar_id=calendar_id,
            start=slot.start,
            end=slot.end,
            summary=topic,
            attendee_email=attendee_email,
            description=description,
        )
    except Exception:
        # Release the slot: a pending row holds it against everyone else, and
        # no meeting was created to justify that.
        _release_claim(session, booking_ref)
        logger.exception("calendar.book: provider rejected booking_ref=%s", booking_ref)
        raise

    if not _confirm_claim(session, booking_ref, event):
        # The stale-claim sweep released this row while the provider call was
        # in flight, so the slot may already be someone else's. Take back the
        # meeting rather than leave one no record points at.
        logger.error(
            "calendar.book: claim expired mid-call booking_ref=%s — cancelling event_id=%s",
            booking_ref, event.event_id,
        )
        try:
            client.cancel_event(calendar_id=calendar_id, event_id=event.event_id)
        except Exception:
            logger.exception(
                "calendar.book: could not cancel orphaned event_id=%s", event.event_id
            )
        return BookingResult(booked=False, booking_ref=booking_ref, reason="claim_expired")

    logger.info(
        "calendar.book: booked booking_ref=%s event_id=%s", booking_ref, event.event_id
    )

    _notify_booking_confirmed(
        session=session,
        booking_ref=booking_ref,
        provider_event_id=event.event_id,
        person_id=person_id,
        phone=phone,
        first_name=first_name,
        email=attendee_email,
        text_consent=text_consent,
        slot_start_utc=slot.start,
        gate_row=gate_row,
        captured_by=gate_row.get("captured_by") if gate_row else None,
    )

    return BookingResult(booked=True, event=event, booking_ref=booking_ref)


def _notify_booking_confirmed(
    *,
    session,
    booking_ref: str,
    provider_event_id: Optional[str],
    person_id: Optional[Any],
    phone: Optional[str],
    first_name: Optional[str],
    email: str,
    text_consent: bool,
    slot_start_utc: datetime,
    gate_row: Optional[Any],
    captured_by: Optional[str],
) -> None:
    """Best-effort notification so WP-GL-10 can schedule confirmation/
    reminder messages. Never raises — a notification failure must not undo
    an already-committed, already-calendared booking.

    Calls src.lending.booking_messages.handle_booking_confirmed directly
    (same process — both routers mount on src.api.main) rather than a
    self-loopback HTTP call to /webhooks/lending/booking-confirmed, which
    that module's own docstring offers as an alternative transport. Import
    is lazy and wrapped: per the agreed merge order (#328 before #323),
    this module will exist by the time this code ships, but must degrade
    to a logged no-op rather than crash a booking if it does not.
    """
    property_address = None
    if gate_row is not None:
        answers = gate_row.get("answers") if hasattr(gate_row, "get") else gate_row["answers"]
        if isinstance(answers, str):
            import json

            answers = json.loads(answers)
        property_address = (answers or {}).get("property_address")

    payload = {
        "booking_ref": booking_ref,
        "provider_event_id": provider_event_id,
        "person_id": str(person_id) if person_id is not None else None,
        "phone": phone,
        "first_name": first_name,
        "email": email,
        "text_consent": bool(text_consent),
        "slot_start_utc": slot_start_utc,
        "property_address": property_address,
        "booked_by": captured_by,
    }

    try:
        from src.lending.booking_messages import handle_booking_confirmed

        # Savepoint, not a bare try/except: handle_booking_confirmed does not
        # commit itself, and a raised exception here must not leave the
        # session's real transaction aborted for whatever the caller does
        # with it next — the booking itself already committed.
        with session.begin_nested():
            handle_booking_confirmed(session, payload)
        session.commit()
    except ImportError:
        # Known, temporary, and every booking hits it until #328 merges —
        # an EXCEPTIONS alert per booking here would be noise, not signal.
        logger.warning(
            "calendar.book: booking_ref=%s — src.lending.booking_messages not "
            "available yet (expected before #328 merges); no reminders scheduled",
            booking_ref,
        )
    except Exception:
        logger.exception(
            "calendar.book: booking_ref=%s — booking-confirmed notification failed, "
            "booking stands, reminders will not fire for this one", booking_ref,
        )
        _alert_booking_confirmed_failed(booking_ref)


def _alert_booking_confirmed_failed(booking_ref: str) -> None:
    """Surface a silent-reminder-gap on EXCEPTIONS. Never raises — an alert
    failure must not compound onto an already-failed notification."""
    try:
        from src.services.relay import exceptions_alert_queue

        exceptions_alert_queue.enqueue_and_attempt(
            venture_key=CALENDAR_VENTURE_KEY,
            rule="booking_confirmed_notify_failed",
            message=(
                f"*Booking confirmed but reminders not scheduled* — `{booking_ref}`\n"
                f"The booking-confirmed notification to WP-GL-10 failed. The booking "
                f"itself is real and calendared; no confirmation/reminder texts will "
                f"fire for it unless this is retried by hand."
            ),
        )
    except Exception:
        logger.exception(
            "calendar.book: booking_ref=%s — EXCEPTIONS alert for the failed "
            "booking-confirmed notification also failed", booking_ref,
        )


def _cached_busy(client, calendar_id: str, start: datetime, end: datetime):
    """Free/busy for display, cached briefly. Falls through on any cache fault.

    Redis being unavailable must not take the booking page down with it, so
    every failure here degrades to a live read rather than raising.
    """
    import json

    key = (
        f"{BUSY_CACHE_PREFIX}{calendar_id}:"
        f"{start.isoformat()}:{end.isoformat()}"
    )
    try:
        from src.core.redis_client import get_redis, redis_available

        if redis_available():
            cached = get_redis().get(key)
            if cached:
                return [
                    BusyBlock(
                        start=datetime.fromisoformat(span["start"]),
                        end=datetime.fromisoformat(span["end"]),
                    )
                    for span in json.loads(cached)
                ]
    except Exception:
        logger.debug("calendar: busy-cache read failed — reading live", exc_info=True)

    busy = client.get_busy(calendar_id=calendar_id, start=start, end=end)

    try:
        from src.core.redis_client import get_redis, redis_available

        if redis_available():
            get_redis().set(
                key,
                json.dumps(
                    [{"start": b.start.isoformat(), "end": b.end.isoformat()} for b in busy]
                ),
                ex=BUSY_CACHE_TTL_SECONDS,
            )
    except Exception:
        logger.debug("calendar: busy-cache write failed", exc_info=True)

    return busy


def _idempotency_key(calendar_id: str, slot: Slot, attendee_email: str) -> str:
    """Stable across retries of the same booking, distinct across bookings.

    The agent loop recovers a crashed step by letting its lease expire and
    re-running it, so this call is replayed whenever a crash lands between
    creating the event and completing the work item.
    """
    material = f"{calendar_id}|{slot.start.isoformat()}|{attendee_email.strip().lower()}"
    return hashlib.sha256(material.encode()).hexdigest()


def _existing_booking(session, idempotency_key: str) -> Optional[BookingResult]:
    """The outcome of an earlier identical booking, or None if this is new."""
    row = session.execute(
        sa_text(
            "SELECT booking_ref, status FROM fa_max_bookings "
            "WHERE idempotency_key = :key"
        ),
        {"key": idempotency_key},
    ).mappings().first()
    if row is None or row["status"] == "cancelled":
        return None

    if row["status"] == "pending":
        # Claimed but unconfirmed: either in flight right now, or orphaned by
        # a crash before the event was created. Reporting it as booked would
        # promise a meeting that may not exist.
        return BookingResult(
            booked=False, booking_ref=row["booking_ref"], reason="in_progress"
        )

    logger.info("calendar.book: replay of booking_ref=%s", row["booking_ref"])
    return BookingResult(
        booked=True, booking_ref=row["booking_ref"], reason="already_booked"
    )


def _claim_slot(
    *,
    session,
    calendar_id: str,
    slot: Slot,
    attendee_email: str,
    topic: str,
    person_id: Optional[Any],
    tracked_link_id: Optional[int],
    idempotency_key: str,
    gate_id: Optional[str] = None,
) -> Optional[str]:
    """Durably claim the slot. Returns the booking ref, or None if lost.

    Commits so the claim survives a crash during the provider call. The
    live-overlap exclusion constraint settles concurrent bookings that both
    passed the availability re-check, whatever their starts or durations.
    """
    from sqlalchemy.exc import IntegrityError

    booking_ref = secrets.token_urlsafe(9)
    try:
        session.execute(
            sa_text(
                """
                INSERT INTO fa_max_bookings
                    (booking_ref, idempotency_key, tracked_link_id, calendar_id,
                     person_id, attendee_email, topic, starts_at, ends_at, status, gate_id)
                VALUES
                    (:booking_ref, :idempotency_key, :tracked_link_id, :calendar_id,
                     :person_id, :attendee_email, :topic, :starts_at, :ends_at, 'pending', :gate_id)
                """
            ),
            {
                "booking_ref": booking_ref,
                "idempotency_key": idempotency_key,
                "tracked_link_id": tracked_link_id,
                "calendar_id": calendar_id,
                "person_id": person_id,
                "attendee_email": attendee_email,
                "topic": topic,
                "starts_at": slot.start,
                "ends_at": slot.end,
                "gate_id": gate_id,
            },
        )
        session.commit()
    except IntegrityError:
        session.rollback()
        return None
    return booking_ref


def _confirm_claim(session, booking_ref: str, event: CalendarEvent) -> bool:
    """Confirm our own pending claim. False if the sweep already released it."""
    result = session.execute(
        sa_text(
            "UPDATE fa_max_bookings "
            "SET status = 'confirmed', provider_event_id = :event_id, updated_at = NOW() "
            "WHERE booking_ref = :booking_ref AND status = 'pending'"
        ),
        {"booking_ref": booking_ref, "event_id": event.event_id},
    )
    session.commit()
    return result.rowcount == 1


def _release_claim(session, booking_ref: str) -> None:
    session.rollback()
    session.execute(
        sa_text(
            "UPDATE fa_max_bookings "
            "SET status = 'cancelled', updated_at = NOW() "
            "WHERE booking_ref = :booking_ref"
        ),
        {"booking_ref": booking_ref},
    )
    session.commit()


def has_live_booking(session, *, tracked_link_id: int) -> bool:
    """Whether a booking link has already produced a booking that still stands.

    Cancelled bookings are excluded so a borrower whose meeting fell through
    can use the same link again rather than having to email in.
    """
    return session.execute(
        sa_text(
            "SELECT 1 FROM fa_max_bookings "
            "WHERE tracked_link_id = :link_id "
            "AND status IN ('pending', 'confirmed', 'reschedule_requested') "
            "LIMIT 1"
        ),
        {"link_id": tracked_link_id},
    ).first() is not None


def reschedule(
    *,
    client: CalendarClient,
    session,
    calendar_id: str,
    booking_ref: str,
    now: datetime,
    window_days: int = 7,
    duration_minutes: int = DEFAULT_SLOT_DURATION_MINUTES,
) -> RescheduleRequest:
    """Page a human with the booking and some alternatives. Moves nothing."""
    booking = session.execute(
        sa_text(
            """
            SELECT provider_event_id, attendee_email, topic, starts_at
            FROM fa_max_bookings
            WHERE booking_ref = :booking_ref
            """
        ),
        {"booking_ref": booking_ref},
    ).mappings().first()
    if booking is None:
        raise ValueError(f"unknown booking_ref {booking_ref!r}")

    current = None
    if booking["provider_event_id"]:
        current = client.get_event(
            calendar_id=calendar_id, event_id=booking["provider_event_id"]
        )

    alternatives = get_slots(
        client=client,
        calendar_id=calendar_id,
        window_start=now,
        window_end=now + timedelta(days=window_days),
        now=now,
        duration_minutes=duration_minutes,
    )[:RESCHEDULE_ALTERNATIVE_LIMIT]

    session.execute(
        sa_text(
            """
            UPDATE fa_max_bookings
            SET status = 'reschedule_requested', updated_at = NOW()
            WHERE booking_ref = :booking_ref
            """
        ),
        {"booking_ref": booking_ref},
    )

    alerted = _alert_exceptions(
        booking_ref=booking_ref,
        attendee_email=booking["attendee_email"],
        topic=booking["topic"],
        starts_at=booking["starts_at"],
        alternatives=alternatives,
    )
    return RescheduleRequest(
        booking_ref=booking_ref,
        current=current,
        alternatives=alternatives,
        alerted=alerted,
    )


def _alert_exceptions(
    *,
    booking_ref: str,
    attendee_email: str,
    topic: str,
    starts_at: datetime,
    alternatives: list[Slot],
) -> bool:
    """Surface the request on the EXCEPTIONS lane. Never raises.

    A failed page must not roll back the status change: the booking really is
    in question either way, and the alert queue retries on its own.
    """
    from src.services.relay import exceptions_alert_queue

    offered = (
        ", ".join(_human_time(slot.start) for slot in alternatives)
        if alternatives
        else "none available in the next week"
    )
    message = (
        f"*Reschedule requested* — {topic}\n"
        f"Booking: `{booking_ref}`\n"
        f"Attendee: {attendee_email}\n"
        f"Currently: {_human_time(starts_at)}\n"
        f"Alternatives: {offered}"
    )
    try:
        return exceptions_alert_queue.enqueue_and_attempt(
            venture_key=CALENDAR_VENTURE_KEY,
            rule=RESCHEDULE_ALERT_RULE,
            message=message,
        )
    except Exception:
        logger.exception(
            "calendar.reschedule: EXCEPTIONS alert failed for booking_ref=%s", booking_ref
        )
        return False


def _human_time(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(_TZ).strftime("%a %d %b %H:%M %Z")


def _is_taken(*, client: CalendarClient, calendar_id: str, slot: Slot) -> bool:
    """Re-read the provider to catch a slot booked since it was offered.

    Availability is read once when an offer is built and the borrower may act
    on it minutes or days later. No calendar provider reserves a slot on read,
    so this narrows the race to the moment between this check and the write.
    """
    conflicts = client.get_busy(
        calendar_id=calendar_id, start=slot.start, end=slot.end
    )
    return any(
        block.start < slot.end and block.end > slot.start for block in conflicts
    )
