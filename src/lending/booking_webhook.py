"""WP-GL-10: GoHighLevel appointment-status webhook -> schedule or cancel the booking's messages.

A GHL workflow ("Appointment status changed" -> Webhook) POSTs here. A cancelled or rescheduled
appointment cancels its pending confirmation / reminders so nobody is texted about a call that no
longer exists. A new / confirmed appointment schedules them (the fallback when the booking flow does
not post ``/booking-confirmed``; see ``_schedule_from_appointment``). Auth is the same shared secret as the other lending GHL webhooks
(``X-Webhook-Secret`` = LENDING_GHL_WEBHOOK_SECRET; closed while unset).

The appointment id is matched against ``booking_messages.provider_event_id`` (the id GHL returned
when WP-GL-5 created the appointment), NOT ``booking_ref``.

UNVERIFIED: the payload field names below follow GHL's public reference and have not been seen in a
real webhook; confirm with one test appointment once GHL access exists.

A rescheduled appointment keeps its GHL id but gets a new time: this endpoint only cancels the old
messages. New ones exist only if the booking flow emits a new booking event for the new time (open
point for the GL-5 owner).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.api.lending_ghl_router import _verify_secret
from src.lending.booking_messages import cancel_by_provider_event, handle_booking_confirmed, handle_booking_gate_failed, handle_nurture_entry
from src.lending.confirmation_tasks import complete_confirmation_task

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])

CANCELLING_STATUSES = {"cancelled": "booking_cancelled", "showed_cancelled": "booking_cancelled",
                       "rescheduled": "booking_rescheduled"}


SCHEDULING_STATUSES = frozenset({"new", "confirmed", "booked"})
RESCHEDULE_GRACE_SECONDS = 120   # a "rescheduled" cancel spares rows the booking flow re-planned this recently

_KNOWN_APPOINTMENT = text("""
    SELECT booking_ref, person_id, property_address, booked_by, first_name, contact_phone, contact_email
      FROM lending.booking_messages
     WHERE provider_event_id = :event_id ORDER BY id LIMIT 1
""")


def _first(body: dict[str, Any], *paths: tuple[str, ...]) -> Optional[str]:
    for path in paths:
        node: Any = body
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
        if node:
            return str(node)
    return None


@router.post("/ghl-appointment")
def ghl_appointment(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _verify_secret(x_webhook_secret)
    appointment_id = _first(body, ("appointmentId",), ("appointment", "id"), ("id",))
    status = (_first(body, ("appointmentStatus",), ("status",), ("appointment", "appointmentStatus")) or "").lower()
    if not appointment_id:
        return {"status": "noop", "reason": "no_appointment_id"}
    if status in SCHEDULING_STATUSES or (status == "rescheduled" and _start_time(body) is not None):
        return _schedule_from_appointment(db, body, appointment_id)
    reason = CANCELLING_STATUSES.get(status)
    if reason is None:
        return {"status": "noop", "reason": "status_not_handled"}
    cancelled = cancel_by_provider_event(db, appointment_id, reason,
                                         spare_replanned_seconds=RESCHEDULE_GRACE_SECONDS if status == "rescheduled" else 0)
    db.commit()
    logger.info("[booking-webhook] appointment status=%s cancelled=%d", status, cancelled)
    return {"status": status, "cancelled": cancelled}


def _start_time(body: dict[str, Any]) -> Optional[datetime]:
    """The appointment start as a timezone-aware datetime, or None when it is missing, unparseable or naive."""
    raw_start = _first(body, ("startTime",), ("appointment", "startTime"), ("calendar", "startTime"))
    try:
        slot = datetime.fromisoformat(raw_start.replace("Z", "+00:00")) if raw_start else None
    except ValueError:
        return None
    return slot if slot is not None and slot.tzinfo is not None else None


def _schedule_from_appointment(db: Session, body: dict[str, Any], appointment_id: str) -> dict[str, Any]:
    """The fallback path when the booking flow does not post ``/booking-confirmed``: a caller creates the GHL
    appointment by hand and this event schedules the confirmation and reminders from what GHL sends (the
    contact's phone, name and email and the start time). There is no property address, caller or text-consent
    flag in a GHL event, so the address phrase is dropped, the confirmation call goes to Josh, and a text goes
    out only if consent was already recorded elsewhere (``has_text_consent``). An appointment the booking flow
    already scheduled keeps its ``booking_ref``, address and booker, so the two paths never double-schedule."""
    slot = _start_time(body)
    if slot is None:
        logger.warning("[booking-webhook] appointment %s has no usable timezone-aware start time; nothing scheduled",
                       appointment_id)
        return {"status": "noop", "reason": "no_start_time"}
    known = db.execute(_KNOWN_APPOINTMENT, {"event_id": appointment_id}).mappings().first()
    payload = {
        "booking_ref": known["booking_ref"] if known else f"ghl-{appointment_id}",
        "provider_event_id": appointment_id,
        "person_id": known["person_id"] if known else None,
        "phone": _first(body, ("phone",), ("contact", "phone")) or (known["contact_phone"] if known else None),
        "first_name": _first(body, ("firstName",), ("contact", "firstName")) or (known["first_name"] if known else None),
        "email": _first(body, ("email",), ("contact", "email")) or (known["contact_email"] if known else None),
        "property_address": known["property_address"] if known else None,
        "booked_by": known["booked_by"] if known else None,
        "slot_start_utc": slot,
    }
    try:
        result = handle_booking_confirmed(db, payload)
        db.commit()
    except Exception as exc:  # class only: the payload carries a phone number
        logger.error("[booking-webhook] appointment %s scheduling failed (%s)", appointment_id, type(exc).__name__)
        db.rollback()
        raise HTTPException(status_code=500, detail="Could not schedule the booking messages") from None
    outcome = "skipped" if result.skip_reason else "scheduled" if result.inserted else "duplicate"
    return {"status": outcome, "inserted": result.inserted, "skip_reason": result.skip_reason}


def _parse_booking(body: dict[str, Any]) -> dict[str, Any]:
    """The booking payload with ``slot_start_utc`` parsed; 422 when a required field is missing or invalid."""
    booking_ref = str(body.get("booking_ref") or "").strip()
    raw_slot = body.get("slot_start_utc")
    if not booking_ref or not isinstance(raw_slot, str):
        raise HTTPException(status_code=422, detail="booking_ref and slot_start_utc are required")
    try:
        slot = datetime.fromisoformat(raw_slot)
    except ValueError:
        raise HTTPException(status_code=422, detail="slot_start_utc must be an ISO 8601 timestamp") from None
    if slot.tzinfo is None:
        raise HTTPException(status_code=422, detail="slot_start_utc must include a timezone")
    return {**body, "booking_ref": booking_ref, "slot_start_utc": slot}


@router.post("/booking-confirmed")
def booking_confirmed(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """The production entry point for ``handle_booking_confirmed``: whatever confirms a booking (the GL-5
    booking flow, or a GHL workflow on appointment created) POSTs the payload documented in
    ``booking_messages``. Idempotent per ``booking_ref``, so a redelivery changes nothing. This endpoint is
    the transport the GL-5 owner can call; it does not read GL-5's gate table, which holds enum codes only
    (no phone, address or consent) and so cannot supply this payload."""
    _verify_secret(x_webhook_secret)
    payload = _parse_booking(body)
    try:
        result = handle_booking_confirmed(db, payload)
        db.commit()
    except Exception as exc:  # class only: the payload carries a phone number
        logger.error("[booking-webhook] booking_ref=%s scheduling failed (%s)", payload["booking_ref"], type(exc).__name__)
        db.rollback()
        raise HTTPException(status_code=500, detail="Could not schedule the booking messages") from None
    outcome = "skipped" if result.skip_reason else "scheduled" if result.inserted else "duplicate"
    return {"status": outcome, "inserted": result.inserted, "skip_reason": result.skip_reason}


@router.post("/confirmation-task-complete")
def confirmation_task_complete(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A caller finished the confirmation call: take it off the 9am list. Same secret as the other lending
    webhooks, so a GHL workflow button or a script can call it."""
    _verify_secret(x_webhook_secret)
    booking_ref = str(body.get("booking_ref") or "").strip()
    if not booking_ref:
        raise HTTPException(status_code=422, detail="booking_ref is required")
    completed = complete_confirmation_task(db, booking_ref)
    db.commit()
    return {"status": "completed" if completed else "no_open_task"}


@router.post("/ghl-nurture")
def ghl_nurture(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A GHL workflow ("Pipeline Stage Changed" -> Webhook) reports a contact entering a stage. Entering Nurture
    before an AI-booked call means the caller's check did not pass: cancel the reminders and send the contact the
    approved message. This is the trigger for the failed check, so it needs no state from WP-GL-5. Any other stage
    is ignored. Add it as a second webhook action on the same workflow that feeds ``/ghl-stage``.
    UNVERIFIED: the field names follow the ``/ghl-stage`` body (stage_name, phone) and GHL's public reference."""
    _verify_secret(x_webhook_secret)
    stage = (_first(body, ("stage_name",), ("stageName",), ("pipelineStageName",)) or "").strip().lower()
    if stage != "nurture":
        return {"status": "noop", "reason": "stage_not_handled"}
    phone = _first(body, ("phone",), ("contact", "phone"))
    if not phone:
        return {"status": "noop", "reason": "no_phone"}
    try:
        outcome = handle_nurture_entry(db, phone)
        db.commit()
    except Exception as exc:  # class only: the payload carries a phone number
        logger.error("[booking-webhook] nurture handling failed (%s)", type(exc).__name__)
        db.rollback()
        raise HTTPException(status_code=500, detail="Could not process the stage change") from None
    return {"status": outcome}


@router.post("/booking-gate-failed")
def booking_gate_failed(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """The caller's check did not pass for an AI-booked call: cancel its reminders and send the contact the one
    "we can't hold the call" message. The booking flow (WP-GL-5) posts here when it releases the slot and moves
    the contact to nurture. Idempotent per ``booking_ref``."""
    _verify_secret(x_webhook_secret)
    booking_ref = str(body.get("booking_ref") or "").strip()
    if not booking_ref:
        raise HTTPException(status_code=422, detail="booking_ref is required")
    try:
        outcome = handle_booking_gate_failed(db, booking_ref)
        db.commit()
    except Exception as exc:
        logger.error("[booking-webhook] booking_ref=%s gate-fail handling failed (%s)", booking_ref, type(exc).__name__)
        db.rollback()
        raise HTTPException(status_code=500, detail="Could not process the gate failure") from None
    return {"status": outcome}
