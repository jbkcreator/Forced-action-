"""WP-GL-10: GoHighLevel appointment-status webhook -> cancel the booking's pending messages.

A GHL workflow ("Appointment status changed" -> Webhook) POSTs here. A cancelled or rescheduled
appointment cancels its pending confirmation / reminders so nobody is texted about a call that no
longer exists. Auth is the same shared secret as the other lending GHL webhooks
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
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.api.lending_ghl_router import _verify_secret
from src.lending.booking_messages import cancel_by_provider_event, handle_booking_confirmed
from src.lending.confirmation_tasks import complete_confirmation_task

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])

CANCELLING_STATUSES = {"cancelled": "booking_cancelled", "showed_cancelled": "booking_cancelled",
                       "rescheduled": "booking_rescheduled"}


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
    reason = CANCELLING_STATUSES.get(status)
    if reason is None:
        return {"status": "noop", "reason": "status_not_handled"}
    cancelled = cancel_by_provider_event(db, appointment_id, reason)
    db.commit()
    logger.info("[booking-webhook] appointment status=%s cancelled=%d", status, cancelled)
    return {"status": status, "cancelled": cancelled}


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
