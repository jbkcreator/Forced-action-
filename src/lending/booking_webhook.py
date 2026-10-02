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
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.api.lending_ghl_router import _verify_secret
from src.lending.booking_messages import cancel_by_provider_event

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
