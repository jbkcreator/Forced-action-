"""WP-GL-10: FastAPI router for GHL appointment-status webhooks.

Mounted at /webhooks/lending/booking (see src/api/main.py).

GHL sends a POST whenever an appointment's status changes (cancelled,
confirmed, rescheduled, showed, noshow, etc.). We only act on:
  - cancelled   → cancel pending reminder rows
  - rescheduled → cancel pending reminder rows (caller reschedules fresh via
                  the same booking flow; new rows will be created by the next
                  booking event from GL-5)

Secret-header authentication mirrors the DND webhook pattern.
Endpoint is closed (405) when LENDING_BOOKING_WEBHOOK_SECRET is unset.

GHL field names come from the GHL v2 appointment webhook docs, but have NOT
been confirmed against a live payload. Verify before enabling in production.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session

from src.api.deps import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending-webhooks"])


def _secret() -> Optional[str]:
    from config.settings import get_settings
    s = get_settings()
    secret = getattr(s, "lending_booking_webhook_secret", None)
    return secret.get_secret_value() if secret else None


def _verify_secret(x_webhook_secret: Optional[str] = Header(default=None)) -> None:
    secret = _secret()
    if not secret:
        raise HTTPException(status_code=405, detail="Booking webhook not configured")
    if not x_webhook_secret or not hmac.compare_digest(
        hashlib.sha256(x_webhook_secret.encode()).digest(),
        hashlib.sha256(secret.encode()).digest(),
    ):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


@router.post("/booking", dependencies=[Depends(_verify_secret)])
async def ghl_booking_webhook(
    request: Request,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Handle GHL appointment status change events.

    Cancels pending booking_messages rows for cancelled or rescheduled
    appointments so contacts do not receive reminders for a booking that
    no longer exists.
    """
    from src.lending.booking_messages import cancel_booking_messages

    body = await request.json()
    event_type = body.get("type") or body.get("eventType") or ""
    appointment_id = (
        body.get("appointmentId")
        or body.get("id")
        or (body.get("appointment") or {}).get("id")
        or ""
    )
    status = (
        body.get("status")
        or body.get("appointmentStatus")
        or (body.get("appointment") or {}).get("appointmentStatus")
        or ""
    ).lower()

    logger.info(
        "[booking-webhook] event_type=%s appointment_id=%s status=%s",
        event_type, appointment_id, status,
    )

    if not appointment_id:
        # GHL occasionally sends test pings with no appointment data.
        return {"status": "noop", "reason": "no_appointment_id"}

    # booking_ref is the GHL appointment id — the same id written by
    # GHL_CalendarClient.create_event and stored in fa_max_calendar_events
    # by WP-GL-5's book() call.
    # DEPENDENCY: this assumes GL-5 stores the GHL appointment id as the
    # booking_ref. Confirm once GL-5 is merged. Until then, cancel is a no-op
    # (no matching rows yet).
    booking_ref = appointment_id

    if status in ("cancelled", "showed_cancelled"):
        n = cancel_booking_messages(db, booking_ref, "booking_cancelled")
        db.commit()
        return {"status": "cancelled", "rows_cancelled": n}

    if status == "rescheduled":
        n = cancel_booking_messages(db, booking_ref, "booking_rescheduled")
        db.commit()
        return {"status": "rescheduled", "rows_cancelled": n}

    return {"status": "noop", "reason": f"unhandled_status={status}"}
