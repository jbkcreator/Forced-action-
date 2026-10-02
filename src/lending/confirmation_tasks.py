"""WP-GL-10: confirmation-call task assignment.

Per the playbook and G5: the caller who booked makes the confirmation call
the day before. AI bookings (booked_by='ai') go to the first caller on shift
the next business day; if no caller is on shift, the task goes to Josh.

This module writes a row to lending.confirmation_tasks (durable record) and,
when the Live adapter is enabled, creates a GHL task on the contact's record.

OPEN DEPENDENCY: there is no on-shift roster in the repo. The on-shift lookup
returns None until one is provided (GL-5 scope or a future task). When None,
the task is assigned to JOSH_FALLBACK_EMAIL as per G5.

GHL task creation is unverified against the live API.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

# G5: no caller on shift → task assigned to Josh.
JOSH_FALLBACK_EMAIL = "jbkantor@gmail.com"

_INSERT = text("""
    INSERT INTO lending.confirmation_tasks
        (booking_ref, assignee, due_date, ghl_contact_id, created_at)
    VALUES
        (:booking_ref, :assignee, :due_date, :ghl_contact_id, NOW())
    ON CONFLICT (booking_ref) DO NOTHING
""")


def assign_confirmation_task(
    db,
    *,
    booking_ref: str,
    booked_by: Optional[str],
    slot_start_utc: datetime,
    ghl_contact_id: Optional[str] = None,
) -> str:
    """Write a confirmation-call task row and return the assignee email.

    Assignee logic (G5):
      - booked_by is a caller id → assigned to that caller
      - booked_by is 'ai' or None → first on-shift caller next business day,
        or JOSH_FALLBACK_EMAIL if the roster is not available

    Due date is the calendar day before the slot (the caller calls the day
    before to confirm).
    """
    due_date = _day_before(slot_start_utc)
    assignee = _resolve_assignee(booked_by)

    db.execute(_INSERT, {
        "booking_ref": booking_ref,
        "assignee": assignee,
        "due_date": due_date,
        "ghl_contact_id": ghl_contact_id,
    })

    logger.info(
        "[confirmation-tasks] booking_ref=%s assignee=%s due_date=%s",
        booking_ref, assignee, due_date,
    )

    _create_ghl_task(booking_ref=booking_ref, assignee=assignee,
                     due_date=due_date, ghl_contact_id=ghl_contact_id)
    return assignee


def _resolve_assignee(booked_by: Optional[str]) -> str:
    """Return the caller email, or Josh as fallback.

    On-shift roster lookup is not yet implemented. Returns JOSH_FALLBACK_EMAIL
    for AI bookings until a roster is provided.
    """
    if booked_by and booked_by != "ai":
        return booked_by  # booked_by is the caller's email in the GL-5 contract
    first_on_shift = _first_on_shift_caller()
    if first_on_shift:
        return first_on_shift
    logger.info("[confirmation-tasks] no on-shift caller — assigning to Josh")
    return JOSH_FALLBACK_EMAIL


def _first_on_shift_caller() -> Optional[str]:
    """Return the first caller on shift next business day, or None.

    OPEN DEPENDENCY (G5, unanswered): the roster source has not been
    identified. This returns None until one is wired in. Options include
    BatchDialer agent sessions or a lending.caller_roster table.
    """
    return None


def _day_before(slot_start_utc: datetime) -> date:
    slot_et = slot_start_utc.astimezone(
        __import__("zoneinfo").ZoneInfo("America/New_York")
    )
    return (slot_et - timedelta(days=1)).date()


def _create_ghl_task(
    *,
    booking_ref: str,
    assignee: str,
    due_date: date,
    ghl_contact_id: Optional[str],
) -> None:
    """Create a task in GHL on the contact's record.

    No-op in fake mode or when GHL is not configured.
    UNVERIFIED field names — confirm against GHL v2 tasks API.
    """
    from config.settings import get_settings
    s = get_settings()
    mode = getattr(s, "lending_ghl_messenger_mode", "fake")
    if mode != "live":
        logger.info(
            "[confirmation-tasks.fake] would-create GHL task booking_ref=%s assignee=%s",
            booking_ref, assignee,
        )
        return
    if not ghl_contact_id:
        logger.info(
            "[confirmation-tasks] no ghl_contact_id — skipping GHL task booking_ref=%s",
            booking_ref,
        )
        return

    try:
        import requests
        api_key = s.ghl_api_key.get_secret_value() if s.ghl_api_key else None
        if not api_key or not s.ghl_location_id:
            return
        payload = {
            "title": f"Confirmation call — booking {booking_ref}",
            "dueDate": due_date.isoformat(),
            "assignedTo": assignee,
            "contactId": ghl_contact_id,
        }
        resp = requests.post(
            f"https://services.leadconnectorhq.com/contacts/{ghl_contact_id}/tasks",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Version": "2021-07-28",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        if not resp.ok:
            logger.warning(
                "[confirmation-tasks] GHL task creation failed HTTP %s: %s",
                resp.status_code, resp.text[:200],
            )
    except Exception:
        logger.exception("[confirmation-tasks] GHL task creation error booking_ref=%s", booking_ref)
