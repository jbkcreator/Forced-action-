"""WP-GL-10: the human confirmation call (the day before the booked call).

A caller-booked call goes to the caller who booked it (``booked_by`` = the caller's login, from the
booking gate) and is due the day before the call (the Friday before, for a Sat/Sun day-before: assumed,
the client has not said). An AI-booked call (``booked_by`` = "ai" or missing) goes to the first caller
on shift the next business day after the booking, or to Josh when no caller is on shift (G5); it is
never due after the call itself.

OPEN DEPENDENCY: no on-shift roster exists in the repo, so ``first_caller_on_shift`` returns None and
every AI booking is assigned to Josh until a roster source is chosen. The task is a row in
``lending.confirmation_tasks``; mirroring it into a GHL task is not built (it needs a GHL contact id
that nothing stores yet), so callers see the task through the table / a report, not in GHL.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from config.lending_reminders import FALLBACK_ASSIGNEE, TIMEZONE

logger = logging.getLogger(__name__)

AI_BOOKER = "ai"

_INSERT = text("""
    INSERT INTO lending.confirmation_tasks (booking_ref, person_id, assignee, due_date)
    VALUES (:booking_ref, :person_id, :assignee, :due_date)
    ON CONFLICT (booking_ref) DO UPDATE SET
        assignee = EXCLUDED.assignee, due_date = EXCLUDED.due_date, cancelled_at = NULL, completed_at = NULL
     WHERE lending.confirmation_tasks.cancelled_at IS NOT NULL
""")
_COMPLETE = text("""
    UPDATE lending.confirmation_tasks SET completed_at = now()
     WHERE booking_ref = :ref AND completed_at IS NULL AND cancelled_at IS NULL
""")


def first_caller_on_shift(on_date: date) -> Optional[str]:
    """The first caller on shift on ``on_date``, or None. No roster source exists yet (open question)."""
    return None


def resolve_assignee(booked_by: Optional[str], due_date: date) -> str:
    if booked_by and booked_by != AI_BOOKER:
        return booked_by
    return first_caller_on_shift(due_date) or FALLBACK_ASSIGNEE


def _is_business_day(day: date) -> bool:
    return day.weekday() < 5


def next_business_day(after: date) -> date:
    day = after + timedelta(days=1)
    while not _is_business_day(day):
        day += timedelta(days=1)
    return day


def previous_business_day(before: date) -> date:
    day = before - timedelta(days=1)
    while not _is_business_day(day):
        day -= timedelta(days=1)
    return day


def due_date_for(booked_by: Optional[str], slot_start_utc: datetime, booked_at: datetime) -> date:
    booked_on = booked_at.astimezone(TIMEZONE).date()
    call_day = slot_start_utc.astimezone(TIMEZONE).date()
    if booked_by in (None, AI_BOOKER):
        return min(next_business_day(booked_on), call_day)
    day_before = call_day - timedelta(days=1)
    if not _is_business_day(day_before):
        day_before = previous_business_day(day_before)
    return max(day_before, booked_on)


def complete_confirmation_task(db, booking_ref: str) -> bool:
    """Mark the confirmation call done so it leaves the morning list. False when there was no open task.
    Does not commit."""
    done = bool(db.execute(_COMPLETE, {"ref": booking_ref}).rowcount)
    logger.info("[confirmation-tasks] booking_ref=%s completed=%s", booking_ref, done)
    return done


def assign_confirmation_task(db, *, booking_ref: str, person_id: Optional[str], booked_by: Optional[str],
                             slot_start_utc: datetime, booked_at: Optional[datetime] = None) -> str:
    """Record the confirmation call and return its assignee. Idempotent on booking_ref (a cancelled task is revived by a rescheduled booking); does not commit."""
    due_date = due_date_for(booked_by, slot_start_utc, booked_at or datetime.now(timezone.utc))
    assignee = resolve_assignee(booked_by, due_date)
    db.execute(_INSERT, {"booking_ref": booking_ref, "person_id": person_id, "assignee": assignee,
                         "due_date": due_date})
    logger.info("[confirmation-tasks] booking_ref=%s due=%s ai_booked=%s", booking_ref, due_date,
                booked_by in (None, AI_BOOKER))
    return assignee
