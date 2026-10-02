"""WP-GL-10: the human confirmation call (the day before the booked call).

A caller-booked call goes to the caller who booked it (``booked_by`` = the caller's login, from the
booking gate). An AI-booked call (``booked_by`` = "ai" or missing) goes to the first caller on shift
the next business day, or to Josh when no caller is on shift (G5).

OPEN DEPENDENCY: no on-shift roster exists in the repo, so ``first_caller_on_shift`` returns None and
every AI booking is assigned to Josh until a roster source is chosen. The task is a row in
``lending.confirmation_tasks``; mirroring it into a GHL task is not built (it needs a GHL contact id
that nothing stores yet), so callers see the task through the table / a report, not in GHL.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import text

from config.lending_reminders import FALLBACK_ASSIGNEE, TIMEZONE

logger = logging.getLogger(__name__)

AI_BOOKER = "ai"

_INSERT = text("""
    INSERT INTO lending.confirmation_tasks (booking_ref, person_id, assignee, due_date)
    VALUES (:booking_ref, :person_id, :assignee, :due_date)
    ON CONFLICT (booking_ref) DO NOTHING
""")


def first_caller_on_shift(on_date: date) -> Optional[str]:
    """The first caller on shift on ``on_date``, or None. No roster source exists yet (open question)."""
    return None


def resolve_assignee(booked_by: Optional[str], due_date: date) -> str:
    if booked_by and booked_by != AI_BOOKER:
        return booked_by
    return first_caller_on_shift(due_date) or FALLBACK_ASSIGNEE


def assign_confirmation_task(db, *, booking_ref: str, person_id: Optional[str], booked_by: Optional[str],
                             slot_start_utc: datetime) -> str:
    """Record the confirmation call and return its assignee. Idempotent on booking_ref; does not commit."""
    due_date = slot_start_utc.astimezone(TIMEZONE).date() - timedelta(days=1)
    assignee = resolve_assignee(booked_by, due_date)
    db.execute(_INSERT, {"booking_ref": booking_ref, "person_id": person_id, "assignee": assignee,
                         "due_date": due_date})
    logger.info("[confirmation-tasks] booking_ref=%s due=%s ai_booked=%s", booking_ref, due_date,
                booked_by in (None, AI_BOOKER))
    return assignee
