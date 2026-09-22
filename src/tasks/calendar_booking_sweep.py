"""Reconcile booked meetings against the calendar they live on.

Two drifts accumulate without this.

A booking cancelled in Google Calendar leaves our record saying confirmed.
Nothing tells us: Google reports a cancellation only if asked, so a client
who deletes a meeting from his own calendar silently desynchronises every
downstream reader — the opportunity still shows a call booked, the slot
stays held, and the borrower may never learn it was called off.

A claim orphaned by a crash leaves a 'pending' row holding a slot with no
meeting behind it. book() resolves its claim on both the success and failure
paths, so this only catches a process dying inside the one API call between
them, but a slot held by nothing is held forever without a sweep.

Detection is a poll rather than Google's push notifications: a watch channel
needs a public HTTPS endpoint and expires every few days, which is a second
thing to operate for a handful of meetings a week. Every other periodic job
in this codebase is a cron line, and this is the same shape.

Dry-run unless --apply, matching this codebase's sweep-task convention
(src/tasks/selfserve_abandonment_sweep.py).

Usage:
    PYTHONPATH=. python -m src.tasks.calendar_booking_sweep
    PYTHONPATH=. python -m src.tasks.calendar_booking_sweep --apply
"""
from __future__ import annotations

import argparse
import logging
from typing import Any

from sqlalchemy import text as sa_text

from config.calendar import CALENDAR_VENTURE_KEY
from src.core.database import get_db_context

logger = logging.getLogger(__name__)

# A claim older than this cannot still be in flight: book() holds one only
# across a single provider call.
STALE_CLAIM_MINUTES = 15

# Past meetings are left alone. A cancellation after the fact changes
# nothing, and sweeping all history would grow without bound.
CANCELLATION_ALERT_RULE = "calendar_booking_cancelled"


def run(dry_run: bool = True) -> dict[str, Any]:
    """Reconcile bookings against the calendar. Returns what changed."""
    from src.services.calendar.client import get_calendar_client, get_calendar_id

    client = _live_client(get_calendar_client)
    calendar_id = get_calendar_id()

    with get_db_context() as db:
        cancelled = _find_cancelled(db, client=client, calendar_id=calendar_id)
        stale = _find_stale_claims(db)

        if not dry_run:
            for booking in cancelled:
                _mark_cancelled(db, booking["booking_ref"])
            for booking_ref in stale:
                _mark_cancelled(db, booking_ref)
            db.commit()

    if not dry_run:
        for booking in cancelled:
            _alert_cancellation(booking)

    logger.info(
        "calendar_booking_sweep: %d cancelled, %d stale claim(s) released%s",
        len(cancelled), len(stale), " (dry run)" if dry_run else "",
    )
    return {
        "cancelled": [b["booking_ref"] for b in cancelled],
        "stale_claims": stale,
        "dry_run": dry_run,
    }


def _live_client(factory):
    """Refuse to sweep against a fake calendar.

    The fake holds no events, so every real booking would read as cancelled
    and the sweep would wipe the whole table. A misconfigured environment
    must fail here rather than quietly destroy state.
    """
    from src.services.calendar.fakes import FakeCalendar

    client = factory()
    if isinstance(client, FakeCalendar):
        raise RuntimeError(
            "calendar_booking_sweep refuses to run against FakeCalendar — "
            "every booking would read as cancelled. Set FA_MAX_CALENDAR_MODE=live."
        )
    return client


def _find_cancelled(db, *, client, calendar_id: str) -> list[dict[str, Any]]:
    """Bookings we still call confirmed that are gone from the calendar."""
    rows = db.execute(
        sa_text(
            """
            SELECT booking_ref, provider_event_id, attendee_email, topic, starts_at
            FROM fa_max_bookings
            WHERE status = 'confirmed'
              AND starts_at > NOW()
              AND provider_event_id IS NOT NULL
            ORDER BY starts_at
            """
        )
    ).mappings().all()

    gone = []
    for row in rows:
        try:
            event = client.get_event(
                calendar_id=calendar_id, event_id=row["provider_event_id"]
            )
        except Exception:
            # One unreadable event must not stop the sweep, and must not be
            # mistaken for a cancellation either.
            logger.warning(
                "calendar_booking_sweep: could not read event for booking_ref=%s",
                row["booking_ref"], exc_info=True,
            )
            continue

        if event is None or event.status == "cancelled":
            gone.append(dict(row))

    return gone


def _find_stale_claims(db) -> list[str]:
    rows = db.execute(
        sa_text(
            """
            SELECT booking_ref
            FROM fa_max_bookings
            WHERE status = 'pending'
              AND created_at < NOW() - make_interval(mins => :minutes)
            """
        ),
        {"minutes": STALE_CLAIM_MINUTES},
    ).mappings().all()
    return [row["booking_ref"] for row in rows]


def _mark_cancelled(db, booking_ref: str) -> None:
    db.execute(
        sa_text(
            "UPDATE fa_max_bookings "
            "SET status = 'cancelled', updated_at = NOW() "
            "WHERE booking_ref = :booking_ref"
        ),
        {"booking_ref": booking_ref},
    )


def _alert_cancellation(booking: dict[str, Any]) -> None:
    """Surface the cancellation to the client. Never raises.

    Deliberately does not email the invitee. Google already offers to send a
    cancellation when a meeting is deleted from the calendar, and we cannot
    tell whether that offer was accepted — so a second message from us is as
    likely to be a confusing duplicate as a useful notice. Whether to send
    one anyway is a client decision, not a default.
    """
    from src.services.relay import exceptions_alert_queue

    starts_at = booking["starts_at"]
    message = (
        f"*Booking cancelled* — {booking['topic']}\n"
        f"Booking: `{booking['booking_ref']}`\n"
        f"Attendee: {booking['attendee_email']}\n"
        f"Was: {starts_at:%a %d %b %H:%M %Z}\n"
        f"Removed from the calendar; our record is now cancelled."
    )
    try:
        exceptions_alert_queue.enqueue_and_attempt(
            venture_key=CALENDAR_VENTURE_KEY,
            rule=CANCELLATION_ALERT_RULE,
            message=message,
        )
    except Exception:
        logger.exception(
            "calendar_booking_sweep: alert failed for booking_ref=%s",
            booking["booking_ref"],
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Reconcile FA Max bookings against the calendar."
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually update bookings (default: dry run)",
    )
    args = parser.parse_args()
    print(run(dry_run=not args.apply))


if __name__ == "__main__":
    main()
