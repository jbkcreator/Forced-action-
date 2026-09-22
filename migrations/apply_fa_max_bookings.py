"""
FA Max calendar bookings (WP-T2-7).

Creates fa_max_bookings: one row per meeting the calendar tool scheduled on
the client's calendar. Keyed by booking_ref rather than the provider's event
id — see the FaMaxBooking model docstring in src/core/models.py for why
identity cannot live on the provider side.

Idempotent: safe to re-run (CREATE TABLE IF NOT EXISTS, CREATE INDEX IF NOT
EXISTS).

Run:
  PYTHONPATH=. python migrations/apply_fa_max_bookings.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS fa_max_bookings (
        id                 BIGSERIAL   PRIMARY KEY,
        booking_ref        TEXT        NOT NULL UNIQUE,
        calendar_id        TEXT        NOT NULL,
        provider_event_id  TEXT,
        person_id          UUID        REFERENCES fa_max_persons(person_id) ON DELETE SET NULL,
        attendee_email     TEXT        NOT NULL,
        topic              TEXT        NOT NULL,
        starts_at          TIMESTAMPTZ NOT NULL,
        ends_at            TIMESTAMPTZ NOT NULL,
        status             TEXT        NOT NULL DEFAULT 'confirmed'
                                       CHECK (status IN ('confirmed', 'cancelled', 'reschedule_requested')),
        created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at         TIMESTAMPTZ,
        CONSTRAINT ck_fa_max_bookings_span CHECK (ends_at > starts_at)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_bookings_person
        ON fa_max_bookings (person_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_bookings_attendee
        ON fa_max_bookings (attendee_email);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_bookings_provider_event
        ON fa_max_bookings (provider_event_id);
    """,
    # The cancellation watcher sweeps forthcoming confirmed bookings on every
    # tick; without this it degrades to a full scan as history accumulates.
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_bookings_upcoming
        ON fa_max_bookings (starts_at)
        WHERE status = 'confirmed';
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_fa_max_bookings: done")


if __name__ == "__main__":
    main()
