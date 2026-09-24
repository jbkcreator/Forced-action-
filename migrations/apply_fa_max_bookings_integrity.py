"""
FA Max booking integrity (WP-T2-7 hardening).

Three guarantees the first cut of fa_max_bookings could not make:

  idempotency_key  — a retried booking returns the original rather than
                     creating a second meeting. The agent loop recovers a
                     crashed step by letting its lease expire and re-running
                     it, so a crash between creating the calendar event and
                     completing the work item WILL replay this call.

  live-slot unique — two bookings cannot hold the same slot on the same
                     calendar. Partial, so a cancelled booking frees its slot
                     for re-booking rather than blocking it forever.

  live-overlap     — two live bookings on one calendar cannot overlap, even
    exclusion        with different starts or durations (needs btree_gist).

  tracked_link_id  — lets the booking page refuse a second live booking from
                     one link, which is otherwise unbounded.

'pending' joins the status vocabulary. A row is written before the calendar
event exists so that a crash in between leaves a stray row rather than a
stray meeting: a row pointing at no event is reconcilable, whereas an event
with no row is invisible to reschedule and cancellation and still sits in
the borrower's inbox.

Idempotent: safe to re-run. Run after apply_fa_max_bookings.py.

Run:
  PYTHONPATH=. python migrations/apply_fa_max_bookings_integrity.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    ALTER TABLE fa_max_bookings
        ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
    """,
    """
    ALTER TABLE fa_max_bookings
        ADD COLUMN IF NOT EXISTS tracked_link_id BIGINT
            REFERENCES tracked_links(id) ON DELETE SET NULL;
    """,
    # Widen the status vocabulary to admit 'pending'. Dropping and re-adding
    # is safe here: the constraint is only validated against existing rows on
    # ADD, and every current status remains permitted.
    """
    ALTER TABLE fa_max_bookings
        DROP CONSTRAINT IF EXISTS fa_max_bookings_status_check;
    """,
    """
    ALTER TABLE fa_max_bookings
        DROP CONSTRAINT IF EXISTS ck_fa_max_bookings_status;
    """,
    """
    ALTER TABLE fa_max_bookings
        ADD CONSTRAINT ck_fa_max_bookings_status
        CHECK (status IN ('pending', 'confirmed', 'cancelled', 'reschedule_requested'));
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_fa_max_bookings_idempotency
        ON fa_max_bookings (idempotency_key)
        WHERE idempotency_key IS NOT NULL;
    """,
    # Partial so a cancelled booking releases its slot. 'reschedule_requested'
    # still holds the slot: the meeting is in question, not gone, and the
    # client has not yet agreed to give the time back.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_fa_max_bookings_live_slot
        ON fa_max_bookings (calendar_id, starts_at)
        WHERE status IN ('pending', 'confirmed', 'reschedule_requested');
    """,
    # The unique index above only catches identical starts. Slots come in 30
    # and 60 minutes on a 30-minute grid, so 14:00-15:00 and 14:30-15:00 have
    # different starts yet overlap; only a range exclusion closes that race.
    """
    CREATE EXTENSION IF NOT EXISTS btree_gist;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'ex_fa_max_bookings_live_overlap'
        ) THEN
            ALTER TABLE fa_max_bookings
                ADD CONSTRAINT ex_fa_max_bookings_live_overlap
                EXCLUDE USING gist (
                    calendar_id WITH =,
                    tstzrange(starts_at, ends_at) WITH &&
                ) WHERE (status IN ('pending', 'confirmed', 'reschedule_requested'));
        END IF;
    END $$;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_bookings_tracked_link
        ON fa_max_bookings (tracked_link_id)
        WHERE status IN ('pending', 'confirmed', 'reschedule_requested');
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_fa_max_bookings_integrity: done")


if __name__ == "__main__":
    main()
