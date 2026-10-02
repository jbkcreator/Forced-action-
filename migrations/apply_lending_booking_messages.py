"""WP-GL-10: lending.booking_messages — durable confirmation and reminder schedule.

One row per (booking_ref, kind). Unique on (booking_ref, kind) so a retried
event from WP-GL-5's booking trigger never inserts a duplicate. Status moves
from 'pending' → 'sent' | 'skipped' | 'cancelled'. Cancellation reason is
stored so the reminder worker can distinguish a skip (too late / already
past) from a cancellation (booking rescheduled or cancelled by the contact).

Indexes:
  - (status, send_at) — the reminder worker's due-row poll (FOR UPDATE SKIP LOCKED)
  - booking_ref — cancel/reschedule by booking

This table is the ONLY path for scheduling and cancelling GL-10 messages.
The reminder worker reads from it. Nothing writes to it except
src/lending/booking_messages.py::schedule_booking_messages() and
::cancel_booking_messages().

Safe to re-run (idempotent DDL).
"""
import logging
import sys

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


DDL = """
CREATE TABLE IF NOT EXISTS lending.booking_messages (
    id                 BIGSERIAL PRIMARY KEY,

    -- Foreign key to the booking. References the booking_ref written by
    -- WP-GL-5's book() call. booking_ref is NOT a FK column here because
    -- GL-5's bookings table lives in the public (FA Max) schema and this
    -- table lives in the lending schema; cross-schema FKs need the table
    -- to exist first. The constraint is enforced at the application layer.
    booking_ref        TEXT        NOT NULL,

    -- 'confirmation' | 'night_before' | 'ninety_min' (see config/lending_reminders.py)
    kind               TEXT        NOT NULL CHECK (kind IN ('confirmation','night_before','ninety_min')),

    -- Channel: 'text' for GHL SMS, 'email' for hello@ fallback (B4 / 10DLC gap)
    channel            TEXT        NOT NULL CHECK (channel IN ('text','email')),

    -- When to send, always UTC. Computed at schedule time from the slot start.
    send_at            TIMESTAMPTZ NOT NULL,

    -- Lifecycle: pending → sent | skipped | cancelled
    status             TEXT        NOT NULL DEFAULT 'pending'
                                   CHECK (status IN ('pending','sent','skipped','cancelled')),
    skip_reason        TEXT,       -- e.g. 'too_late', 'already_past', 'suppressed', 'no_consent'
    cancel_reason      TEXT,       -- e.g. 'booking_cancelled', 'booking_rescheduled'

    -- Contact info at schedule time (denormalised so the worker does not need
    -- to join back to GL-5's booking row while processing).
    first_name         TEXT        NOT NULL DEFAULT '',
    contact_phone      TEXT,       -- normalized E.164; NULL if email-only
    contact_email      TEXT,       -- NULL if text-only and consent given
    property_address   TEXT,       -- NULL → templates drop the address phrase (B3)
    slot_start_utc     TIMESTAMPTZ NOT NULL,

    -- Attribution
    booked_by          TEXT,       -- caller seat id, or 'ai' for GHL Conversation AI
    text_consent        BOOLEAN     NOT NULL DEFAULT FALSE,
                                   -- caller logged "Is it okay if we text you?" yes (G6)

    -- Audit
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at            TIMESTAMPTZ,
    worker_id          TEXT        -- which reminder_worker instance processed this row

    -- Idempotency: one row per (booking_ref, kind). A retried booking event
    -- calling schedule_booking_messages() is a no-op.
);

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_booking_messages_ref_kind'
    ) THEN
        ALTER TABLE lending.booking_messages
            ADD CONSTRAINT uq_booking_messages_ref_kind
            UNIQUE (booking_ref, kind);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_booking_messages_due
    ON lending.booking_messages (status, send_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_booking_messages_booking_ref
    ON lending.booking_messages (booking_ref);
"""


def run(connection_string: str | None = None) -> None:
    import os
    url = connection_string or os.environ["DATABASE_URL"]
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text(DDL))
    logger.info("apply_lending_booking_messages: done")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
    sys.exit(0)
