"""WP-GL-10: lending.confirmation_tasks — durable confirmation-call task record.

One row per booking. ON CONFLICT (booking_ref) DO NOTHING so a retried
booking event is idempotent.

Safe to re-run.
"""
import logging
import sys

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS lending.confirmation_tasks (
    id              BIGSERIAL PRIMARY KEY,
    booking_ref     TEXT        NOT NULL,
    assignee        TEXT        NOT NULL,  -- caller email or jbkantor@gmail.com (G5)
    due_date        DATE        NOT NULL,  -- calendar day before the slot
    ghl_contact_id  TEXT,                  -- GHL contactId; NULL until GL-5 wires it
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ           -- set when the confirmation call is logged
);

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_confirmation_tasks_booking_ref'
    ) THEN
        ALTER TABLE lending.confirmation_tasks
            ADD CONSTRAINT uq_confirmation_tasks_booking_ref UNIQUE (booking_ref);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_confirmation_tasks_due_date
    ON lending.confirmation_tasks (due_date, assignee)
    WHERE completed_at IS NULL;
"""


def run(connection_string: str | None = None) -> None:
    import os
    url = connection_string or os.environ["DATABASE_URL"]
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text(DDL))
    logger.info("apply_lending_confirmation_tasks: done")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
    sys.exit(0)
