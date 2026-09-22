"""
Create abandonment_sequences — WP-T2-5 Abandonment Agent touch queue.

One row per scheduled touch (5 rows per sequence). The worker polls
due_at <= now() AND sent_at IS NULL AND cancelled_at IS NULL to fire
touches in order. Dedup is enforced by the unique index on
(person_id, touch_number) where sent_at IS NULL and cancelled_at IS NULL
— a second portal.stall for the same person while a sequence is active
is a no-op.
"""
import logging
from sqlalchemy import create_engine, text
from config.settings import get_settings

logger = logging.getLogger(__name__)


def apply(engine=None):
    if engine is None:
        engine = create_engine(get_settings().database_url)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS abandonment_sequences (
                id                  bigserial PRIMARY KEY,
                person_id           uuid NOT NULL REFERENCES fa_max_persons(person_id) ON DELETE CASCADE,
                contact_email       text,
                opportunity_id      uuid,
                borrower_first_name text,
                touch_number        int NOT NULL CHECK (touch_number BETWEEN 1 AND 5),
                due_at              timestamptz NOT NULL,
                sent_at             timestamptz,
                cancelled_at        timestamptz,
                cancel_reason       text,
                channel             text NOT NULL DEFAULT 'email',
                idempotency_key     text NOT NULL,
                created_at          timestamptz NOT NULL DEFAULT now(),
                CONSTRAINT uq_abandonment_idempotency UNIQUE (idempotency_key)
            )
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_abseq_due
                ON abandonment_sequences (due_at)
                WHERE sent_at IS NULL AND cancelled_at IS NULL
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_abseq_person
                ON abandonment_sequences (person_id)
                WHERE sent_at IS NULL AND cancelled_at IS NULL
        """))

        logger.info("abandonment_sequences table and indexes applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
