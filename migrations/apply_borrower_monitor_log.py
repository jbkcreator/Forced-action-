"""
Create borrower_monitor_log — idempotency table for WP-6 Repeat & Maturity Engine.

One row per (buyer_entity_id, monitor_type, source_event_id) ensures each
trigger fires at most once per borrower per source event, even if the engine
runs multiple times in the same day.
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
            CREATE TABLE IF NOT EXISTS borrower_monitor_log (
                id              bigserial PRIMARY KEY,
                buyer_entity_id int NOT NULL REFERENCES buyer_entities(id) ON DELETE CASCADE,
                monitor_type    text NOT NULL,
                source_event_id bigint REFERENCES borrower_ledger_events(id) ON DELETE CASCADE,
                slack_ts        text,
                fired_at        timestamptz NOT NULL DEFAULT now(),
                CONSTRAINT uq_monitor_fire
                    UNIQUE (buyer_entity_id, monitor_type, source_event_id),
                CONSTRAINT ck_monitor_type CHECK (monitor_type IN (
                    'loan_maturity', 'next_project', 'dscr_day120', 'portfolio_expansion'
                ))
            )
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_bml_entity
                ON borrower_monitor_log (buyer_entity_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_bml_fired_at
                ON borrower_monitor_log (fired_at DESC)
        """))

        logger.info("borrower_monitor_log table and indexes applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
