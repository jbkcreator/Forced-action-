"""Allow one source record to represent different borrower/event transitions."""
import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logger = logging.getLogger(__name__)


def apply(engine=None):
    engine = engine or create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE borrower_ledger_events DROP CONSTRAINT IF EXISTS uq_ble_source"))
        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'uq_ble_source_event_entity'
                ) THEN
                    ALTER TABLE borrower_ledger_events
                    ADD CONSTRAINT uq_ble_source_event_entity
                    UNIQUE (source_table, source_id, event_type, buyer_entity_id);
                END IF;
            END $$
        """))
    logger.info("borrower ledger event identity constraint applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
