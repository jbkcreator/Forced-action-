"""
Create borrower_ledger_events table and borrower_timeline view.

borrower_ledger_events is the append-only longitudinal event timeline for
each canonical buyer/borrower (buyer_entities row). One row per meaningful
event — deed acquisitions, foreclosures, permits, liens, legal proceedings,
tax delinquencies, and opportunities.

borrower_timeline is a read-only view joining events to buyer_entities and
properties, consumed by WP-6 (Repeat & Maturity Engine) and WP-9 (Dial List).
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
            CREATE TABLE IF NOT EXISTS borrower_ledger_events (
                id              bigserial PRIMARY KEY,
                buyer_entity_id int NOT NULL REFERENCES buyer_entities(id) ON DELETE CASCADE,
                event_type      text NOT NULL,
                event_date      date NOT NULL,
                property_id     int REFERENCES properties(id) ON DELETE SET NULL,
                source_table    text NOT NULL,
                source_id       int  NOT NULL,
                summary         text,
                amount          numeric(14, 2),
                meta            jsonb,
                created_at      timestamptz NOT NULL DEFAULT now(),
                CONSTRAINT uq_ble_source UNIQUE (source_table, source_id),
                CONSTRAINT ck_ble_event_type CHECK (event_type IN (
                    'deed_acquisition','deed_sale',
                    'foreclosure_filed','foreclosure_resolved',
                    'permit_filed','permit_closed',
                    'lien_filed','lien_released',
                    'legal_proceeding_filed',
                    'tax_delinquency',
                    'opportunity_opened','opportunity_closed'
                ))
            )
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_ble_entity_date
                ON borrower_ledger_events (buyer_entity_id, event_date DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_ble_property
                ON borrower_ledger_events (property_id)
                WHERE property_id IS NOT NULL
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_ble_event_type
                ON borrower_ledger_events (event_type)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_ble_event_date
                ON borrower_ledger_events (event_date)
        """))

        conn.execute(text("""
            CREATE OR REPLACE VIEW borrower_timeline AS
            SELECT
                be.id            AS buyer_entity_id,
                be.canonical_name,
                be.entity_type,
                be.confidence_score,
                ble.id           AS event_id,
                ble.event_type,
                ble.event_date,
                p.id             AS property_id,
                p.address        AS property_address,
                p.county_id,
                ble.summary,
                ble.amount,
                ble.meta,
                ble.source_table,
                ble.source_id
            FROM borrower_ledger_events ble
            JOIN buyer_entities be ON be.id = ble.buyer_entity_id
            LEFT JOIN properties p  ON p.id  = ble.property_id
            ORDER BY ble.buyer_entity_id, ble.event_date DESC
        """))

        logger.info(
            "borrower_ledger_events table, indexes, and borrower_timeline view applied"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
