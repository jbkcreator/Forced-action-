"""Auto-converted from alembic migration `fa080_dfy_lite_orders` (revision fa080_dfy_lite_orders).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa080_dfy_lite_orders.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE IF NOT EXISTS dfy_lite_orders (
            id                      SERIAL PRIMARY KEY,
            subscriber_id           INTEGER NOT NULL REFERENCES subscribers(id),
            property_id             INTEGER NOT NULL REFERENCES properties(id),
            sent_lead_id            INTEGER,
            source_lead_purchase_id INTEGER,
            status                  VARCHAR(30) NOT NULL DEFAULT 'Order_Received'
                CONSTRAINT ck_dfy_lite_status CHECK (status IN (
                    'Order_Received', 'Signal_Compiled', 'Pitch_Generated',
                    'Needs_Review', 'Delivered', 'Signal_Failed', 'Pitch_Failed', 'Cancelled'
                )),
            pitch_type              VARCHAR(50) NOT NULL,
            offer_angle             VARCHAR(50),
            target_vertical         VARCHAR(50) NOT NULL,
            selected_output_formats JSONB       NOT NULL DEFAULT '[]',
            custom_instructions     TEXT,
            distress_stack_json     JSONB,
            property_snapshot_json  JSONB,
            generated_outputs_json  JSONB,
            pitch_generation_number INTEGER NOT NULL DEFAULT 1,
            pitch_generation_limit  INTEGER NOT NULL DEFAULT 3,
            generated_by            VARCHAR(30) NOT NULL DEFAULT 'claude',
            reviewed_at             TIMESTAMPTZ,
            delivered_at            TIMESTAMPTZ,
            error_reason            TEXT,
            created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

CREATE INDEX IF NOT EXISTS idx_dfy_lite_sub_id   ON dfy_lite_orders (subscriber_id);

CREATE INDEX IF NOT EXISTS idx_dfy_lite_prop_id  ON dfy_lite_orders (property_id);

CREATE INDEX IF NOT EXISTS idx_dfy_lite_status   ON dfy_lite_orders (status);

CREATE INDEX IF NOT EXISTS idx_dfy_lite_created_at ON dfy_lite_orders (created_at);

CREATE INDEX IF NOT EXISTS idx_dfy_lite_sub_prop ON dfy_lite_orders (subscriber_id, property_id);

CREATE INDEX IF NOT EXISTS idx_dfy_lite_sent_lead ON dfy_lite_orders (sent_lead_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa080_dfy_lite_orders")


if __name__ == "__main__":
    main()
