"""Auto-converted from alembic migration `fa110_platform_revenue_ledger` (revision fa110_platform_revenue_ledger).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa110_platform_revenue_ledger.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE platform_revenue_ledger (
    id BIGSERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    product_type VARCHAR(40) NOT NULL, 
    amount_cents INTEGER NOT NULL, 
    property_id INTEGER, 
    source_table VARCHAR(60) NOT NULL, 
    source_id BIGINT NOT NULL, 
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    refunded_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_revenue_ledger_source UNIQUE (source_table, source_id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(property_id) REFERENCES properties (id)
);

CREATE INDEX idx_revenue_ledger_subscriber ON platform_revenue_ledger (subscriber_id);

CREATE INDEX idx_revenue_ledger_property ON platform_revenue_ledger (property_id);

CREATE INDEX idx_revenue_ledger_occurred_at ON platform_revenue_ledger (occurred_at);

CREATE INDEX idx_revenue_ledger_product_type ON platform_revenue_ledger (product_type);

CREATE TABLE platform_cost_attribution (
    id BIGSERIAL NOT NULL, 
    enrichment_usage_log_id INTEGER NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    property_id INTEGER NOT NULL, 
    attribution_method VARCHAR(40) NOT NULL, 
    attributed_cost_cents INTEGER NOT NULL, 
    computed_for_date DATE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(enrichment_usage_log_id) REFERENCES enrichment_usage_logs (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(property_id) REFERENCES properties (id)
);

CREATE INDEX idx_cost_attribution_subscriber ON platform_cost_attribution (subscriber_id);

CREATE INDEX idx_cost_attribution_property ON platform_cost_attribution (property_id);

CREATE INDEX idx_cost_attribution_method_date ON platform_cost_attribution (attribution_method, computed_for_date);

CREATE UNIQUE INDEX uq_cost_attribution_direct_purchase ON platform_cost_attribution (enrichment_usage_log_id, subscriber_id) WHERE attribution_method = 'direct_purchase';

CREATE UNIQUE INDEX uq_cost_attribution_zip_territory_daily ON platform_cost_attribution (enrichment_usage_log_id, subscriber_id, computed_for_date) WHERE attribution_method = 'zip_territory_highest_vertical';
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa110_platform_revenue_ledger")


if __name__ == "__main__":
    main()
