"""Auto-converted from alembic migration `fa111_algorithmic_variance_log` (revision fa111_algorithmic_variance_log).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa111_algorithmic_variance_log.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE algorithmic_variance_log (
    id SERIAL NOT NULL, 
    lead_id INTEGER, 
    property_id INTEGER, 
    subscriber_id INTEGER, 
    county VARCHAR(50), 
    vertical VARCHAR(50), 
    lead_tier VARCHAR(20), 
    spend_ratio NUMERIC(14, 6), 
    threshold NUMERIC(14, 6) NOT NULL, 
    selected_path VARCHAR(20) NOT NULL, 
    provider VARCHAR(32), 
    paid_lookup_allowed BOOLEAN DEFAULT false NOT NULL, 
    routing_reason VARCHAR(32) NOT NULL, 
    lookup_success BOOLEAN DEFAULT false NOT NULL, 
    cost_cents INTEGER DEFAULT '0' NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_avl_selected_path CHECK (selected_path IN ('paid_trace','free_cross_match','blocked','override_paid')), 
    CONSTRAINT check_avl_routing_reason CHECK (routing_reason IN ('spend_ratio_safe','spend_ratio_exceeded','zero_revenue_guard','missing_telemetry_guard','manual_override')), 
    FOREIGN KEY(property_id) REFERENCES properties (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_avl_lead_id ON algorithmic_variance_log (lead_id);

CREATE INDEX idx_avl_property_id ON algorithmic_variance_log (property_id);

CREATE INDEX idx_avl_subscriber_id ON algorithmic_variance_log (subscriber_id);

CREATE INDEX idx_avl_created_at ON algorithmic_variance_log (created_at);

CREATE INDEX idx_avl_subscriber_created ON algorithmic_variance_log (subscriber_id, created_at);

CREATE INDEX idx_avl_property_created ON algorithmic_variance_log (property_id, created_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa111_algorithmic_variance_log")


if __name__ == "__main__":
    main()
