"""Auto-converted from alembic migration `vendor_cost_pause_monitor` (revision vendor_cost_pause_monitor).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_vendor_cost_pause_monitor.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE IF NOT EXISTS vendor_cost_pauses (
            id SERIAL PRIMARY KEY,
            vendor VARCHAR(30) NOT NULL,
            pause_target VARCHAR(80) NOT NULL,
            source_table VARCHAR(80),
            source_key VARCHAR(120),
            reason TEXT NOT NULL,
            anomaly_score NUMERIC(10,4),
            today_cost_usd NUMERIC(10,6),
            baseline_avg_usd NUMERIC(10,6),
            baseline_stddev_usd NUMERIC(10,6),
            threshold_usd NUMERIC(10,6),
            sample_n INTEGER,
            window_days INTEGER NOT NULL DEFAULT 14,
            paused_at TIMESTAMP NOT NULL DEFAULT now(),
            auto_resume_at TIMESTAMP,
            resumed_at TIMESTAMP,
            status VARCHAR(20) NOT NULL DEFAULT 'active',
            created_by VARCHAR(40) NOT NULL DEFAULT 'cost_monitor',
            resumed_by VARCHAR(80),
            metadata_json JSONB DEFAULT '{}',
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            updated_at TIMESTAMP NOT NULL DEFAULT now(),
            CONSTRAINT check_vendor_cost_pause_status
                CHECK (status IN ('active','auto_resumed','manually_resumed','superseded'))
        );

CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_vendor ON vendor_cost_pauses (vendor);

CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_target ON vendor_cost_pauses (pause_target);

CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_vendor_target ON vendor_cost_pauses (vendor, pause_target);

CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_status_resume ON vendor_cost_pauses (status, auto_resume_at);

CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_paused_at ON vendor_cost_pauses (paused_at);

ALTER TABLE api_usage_logs ADD COLUMN IF NOT EXISTS graph_name VARCHAR(60);

ALTER TABLE api_usage_logs ADD COLUMN IF NOT EXISTS pause_target VARCHAR(80);

ALTER TABLE api_usage_logs ADD COLUMN IF NOT EXISTS blocked_by_pause BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE api_usage_logs ADD COLUMN IF NOT EXISTS block_reason VARCHAR(120);

CREATE INDEX IF NOT EXISTS idx_api_usage_graph_name ON api_usage_logs (graph_name);

CREATE INDEX IF NOT EXISTS idx_api_usage_pause_target ON api_usage_logs (pause_target);

CREATE INDEX IF NOT EXISTS idx_api_usage_pause_created ON api_usage_logs (pause_target, created_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied vendor_cost_pause_monitor")


if __name__ == "__main__":
    main()
