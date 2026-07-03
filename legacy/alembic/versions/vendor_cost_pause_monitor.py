"""vendor_cost_pause_monitor

Creates vendor_cost_pauses table and extends api_usage_logs with columns
needed for vendor cost monitoring: pause_target, graph_name, blocked_by_pause,
and block_reason.

Revision ID: vendor_cost_pause_monitor
Revises:     y9z0a1b2c3d4_add_raw_response
Create Date: 2026-05-26
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "vendor_cost_pause_monitor"
down_revision = "y9z0a1b2c3d4_add_raw_response"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -- vendor_cost_pauses table (idempotent) ------------------------------------
    op.execute("""
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
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_vendor ON vendor_cost_pauses (vendor)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_target ON vendor_cost_pauses (pause_target)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_vendor_target ON vendor_cost_pauses (vendor, pause_target)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_status_resume ON vendor_cost_pauses (status, auto_resume_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_vendor_cost_pause_paused_at ON vendor_cost_pauses (paused_at)")

    # -- api_usage_logs column additions (idempotent) ----------------------------
    op.execute("ALTER TABLE api_usage_logs ADD COLUMN IF NOT EXISTS graph_name VARCHAR(60)")
    op.execute("ALTER TABLE api_usage_logs ADD COLUMN IF NOT EXISTS pause_target VARCHAR(80)")
    op.execute("ALTER TABLE api_usage_logs ADD COLUMN IF NOT EXISTS blocked_by_pause BOOLEAN NOT NULL DEFAULT false")
    op.execute("ALTER TABLE api_usage_logs ADD COLUMN IF NOT EXISTS block_reason VARCHAR(120)")

    op.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_graph_name ON api_usage_logs (graph_name)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_pause_target ON api_usage_logs (pause_target)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_pause_created ON api_usage_logs (pause_target, created_at)")


def downgrade() -> None:
    # api_usage_logs column removals
    op.drop_index("idx_api_usage_pause_created", table_name="api_usage_logs")
    op.drop_index("idx_api_usage_pause_target", table_name="api_usage_logs")
    op.drop_index("idx_api_usage_graph_name", table_name="api_usage_logs")
    op.drop_column("api_usage_logs", "block_reason")
    op.drop_column("api_usage_logs", "blocked_by_pause")
    op.drop_column("api_usage_logs", "pause_target")
    op.drop_column("api_usage_logs", "graph_name")

    # vendor_cost_pauses table removal
    op.drop_index("idx_vendor_cost_pause_paused_at", table_name="vendor_cost_pauses")
    op.drop_index("idx_vendor_cost_pause_status_resume", table_name="vendor_cost_pauses")
    op.drop_index("idx_vendor_cost_pause_vendor_target", table_name="vendor_cost_pauses")
    op.drop_index("idx_vendor_cost_pause_target", table_name="vendor_cost_pauses")
    op.drop_index("idx_vendor_cost_pause_vendor", table_name="vendor_cost_pauses")
    op.drop_table("vendor_cost_pauses")