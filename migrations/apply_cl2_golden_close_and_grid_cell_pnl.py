"""
CLONE-v2.2 CL2 — golden CLOSE library + grid-cell P&L framework.

Creates two new tables:

- golden_close_chains: one row per closed deal holding the full winning
  chain (first signal -> ... -> account expansion) as a portable snapshot,
  per docs on GoldenCloseChain in src/core/models.py. Built as CL2's own
  working shape (not yet reconciled against LEARN-v2.2 / L4's data model,
  per lead guidance to build ahead of L4 rather than block on it).
- grid_cell_pnl: per (county, distress_type, buyer_vertical, offer_step,
  period) additive-rollup P&L, generalizing the existing
  PlatformRevenueLedger/PlatformCostAttribution pattern down to the cell
  level — see GridCellPnl in src/core/models.py.

Neither table touches existing schema — this is pure addition, safe to run
against the shared DB at any time.

Run once:
    PYTHONPATH=. python migrations/apply_cl2_golden_close_and_grid_cell_pnl.py

Idempotent (CREATE TABLE IF NOT EXISTS / index IF NOT EXISTS).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS golden_close_chains (
        id BIGSERIAL PRIMARY KEY,
        deal_id INTEGER NOT NULL REFERENCES deal_outcomes(id),
        venture VARCHAR(60) NOT NULL DEFAULT 'hillsborough_distress',
        schema_version INTEGER NOT NULL DEFAULT 1,
        subscriber_id INTEGER REFERENCES subscribers(id),
        property_id INTEGER REFERENCES properties(id),
        county_id VARCHAR(50),
        distress_type VARCHAR(50),
        buyer_vertical VARCHAR(50),
        offer_step VARCHAR(50),
        deal_amount NUMERIC(12, 2),
        days_to_close INTEGER,
        chain_stages JSONB NOT NULL DEFAULT '[]'::jsonb,
        status VARCHAR(20) NOT NULL DEFAULT 'draft',
        authored_by VARCHAR(120) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT check_golden_close_chain_status
            CHECK (status IN ('draft','verified','promoted_to_playbook','retired'))
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_golden_close_chains_deal_venture ON golden_close_chains (deal_id, venture)",
    "CREATE INDEX IF NOT EXISTS idx_golden_close_chains_cell ON golden_close_chains (county_id, distress_type, buyer_vertical, offer_step)",
    "CREATE INDEX IF NOT EXISTS idx_golden_close_chains_venture_status ON golden_close_chains (venture, status)",
    "CREATE INDEX IF NOT EXISTS ix_golden_close_chains_deal_id ON golden_close_chains (deal_id)",
    "CREATE INDEX IF NOT EXISTS ix_golden_close_chains_subscriber_id ON golden_close_chains (subscriber_id)",
    "CREATE INDEX IF NOT EXISTS ix_golden_close_chains_property_id ON golden_close_chains (property_id)",
    """
    CREATE TABLE IF NOT EXISTS grid_cell_pnl (
        id BIGSERIAL PRIMARY KEY,
        county_id VARCHAR(50) NOT NULL,
        distress_type VARCHAR(50) NOT NULL,
        buyer_vertical VARCHAR(50) NOT NULL,
        offer_step VARCHAR(50) NOT NULL,
        period_start DATE NOT NULL,
        period_end DATE NOT NULL,
        revenue_cents BIGINT NOT NULL DEFAULT 0,
        cost_cents BIGINT NOT NULL DEFAULT 0,
        contribution_margin_cents BIGINT NOT NULL DEFAULT 0,
        deal_count INTEGER NOT NULL DEFAULT 0,
        computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_grid_cell_pnl_cell_period
            UNIQUE (county_id, distress_type, buyer_vertical, offer_step, period_start, period_end)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_grid_cell_pnl_cell ON grid_cell_pnl (county_id, distress_type, buyer_vertical, offer_step)",
    "CREATE INDEX IF NOT EXISTS idx_grid_cell_pnl_period ON grid_cell_pnl (period_start, period_end)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        tables = db.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_name IN ('golden_close_chains', 'grid_cell_pnl')
            ORDER BY table_name
        """)).fetchall()
    print("tables present:", [t.table_name for t in tables])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
