"""
QUALITY-v2.2 Q2 — Agent-level P&L ledger schema.

Four DDL changes:
  1. agent_pnl — one row per (seat, period_month), written monthly by
     src/tasks/agent_pnl_monthly.py. Stores the four P&L terms plus the
     binding_constraint label required by §9.5's bottleneck scorecard and
     queue_dwell_median_minutes (Vera's scorecard item #8, reported alongside
     but never included in the cost formula — it is a latency metric, not
     a dollar figure; see QUALITY-v2.2 analysis §5c on why approval timestamps
     cannot proxy for attention).

  2. agent_manual_cost_entries — a sibling table for costs that have no
     automatic source today (Dev Shop contractor invoices, Instantly flat-plan
     cost, Synthflow). NOT an extension of marketing_spend — that table's
     channel must use the CAC compiler's utm_source vocabulary; adding a
     non-CAC entry there silently breaks the spend↔revenue join.

  3. platform_revenue_ledger.opportunity_thread_id — the single missing link
     between a payment row and the thread that caused it. Added nullable, left
     NULL. Population (wiring through _on_checkout_completed) is deferred —
     touching that fast/deferred-split handler without tracing UTM→thread_id
     through checkout metadata is too risky for this build.

  4. enrichment_usage_logs.caller — enables per-seat data-cost attribution.
     Column is added here; callers must pass caller='hunter' (or the relevant
     seat) to enrichment_log.log_usage() to populate it. Hunter's enrichment
     shows $0 until that wiring lands — honest for the first P&L run.

    PYTHONPATH=. python migrations/apply_agent_pnl.py

Idempotent (CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS agent_pnl (
        seat                       VARCHAR(20)    NOT NULL,
        period_month               DATE           NOT NULL,
        attributed_gp_cents        BIGINT         NOT NULL DEFAULT 0,
        compute_cost_cents         BIGINT         NOT NULL DEFAULT 0,
        data_cost_cents            BIGINT         NOT NULL DEFAULT 0,
        founder_minutes_cost_cents BIGINT         NOT NULL DEFAULT 0,
        net_contribution_cents     BIGINT         NOT NULL DEFAULT 0,
        binding_constraint         VARCHAR(40),
        approval_count             INTEGER        NOT NULL DEFAULT 0,
        queue_dwell_median_minutes NUMERIC(8, 2),
        created_at                 TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
        PRIMARY KEY (seat, period_month),
        CONSTRAINT ck_agent_pnl_seat CHECK (
            seat IN ('vera', 'cora', 'hunter', 'relay', 'dev_shop', 'lifecycle')
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_manual_cost_entries (
        id           BIGSERIAL PRIMARY KEY,
        seat         VARCHAR(20)   NOT NULL,
        period_month DATE          NOT NULL,
        vendor       VARCHAR(40)   NOT NULL,
        description  TEXT,
        amount_cents INTEGER       NOT NULL,
        entered_by   VARCHAR(100)  NOT NULL DEFAULT 'admin',
        created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_agent_manual_seat CHECK (
            seat IN ('vera', 'cora', 'hunter', 'relay', 'dev_shop', 'lifecycle')
        ),
        CONSTRAINT ck_agent_manual_amount_nonneg CHECK (amount_cents >= 0)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_agent_manual_seat_month ON agent_manual_cost_entries (seat, period_month)",
    """
    ALTER TABLE platform_revenue_ledger
        ADD COLUMN IF NOT EXISTS opportunity_thread_id VARCHAR(20)
    """,
    "CREATE INDEX IF NOT EXISTS idx_revenue_ledger_thread ON platform_revenue_ledger (opportunity_thread_id) WHERE opportunity_thread_id IS NOT NULL",
    """
    ALTER TABLE enrichment_usage_logs
        ADD COLUMN IF NOT EXISTS caller VARCHAR(40)
    """,
    "CREATE INDEX IF NOT EXISTS idx_enrichment_caller ON enrichment_usage_logs (caller) WHERE caller IS NOT NULL",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'agent_pnl'
            ORDER BY ordinal_position
        """)).fetchall()
    print("agent_pnl columns:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    sys.exit(main())
