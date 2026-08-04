"""
QUALITY-v2.2 Q4 — revenue canary + source-failover schema.

Four pieces:
  1. revenue_canary_alert_log — dedup log for the revenue canary sweep's
     alert emails, same shape as RevenueHeartbeatAlertLog
     (models.py:3909) / ScraperAlertLog (models.py:1976).

  2. revenue_canary_probe_log — a dedicated table the entitlement/delivery
     canary checks INSERT..ON CONFLICT DO UPDATE against, proving the exact
     SQL idiom record_revenue()/SentLead's upsert depend on still works,
     WITHOUT touching platform_revenue_ledger or sent_leads (both read
     unfiltered by product_type in revenue_fulfillment_heartbeat.py's daily
     reconciliation — a stray canary row there would misreport real revenue)
     and without creating a permanent synthetic Subscriber/Property row this
     codebase has no is_test/is_canary flag to let real sweeps skip.

  3. source_failover_log — event log for QUALITY-v2.2 Q4's named-alternate
     source failover plumbing (decision A2-revised / E3-revised): every
     SLA-breach-driven switch attempt, whether it actually switched or
     logged "no alternate configured", is recorded here.

  4. county_sources gets five new columns — the named-alternate plumbing
     itself. Every alternate starts NULL; the failover path stays INERT
     until a real backup source is researched and named (out of scope for
     this build, per decision E3-revised).

    PYTHONPATH=. python migrations/apply_revenue_canaries_and_failover.py

Idempotent (CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS / guarded
DO block for the CHECK constraint, since Postgres has no
ADD CONSTRAINT IF NOT EXISTS).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS revenue_canary_alert_log (
        id          BIGSERIAL PRIMARY KEY,
        check_name  VARCHAR(20) NOT NULL,
        alerted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_revenue_canary_alert_log_lookup "
    "ON revenue_canary_alert_log (check_name, alerted_at)",

    """
    CREATE TABLE IF NOT EXISTS revenue_canary_probe_log (
        check_name  VARCHAR(20) PRIMARY KEY,
        probe_value BIGINT      NOT NULL DEFAULT 0,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS source_failover_log (
        id          BIGSERIAL PRIMARY KEY,
        source_type VARCHAR(50) NOT NULL,
        county_id   VARCHAR(50) NOT NULL,
        event_type  VARCHAR(30) NOT NULL,
        detail      TEXT,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_source_failover_log_event_type CHECK (event_type IN (
            'switched_to_alternate', 'no_alternate_configured', 'switched_back_to_primary'
        ))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_source_failover_log_lookup "
    "ON source_failover_log (source_type, county_id, occurred_at)",

    "ALTER TABLE county_sources ADD COLUMN IF NOT EXISTS alternate_source_name VARCHAR(100)",
    "ALTER TABLE county_sources ADD COLUMN IF NOT EXISTS alternate_url TEXT",
    "ALTER TABLE county_sources ADD COLUMN IF NOT EXISTS active_source VARCHAR(10) NOT NULL DEFAULT 'primary'",
    "ALTER TABLE county_sources ADD COLUMN IF NOT EXISTS failover_confidence_penalty INTEGER NOT NULL DEFAULT 20",
    "ALTER TABLE county_sources ADD COLUMN IF NOT EXISTS switched_to_alternate_at TIMESTAMPTZ",

    # Postgres has no ADD CONSTRAINT IF NOT EXISTS — guard with a DO block
    # against pg_constraint so re-running this script is a no-op.
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'ck_county_sources_active_source'
        ) THEN
            ALTER TABLE county_sources
                ADD CONSTRAINT ck_county_sources_active_source
                CHECK (active_source IN ('primary', 'alternate'));
        END IF;
    END $$
    """,

    # Partial index — cheap because active_source = 'alternate' should be
    # rare-to-never (plumbing is inert until a real alternate is named).
    "CREATE INDEX IF NOT EXISTS idx_county_sources_active_source "
    "ON county_sources (active_source) WHERE active_source = 'alternate'",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'county_sources'
              AND column_name IN (
                  'alternate_source_name', 'alternate_url', 'active_source',
                  'failover_confidence_penalty', 'switched_to_alternate_at'
              )
            ORDER BY column_name
        """)).fetchall()
    print("county_sources new columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    sys.exit(main())
