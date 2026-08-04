"""
CLONE-v2.2 CL3 — per-venture Relay + source configuration.

Creates the `ventures` table (see Venture in src/core/models.py) and hangs
the existing county/source and Relay-queue tables off it:

- ventures                        NEW. One row per business running on this
                                  agent fleet — the Relay sending identity
                                  and geography that used to be single-valued
                                  env globals in config/settings.py.
- counties.venture_key            NEW COLUMN, FK -> ventures.venture_key.
- counties.zip_prefixes           NEW COLUMN. Was hardcoded to [] in
                                  county_config.py, making is_zip_in_county()
                                  permanently False.
- relay_approval_queue.venture_key  NEW COLUMN, FK -> ventures.venture_key.
                                  Scopes the sweep batch, Slack channel,
                                  Instantly campaign and daily-ceiling
                                  counter per venture.

STATEMENT ORDER MATTERS. `ventures` must exist AND hold the
'hillsborough_distress' row before either venture_key column is added: those
columns are NOT NULL DEFAULT 'hillsborough_distress' with a foreign key, so
adding them against an empty `ventures` table would fail the FK for every
existing row. The seed row is written from the same defaults
config/settings.py uses today, so the resolver
(src/utils/venture_config.get_venture_config) returns byte-identical values
before and after this migration.

NOT applied by this agent — flagged for manual review because unlike CL2
this ALTERs two live tables (`counties`, `relay_approval_queue`) rather than
only creating new ones. Every change is additive with a constant default, so
PostgreSQL 11+ adds them without a table rewrite, but it still touches live
data. Run once:

    PYTHONPATH=. python migrations/apply_cl3_venture_config.py

Idempotent (CREATE TABLE / ADD COLUMN / CREATE INDEX IF NOT EXISTS,
pg_constraint guards for CHECKs and FKs, ON CONFLICT DO NOTHING for the seed).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

# Step 1 — the table itself, with every CHECK the ORM declares.
CREATE_VENTURES = """
CREATE TABLE IF NOT EXISTS ventures (
    id SERIAL PRIMARY KEY,
    venture_key VARCHAR(60) NOT NULL UNIQUE,
    display_name VARCHAR(120) NOT NULL,
    brand_name VARCHAR(120) NOT NULL,
    postal_address TEXT,
    state VARCHAR(2) NOT NULL DEFAULT 'FL',
    bankruptcy_court_code VARCHAR(10) NOT NULL DEFAULT 'flmb',
    default_bankruptcy_division VARCHAR(10) NOT NULL DEFAULT '8:',
    template_county_id VARCHAR(50),
    relay_slack_channel VARCHAR(120),
    relay_approvers JSONB NOT NULL DEFAULT '[]'::jsonb,
    relay_instantly_campaign_id VARCHAR(64),
    relay_instantly_sender_email VARCHAR(200),
    relay_send_window_start SMALLINT NOT NULL DEFAULT 11,
    relay_send_window_end SMALLINT NOT NULL DEFAULT 18,
    relay_send_window_timezone VARCHAR(60) NOT NULL DEFAULT 'America/New_York',
    relay_daily_ceiling INTEGER NOT NULL DEFAULT 20,
    kill_switch_feature VARCHAR(60) NOT NULL DEFAULT 'relay_global',
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ,
    CONSTRAINT ck_ventures_send_window_start
        CHECK (relay_send_window_start >= 0 AND relay_send_window_start <= 24),
    CONSTRAINT ck_ventures_send_window_end
        CHECK (relay_send_window_end >= 0 AND relay_send_window_end <= 24),
    CONSTRAINT ck_ventures_send_window_order
        CHECK (relay_send_window_start < relay_send_window_end),
    CONSTRAINT ck_ventures_daily_ceiling CHECK (relay_daily_ceiling > 0)
)
"""

# Step 2 — venture #1, matching config/settings.py's current defaults so the
# resolver returns the same values before and after this migration. Slack
# channel / Instantly ids are left NULL on purpose: the resolver falls back
# to the live env values for those, which is where they are set today.
SEED_VENTURE_ONE = """
INSERT INTO ventures (
    venture_key, display_name, brand_name, state,
    bankruptcy_court_code, default_bankruptcy_division, template_county_id,
    relay_send_window_start, relay_send_window_end, relay_send_window_timezone,
    relay_daily_ceiling, kill_switch_feature, is_active
)
VALUES (
    'hillsborough_distress', 'Hillsborough Distressed Property', 'Forced Action', 'FL',
    'flmb', '8:', 'hillsborough',
    11, 18, 'America/New_York',
    20, 'relay_global', true
)
ON CONFLICT (venture_key) DO NOTHING
"""

# Step 3 — hang the existing tables off it. Columns first, then the FKs as
# separate guarded statements: ADD COLUMN IF NOT EXISTS with an inline
# REFERENCES clause would re-attempt the FK on a re-run of a partially
# applied migration, so the constraint gets its own idempotency guard.
ALTER_STATEMENTS = [
    "ALTER TABLE counties ADD COLUMN IF NOT EXISTS venture_key VARCHAR(60) NOT NULL DEFAULT 'hillsborough_distress'",
    "ALTER TABLE counties ADD COLUMN IF NOT EXISTS zip_prefixes JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS venture_key VARCHAR(60) NOT NULL DEFAULT 'hillsborough_distress'",
]

FOREIGN_KEYS = [
    ("counties", "fk_counties_venture_key", "venture_key"),
    ("relay_approval_queue", "fk_relay_approval_queue_venture_key", "venture_key"),
]

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_ventures_is_active ON ventures (is_active)",
    "CREATE INDEX IF NOT EXISTS idx_counties_venture_key ON counties (venture_key)",
    "CREATE INDEX IF NOT EXISTS ix_relay_approval_queue_venture_status ON relay_approval_queue (venture_key, status)",
]


def _add_foreign_key(db, table: str, constraint: str, column: str) -> None:
    db.execute(text(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = '{constraint}'
            ) THEN
                ALTER TABLE {table}
                    ADD CONSTRAINT {constraint}
                    FOREIGN KEY ({column}) REFERENCES ventures (venture_key);
            END IF;
        END $$;
    """))


def main() -> int:
    with get_db_context() as db:
        db.execute(text(CREATE_VENTURES))
        db.execute(text(SEED_VENTURE_ONE))
        db.commit()

        for stmt in ALTER_STATEMENTS:
            db.execute(text(stmt))
        for table, constraint, column in FOREIGN_KEYS:
            _add_foreign_key(db, table, constraint, column)
        for stmt in INDEX_STATEMENTS:
            db.execute(text(stmt))
        db.commit()

        ventures = db.execute(text(
            "SELECT venture_key, state, relay_daily_ceiling FROM ventures ORDER BY venture_key"
        )).fetchall()
        counties_cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'counties' AND column_name IN ('venture_key', 'zip_prefixes')
            ORDER BY column_name
        """)).fetchall()
        queue_cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'relay_approval_queue' AND column_name = 'venture_key'
        """)).fetchall()

    print("ventures rows:", [(v.venture_key, v.state, v.relay_daily_ceiling) for v in ventures])
    print("counties columns added:", [c.column_name for c in counties_cols])
    print("relay_approval_queue columns added:", [c.column_name for c in queue_cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
