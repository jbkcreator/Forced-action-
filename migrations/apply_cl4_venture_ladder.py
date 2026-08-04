"""
CLONE-v2.2 CL4 — autonomous venture ladder.

Builds the schema the seven-rung ladder
(radar -> probe -> pilot -> unit_economics -> cell -> spin_up -> portfolio)
runs on, on top of CL3's `ventures` table:

- ventures.ladder_stage          NEW COLUMN, default 'radar'.
- ventures.ladder_entered_at     NEW COLUMN, default now().
- venture_ladder_evidence        NEW. Typed, JSONB-payload evidence rows —
                                 market scores, scrape samples, and presell
                                 commitments. Gates count and sum these.
- venture_ladder_events          NEW. Append-only audit of every advance,
                                 refusal and auto-double, with the computed
                                 gate values frozen into gate_results.
- outbound_drafts.venture_key    NEW COLUMN, FK -> ventures.venture_key.
- outbound_drafts.replied_at     NEW COLUMN. Makes reply rate answerable in
                                 SQL per (venture_key, cell_id) — the number
                                 the auto-double rule scales on. Dual-written
                                 by src/agents/cora/opportunity_state.py:
                                 mark_replied(); Cora's file store stays the
                                 canonical transition log.

STATEMENT ORDER MATTERS, twice.

1. `ladder_stage` is added with DEFAULT 'radar', then venture #1 is backfilled
   to 'portfolio' BEFORE the CHECK constraint goes on. Venture #1 is already
   live and sending; leaving it at 'radar' would be a lie the evaluator acts
   on — it would try to re-probe a running business, and (worse) a radar-stage
   venture is one the ladder considers unproven.

2. `outbound_drafts.venture_key` is NOT NULL DEFAULT 'hillsborough_distress'
   with an FK, so `ventures` must already hold that row — CL3 seeds it. This
   migration will fail loudly rather than silently if CL3 has not run.

Every existing outbound_drafts row predates CL3 and therefore belongs to
venture #1 by definition, which is exactly what the column default backfills.

Run once:

    PYTHONPATH=. python migrations/apply_cl4_venture_ladder.py

Idempotent (ADD COLUMN / CREATE TABLE / CREATE INDEX IF NOT EXISTS,
pg_constraint guards for CHECKs and FKs, and a backfill guarded on the
current value so a re-run cannot demote a venture that has since advanced).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

# Guard: the FKs below all point at ventures.venture_key.
REQUIRE_CL3 = """
SELECT EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_name = 'ventures'
) AS has_ventures
"""

# Step 1 — ladder state on the venture itself.
LADDER_COLUMNS = [
    "ALTER TABLE ventures ADD COLUMN IF NOT EXISTS ladder_stage VARCHAR(30) NOT NULL DEFAULT 'radar'",
    "ALTER TABLE ventures ADD COLUMN IF NOT EXISTS ladder_entered_at TIMESTAMPTZ NOT NULL DEFAULT now()",
]

# Step 2 — venture #1 is a running business, not a candidate. Guarded on
# ladder_stage = 'radar' so re-running this script never walks a venture
# backwards from wherever the ladder has since moved it.
BACKFILL_VENTURE_ONE = """
UPDATE ventures
SET ladder_stage = 'portfolio',
    ladder_entered_at = COALESCE(created_at, now())
WHERE venture_key = 'hillsborough_distress'
  AND ladder_stage = 'radar'
"""

# Step 3 — evidence and audit. Both reference ventures inline: they are new
# tables, so there is no partially-applied state in which the inline FK gets
# re-attempted against existing rows.
CREATE_EVIDENCE = """
CREATE TABLE IF NOT EXISTS venture_ladder_evidence (
    id BIGSERIAL PRIMARY KEY,
    venture_key VARCHAR(60) NOT NULL REFERENCES ventures (venture_key),
    stage VARCHAR(30) NOT NULL,
    evidence_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_ref VARCHAR(200),
    verified BOOLEAN NOT NULL DEFAULT false,
    recorded_by VARCHAR(120) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_venture_ladder_evidence_source_ref
        UNIQUE (venture_key, evidence_type, source_ref)
)
"""

CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS venture_ladder_events (
    id BIGSERIAL PRIMARY KEY,
    venture_key VARCHAR(60) NOT NULL REFERENCES ventures (venture_key),
    from_stage VARCHAR(30) NOT NULL,
    to_stage VARCHAR(30) NOT NULL,
    decision VARCHAR(20) NOT NULL,
    gate_results JSONB NOT NULL DEFAULT '{}'::jsonb,
    blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    actor VARCHAR(120) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_venture_ladder_events_decision
        CHECK (decision IN ('advanced', 'blocked', 'auto_double', 'demoted'))
)
"""

# Step 4 — reply attribution on Cora's drafts.
DRAFT_COLUMNS = [
    "ALTER TABLE outbound_drafts ADD COLUMN IF NOT EXISTS venture_key VARCHAR(60) NOT NULL DEFAULT 'hillsborough_distress'",
    "ALTER TABLE outbound_drafts ADD COLUMN IF NOT EXISTS replied_at TIMESTAMPTZ",
]

CHECK_CONSTRAINTS = [
    (
        "ventures",
        "ck_ventures_ladder_stage",
        "ladder_stage IN ('radar', 'probe', 'pilot', 'unit_economics', "
        "'cell', 'spin_up', 'portfolio')",
    ),
]

FOREIGN_KEYS = [
    ("outbound_drafts", "fk_outbound_drafts_venture_key", "venture_key"),
]

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_ventures_ladder_stage ON ventures (ladder_stage)",
    "CREATE INDEX IF NOT EXISTS ix_venture_ladder_evidence_key_type "
    "ON venture_ladder_evidence (venture_key, evidence_type)",
    "CREATE INDEX IF NOT EXISTS ix_venture_ladder_events_key_created "
    "ON venture_ladder_events (venture_key, created_at)",
    "CREATE INDEX IF NOT EXISTS ix_venture_ladder_events_key_decision "
    "ON venture_ladder_events (venture_key, decision)",
    "CREATE INDEX IF NOT EXISTS ix_outbound_drafts_venture_cell_created "
    "ON outbound_drafts (venture_key, cell_id, created_at)",
    # cell_reply_rates() joins outbound_drafts -> relay_approval_queue on
    # (thread_id, venture_key); that table had no thread_id index.
    "CREATE INDEX IF NOT EXISTS ix_relay_approval_queue_thread_venture "
    "ON relay_approval_queue (thread_id, venture_key)",
]


def _add_check_constraint(db, table: str, constraint: str, expression: str) -> None:
    db.execute(text(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = '{constraint}'
            ) THEN
                ALTER TABLE {table} ADD CONSTRAINT {constraint} CHECK ({expression});
            END IF;
        END $$;
    """))


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
        if not db.execute(text(REQUIRE_CL3)).scalar():
            print(
                "ABORT: `ventures` does not exist — run "
                "migrations/apply_cl3_venture_config.py first.",
                file=sys.stderr,
            )
            return 2

        for stmt in LADDER_COLUMNS:
            db.execute(text(stmt))
        backfilled = db.execute(text(BACKFILL_VENTURE_ONE)).rowcount
        db.commit()

        for table, constraint, expression in CHECK_CONSTRAINTS:
            _add_check_constraint(db, table, constraint, expression)
        db.execute(text(CREATE_EVIDENCE))
        db.execute(text(CREATE_EVENTS))
        for stmt in DRAFT_COLUMNS:
            db.execute(text(stmt))
        for table, constraint, column in FOREIGN_KEYS:
            _add_foreign_key(db, table, constraint, column)
        for stmt in INDEX_STATEMENTS:
            db.execute(text(stmt))
        db.commit()

        stages = db.execute(text(
            "SELECT venture_key, ladder_stage, relay_daily_ceiling "
            "FROM ventures ORDER BY venture_key"
        )).fetchall()
        new_tables = db.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_name IN ('venture_ladder_evidence', 'venture_ladder_events')
            ORDER BY table_name
        """)).fetchall()
        draft_cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'outbound_drafts'
              AND column_name IN ('venture_key', 'replied_at')
            ORDER BY column_name
        """)).fetchall()

    print("venture stages:", [(v.venture_key, v.ladder_stage, v.relay_daily_ceiling) for v in stages])
    print("venture #1 backfilled to portfolio:", bool(backfilled))
    print("tables created:", [t.table_name for t in new_tables])
    print("outbound_drafts columns added:", [c.column_name for c in draft_cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
