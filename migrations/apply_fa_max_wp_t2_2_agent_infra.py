"""FA Max WP-T2-2 — Attribution + Snooze/Revise + Tool Call Log: schema migration.

Idempotent. Safe to re-run (ADD COLUMN IF NOT EXISTS / CREATE TABLE IF NOT
EXISTS). Apply after WP-1 (apply_fa_max_wp1_remaining.py) and WP-2
(apply_fa_max_wp2_queues.py) — this extends fa_max_interactions,
fa_max_opportunities and relay_approval_queue, all created by those.

What this does:
1. Adds agent_name to fa_max_interactions (nullable text) — which agent
   authored/drove this interaction, populated going forward by
   write_interaction()/mark_sent() callers. Direction continues to be
   captured by the existing autonomy_tier_at_time column semantics; no new
   direction column is added.
2. Adds a write-once origin_interaction_id FK on fa_max_opportunities,
   pointing at the interaction that triggered the opportunity's creation.
   NULL = unattributed = counts as zero for Tier C causal evidence.
3. Extends relay_approval_queue with Snooze/Revise support: eligible_at
   (distinct from fa_max_work_queue.available_at, a different table),
   original_draft, final_content, revision_count, last_revised_by,
   last_revised_at, material_edit.
4. Creates fa_max_tool_call_log — a per-tool-call audit trail, separate
   from agent_decisions (which records agent DECISIONS, not every
   individual tool invocation inside a decision).
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "ADD agent_name to fa_max_interactions",
        """
        ALTER TABLE fa_max_interactions
            ADD COLUMN IF NOT EXISTS agent_name VARCHAR(120);
        CREATE INDEX IF NOT EXISTS ix_fa_max_interaction_agent_name
            ON fa_max_interactions (agent_name)
            WHERE agent_name IS NOT NULL;
        """,
    ),
    (
        "ADD origin_interaction_id to fa_max_opportunities",
        """
        ALTER TABLE fa_max_opportunities
            ADD COLUMN IF NOT EXISTS origin_interaction_id UUID;
        DO $$ BEGIN
            ALTER TABLE fa_max_opportunities
                ADD CONSTRAINT fk_fa_max_opp_origin_interaction
                FOREIGN KEY (origin_interaction_id)
                REFERENCES fa_max_interactions(interaction_id);
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        CREATE INDEX IF NOT EXISTS ix_fa_max_opp_origin_interaction
            ON fa_max_opportunities (origin_interaction_id)
            WHERE origin_interaction_id IS NOT NULL;
        """,
    ),
    (
        "ADD Snooze/Revise columns to relay_approval_queue",
        """
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS eligible_at TIMESTAMPTZ;
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS original_draft TEXT;
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS final_content TEXT;
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS revision_count INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS last_revised_by VARCHAR(120);
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS last_revised_at TIMESTAMPTZ;
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS material_edit BOOLEAN;
        CREATE INDEX IF NOT EXISTS ix_relay_approval_queue_eligible_at
            ON relay_approval_queue (eligible_at)
            WHERE eligible_at IS NOT NULL;
        """,
    ),
    (
        "CREATE fa_max_tool_call_log",
        """
        CREATE TABLE IF NOT EXISTS fa_max_tool_call_log (
            id            BIGSERIAL PRIMARY KEY,
            work_item_id  UUID,
            agent_name    VARCHAR(120) NOT NULL,
            tool_name     VARCHAR(120) NOT NULL,
            input         JSONB,
            output        JSONB,
            duration_ms   INTEGER,
            status        VARCHAR(20) NOT NULL
                              CHECK (status IN ('success', 'error', 'blocked')),
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS ix_fa_max_tool_call_log_work_item
            ON fa_max_tool_call_log (work_item_id);
        CREATE INDEX IF NOT EXISTS ix_fa_max_tool_call_log_agent_tool
            ON fa_max_tool_call_log (agent_name, tool_name, created_at DESC);
        """,
    ),
]


def main() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  -> {label}")
            session.execute(text(sql))
        session.commit()

    with get_db_context() as session:
        agent_name_col = session.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_name = 'fa_max_interactions' AND column_name = 'agent_name'"
            )
        ).scalar()
        origin_col = session.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_name = 'fa_max_opportunities' AND column_name = 'origin_interaction_id'"
            )
        ).scalar()
        eligible_col = session.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_name = 'relay_approval_queue' AND column_name = 'eligible_at'"
            )
        ).scalar()
        tool_log_table = session.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'fa_max_tool_call_log'"
            )
        ).scalar()

    print("\nVerification:")
    print(f"  fa_max_interactions.agent_name column:            {bool(agent_name_col)}")
    print(f"  fa_max_opportunities.origin_interaction_id column: {bool(origin_col)}")
    print(f"  relay_approval_queue.eligible_at column:           {bool(eligible_col)}")
    print(f"  fa_max_tool_call_log table present:                {bool(tool_log_table)}")


if __name__ == "__main__":
    main()
