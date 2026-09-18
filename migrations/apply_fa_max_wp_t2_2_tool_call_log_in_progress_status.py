"""FA Max WP-T2-2 review fix — add 'in_progress' to fa_max_tool_call_log.status.

Closes the audit-ordering gap in the original agent loop: a single
post-execution log_tool_call() write meant a tool's side effect (e.g. an
outbound send via the `send` tool) could complete BEFORE any audit row
existed, or with no row at all if the write failed after the fact. The
loop now writes an 'in_progress' row via
src.services.fa_max_tool_log.start_tool_call() BEFORE the tool executes,
then updates it to its true final status via finish_tool_call() after —
see src/agents/fa_max/agent_graph.py's _node_tool_step. This migration
only widens the CHECK constraint to allow the new transient status value;
the original apply_fa_max_wp_t2_2_agent_infra.py CREATE TABLE statement
declared the constraint inline with no explicit name, so Postgres
auto-named it fa_max_tool_call_log_status_check — this migration looks
that name up dynamically (rather than hardcoding it) so it works whether
the table was created by that raw DDL or by Base.metadata.create_all()
(which uses the named constraint ck_fa_max_tool_call_log_status from
src/core/models.py). Whichever constraint exists is dropped and replaced
with the named ck_fa_max_tool_call_log_status form, so future
Base.metadata.create_all() runs and this migration agree.

Idempotent. Safe to re-run.
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

NEW_CONSTRAINT_NAME = "ck_fa_max_tool_call_log_status"
NEW_CONSTRAINT_SQL = "status IN ('in_progress', 'success', 'error', 'blocked')"


def main() -> None:
    with get_db_context() as session:
        existing_name = session.execute(
            text(
                "SELECT con.conname "
                "FROM pg_constraint con "
                "JOIN pg_class rel ON rel.oid = con.conrelid "
                "WHERE rel.relname = 'fa_max_tool_call_log' AND con.contype = 'c' "
                "AND pg_get_constraintdef(con.oid) LIKE '%%status%%'"
            )
        ).scalar()

        if existing_name:
            print(f"  -> dropping existing status CHECK constraint {existing_name!r}")
            session.execute(
                text(f'ALTER TABLE fa_max_tool_call_log DROP CONSTRAINT "{existing_name}"')
            )
        else:
            print("  -> no existing status CHECK constraint found on fa_max_tool_call_log")

        print(f"  -> adding {NEW_CONSTRAINT_NAME!r} allowing 'in_progress'")
        session.execute(
            text(
                f"ALTER TABLE fa_max_tool_call_log ADD CONSTRAINT {NEW_CONSTRAINT_NAME} "
                f"CHECK ({NEW_CONSTRAINT_SQL})"
            )
        )
        session.commit()

    with get_db_context() as session:
        constraint_def = session.execute(
            text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                f"WHERE conname = '{NEW_CONSTRAINT_NAME}'"
            )
        ).scalar()

    print("\nVerification:")
    print(f"  {NEW_CONSTRAINT_NAME}: {constraint_def}")


if __name__ == "__main__":
    main()
