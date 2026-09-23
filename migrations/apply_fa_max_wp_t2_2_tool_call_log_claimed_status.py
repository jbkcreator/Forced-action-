"""FA Max WP-T2-2 review fix — add 'claimed' to fa_max_tool_call_log.status.

WP-T2-2 review round 5 identified that fa_max_tool_log.claim_send_attempt()
(the atomic check a `send` tool call makes right before it would cause a
real side effect) could race the agent loop's own timeout handler for the
row's lock: both wrote to the SAME fa_max_tool_call_log row from different
sessions/transactions, and claim_send_attempt() shared the send tool's own
long-lived session -- meaning the row lock it took stayed held for the
ENTIRE duration of the send tool's remaining work (including
relay.queue.enqueue(), which can itself contact Slack), not just for the
claim's own instant. A concurrently-firing timeout's finish_tool_call()
UPDATE on that same row would then BLOCK waiting on that lock -- turning
the configured timeout into "wait for the (theoretically) timed-out call to
finish anyway," defeating the entire point of having a timeout.

The fix (see src/services/fa_max_tool_log.py and
src/agents/fa_max/agent_graph.py) is two-part:
  1. claim_send_attempt() now opens and commits its OWN short-lived
     transaction, releasing the row lock immediately rather than holding it
     for the rest of the tool call.
  2. The claim now transitions status 'in_progress' -> 'claimed' (this
     migration's new value) rather than just stamping a marker in `output`.
     The agent loop's timeout handler passes require_status='in_progress'
     to finish_tool_call(), so if a claim has already promoted the row past
     'in_progress', the timeout write becomes a no-op rather than
     clobbering the 'claimed' state back to 'error' -- preserving the
     signal that "a real send was let through even though the loop gave up
     waiting for it," rather than erasing it. The later reconciliation
     callback (fired when the orphaned thread truly finishes) still writes
     the row's final status ('success'/'error') unconditionally, since that
     IS the authoritative end state.

Idempotent. Safe to re-run.
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

NEW_CONSTRAINT_NAME = "ck_fa_max_tool_call_log_status"
NEW_CONSTRAINT_SQL = "status IN ('in_progress', 'claimed', 'success', 'error', 'blocked')"


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

        print(f"  -> adding {NEW_CONSTRAINT_NAME!r} allowing 'claimed'")
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
