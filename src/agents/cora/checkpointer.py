"""
Thin wrapper around src.agents.checkpoint.checkpoint_saver() for Cora's own
use. Reuses the existing, already-migrated LangGraph checkpoint tables
(checkpoints/checkpoint_blobs/checkpoint_writes/checkpoint_migrations, in
whatever schema config.agents.AgentsSettings.agents_checkpoint_schema names
— default "langgraph") — no new migration, since those tables carry zero
app-specific columns and are keyed purely by thread_id/checkpoint_ns.

Centralizes the "open saver -> compile with checkpointer -> invoke with
thread_id -> return final state" pattern so main_graph.py doesn't repeat the
`with checkpoint_saver() as saver: ...` boilerplate at every call site.
"""
from __future__ import annotations

from typing import Any, Dict

from src.agents.checkpoint import checkpoint_saver


def run_with_checkpoint(builder, thread_id: str, initial_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    builder: an uncompiled langgraph.graph.StateGraph (main_graph.py's
    build_cora_main_graph()). Compiles it with a PostgresSaver checkpointer
    scoped to thread_id=opportunity_thread_id, invokes, returns final state.

    checkpoint_ns="cora" pins every Cora checkpoint into its own namespace
    within the shared checkpoint tables. The tables are reused as-is (see
    module docstring), but isolation from anything else that ever starts
    checkpointing against them shouldn't depend only on thread_id formats
    happening not to collide — checkpoint_ns makes it explicit instead of
    incidental.
    """
    with checkpoint_saver() as saver:
        graph = builder.compile(checkpointer=saver)
        final = graph.invoke(
            initial_state,
            config={"configurable": {"thread_id": thread_id, "checkpoint_ns": "cora"}},
        )
        return dict(final)
