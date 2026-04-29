"""
Hello-world graph — the smoke test for the Cora infrastructure.

Two nodes:
  1. load_profile — calls the get_subscriber_profile read tool
  2. summarize    — renders a one-line summary into state

Proves end-to-end that:
  - AgentsSettings loads
  - PostgresSaver writes checkpoints
  - Tool registry resolves a tool by name
  - A read tool executes against the real database
  - LangGraph compiles, invokes, and completes with a final state

Not production — purely a scaffolding test. Delete or disable after the
real graphs are in place.
"""

from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.tools.read_tools import get_subscriber_profile


class HelloWorldState(TypedDict, total=False):
	subscriber_id: int
	profile: dict
	summary: str
	node_call_count: int


def _node_load_profile(state: HelloWorldState) -> HelloWorldState:
	subscriber_id = state["subscriber_id"]
	profile = get_subscriber_profile(subscriber_id)
	return {
		"profile": profile,
		"node_call_count": state.get("node_call_count", 0) + 1,
	}


def _node_summarize(state: HelloWorldState) -> HelloWorldState:
	profile = state.get("profile") or {}
	if not profile:
		summary = f"Subscriber {state['subscriber_id']} not found"
	else:
		summary = (
			f"Subscriber {profile.get('id')} "
			f"tier={profile.get('tier')} "
			f"status={profile.get('status')} "
			f"vertical={profile.get('vertical')}"
		)
	return {
		"summary": summary,
		"node_call_count": state.get("node_call_count", 0) + 1,
	}


def build_hello_world_graph():
	"""Compile the hello-world graph without a checkpointer (callers can add one)."""
	builder = StateGraph(HelloWorldState)
	builder.add_node("load_profile", _node_load_profile)
	builder.add_node("summarize", _node_summarize)
	builder.add_edge(START, "load_profile")
	builder.add_edge("load_profile", "summarize")
	builder.add_edge("summarize", END)
	return builder


def run(subscriber_id: int, thread_id: str = "hello-world-smoke") -> dict:
	"""
	Run the hello-world graph end-to-end with real Postgres checkpointing.

	Returns the final state dict.
	"""
	from src.agents.checkpoint import checkpoint_saver

	with checkpoint_saver() as saver:
		graph = build_hello_world_graph().compile(checkpointer=saver)
		final = graph.invoke(
			{"subscriber_id": subscriber_id},
			config={"configurable": {"thread_id": thread_id}},
		)
		return dict(final)
