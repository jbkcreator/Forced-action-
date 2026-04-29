"""
Shared state TypedDicts for Cora graphs.

Every graph carries CoraState (or a subclass of it) through its nodes.
LangGraph uses these TypedDicts to type-check node inputs/outputs and to
serialize state to the Postgres checkpoint store.

Per-graph subclasses live alongside their graph module (e.g. FOMOState in
src/agents/graphs/fomo.py) and add graph-specific fields on top of CoraState.
"""

from typing import Literal, Optional, TypedDict


TerminalStatus = Literal["completed", "aborted", "escalated", "failed"]
KillSwitchColor = Literal["green", "yellow", "red"]


class CoraState(TypedDict, total=False):
	"""
	Base state for every Cora decision.

	`total=False` means every field is optional. Graphs populate the subset
	they need; missing fields default to absent (not None). This keeps
	subclasses from needing to re-declare every inherited field as Optional.
	"""

	# ── Identifiers (set by the supervisor before a subgraph runs) ────────────
	decision_id: str              # UUID, primary key in agent audit log
	subscriber_id: int
	graph_name: str
	event_type: str
	event_payload: dict            # raw event data from the source

	# ── Context (populated by decision_hierarchy and read tools) ──────────────
	subscriber_profile: dict
	segment: str
	revenue_signal_score: int
	wallet_state: dict
	zip_activity: dict
	learning_card: dict
	ab_variant: str
	guardrails_in_scope: dict
	kill_switch_color: KillSwitchColor

	# ── Decision outputs ──────────────────────────────────────────────────────
	proposed_action: str
	action_allowed: bool
	action_blocked_reason: str     # populated when action_allowed is False
	compliance_allowed: bool
	compliance_blocked_reason: str

	# ── Message composition (when applicable) ─────────────────────────────────
	message_body: str
	campaign: str
	variant_id: str

	# ── Token / cost tracking (every Claude call increments these) ────────────
	tokens_used: int
	cost_usd: float
	node_call_count: int           # circuit-breaker input

	# ── Terminal outcome ──────────────────────────────────────────────────────
	terminal_status: TerminalStatus
	failure_reason: str
