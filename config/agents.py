"""
Lifecycle Agents runtime configuration.

Inherits from AppSettings so every shared key (DATABASE_URL, REDIS_URL,
ANTHROPIC_API_KEY, Stripe, Twilio, guardrails, etc.) is available unchanged.
Adds the LangGraph-specific keys the agents process needs.

Usage:
    from config.agents import get_agents_settings
    settings = get_agents_settings()
"""

from functools import lru_cache
from typing import List, Optional

from pydantic import Field, SecretStr

from config.settings import AppSettings


class AgentsSettings(AppSettings):
	"""Extends AppSettings with LangGraph-specific runtime configuration."""

	# ── Checkpoint store ──────────────────────────────────────────────────────
	agents_checkpoint_schema: str = Field(
		default="langgraph",
		env="AGENTS_CHECKPOINT_SCHEMA",
		description="Postgres schema that holds LangGraph checkpoint tables",
	)

	# ── Concurrency & budgets ─────────────────────────────────────────────────
	agents_worker_concurrency: int = Field(
		default=5,
		env="AGENTS_WORKER_CONCURRENCY",
		description="Max concurrent graph executions per worker process",
	)
	agents_max_tokens_per_decision: int = Field(
		default=3000,
		env="AGENTS_MAX_TOKENS_PER_DECISION",
		description="Per-graph hard cap on total Claude tokens across one decision",
	)
	agents_max_cost_usd_per_decision: float = Field(
		default=0.10,
		env="AGENTS_MAX_COST_USD_PER_DECISION",
		description="Per-graph hard cap on total USD cost across one decision",
	)
	agents_max_node_calls_per_decision: int = Field(
		default=50,
		env="AGENTS_MAX_NODE_CALLS_PER_DECISION",
		description="Circuit breaker for a graph looping through nodes",
	)

	# ── Founder cost constants (QUALITY-v2.2 Q2 P&L rollup) ──────────────────
	founder_minutes_per_approval: int = Field(
		default=5,
		env="FOUNDER_MINUTES_PER_APPROVAL",
		description=(
			"Minutes of founder attention per approval/rejection in relay_approval_queue. "
			"Ratified by Josh; do not change without a new ratification."
		),
	)
	founder_minute_rate_cents: int = Field(
		default=417,
		env="FOUNDER_MINUTE_RATE_CENTS",
		description="Cents per founder-minute for the P&L formula. $4.17/min ($250/hr), ratified by Josh.",
	)

	# ── LangSmith tracing ─────────────────────────────────────────────────────
	langsmith_api_key: Optional[SecretStr] = Field(
		default=None,
		env="LANGSMITH_API_KEY",
	)
	langsmith_project: str = Field(
		default="forced-action-agents",
		env="LANGSMITH_PROJECT",
	)
	langsmith_tracing: bool = Field(
		default=False,
		env="LANGSMITH_TRACING",
		description="Enable LangSmith trace uploads",
	)
	langsmith_endpoint: str = Field(
		default="https://api.smith.langchain.com",
		env="LANGSMITH_ENDPOINT",
		description="LangSmith API endpoint (US default; EU is https://eu.api.smith.langchain.com)",
	)

	# ── Logging ───────────────────────────────────────────────────────────────
	agents_log_level: str = Field(
		default="INFO",
		env="AGENTS_LOG_LEVEL",
	)

	# ── Graph enable list & kill switches ─────────────────────────────────────
	agents_graphs_enabled: str = Field(
		default=(
			"fomo,abandonment_wave1,abandonment_wave2,retention,"
			"wallet_to_lock_close,ap_lite_close,human_close_route,"
			"synthflow_voice_drop,accelerated_wallet_push,reactivation,"
			"dfy_lite_pitch,quora_channel,new_lead_voice_call"
		),
		env="AGENTS_GRAPHS_ENABLED",
		description="Comma-separated list of graphs the supervisor may route to",
	)
	agents_global_kill_switch: bool = Field(
		default=False,
		env="AGENTS_GLOBAL_KILL_SWITCH",
		description="Master off-switch — supervisor idles when true",
	)

	# ── Event sources ─────────────────────────────────────────────────────────
	agents_event_source_redis: bool = Field(
		default=True,
		env="AGENTS_EVENT_SOURCE_REDIS",
	)
	agents_event_source_postgres: bool = Field(
		default=True,
		env="AGENTS_EVENT_SOURCE_POSTGRES",
	)
	lifecycle_queue_stale_processing_seconds: int = Field(
		default=600,
		env="LIFECYCLE_QUEUE_STALE_SECONDS",
		description=(
			"A lifecycle_event_queue row stuck at status='processing' longer than this "
			"is treated as an abandoned claim (crashed worker) and reclaimed by the next "
			"sweep. Must stay comfortably longer than the slowest legitimate graph run — "
			"live agent_decisions data is too thin to derive this empirically yet (26 "
			"completed rows, max 1.6s), so 600s is a conservative placeholder, not a "
			"measured value. Raise it if a real run is ever observed to approach it."
		),
	)

	# ── FA Max agent runtime (WP-T2-2) ───────────────────────────────────────
	fa_max_agent_max_tool_calls: int = Field(
		default=8,
		ge=1,
		env="FA_MAX_AGENT_MAX_TOOL_CALLS",
		description=(
			"Bounded tool-call loop ceiling for the FA Max agent worker "
			"(src/agents/fa_max/worker.py) -- caps how many tool calls a single "
			"claimed work item may make before the loop force-exits, leaving the "
			"item to reclaim_expired_work_items()'s existing lease mechanism "
			"rather than looping indefinitely on a stuck task."
		),
	)

	fa_max_agent_tool_timeout_seconds: int = Field(
		default=30,
		ge=1,
		env="FA_MAX_AGENT_TOOL_TIMEOUT_SECONDS",
		description=(
			"Per-tool-call wall-clock ceiling for the FA Max agent loop "
			"(src/agents/fa_max/agent_graph.py). fa_max_agent_max_tool_calls bounds "
			"how MANY calls a work item may make; this bounds how LONG any single "
			"call may run -- without it, one hung external call (a slow DB query, "
			"a stalled Slack/enrichment request) could hold the worker indefinitely "
			"even though the call count never advances. A timed-out call is logged "
			"status='error' in fa_max_tool_call_log and stops the loop, exactly like "
			"any other tool exception."
		),
	)

	# ── Helpers ───────────────────────────────────────────────────────────────
	@property
	def enabled_graphs(self) -> List[str]:
		"""Parsed list of currently-enabled graphs."""
		return [g.strip() for g in self.agents_graphs_enabled.split(",") if g.strip()]

	def graph_is_enabled(self, graph_name: str) -> bool:
		"""Kill-switch check used by the supervisor on every event."""
		if self.agents_global_kill_switch:
			return False
		return graph_name in self.enabled_graphs


@lru_cache
def get_agents_settings() -> AgentsSettings:
	"""Load and cache agents settings. Called once at agents process startup."""
	return AgentsSettings()
