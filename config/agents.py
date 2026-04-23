"""
Cora Agents runtime configuration.

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

	# ── Logging ───────────────────────────────────────────────────────────────
	agents_log_level: str = Field(
		default="INFO",
		env="AGENTS_LOG_LEVEL",
	)

	# ── Graph enable list & kill switches ─────────────────────────────────────
	agents_graphs_enabled: str = Field(
		default="hello_world,fomo,abandonment_wave1,abandonment_wave2,retention",
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
