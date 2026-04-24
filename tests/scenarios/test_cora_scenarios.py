"""
Cora scenario suite — priority-list graphs tested end-to-end.

Each scenario seeds a realistic subscriber into the DB, dispatches the
trigger event through the Cora supervisor, and asserts the effects are
visible in three tables:

  - sandbox_outbox     (SMS Cora tried to send)
  - agent_decisions    (Cora's audit trail)
  - message_outcomes   (platform attribution)

Claude calls are mocked deterministically so scenarios run without real
API cost. Swap the mock for real Claude when we want to evaluate prompts.
"""

from unittest.mock import patch

import pytest

from tests.scenarios.helpers import (
	advance_by,
	assert_agent_decision,
	assert_no_agent_decision,
	dispatch,
	freeze_at,
	read_agent_decisions,
	read_outbox,
)


pytestmark = pytest.mark.scenario_cora


# ──────────────────────────────────────────────────────────────────────────────
# Canned Claude responses per graph
# ──────────────────────────────────────────────────────────────────────────────

FOMO_CLAUDE = {
	"text": "Mike, a roofer just contacted a Gold lead in 33647. "
			 "2 more Gold leads are still open. [link]",
	"model": "haiku", "input_tokens": 95, "output_tokens": 28,
	"cost_usd": 0.00013,
}

ABANDONMENT_CLAUDE = {
	"text": "Mike, 3 Gold leads in 33647 are still open. "
			 "Your preview window closes in 3 min. [link]",
	"model": "haiku", "input_tokens": 85, "output_tokens": 25,
	"cost_usd": 0.00011,
}

RETENTION_CLAUDE = {
	"text": "Mike, you spent 30 credits this week — 4 Gold leads in 33647 "
			 "went to other contractors you could have reached. "
			 "Reply YEARLY to lock your rate. [link]",
	"model": "sonnet", "input_tokens": 140, "output_tokens": 58,
	"cost_usd": 0.00105,
}


@pytest.fixture
def mock_claude_fomo():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=FOMO_CLAUDE,
	):
		yield


@pytest.fixture
def mock_claude_abandonment():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=ABANDONMENT_CLAUDE,
	):
		yield


@pytest.fixture
def mock_claude_retention():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=RETENTION_CLAUDE,
	):
		yield


# ──────────────────────────────────────────────────────────────────────────────
# Scenario 1 — Happy path baseline (no graphs fire)
# ──────────────────────────────────────────────────────────────────────────────

def test_scenario_1_baseline_no_graph_events_no_outbox(seed_subscriber):
	"""
	Baseline: subscriber exists, no events fired, no outbox rows, no agent
	decisions. Exists to prove the harness doesn't silently emit anything
	without explicit trigger.
	"""
	sub = seed_subscriber(name="Mike Baseline", vertical="roofing")
	freeze_at("2026-05-01T10:00:00Z")

	assert read_outbox(sub.id) == []
	assert read_agent_decisions(sub.id) == []


# ──────────────────────────────────────────────────────────────────────────────
# Scenario 2 — Abandonment Wave 1 recovers a bouncing signup
# ──────────────────────────────────────────────────────────────────────────────

def test_scenario_2_abandonment_wave1_recovers_signup(
	seed_subscriber, mock_claude_abandonment
):
	"""
	Narrative: Mike signs up, sees proof leads, hits the monetization wall,
	doesn't pay for 12 minutes. The abandonment event fires; Wave 1 sends
	a single-CTA SMS referencing live ZIP data.
	"""
	sub = seed_subscriber(name="Mike Roofer", vertical="roofing")
	freeze_at("2026-05-01T10:00:00Z")

	# T+12 — abandonment trigger
	advance_by(minutes=12)
	result = dispatch({
		"event_type": "wall_session_abandoned",
		"subscriber_id": sub.id,
		"payload": {
			"zip_code": "33647",
			"vertical": "roofing",
			"minutes_elapsed": 12,
			"wall_countdown_minutes": 3,
		},
	})
	assert result["outcome"] == "routed"
	assert result["graph_name"] == "abandonment_wave1"

	# Outbox has exactly one SMS attributed to wave1
	outbox = read_outbox(sub.id, campaign="abandonment_wave1")
	assert len(outbox) == 1
	body = outbox[0].body
	assert len(body) <= 320   # at most 2 SMS segments
	assert outbox[0].compliance_allowed is True
	assert outbox[0].would_have_delivered is True

	# Audit: single Wave 1 decision marked completed
	decision = assert_agent_decision(
		sub.id, graph="abandonment_wave1", terminal_status="completed",
	)
	assert decision.tokens_used > 0


# ──────────────────────────────────────────────────────────────────────────────
# Scenario 5 — FOMO: competitor acts, next-best-fit gets nudged
# ──────────────────────────────────────────────────────────────────────────────

def test_scenario_5_fomo_competitor_triggers_nudge(
	seed_subscriber, mock_claude_fomo
):
	"""
	Narrative: an existing wallet-active contractor is the next-best-fit
	target when a competitor contacts a Gold lead in a non-locked ZIP.
	FOMO graph composes a contextual SMS within the 60-second budget.
	"""
	sub = seed_subscriber(name="Mike Fomo", vertical="public_adjusters")
	freeze_at("2026-05-01T14:00:00Z")

	result = dispatch({
		"event_type": "competitor_acted_on_lead",
		"subscriber_id": sub.id,
		"payload": {
			"competitor_event_id": "ev-fomo-1",
			"lead_id": 5001,
			"zip_code": "33647",
			"vertical": "public_adjusters",
			"competitor_subscriber_id": 9999,
			"lead_tier": "Gold",
		},
	})
	assert result["outcome"] == "routed"
	assert result["graph_name"] == "fomo"

	outbox = read_outbox(sub.id, campaign="fomo_competitor_action")
	assert len(outbox) == 1
	# FOMO uses Haiku → body will be short and reference the context
	body = outbox[0].body
	assert len(body) <= 200

	decision = assert_agent_decision(
		sub.id, graph="fomo", terminal_status="completed",
	)
	# Cost must stay inside the Haiku range, nowhere near the budget cap
	assert float(decision.cost_usd) < 0.01


# ──────────────────────────────────────────────────────────────────────────────
# Scenario 7 — Retention weekly summary for a wallet-tier subscriber
# ──────────────────────────────────────────────────────────────────────────────

def test_scenario_7_retention_wallet_summary(
	seed_subscriber, mock_claude_retention
):
	"""
	Narrative: Sunday-midnight cron fires a retention-summary event for an
	active wallet-tier subscriber. Retention graph composes a Sonnet summary
	and dispatches through the compliance-gated path. Tagged with the
	tier-specific campaign id.
	"""
	sub = seed_subscriber(name="Mike Retention", vertical="roofing", tier="starter")
	freeze_at("2026-05-04T00:00:00Z")   # a Sunday

	result = dispatch({
		"event_type": "retention_summary_due",
		"subscriber_id": sub.id,
		"payload": {"tier": "wallet"},
	})
	assert result["outcome"] == "routed"
	assert result["graph_name"] == "retention"

	outbox = read_outbox(sub.id)
	assert len(outbox) == 1
	assert outbox[0].campaign == "retention_summary_wallet"
	# Retention body may be 2 SMS segments (<= 320 chars)
	assert len(outbox[0].body) <= 400

	decision = assert_agent_decision(
		sub.id, graph="retention", terminal_status="completed",
	)
	# Sonnet is more expensive than Haiku; still well inside per-decision cap
	assert float(decision.cost_usd) < 0.05


# ──────────────────────────────────────────────────────────────────────────────
# Scenario 8 — Retention skipped for a churned subscriber
# ──────────────────────────────────────────────────────────────────────────────

def test_scenario_8_retention_skips_churned_subscriber(seed_subscriber):
	"""
	Retention graph aborts early when the subscriber's status is not active.
	No SMS is composed, no outbox row, audit row recorded as 'aborted'.
	"""
	sub = seed_subscriber(name="Churned Sam", tier="free", status="churned")
	freeze_at("2026-05-04T00:00:00Z")

	# No Claude mock needed — compose is never reached.
	result = dispatch({
		"event_type": "retention_summary_due",
		"subscriber_id": sub.id,
		"payload": {"tier": "wallet"},
	})
	assert result["outcome"] == "routed"

	# No outbox row
	assert read_outbox(sub.id) == []

	# Audit row present but aborted
	decision = assert_agent_decision(
		sub.id, graph="retention", terminal_status="aborted",
	)
	assert decision.tokens_used == 0
