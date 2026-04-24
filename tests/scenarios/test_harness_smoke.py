"""
Smoke tests for the scenario harness itself.

These verify:
  - seed_subscriber creates + tears down correctly
  - clock helpers work through the `freeze_at` re-export
  - dispatch() routes events through the supervisor
  - read_outbox / read_agent_decisions round-trip against the real DB

Once this passes we know the harness is ready for real scenario tests.
"""

from unittest.mock import patch

import pytest

from tests.scenarios.helpers import (
	advance_by,
	assert_agent_decision,
	clear_outbox,
	dispatch,
	freeze_at,
	read_agent_decisions,
	read_outbox,
)


pytestmark = pytest.mark.scenario_cora


def test_seed_subscriber_creates_active_subscriber(seed_subscriber):
	sub = seed_subscriber(name="Harness Smoke", vertical="roofing")
	assert sub.id > 0
	assert sub.status == "active"
	assert sub.tier == "free"
	assert sub.event_feed_uuid is not None
	assert hasattr(sub, "_test_phone")


def test_clock_helpers_are_reexported():
	frozen = freeze_at("2026-05-01T10:00:00Z")
	assert frozen.isoformat() == "2026-05-01T10:00:00+00:00"
	advance_by(minutes=12)
	from src.core import clock
	assert clock.now().isoformat() == "2026-05-01T10:12:00+00:00"


def test_dispatch_routes_unknown_event(seed_subscriber):
	"""No mocks needed — unknown event drops at supervisor without graph work."""
	sub = seed_subscriber()
	result = dispatch({
		"event_type": "this_event_does_not_exist",
		"subscriber_id": sub.id,
		"payload": {},
	})
	assert result["outcome"] == "dropped_unknown_event"
	# Audit row written for drops
	decisions = read_agent_decisions(subscriber_id=sub.id)
	assert len(decisions) >= 1
	assert decisions[0].terminal_status == "aborted"


def test_fomo_scenario_writes_outbox_and_audit(seed_subscriber):
	"""
	Minimal Cora scenario: seed subscriber, dispatch FOMO event, assert
	outbox + agent_decisions. Claude mocked, send_sms goes through the
	sandbox path writing to sandbox_outbox.
	"""
	sub = seed_subscriber(name="Mike Harness", vertical="public_adjusters")

	fake_claude = {
		"text": "Mike, a competitor just acted on a Gold lead in 33647. [link]",
		"model": "haiku",
		"input_tokens": 80,
		"output_tokens": 22,
		"cost_usd": 0.00012,
	}

	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=fake_claude,
	):
		result = dispatch({
			"event_type": "competitor_acted_on_lead",
			"subscriber_id": sub.id,
			"payload": {
				"competitor_event_id": "ev-1",
				"lead_id": 1001,
				"zip_code": "33647",
				"vertical": "public_adjusters",
				"lead_tier": "Gold",
			},
		})

	assert result["outcome"] == "routed"
	assert result["graph_name"] == "fomo"

	outbox = read_outbox(subscriber_id=sub.id)
	assert len(outbox) >= 1
	latest = outbox[0]
	assert latest.channel == "sms"
	assert latest.campaign == "fomo_competitor_action"
	assert latest.compliance_allowed is True
	assert latest.would_have_delivered is True

	decision = assert_agent_decision(sub.id, graph="fomo", terminal_status="completed")
	assert decision.tokens_used > 0
	assert float(decision.cost_usd) > 0


def test_clear_outbox_removes_captured_rows(seed_subscriber):
	sub = seed_subscriber()

	fake = {
		"text": "hi", "model": "haiku",
		"input_tokens": 5, "output_tokens": 2, "cost_usd": 0.00001,
	}
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=fake,
	):
		dispatch({
			"event_type": "competitor_acted_on_lead",
			"subscriber_id": sub.id,
			"payload": {
				"zip_code": "33647", "vertical": "roofing", "lead_tier": "Gold",
			},
		})

	assert len(read_outbox(sub.id)) >= 1
	clear_outbox(subscriber_id=sub.id)
	assert read_outbox(sub.id) == []
