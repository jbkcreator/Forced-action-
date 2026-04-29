"""
Supervisor-level integration tests.

Exercises the routing table, kill-switch enforcement, idempotency, and
error handling. Downstream graphs are mocked so we're testing the
supervisor's policy layer, not each graph's internals.
"""

from unittest.mock import patch

import pytest

from src.agents.supervisor import dispatch_event


def test_unknown_event_is_dropped_with_reason():
	with patch("src.agents.supervisor.log_decision"):
		r = dispatch_event({
			"event_type": "not_a_real_event",
			"subscriber_id": 1,
			"payload": {},
		})
	assert r["outcome"] == "dropped_unknown_event"
	assert r["handled"] is False
	assert r["graph_name"] is None


def test_global_kill_switch_stops_routing():
	from config.agents import get_agents_settings
	settings = get_agents_settings()
	settings.agents_global_kill_switch = True
	try:
		with patch("src.agents.supervisor.log_decision"):
			r = dispatch_event({
				"event_type": "competitor_acted_on_lead",
				"subscriber_id": 1,
				"payload": {},
			})
		assert r["outcome"] == "dropped_kill_switch"
	finally:
		settings.agents_global_kill_switch = False


def test_per_graph_kill_switch_stops_routing():
	from config.agents import get_agents_settings
	settings = get_agents_settings()
	original = settings.agents_graphs_enabled
	settings.agents_graphs_enabled = "hello_world"  # excludes fomo
	try:
		with patch("src.agents.supervisor.log_decision"):
			r = dispatch_event({
				"event_type": "competitor_acted_on_lead",
				"subscriber_id": 1,
				"payload": {},
			})
		assert r["outcome"] == "dropped_kill_switch"
		assert "graph_disabled:fomo" in r["reason"]
	finally:
		settings.agents_graphs_enabled = original


def _patch_spec_runner(event_type: str, **runner_kwargs):
	"""
	Replace a GraphSpec's runner in-place for the duration of a test.

	The supervisor resolves the runner via `get_graph_spec(event_type).runner`,
	which holds a frozen reference to the original callable. Patching the
	module-level symbol (e.g. `src.agents.router._run_fomo`) does nothing
	because the GraphSpec dataclass already captured the function. So we
	mutate EVENT_TO_GRAPH itself.
	"""
	from dataclasses import replace
	from unittest.mock import MagicMock
	from src.agents.router import EVENT_TO_GRAPH

	mock = MagicMock(**runner_kwargs)
	original = EVENT_TO_GRAPH[event_type]
	EVENT_TO_GRAPH[event_type] = replace(original, runner=mock)
	return mock, original


def _restore_spec(event_type: str, original):
	from src.agents.router import EVENT_TO_GRAPH
	EVENT_TO_GRAPH[event_type] = original


def test_routed_event_calls_graph_runner():
	"""Routing table actually calls the matching runner."""
	mock_runner, original = _patch_spec_runner(
		"competitor_acted_on_lead",
		return_value={"terminal_status": "completed"},
	)
	try:
		with patch("src.agents.supervisor.log_decision"):
			r = dispatch_event({
				"event_type": "competitor_acted_on_lead",
				"subscriber_id": 107,
				"payload": {"zip_code": "33647", "vertical": "x", "lead_tier": "Gold"},
			})
	finally:
		_restore_spec("competitor_acted_on_lead", original)
	assert r["outcome"] == "routed"
	assert r["graph_name"] == "fomo"
	mock_runner.assert_called_once()
	kwargs = mock_runner.call_args.kwargs
	assert kwargs["subscriber_id"] == 107
	assert "decision_id" in kwargs


def test_graph_exception_records_failed_audit_row():
	mock_runner, original = _patch_spec_runner(
		"competitor_acted_on_lead",
		side_effect=RuntimeError("boom"),
	)
	try:
		with patch("src.agents.supervisor.log_decision") as mock_log:
			r = dispatch_event({
				"event_type": "competitor_acted_on_lead",
				"subscriber_id": 107,
				"payload": {"zip_code": "33647", "vertical": "x", "lead_tier": "Gold"},
			})
	finally:
		_restore_spec("competitor_acted_on_lead", original)
	assert r["outcome"] == "routed"
	assert "exception:RuntimeError" in r["reason"]
	final_call_kwargs = mock_log.call_args.kwargs
	assert final_call_kwargs["terminal_status"] == "failed"


def test_retention_adapter_translates_payload_to_tier():
	"""retention_summary_due routes through the adapter which pulls 'tier' out of payload."""
	with patch("src.agents.router._run_retention_inner",
			   return_value={"terminal_status": "completed"}) as mock_inner, \
		 patch("src.agents.supervisor.log_decision"):
		r = dispatch_event({
			"event_type": "retention_summary_due",
			"subscriber_id": 42,
			"payload": {"tier": "lock"},
		})
	assert r["outcome"] == "routed"
	assert r["graph_name"] == "retention"
	# The adapter itself reads the `tier` key, so we patched its inner target.
	mock_inner.assert_called_once()
	kwargs = mock_inner.call_args.kwargs
	assert kwargs["subscriber_id"] == 42
	assert kwargs["tier_cohort"] == "lock"


def test_wave2_without_decision_id_is_rejected():
	with patch("src.agents.supervisor.log_decision"):
		r = dispatch_event({
			"event_type": "abandonment_click_no_complete",
			"subscriber_id": 42,
			"payload": {"lead_tier_viewed": "Gold"},
			# deliberately no decision_id
		})
	assert r["outcome"] == "dropped_unknown_event"
	assert "wave2_missing_decision_id" in r["reason"]


def test_wave2_with_decision_id_routes_through():
	mock_runner, original = _patch_spec_runner(
		"abandonment_click_no_complete",
		return_value={"terminal_status": "completed"},
	)
	try:
		with patch("src.agents.supervisor.log_decision"):
			r = dispatch_event({
				"event_type": "abandonment_click_no_complete",
				"subscriber_id": 42,
				"payload": {"lead_tier_viewed": "Gold"},
				"decision_id": "shared-uuid",
			})
	finally:
		_restore_spec("abandonment_click_no_complete", original)
	assert r["outcome"] == "routed"
	mock_runner.assert_called_once()
	assert mock_runner.call_args.kwargs["decision_id"] == "shared-uuid"
