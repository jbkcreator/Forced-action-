"""
Tests for the decision_hierarchy_check shared subgraph.

Exercises the 6-step gate across happy paths, guardrail blocks, kill-switch
colours, and fail-safe behaviour on unknown features. Each test invokes
run_decision_hierarchy() end-to-end so the full graph topology is covered.
"""

from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy


# ──────────────────────────────────────────────────────────────────────────────
# Happy paths
# ──────────────────────────────────────────────────────────────────────────────

def test_passthrough_no_constraints_is_allowed():
	r = run_decision_hierarchy({"subscriber_id": 1, "graph_name": "test"})
	assert r["action_allowed"] is True
	assert r["use_fallback"] is False
	assert r["kill_switch_color"] == "green"


def test_guardrail_in_range_allows():
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"decision_type": "lock_pricing",
		"proposed_value": 19700,   # $197, inside 14700–24700
	})
	assert r["action_allowed"] is True
	assert "guardrail:pass" in r["hierarchy_path"]


# ──────────────────────────────────────────────────────────────────────────────
# Guardrail blocks
# ──────────────────────────────────────────────────────────────────────────────

def test_guardrail_over_max_blocks():
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"decision_type": "lock_pricing",
		"proposed_value": 30000,
	})
	assert r["action_allowed"] is False
	assert r["action_blocked_reason"].startswith("guardrail:")


def test_unknown_guardrail_fails_safe():
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"decision_type": "bogus_guardrail",
		"proposed_value": 100,
	})
	assert r["action_allowed"] is False
	assert "unknown_guardrail" in r["action_blocked_reason"]


def test_guardrail_missing_value_when_type_provided_blocks():
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"decision_type": "lock_pricing",
		# deliberately no proposed_value
	})
	assert r["action_allowed"] is False
	assert "proposed_value" in r["action_blocked_reason"]


# ──────────────────────────────────────────────────────────────────────────────
# Kill-switch
# ──────────────────────────────────────────────────────────────────────────────

def test_kill_switch_red_blocks():
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"kill_switch_feature": "first_payment_rate",
		"kill_switch_observed_value": 10,   # red
	})
	assert r["action_allowed"] is False
	assert r["kill_switch_color"] == "red"


def test_kill_switch_yellow_allows_with_fallback():
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"kill_switch_feature": "first_payment_rate",
		"kill_switch_observed_value": 25,   # yellow
	})
	assert r["action_allowed"] is True
	assert r["use_fallback"] is True
	assert r["kill_switch_color"] == "yellow"


def test_kill_switch_green_allows_without_fallback():
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"kill_switch_feature": "first_payment_rate",
		"kill_switch_observed_value": 35,
	})
	assert r["action_allowed"] is True
	assert r["use_fallback"] is False


def test_kill_switch_unknown_feature_fails_safe():
	"""Architecture doc rule: unknown colour is treated as RED (blocks)."""
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"kill_switch_feature": "does_not_exist",
	})
	assert r["action_allowed"] is False
	assert r["kill_switch_color"] == "unknown"


# ──────────────────────────────────────────────────────────────────────────────
# Hierarchy audit
# ──────────────────────────────────────────────────────────────────────────────

def test_hierarchy_path_short_circuits_on_guardrail_block():
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"decision_type": "lock_pricing",
		"proposed_value": 30000,
	})
	# Guardrail blocked at step 1 — no later steps should have fired.
	assert r["hierarchy_path"][0].startswith("guardrail:block")
	assert len(r["hierarchy_path"]) == 1


def test_hierarchy_path_records_all_steps_on_happy_path():
	r = run_decision_hierarchy({
		"subscriber_id": 1,
		"graph_name": "test",
		"decision_type": "lock_pricing",
		"proposed_value": 19700,
		"kill_switch_feature": "first_payment_rate",
		"kill_switch_observed_value": 35,
	})
	path_prefixes = [p.split(":")[0] for p in r["hierarchy_path"]]
	for step in ("guardrail", "learning_card", "segment", "ab", "kill_switch"):
		assert step in path_prefixes
