"""
Tests for the 5 gating tools.

Covers registration sanity, happy paths, fail-safe behavior on unknown
inputs, and directionality of kill-switch metrics (higher-is-better vs
lower-is-better).
"""

from unittest.mock import MagicMock

import pytest

from src.agents.tools import gating_tools
from src.agents.tools.registry import TOOL_REGISTRY


GATING_NAMES = [
	"guardrail_check",
	"compliance_check",
	"kill_switch_status",
	"budget_check",
	"ab_variant_assign",
]


# ──────────────────────────────────────────────────────────────────────────────
# Registration
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("name", GATING_NAMES)
def test_gating_tool_registered(name):
	spec = TOOL_REGISTRY[name]
	assert spec.category == "gating"
	assert spec.idempotent is True
	assert spec.requires_compliance is False


def test_five_gating_tools_exist():
	gating = {s.name for s in TOOL_REGISTRY.values() if s.category == "gating"}
	for name in GATING_NAMES:
		assert name in gating


# ──────────────────────────────────────────────────────────────────────────────
# guardrail_check
# ──────────────────────────────────────────────────────────────────────────────

def test_guardrail_check_allows_value_in_range():
	result = gating_tools.guardrail_check("lock_pricing", 19700)  # $197
	assert result["allowed"] is True
	assert result["reason"] == "within_bounds"


def test_guardrail_check_rejects_over_max():
	result = gating_tools.guardrail_check("lock_pricing", 30000)  # $300
	assert result["allowed"] is False
	assert "bound" in result


def test_guardrail_check_rejects_under_min():
	result = gating_tools.guardrail_check("lock_pricing", 10000)  # $100
	assert result["allowed"] is False


def test_guardrail_check_unknown_fails_safe():
	result = gating_tools.guardrail_check("does_not_exist", 100)
	assert result["allowed"] is False
	assert result["reason"] == "unknown_guardrail"


# ──────────────────────────────────────────────────────────────────────────────
# kill_switch_status
# ──────────────────────────────────────────────────────────────────────────────

def test_kill_switch_higher_is_better_green():
	result = gating_tools.kill_switch_status("first_payment_rate", 35)
	assert result["color"] == "green"


def test_kill_switch_higher_is_better_yellow():
	result = gating_tools.kill_switch_status("first_payment_rate", 25)
	assert result["color"] == "yellow"


def test_kill_switch_higher_is_better_red():
	result = gating_tools.kill_switch_status("first_payment_rate", 15)
	assert result["color"] == "red"


def test_kill_switch_lower_is_better_green():
	result = gating_tools.kill_switch_status("cac_paid_channels", 20)
	assert result["color"] == "green"


def test_kill_switch_lower_is_better_yellow():
	result = gating_tools.kill_switch_status("cac_paid_channels", 30)
	assert result["color"] == "yellow"


def test_kill_switch_lower_is_better_red():
	result = gating_tools.kill_switch_status("cac_paid_channels", 50)
	assert result["color"] == "red"


def test_kill_switch_unknown_fails_safe():
	result = gating_tools.kill_switch_status("bogus")
	assert result["color"] == "unknown"
	assert result["reason"] == "unknown_feature"


def test_kill_switch_no_observed_value_unknown():
	result = gating_tools.kill_switch_status("first_payment_rate")
	assert result["color"] == "unknown"
	assert result["reason"] == "no_observed_value"


# ──────────────────────────────────────────────────────────────────────────────
# budget_check
# ──────────────────────────────────────────────────────────────────────────────

def test_budget_check_within_limits():
	result = gating_tools.budget_check(tokens_used=1000, cost_usd=0.05)
	assert result["allowed"] is True
	assert result["tokens_remaining"] > 0
	assert result["cost_remaining_usd"] > 0


def test_budget_check_tokens_exceeded():
	result = gating_tools.budget_check(tokens_used=10000, cost_usd=0.05)
	assert result["allowed"] is False
	assert result["reason"] == "token_budget_exceeded"


def test_budget_check_cost_exceeded():
	result = gating_tools.budget_check(tokens_used=100, cost_usd=1.0)
	assert result["allowed"] is False
	assert result["reason"] == "cost_budget_exceeded"


# ──────────────────────────────────────────────────────────────────────────────
# compliance_check
# ──────────────────────────────────────────────────────────────────────────────

def test_compliance_check_missing_subscriber():
	sess = MagicMock()
	q = MagicMock()
	q.filter.return_value = q
	q.order_by.return_value = q
	q.first.return_value = None
	sess.query.return_value = q

	result = gating_tools.compliance_check(999999, session=sess)
	assert result["can_send"] is False
	assert result["reason"] == "subscriber_not_found"


# ──────────────────────────────────────────────────────────────────────────────
# ab_variant_assign
# ──────────────────────────────────────────────────────────────────────────────

def test_ab_variant_returns_variant_when_assigned(monkeypatch):
	from src.services import ab_engine

	monkeypatch.setattr(ab_engine, "assign_variant", lambda sub, test, db: "a")
	result = gating_tools.ab_variant_assign(123, "test_x", session=MagicMock())
	assert result["variant"] == "a"
	assert result["assigned"] is True
	assert result["traffic_capped"] is False


def test_ab_variant_returns_none_when_traffic_capped(monkeypatch):
	from src.services import ab_engine

	monkeypatch.setattr(ab_engine, "assign_variant", lambda sub, test, db: None)
	result = gating_tools.ab_variant_assign(123, "test_x", session=MagicMock())
	assert result["variant"] is None
	assert result["assigned"] is False
	assert result["traffic_capped"] is True
