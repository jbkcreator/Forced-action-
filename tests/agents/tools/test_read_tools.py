"""
Tests for the 12 Cora read tools.

These tests verify that each tool:
  - Is registered with category='read'
  - Returns a well-formed shape (dict / list of dicts)
  - Handles the 'no data' case without raising
  - Is tagged idempotent=True

Where a tool wraps a pure-config function (get_guardrail) we assert on the
real return value. Where a tool needs a DB, we use a mocked Session that
returns None/empty results — enough to exercise the branching logic.
"""

from unittest.mock import MagicMock, patch  # noqa: F401  (patch used inline)

import pytest

from src.agents.tools import read_tools
from src.agents.tools.registry import TOOL_REGISTRY


READ_TOOL_NAMES = [
	"get_subscriber_profile",
	"get_segment_and_score",
	"get_wallet_state",
	"get_zip_activity",
	"get_lead_pool",
	"get_competition_status",
	"get_recent_messages",
	"get_learning_card",
	"get_guardrail",
	"get_ab_variant",
	"get_deal_history",
	"check_opt_in",
]


# ──────────────────────────────────────────────────────────────────────────────
# Registration sanity checks
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("name", READ_TOOL_NAMES)
def test_read_tool_is_registered(name):
	assert name in TOOL_REGISTRY, f"{name} must be registered"
	spec = TOOL_REGISTRY[name]
	assert spec.category == "read"
	assert spec.idempotent is True
	assert spec.requires_compliance is False


def test_all_twelve_read_tools_exist():
	registered_read = [
		s.name for s in TOOL_REGISTRY.values() if s.category == "read"
	]
	for name in READ_TOOL_NAMES:
		assert name in registered_read


# ──────────────────────────────────────────────────────────────────────────────
# Pure-config tool — exercise real return
# ──────────────────────────────────────────────────────────────────────────────

def test_get_guardrail_returns_known_bound():
	result = read_tools.get_guardrail("lock_pricing")
	assert isinstance(result, dict)
	assert "min_cents" in result
	assert "max_cents" in result
	assert result["min_cents"] < result["max_cents"]


def test_get_guardrail_returns_empty_for_unknown():
	assert read_tools.get_guardrail("this_guardrail_does_not_exist") == {}


# ──────────────────────────────────────────────────────────────────────────────
# DB-backed tools — mocked session, no-data branch
# ──────────────────────────────────────────────────────────────────────────────

def _mock_session_returning_none():
	"""A Session stub where every filtered query returns nothing."""
	sess = MagicMock()
	q = MagicMock()
	q.filter.return_value = q
	q.order_by.return_value = q
	q.limit.return_value = q
	q.join.return_value = q
	q.first.return_value = None
	q.all.return_value = []
	q.scalar.return_value = 0
	sess.query.return_value = q
	return sess


def test_get_subscriber_profile_empty_for_missing():
	sess = _mock_session_returning_none()
	result = read_tools.get_subscriber_profile(999, session=sess)
	assert result == {}


def test_get_segment_and_score_defaults_for_missing():
	sess = _mock_session_returning_none()
	result = read_tools.get_segment_and_score(999, session=sess)
	assert result["segment"] == "new"
	assert result["revenue_signal_score"] == 0
	assert result["classified_at"] is None


def test_get_wallet_state_defaults_for_unenrolled():
	sess = _mock_session_returning_none()
	result = read_tools.get_wallet_state(999, session=sess)
	assert result["enrolled"] is False
	assert result["credits_remaining"] == 0
	assert result["tier"] is None


def test_get_recent_messages_returns_list_for_empty():
	sess = _mock_session_returning_none()
	result = read_tools.get_recent_messages(999, session=sess)
	assert result == []


def test_get_deal_history_returns_list_for_empty():
	sess = _mock_session_returning_none()
	result = read_tools.get_deal_history(999, session=sess)
	assert result == []


def test_get_learning_card_empty_when_none_exists():
	sess = _mock_session_returning_none()
	result = read_tools.get_learning_card("general", session=sess)
	assert result == {}


def test_get_ab_variant_empty_when_unassigned():
	sess = _mock_session_returning_none()
	result = read_tools.get_ab_variant(999, "some_test", session=sess)
	assert result == {}


def test_get_lead_pool_returns_list_for_empty():
	sess = _mock_session_returning_none()
	result = read_tools.get_lead_pool("33647", vertical="roofing", session=sess)
	assert result == []


def test_get_competition_status_shape():
	sess = _mock_session_returning_none()
	result = read_tools.get_competition_status("33647", vertical="roofing", session=sess)
	assert result["zip"] == "33647"
	assert result["is_locked"] is False
	assert result["lock_holder_subscriber_id"] is None
	assert result["active_wallet_users_in_vertical"] == 0


def test_get_zip_activity_reads_urgency_and_messages():
	"""Urgency count comes from urgency_engine; DB mocked to 0 recent messages."""
	sess = _mock_session_returning_none()
	with patch("src.services.urgency_engine.get_active_count", return_value=3):
		result = read_tools.get_zip_activity("33647", vertical="roofing", session=sess)
	assert result["zip"] == "33647"
	assert result["active_viewers"] == 3
	assert result["messages_last_24h"] == 0


def test_check_opt_in_empty_number():
	sess = _mock_session_returning_none()
	result = read_tools.check_opt_in("+15555550000", session=sess)
	assert result["phone"] == "+15555550000"
	assert result["has_opt_in"] is False
	assert result["has_opt_out"] is False
	assert result["can_send_marketing"] is False
