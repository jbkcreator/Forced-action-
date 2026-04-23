"""
Tests for the 2 write tools: send_sms and log_decision.

send_sms tests exercise:
  - Registration as write + idempotent + requires_compliance
  - Missing opt-in short-circuits with reason='no_phone'
  - Duplicate (subscriber, campaign, variant) within 24h returns reason='duplicate'
  - Compliance failure returns reason='opted_out_or_twilio_error'
  - Happy path writes a MessageOutcome row

log_decision tests exercise:
  - Registration as write + idempotent (no compliance)
  - Upsert: second call with same decision_id merges into existing row
  - terminal_status validation
  - Counters take max across updates
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.agents.tools import write_tools
from src.agents.tools.registry import TOOL_REGISTRY


# ──────────────────────────────────────────────────────────────────────────────
# Registration
# ──────────────────────────────────────────────────────────────────────────────

def test_send_sms_registered_as_write_compliance_idempotent():
	spec = TOOL_REGISTRY["send_sms"]
	assert spec.category == "write"
	assert spec.idempotent is True
	assert spec.requires_compliance is True


def test_log_decision_registered_as_write_idempotent_no_compliance():
	spec = TOOL_REGISTRY["log_decision"]
	assert spec.category == "write"
	assert spec.idempotent is True
	assert spec.requires_compliance is False


# ──────────────────────────────────────────────────────────────────────────────
# send_sms
# ──────────────────────────────────────────────────────────────────────────────

def _mock_query_returning(*, opt_in=None, duplicate=None):
	"""Session mock where the first .first() returns opt_in, second returns duplicate."""
	sess = MagicMock()
	opt_in_q = MagicMock()
	opt_in_q.filter.return_value = opt_in_q
	opt_in_q.order_by.return_value = opt_in_q
	opt_in_q.first.return_value = opt_in

	dup_q = MagicMock()
	dup_q.filter.return_value = dup_q
	dup_q.first.return_value = duplicate

	# First .query() returns opt-in query, second returns duplicate-check query.
	sess.query.side_effect = [opt_in_q, dup_q, MagicMock()]
	return sess


def test_send_sms_returns_no_phone_when_subscriber_has_no_optin():
	sess = _mock_query_returning(opt_in=None)
	result = write_tools.send_sms(
		subscriber_id=42,
		body="test",
		campaign="smoke",
		variant_id="a",
		session=sess,
	)
	assert result["sent"] is False
	assert result["reason"] == "no_phone"


def test_send_sms_returns_duplicate_when_recent_send_exists():
	opt_in = MagicMock(phone="+15555550000")
	duplicate = MagicMock(id=17)
	sess = _mock_query_returning(opt_in=opt_in, duplicate=duplicate)

	result = write_tools.send_sms(
		subscriber_id=42,
		body="test",
		campaign="smoke",
		variant_id="a",
		session=sess,
	)
	assert result["sent"] is False
	assert result["reason"] == "duplicate"
	assert result["message_outcome_id"] == 17


def test_send_sms_returns_block_when_compliance_fails():
	opt_in = MagicMock(phone="+15555550000")
	sess = _mock_query_returning(opt_in=opt_in, duplicate=None)

	with patch("src.services.sms_compliance.send_sms", return_value=False):
		result = write_tools.send_sms(
			subscriber_id=42,
			body="test",
			campaign="smoke",
			variant_id="a",
			session=sess,
		)
	assert result["sent"] is False
	assert result["reason"] == "opted_out_or_twilio_error"


def test_send_sms_happy_path_writes_message_outcome():
	opt_in = MagicMock(phone="+15555550000")
	sess = _mock_query_returning(opt_in=opt_in, duplicate=None)

	with patch("src.services.sms_compliance.send_sms", return_value=True):
		result = write_tools.send_sms(
			subscriber_id=42,
			body="test",
			campaign="smoke",
			variant_id="a",
			session=sess,
		)
	assert result["sent"] is True
	assert result["reason"] == "ok"
	# sess.add() was called for the MessageOutcome row
	assert sess.add.called


# ──────────────────────────────────────────────────────────────────────────────
# log_decision
# ──────────────────────────────────────────────────────────────────────────────

def test_log_decision_validates_terminal_status():
	with pytest.raises(ValueError, match="terminal_status must be one of"):
		write_tools.log_decision(
			decision_id=str(uuid.uuid4()),
			graph_name="x",
			terminal_status="bogus",
		)


def test_log_decision_upsert_idempotent_against_real_db():
	"""Runs against the real DB — exercises the genuine upsert path."""
	decision_id = str(uuid.uuid4())

	# 1st call — insert
	r1 = write_tools.log_decision(
		decision_id=decision_id,
		graph_name="test_graph",
		subscriber_id=None,
		event_type="unit_test",
	)
	assert r1["decision_id"] == decision_id
	assert r1["terminal_status"] is None
	assert r1["tokens_used"] == 0

	# 2nd call — finalize
	r2 = write_tools.log_decision(
		decision_id=decision_id,
		graph_name="test_graph",
		terminal_status="completed",
		tokens_used=500,
		cost_usd=0.002,
		summary={"note": "unit test"},
	)
	assert r2["decision_id"] == decision_id
	assert r2["terminal_status"] == "completed"
	assert r2["tokens_used"] == 500
	assert r2["completed_at"] is not None

	# 3rd call with smaller counters — must not regress
	r3 = write_tools.log_decision(
		decision_id=decision_id,
		graph_name="test_graph",
		tokens_used=1,
		cost_usd=0.0001,
	)
	assert r3["tokens_used"] == 500  # took max, did not regress
	assert float(r3["cost_usd"]) == pytest.approx(0.002)
