"""
Tests for the compose_and_send_compliant_sms shared subgraph.

Claude calls are mocked deterministically so tests are fast and reproducible.
send_sms and log_decision are patched at the module level to isolate the
subgraph routing / state-flow logic from downstream service behaviour.
"""

import uuid
from unittest.mock import patch

from src.agents.subgraphs.compose_and_send import run_compose_and_send


def _fake_claude(text="Hi there", tokens=(50, 20), cost=0.0001):
	"""Return a deterministic call_claude_with_usage mock."""
	return {
		"text": text,
		"model": "haiku",
		"input_tokens": tokens[0],
		"output_tokens": tokens[1],
		"cost_usd": cost,
	}


# ──────────────────────────────────────────────────────────────────────────────
# Budget precheck
# ──────────────────────────────────────────────────────────────────────────────

def test_budget_exceeded_aborts_before_compose():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage"
	) as mock_cc, patch(
		"src.agents.subgraphs.compose_and_send.send_sms"
	) as mock_send, patch(
		"src.agents.subgraphs.compose_and_send.log_decision"
	) as mock_log:
		r = run_compose_and_send({
			"decision_id": str(uuid.uuid4()),
			"graph_name": "t",
			"subscriber_id": 1,
			"campaign": "c",
			"claude_task_type": "sms_copy",
			"system_prompt": "", "user_prompt": "",
			"tokens_used": 10_000,  # over cap
		})
		assert r["terminal_status"] == "aborted"
		assert "budget" in r["failure_reason"]
		mock_cc.assert_not_called()
		mock_send.assert_not_called()
		# Audit row still written
		mock_log.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────────
# Compose step
# ──────────────────────────────────────────────────────────────────────────────

def test_compose_fallback_skips_claude():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage"
	) as mock_cc, patch(
		"src.agents.subgraphs.compose_and_send.compliance_check",
		return_value={"can_send": True, "reason": "ok"},
	), patch(
		"src.agents.subgraphs.compose_and_send.send_sms",
		return_value={"sent": True, "reason": "ok", "message_outcome_id": 1},
	), patch(
		"src.agents.subgraphs.compose_and_send.log_decision"
	):
		r = run_compose_and_send({
			"decision_id": str(uuid.uuid4()),
			"graph_name": "t",
			"subscriber_id": 1,
			"campaign": "c",
			"system_prompt": "", "user_prompt": "",
			"use_fallback": True,
			"ab_fallback_body": "Static body",
		})
		mock_cc.assert_not_called()
		assert r["message_body"] == "Static body"
		assert r["tokens_used"] == 0


def test_compose_uses_claude_when_no_fallback():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=_fake_claude("Dynamic body"),
	), patch(
		"src.agents.subgraphs.compose_and_send.compliance_check",
		return_value={"can_send": True, "reason": "ok"},
	), patch(
		"src.agents.subgraphs.compose_and_send.send_sms",
		return_value={"sent": True, "reason": "ok", "message_outcome_id": 1},
	), patch(
		"src.agents.subgraphs.compose_and_send.log_decision"
	):
		r = run_compose_and_send({
			"decision_id": str(uuid.uuid4()),
			"graph_name": "t",
			"subscriber_id": 1,
			"campaign": "c",
			"claude_task_type": "sms_copy",
			"system_prompt": "S", "user_prompt": "U",
		})
		assert r["message_body"] == "Dynamic body"
		assert r["tokens_used"] == 70       # 50 + 20
		assert r["cost_usd"] > 0
		assert r["terminal_status"] == "completed"


def test_compose_claude_error_fails_gracefully():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		side_effect=RuntimeError("api down"),
	), patch(
		"src.agents.subgraphs.compose_and_send.log_decision"
	):
		r = run_compose_and_send({
			"decision_id": str(uuid.uuid4()),
			"graph_name": "t",
			"subscriber_id": 1,
			"campaign": "c",
			"claude_task_type": "sms_copy",
			"system_prompt": "", "user_prompt": "",
		})
		assert r["terminal_status"] == "failed"
		assert "compose" in r["failure_reason"]


# ──────────────────────────────────────────────────────────────────────────────
# Compliance gate
# ──────────────────────────────────────────────────────────────────────────────

def test_compliance_block_aborts_before_send():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=_fake_claude(),
	), patch(
		"src.agents.subgraphs.compose_and_send.compliance_check",
		return_value={"can_send": False, "reason": "opted_out"},
	), patch(
		"src.agents.subgraphs.compose_and_send.send_sms"
	) as mock_send, patch(
		"src.agents.subgraphs.compose_and_send.log_decision"
	):
		r = run_compose_and_send({
			"decision_id": str(uuid.uuid4()),
			"graph_name": "t",
			"subscriber_id": 1,
			"campaign": "c",
			"claude_task_type": "sms_copy",
			"system_prompt": "", "user_prompt": "",
		})
		assert r["terminal_status"] == "aborted"
		assert r["compliance_allowed"] is False
		assert r["compliance_reason"] == "opted_out"
		mock_send.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# Send + log
# ──────────────────────────────────────────────────────────────────────────────

def test_happy_path_sends_and_completes():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=_fake_claude("Body"),
	), patch(
		"src.agents.subgraphs.compose_and_send.compliance_check",
		return_value={"can_send": True, "reason": "ok"},
	), patch(
		"src.agents.subgraphs.compose_and_send.send_sms",
		return_value={"sent": True, "reason": "ok", "message_outcome_id": 42},
	), patch(
		"src.agents.subgraphs.compose_and_send.log_decision"
	) as mock_log:
		r = run_compose_and_send({
			"decision_id": "test-id-xyz",
			"graph_name": "t",
			"subscriber_id": 1,
			"campaign": "c",
			"claude_task_type": "sms_copy",
			"system_prompt": "", "user_prompt": "U",
		})
		assert r["sent"] is True
		assert r["message_outcome_id"] == 42
		assert r["terminal_status"] == "completed"
		# log_decision called once with final state (1 call at send_and_log)
		assert mock_log.call_count == 1
		final_kwargs = mock_log.call_args.kwargs
		assert final_kwargs["terminal_status"] == "completed"


def test_duplicate_send_still_completes():
	"""send_sms idempotency returns reason='duplicate' — treat as completed, not failed."""
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=_fake_claude(),
	), patch(
		"src.agents.subgraphs.compose_and_send.compliance_check",
		return_value={"can_send": True, "reason": "ok"},
	), patch(
		"src.agents.subgraphs.compose_and_send.send_sms",
		return_value={"sent": False, "reason": "duplicate", "message_outcome_id": 7},
	), patch(
		"src.agents.subgraphs.compose_and_send.log_decision"
	):
		r = run_compose_and_send({
			"decision_id": str(uuid.uuid4()),
			"graph_name": "t",
			"subscriber_id": 1,
			"campaign": "c",
			"claude_task_type": "sms_copy",
			"system_prompt": "", "user_prompt": "U",
		})
		assert r["terminal_status"] == "completed"
		assert r["send_reason"] == "duplicate"


def test_send_error_status_is_aborted():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=_fake_claude(),
	), patch(
		"src.agents.subgraphs.compose_and_send.compliance_check",
		return_value={"can_send": True, "reason": "ok"},
	), patch(
		"src.agents.subgraphs.compose_and_send.send_sms",
		return_value={"sent": False, "reason": "opted_out_or_twilio_error"},
	), patch(
		"src.agents.subgraphs.compose_and_send.log_decision"
	):
		r = run_compose_and_send({
			"decision_id": str(uuid.uuid4()),
			"graph_name": "t",
			"subscriber_id": 1,
			"campaign": "c",
			"claude_task_type": "sms_copy",
			"system_prompt": "", "user_prompt": "U",
		})
		assert r["terminal_status"] == "aborted"


def test_empty_body_is_failed():
	with patch(
		"src.agents.subgraphs.compose_and_send.call_claude_with_usage",
		return_value=_fake_claude(text="   "),    # whitespace only
	), patch(
		"src.agents.subgraphs.compose_and_send.compliance_check",
		return_value={"can_send": True, "reason": "ok"},
	), patch(
		"src.agents.subgraphs.compose_and_send.send_sms"
	) as mock_send, patch(
		"src.agents.subgraphs.compose_and_send.log_decision"
	):
		r = run_compose_and_send({
			"decision_id": str(uuid.uuid4()),
			"graph_name": "t",
			"subscriber_id": 1,
			"campaign": "c",
			"claude_task_type": "sms_copy",
			"system_prompt": "", "user_prompt": "U",
		})
		assert r["terminal_status"] == "failed"
		assert r["send_reason"] == "empty_message_body"
		mock_send.assert_not_called()
