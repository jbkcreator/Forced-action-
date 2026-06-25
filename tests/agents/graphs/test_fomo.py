"""
Integration tests for the FOMO graph.

Claude, compliance, and send_sms are mocked at the compose_and_send module
boundary so tests are deterministic and cost $0. log_decision is also
mocked so tests don't write audit rows during the suite.
"""

import uuid
from unittest.mock import patch

import pytest

from src.agents.graphs.fomo import run_fomo


FAKE_CLAUDE = {
	"text": "Amal, competitor just contacted a Gold lead in 33647. [link]",
	"model": "haiku",
	"input_tokens": 100,
	"output_tokens": 30,
	"cost_usd": 0.0001,
}


def _payload(**overrides):
	p = {
		"competitor_event_id": str(uuid.uuid4()),
		"lead_id": 100,
		"zip_code": "33647",
		"vertical": "public_adjusters",
		"competitor_subscriber_id": 42,
		"lead_tier": "Gold",
	}
	p.update(overrides)
	return p


_FAKE_SEGMENT = {
	"segment": "high_intent",
	"revenue_signal_score": 80,
	"revenue_signal_band": "very_high",
	"last_significant_action_at": None,
	"revenue_signal_last_action": None,
	"classified_at": None,
	"reason": "test",
}


_FAKE_PROFILE = {
	"id": 107,
	"name": "Amal Test",
	"tier": "gold",
	"vertical": "roofing",
	"status": "active",
	"county_id": "hillsborough",
}

_FAKE_ZIP_ACTIVITY = {"active_viewers": 3}
_FAKE_COMPETITION = {"is_locked": False, "active_wallet_users_in_vertical": 1}


_FAKE_HIERARCHY = {
	"action_allowed": True,
	"use_fallback": False,
	"kill_switch_color": "green",
	"revenue_signal_score": 80,
}


def _with_happy_mocks():
	return [
		patch("src.agents.graphs.fomo.get_subscriber_profile", return_value=_FAKE_PROFILE),
		patch("src.agents.graphs.fomo.get_zip_activity", return_value=_FAKE_ZIP_ACTIVITY),
		patch("src.agents.graphs.fomo.get_competition_status", return_value=_FAKE_COMPETITION),
		patch("src.agents.graphs.fomo.get_segment_and_score", return_value=_FAKE_SEGMENT),
		patch("src.agents.graphs.fomo.run_decision_hierarchy", return_value=_FAKE_HIERARCHY),
		patch("src.agents.graphs.fomo.get_cached_metric", return_value=None),
		patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage",
			  return_value=FAKE_CLAUDE),
		patch("src.agents.subgraphs.compose_and_send.compliance_check",
			  return_value={"can_send": True, "reason": "ok"}),
		patch("src.agents.subgraphs.compose_and_send.send_sms",
			  return_value={"sent": True, "reason": "ok", "message_outcome_id": 1}),
		patch("src.agents.subgraphs.compose_and_send.log_decision"),
	]


def test_happy_path_composes_and_sends():
	patches = _with_happy_mocks()
	for p in patches:
		p.start()
	try:
		r = run_fomo(event_payload=_payload(), subscriber_id=107)
	finally:
		for p in patches:
			p.stop()

	assert r["terminal_status"] == "completed"
	assert r["sent"] is True
	assert r["message_body"]
	assert r["tokens_used"] > 0


def test_subscriber_not_found_aborts_without_compose():
	with patch("src.agents.graphs.fomo.get_subscriber_profile", return_value={}), \
		 patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
		r = run_fomo(event_payload=_payload(), subscriber_id=999999)
	assert r["terminal_status"] == "aborted"
	assert "subscriber_not_found" in r["failure_reason"]
	mock_cc.assert_not_called()


def test_subscriber_not_found_logs_review_capture_and_publishes_feedback_candidate():
	with patch("src.agents.graphs.fomo.get_subscriber_profile", return_value={}), \
		 patch("src.agents.graphs.fomo.publish_feedback_ritual_candidate") as mock_publish, \
		 patch("src.agents.graphs.fomo.log_decision") as mock_log:
		r = run_fomo(
			event_payload=_payload(zip_code="33647", vertical="roofing"),
			subscriber_id=999999,
			decision_id="decision-fomo-999999",
		)
	assert r["terminal_status"] == "aborted"
	assert r["failure_reason"] == "fomo:subscriber_not_found"
	mock_publish.assert_called_once_with(
		decision_id="decision-fomo-999999",
		graph_name="fomo",
		terminal_status="aborted",
	)
	summary = mock_log.call_args.kwargs["summary"]
	assert summary["failure_reason"] == "fomo:subscriber_not_found"
	assert summary["early_abort"] is True
	assert summary["review_capture"]["raw_input_text"] == (
		"competitor_acted_on_lead zip_code=33647 vertical=roofing lead_tier=Gold"
	)
	assert summary["review_capture"]["generated_output_text"] == ""
	assert summary["review_capture"]["review_flag"] is True
	assert summary["review_capture"]["review_flag_reason"] == "fomo:subscriber_not_found"


def test_zip_already_locked_aborts():
	with patch("src.agents.graphs.fomo.get_subscriber_profile",
			   return_value={"id": 107, "tier": "starter", "vertical": "roofing", "status": "active"}), \
		 patch("src.agents.graphs.fomo.get_zip_activity", return_value={"active_viewers": 0}), \
		 patch("src.agents.graphs.fomo.get_competition_status",
			   return_value={"is_locked": True, "lock_holder_subscriber_id": 99}), \
		 patch("src.agents.graphs.fomo.get_segment_and_score", return_value=_FAKE_SEGMENT), \
		 patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
		r = run_fomo(event_payload=_payload(), subscriber_id=107)
	assert r["terminal_status"] == "aborted"
	assert "zip_already_locked" in r["failure_reason"]
	mock_cc.assert_not_called()


def test_compliance_block_is_aborted_not_failed():
	with patch("src.agents.graphs.fomo.get_segment_and_score", return_value=_FAKE_SEGMENT), \
		 patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage",
			   return_value=FAKE_CLAUDE), \
		 patch("src.agents.subgraphs.compose_and_send.compliance_check",
			   return_value={"can_send": False, "reason": "opted_out"}), \
		 patch("src.agents.subgraphs.compose_and_send.send_sms") as mock_send, \
		 patch("src.agents.subgraphs.compose_and_send.log_decision"):
		r = run_fomo(event_payload=_payload(), subscriber_id=107)
	assert r["terminal_status"] == "aborted"
	mock_send.assert_not_called()
