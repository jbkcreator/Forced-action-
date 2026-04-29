"""
Integration tests for the Abandonment Pressure Wave 1 + Wave 2 graphs.
"""

import uuid
from unittest.mock import patch

from src.agents.graphs.abandonment import run_wave1, run_wave2


FAKE_CLAUDE = {
	"text": "Amal, 3 Gold leads still open. 2 min left. [link]",
	"model": "haiku", "input_tokens": 70, "output_tokens": 20, "cost_usd": 0.00009,
}


def _happy_mocks():
	return [
		patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage",
			  return_value=FAKE_CLAUDE),
		patch("src.agents.subgraphs.compose_and_send.compliance_check",
			  return_value={"can_send": True, "reason": "ok"}),
		patch("src.agents.subgraphs.compose_and_send.send_sms",
			  return_value={"sent": True, "reason": "ok", "message_outcome_id": 2}),
		patch("src.agents.subgraphs.compose_and_send.log_decision"),
	]


def _start(patches):
	for p in patches:
		p.start()


def _stop(patches):
	for p in patches:
		p.stop()


def test_wave1_happy_path_sends_and_schedules_wave2():
	patches = _happy_mocks()
	_start(patches)
	try:
		r = run_wave1(
			event_payload={"zip_code": "33647", "vertical": "public_adjusters",
						   "minutes_elapsed": 12, "wall_countdown_minutes": 3},
			subscriber_id=107,
		)
	finally:
		_stop(patches)
	assert r["terminal_status"] == "completed"
	assert r["sent"] is True
	assert r["wave2_scheduled_at"] is not None


def test_wave1_missing_subscriber_aborts():
	with patch("src.agents.graphs.abandonment.get_subscriber_profile", return_value={}), \
		 patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
		r = run_wave1(event_payload={}, subscriber_id=999999)
	assert r["terminal_status"] == "aborted"
	mock_cc.assert_not_called()


def test_wave2_early_exit_when_user_already_converted():
	converted_profile = {
		"id": 107, "tier": "starter", "status": "active",
		"vertical": "public_adjusters", "has_saved_card": True,
	}
	with patch("src.agents.graphs.abandonment.get_subscriber_profile",
			   return_value=converted_profile), \
		 patch("src.agents.graphs.abandonment.get_wallet_state",
			   return_value={"enrolled": False, "credits_remaining": 0}), \
		 patch("src.agents.graphs.abandonment.get_zip_activity",
			   return_value={"active_viewers": 0}), \
		 patch("src.agents.graphs.abandonment.get_recent_messages",
			   return_value=[]), \
		 patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
		r = run_wave2(
			event_payload={"lead_tier_viewed": "Gold"},
			subscriber_id=107,
			decision_id=str(uuid.uuid4()),
		)
	assert r["terminal_status"] == "completed"
	assert r["wave1_already_converted"] is True
	mock_cc.assert_not_called()


def test_wave2_sends_when_user_has_not_converted():
	profile = {
		"id": 107, "tier": "free", "status": "active",
		"vertical": "public_adjusters", "has_saved_card": False,
	}
	patches = _happy_mocks() + [
		patch("src.agents.graphs.abandonment.get_subscriber_profile", return_value=profile),
		patch("src.agents.graphs.abandonment.get_wallet_state",
			  return_value={"enrolled": False, "credits_remaining": 0}),
		patch("src.agents.graphs.abandonment.get_zip_activity",
			  return_value={"active_viewers": 2}),
		patch("src.agents.graphs.abandonment.get_recent_messages", return_value=[]),
	]
	_start(patches)
	try:
		r = run_wave2(
			event_payload={"lead_tier_viewed": "Gold", "wall_countdown_minutes": 1},
			subscriber_id=107,
			decision_id=str(uuid.uuid4()),
		)
	finally:
		_stop(patches)
	assert r["terminal_status"] == "completed"
	assert r["sent"] is True
	assert r.get("wave1_already_converted") is False
