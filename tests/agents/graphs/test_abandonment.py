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

_FAKE_PROFILE = {
	"id": 107,
	"name": "Amal Test",
	"tier": "free",
	"status": "active",
	"vertical": "public_adjusters",
	"county_id": "hillsborough",
	"has_saved_card": False,
}

_FAKE_SEGMENT = {
	"segment": "high_intent",
	"revenue_signal_score": 75,
	"revenue_signal_band": "high",
	"last_significant_action_at": None,
	"revenue_signal_last_action": None,
	"classified_at": None,
	"reason": "test",
}

_FAKE_HIERARCHY = {
	"action_allowed": True,
	"use_fallback": False,
	"kill_switch_color": "green",
	"revenue_signal_score": 75,
}


def _happy_mocks():
	return [
		patch("src.agents.graphs.abandonment.get_subscriber_profile",
			  return_value=_FAKE_PROFILE),
		patch("src.agents.graphs.abandonment.get_wallet_state",
			  return_value={"enrolled": False, "credits_remaining": 0}),
		patch("src.agents.graphs.abandonment.get_zip_activity",
			  return_value={"active_viewers": 3}),
		patch("src.agents.graphs.abandonment.get_recent_messages", return_value=[]),
		patch("src.agents.graphs.abandonment.get_segment_and_score",
			  return_value=_FAKE_SEGMENT),
		patch("src.agents.graphs.abandonment.run_decision_hierarchy",
			  return_value=_FAKE_HIERARCHY),
		patch("src.agents.graphs.abandonment.get_cached_metric", return_value=None),
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
		"county_id": "hillsborough",
	}
	with patch("src.agents.graphs.abandonment.get_subscriber_profile",
			   return_value=converted_profile), \
		 patch("src.agents.graphs.abandonment.get_wallet_state",
			   return_value={"enrolled": False, "credits_remaining": 0}), \
		 patch("src.agents.graphs.abandonment.get_zip_activity",
			   return_value={"active_viewers": 0}), \
		 patch("src.agents.graphs.abandonment.get_recent_messages",
			   return_value=[]), \
		 patch("src.agents.graphs.abandonment.get_segment_and_score",
			   return_value=_FAKE_SEGMENT), \
		 patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
		r = run_wave2(
			event_payload={"lead_tier_viewed": "Gold"},
			subscriber_id=107,
			decision_id=str(uuid.uuid4()),
		)
	assert r["terminal_status"] == "completed"
	assert r["wave1_already_converted"] is True
	mock_cc.assert_not_called()


def test_wave2_early_exit_when_recent_message_stamped_unlock_conversion():
	"""D7: a hot-lead unlock between Wave 1 and Wave 2 stamps a
	message_outcomes row with conversion_type='unlock' (via
	record_nudge_conversion) — Wave 2 must read that stamp and self-skip,
	exactly like the has_saved_card path already covered above."""
	unconverted_profile = {**_FAKE_PROFILE, "has_saved_card": False}
	with patch("src.agents.graphs.abandonment.get_subscriber_profile",
			   return_value=unconverted_profile), \
		 patch("src.agents.graphs.abandonment.get_wallet_state",
			   return_value={"enrolled": False, "credits_remaining": 0}), \
		 patch("src.agents.graphs.abandonment.get_zip_activity",
			   return_value={"active_viewers": 0}), \
		 patch("src.agents.graphs.abandonment.get_recent_messages",
			   return_value=[{"conversion_type": "unlock"}]), \
		 patch("src.agents.graphs.abandonment.get_segment_and_score",
			   return_value=_FAKE_SEGMENT), \
		 patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
		r = run_wave2(
			event_payload={"lead_tier_viewed": "Gold"},
			subscriber_id=107,
			decision_id=str(uuid.uuid4()),
		)
	assert r["terminal_status"] == "completed"
	assert r["wave1_already_converted"] is True
	assert r["failure_reason"] == "wave2_skipped_user_already_converted"
	mock_cc.assert_not_called()


def test_wave2_sends_when_user_has_not_converted():
	patches = _happy_mocks() + [
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
