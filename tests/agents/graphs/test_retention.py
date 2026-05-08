"""
Integration tests for the Retention Summaries graph.
"""

from unittest.mock import patch

from src.agents.graphs.retention import run_retention, run_retention_batch


FAKE_CLAUDE = {
	"text": "Amal, 30 credits this week — 4 Gold leads you missed in 33647. Reply YEARLY. [link]",
	"model": "sonnet", "input_tokens": 150, "output_tokens": 55, "cost_usd": 0.0012,
}


_FAKE_PROFILE = {
	"id": 107, "tier": "wallet", "status": "active", "vertical": "roofing",
	"county_id": "hillsborough", "founding_member": False,
	"email": "test@example.com", "name": "Amal Test",
	"has_saved_card": True, "auto_mode_enabled": False,
	"referral_code": None, "created_at": None, "billing_date": None,
}


def _happy_mocks():
	return [
		patch("src.agents.graphs.retention.get_subscriber_profile",
			  return_value=_FAKE_PROFILE),
		patch("src.agents.graphs.retention.get_wallet_state",
			  return_value={"credits_used_total": 30, "credits_remaining": 5}),
		patch("src.agents.graphs.retention.get_deal_history",
			  return_value=[{"lead_source": "zip:33647", "deal_amount": 12000}]),
		patch("src.agents.graphs.retention.get_subscriber_territories",
			  return_value=["33647"]),
		patch("src.agents.graphs.retention.get_lead_pool",
			  return_value=[{"tier": "Gold"}, {"tier": "Gold"}]),
		patch("src.agents.graphs.retention.get_zip_activity",
			  return_value={"active_viewers": 3}),
		patch("src.agents.subgraphs.decision_hierarchy.kill_switch_status",
			  return_value={"color": "green", "observed_value": None, "action": "proceed"}),
		patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage",
			  return_value=FAKE_CLAUDE),
		patch("src.agents.subgraphs.compose_and_send.compliance_check",
			  return_value={"can_send": True, "reason": "ok"}),
		patch("src.agents.subgraphs.compose_and_send.send_sms",
			  return_value={"sent": True, "reason": "ok", "message_outcome_id": 3}),
		patch("src.agents.subgraphs.compose_and_send.log_decision"),
	]


def _start(patches):
	for p in patches:
		p.start()


def _stop(patches):
	for p in patches:
		p.stop()


def test_retention_happy_path_sends():
	patches = _happy_mocks()
	_start(patches)
	try:
		r = run_retention(subscriber_id=107, tier_cohort="wallet")
	finally:
		_stop(patches)
	assert r["terminal_status"] == "completed"
	assert r["sent"] is True


def test_retention_skips_non_active_subscriber():
	with patch("src.agents.graphs.retention.get_subscriber_profile",
			   return_value={"id": 107, "status": "churned", "tier": "free"}), \
		 patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage") as mock_cc:
		r = run_retention(subscriber_id=107, tier_cohort="wallet")
	assert r["terminal_status"] == "aborted"
	assert "status_churned" in r["failure_reason"]
	mock_cc.assert_not_called()


def test_retention_batch_runs_all_subscribers():
	patches = _happy_mocks()
	_start(patches)
	try:
		results = run_retention_batch([107, 107, 107], tier_cohort="lock")
	finally:
		_stop(patches)
	assert len(results) == 3
	assert all(r["terminal_status"] == "completed" for r in results)
