"""
Integration tests for the Retention Summaries graph.
"""

from unittest.mock import patch

from src.agents.graphs.retention import run_retention, run_retention_batch


FAKE_CLAUDE = {
	"text": "Amal, 30 credits this week — 4 Gold leads you missed in 33647. Reply YEARLY. [link]",
	"model": "sonnet", "input_tokens": 150, "output_tokens": 55, "cost_usd": 0.0012,
}


def _happy_mocks():
	return [
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
