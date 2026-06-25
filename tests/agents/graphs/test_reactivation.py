from unittest.mock import patch

from src.agents.graphs.reactivation import run_reactivation


def test_reactivation_logs_review_capture_and_publishes_feedback_candidate():
    with patch(
        "src.agents.graphs.reactivation.get_subscriber_profile",
        return_value={"id": 42, "phone": None, "email": None, "name": "Test User"},
    ), patch(
        "src.agents.graphs.reactivation.publish_feedback_ritual_candidate"
    ) as mock_publish, patch(
        "src.agents.graphs.reactivation.log_decision"
    ) as mock_log:
        result = run_reactivation(
            event_payload={"cohort": "county_live", "county_id": "dallas"},
            subscriber_id=42,
            decision_id="decision-reactivation-42",
        )

    assert result["terminal_status"] == "aborted"
    assert result["failure_reason"] == "reactivation:no_contact_info"
    mock_publish.assert_called_once_with(
        decision_id="decision-reactivation-42",
        graph_name="reactivation",
        terminal_status="aborted",
    )
    summary = mock_log.call_args.kwargs["summary"]
    assert summary["failure_reason"] == "reactivation:no_contact_info"
    assert summary["early_abort"] is True
    assert summary["review_capture"]["raw_input_text"] == (
        "reactivation_outreach cohort=county_live county_id=dallas"
    )
    assert summary["review_capture"]["generated_output_text"] == ""
    assert summary["review_capture"]["review_flag"] is True
    assert (
        summary["review_capture"]["review_flag_reason"]
        == "reactivation:no_contact_info"
    )
