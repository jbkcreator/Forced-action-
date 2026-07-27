from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.agents.cora import opportunity_state, store
from src.agents.cora.subgraphs import reply
from tests.agents.cora.conftest import classify_result, compose_result
from tests.agents.cora.fixtures.replies import REPLIES


def _seed_parent_draft(thread_id: str, cell_id: str = "cell_1_founder_tier_blitz") -> None:
    store.append_draft(store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id=thread_id, buyer_entity_id=1,
        cell_id=cell_id, offer="founder_tier", avenue="flippers", angle="scarcity_seat_number",
        subject="Founding seat", body="Noticed your purchases.", facts_used=[], source_refs=[],
        recommended_channel="email", confidence_score=90,
    ))
    opportunity_state.mark_targeted(thread_id)
    opportunity_state.mark_touched(thread_id)


# Acceptance item 6: classify 10 seeded replies.
def test_ten_seeded_replies_classify_and_route_correctly(not_suppressed_db, mock_claude, monkeypatch):
    monkeypatch.setattr("src.services.email_suppression.suppress_contact", MagicMock())

    for i, fixture in enumerate(REPLIES):
        thread_id = f"{fixture['opportunity_thread_id']}-{i}"
        payload = dict(fixture, opportunity_thread_id=thread_id)
        _seed_parent_draft(thread_id)

        if fixture["expected_subtype"] == "UNSUBSCRIBE":
            mock_claude.side_effect = [classify_result(fixture["expected_intent"], fixture["expected_subtype"])]
        else:
            mock_claude.side_effect = [
                classify_result(fixture["expected_intent"], fixture["expected_subtype"]),
                compose_result("Re: your reply", "Thanks for the reply — following up."),
            ]

        result = reply.run_reply(payload, db=not_suppressed_db)
        assert result["terminal_status"] == "completed", (fixture, result)
        assert result["intent"] == fixture["expected_intent"]
        assert result["subtype"] == fixture["expected_subtype"]

        record = store.read_replies(opportunity_thread_id=thread_id)[0]
        if fixture["expected_subtype"] == "UNSUBSCRIBE":
            assert record["status"] == "suppressed"
        else:
            assert record["status"] == "pending_approval"


# Acceptance item 10: unmatched reply -> manual review.
def test_unmatched_reply_goes_to_manual_review(not_suppressed_db, mock_claude):
    result = reply.run_reply(
        {
            "opportunity_thread_id": None, "from_address": "unknown@example.com",
            "subject": "Re: ?", "body_text": "who is this?", "received_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "completed"
    assert result["reject_reason"] == "manual_review"
    mock_claude.assert_not_called()


def test_thread_with_no_prior_draft_goes_to_manual_review(not_suppressed_db, mock_claude):
    result = reply.run_reply(
        {
            "opportunity_thread_id": "OPP-NEVER-DRAFTED", "from_address": "x@example.com",
            "subject": "Re: ?", "body_text": "hello", "received_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )
    assert result["reject_reason"] == "manual_review"
    mock_claude.assert_not_called()


def test_unsubscribe_triggers_real_suppression_write_path(not_suppressed_db, mock_claude, monkeypatch):
    suppress_mock = MagicMock()
    monkeypatch.setattr("src.services.email_suppression.suppress_contact", suppress_mock)

    thread_id = "OPP-TEST-UNSUB"
    _seed_parent_draft(thread_id)
    mock_claude.side_effect = [classify_result("HOSTILE", "UNSUBSCRIBE")]

    result = reply.run_reply(
        {
            "opportunity_thread_id": thread_id, "from_address": "unsub@example.com",
            "subject": "Re:", "body_text": "unsubscribe me", "received_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "completed"
    suppress_mock.assert_called_once_with(not_suppressed_db, email="unsub@example.com", source="cora_reply_unsubscribe")
    assert opportunity_state.current_status(thread_id) == "closed"


@pytest.mark.integration
def test_real_claude_classification_on_seeded_replies(fresh_db):
    """Real Claude classification against all 10 seeded replies — no mocking."""
    correct = 0
    for i, fixture in enumerate(REPLIES):
        thread_id = f"{fixture['opportunity_thread_id']}-real-{i}"
        payload = dict(fixture, opportunity_thread_id=thread_id)
        _seed_parent_draft(thread_id)
        result = reply.run_reply(payload, db=fresh_db)
        assert result["terminal_status"] == "completed", (fixture, result)
        if result.get("intent") == fixture["expected_intent"]:
            correct += 1
    assert correct >= 8  # best-effort — real LLM classification, not a hard 10/10 guarantee
