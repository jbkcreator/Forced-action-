from __future__ import annotations

import pytest

from src.agents.cora import store
from src.agents.cora.subgraphs import pre_call
from tests.agents.cora.conftest import brief_result
from tests.agents.cora.fixtures.whales import WHALES


def _seed_conversation(thread_id: str) -> None:
    store.append_draft(store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id=thread_id, buyer_entity_id=1,
        cell_id="founder_tier_blitz", offer="founder_tier", avenue="flippers",
        angle="scarcity_seat_number", subject="Founding seat", body="Noticed your purchases.",
        facts_used=[], source_refs=[], recommended_channel="email", confidence_score=90,
    ))
    store.append_reply(store.ReplyRecord(
        reply_id=store.new_reply_id(), opportunity_thread_id=thread_id, from_address="p@example.com",
        subject="Re:", body_text="Yes let's talk", received_at=store.now().isoformat(),
        intent="INTERESTED", subtype="BOOKING_REQUEST", status="pending_approval",
    ))


# Acceptance item 13: booked call -> complete pre-call brief.
def test_booked_call_produces_complete_brief(not_suppressed_db, mock_claude):
    whale = WHALES[0]
    thread_id = whale["opportunity_thread_id"]
    _seed_conversation(thread_id)
    mock_claude.return_value = brief_result(
        "Hey, saw you've been active lately — got a minute?", "Confirm interest and book next step.",
    )

    result = pre_call.run_pre_call(
        {
            "opportunity_thread_id": thread_id, "call_booked_at": store.now().isoformat(),
            "rep": "test-rep", "scheduled_for": store.now().isoformat(), "buyer_entity": whale,
        },
        db=not_suppressed_db,
    )

    assert result["terminal_status"] == "completed"
    assert result["brief_id"]
    brief = store.read_pre_call_briefs(opportunity_thread_id=thread_id)[0]
    content = brief["content"]
    for key in (
        "prospect_identity", "hunter_facts", "why_now_catalyst", "prior_messages_and_replies",
        "current_reply_intent", "likely_objections", "recommended_offer", "pricing_context",
        "suggested_opening", "call_objective", "relevant_links",
    ):
        assert key in content, key
    assert content["current_reply_intent"] == "INTERESTED"
    assert content["suggested_opening"]
    assert content["call_objective"]
    assert len(content["prior_messages_and_replies"]) == 2  # 1 draft + 1 reply


def test_brief_survives_claude_failure_with_empty_opening(not_suppressed_db, mock_claude):
    whale = WHALES[1]
    thread_id = whale["opportunity_thread_id"]
    _seed_conversation(thread_id)
    mock_claude.side_effect = RuntimeError("simulated Claude outage")

    result = pre_call.run_pre_call(
        {
            "opportunity_thread_id": thread_id, "call_booked_at": store.now().isoformat(),
            "rep": None, "scheduled_for": None, "buyer_entity": whale,
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "completed"  # brief still persists, just with empty LLM fields
    brief = store.read_pre_call_briefs(opportunity_thread_id=thread_id)[0]
    assert brief["content"]["suggested_opening"] == ""
    assert brief["content"]["call_objective"] == ""


@pytest.mark.integration
def test_real_claude_brief_generation(fresh_db):
    whale = WHALES[2]
    thread_id = whale["opportunity_thread_id"]
    _seed_conversation(thread_id)
    result = pre_call.run_pre_call(
        {
            "opportunity_thread_id": thread_id, "call_booked_at": store.now().isoformat(),
            "rep": "real-rep", "scheduled_for": store.now().isoformat(), "buyer_entity": whale,
        },
        db=fresh_db,
    )
    assert result["terminal_status"] == "completed"
    brief = store.read_pre_call_briefs(opportunity_thread_id=thread_id)[0]
    assert brief["content"]["suggested_opening"]
    assert brief["content"]["call_objective"]
