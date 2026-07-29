from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.agents.cora import opportunity_state, store
from src.agents.cora.subgraphs import reply
from tests.agents.cora.conftest import classify_result, compose_result
from tests.agents.cora.fixtures.replies import REPLIES


def _seed_parent_draft(db, thread_id: str, cell_id: str = "founder_tier_blitz", contact_email: str = None) -> None:
    store.append_draft(db, store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id=thread_id, buyer_entity_id=1,
        cell_id=cell_id, offer="founder_tier", avenue="flippers", angle="scarcity_seat_number",
        subject="Founding seat", body="Noticed your purchases.", facts_used=[], source_refs=[],
        recommended_channel="email", confidence_score=90, contact_email=contact_email,
    ))
    opportunity_state.mark_targeted(thread_id)
    opportunity_state.mark_touched(thread_id)


def test_match_thread_resolves_by_contact_email_when_thread_id_unknown(not_suppressed_db, mock_claude):
    thread_id = "OPP-EMAIL-MATCH"
    _seed_parent_draft(not_suppressed_db, thread_id, contact_email="prospect@example.com")
    mock_claude.side_effect = [classify_result("TIMING", "NOT_NOW"), compose_result("Re:", "No worries, following up later.")]

    result = reply.run_reply(
        {
            "opportunity_thread_id": None, "from_address": "prospect@example.com",
            "subject": "Re:", "body_text": "not now", "received_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "completed"
    assert result["reject_reason"] is None
    record = store.read_replies()[-1]
    assert record["opportunity_thread_id"] == thread_id


# Live-caught bug (2026-07-28): ReplyRecord had no fields for the composed
# response text — _node_compose_response generated it via a real Claude call
# every time, but _node_persist never read it, so it was silently discarded.
# Fixed — this pins that the composed text actually reaches the store.
def test_composed_response_text_is_persisted(not_suppressed_db, mock_claude):
    thread_id = "OPP-RESPONSE-PERSIST"
    _seed_parent_draft(not_suppressed_db, thread_id, contact_email="prospect@example.com")
    mock_claude.side_effect = [
        classify_result("INTERESTED", None),
        compose_result("Re: Founding seat", "Great, here's more detail on the offer."),
    ]

    result = reply.run_reply(
        {
            "opportunity_thread_id": thread_id, "from_address": "prospect@example.com",
            "subject": "Re:", "body_text": "tell me more", "received_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )

    assert result["terminal_status"] == "completed"
    record = store.read_replies(opportunity_thread_id=thread_id)[0]
    assert record["status"] == "pending_approval"
    assert record["response_subject"] == "Re: Founding seat"
    assert record["response_body"] == "Great, here's more detail on the offer."


def test_match_thread_unmatched_email_goes_to_manual_review(not_suppressed_db, mock_claude):
    result = reply.run_reply(
        {
            "opportunity_thread_id": None, "from_address": "never-drafted@example.com",
            "subject": "Re:", "body_text": "who is this", "received_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )
    assert result["reject_reason"] == "manual_review"
    mock_claude.assert_not_called()


def test_find_opportunity_thread_id_by_email_case_insensitive_and_most_recent(fresh_db):
    store.append_draft(fresh_db, store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id="OPP-OLD", buyer_entity_id=1,
        cell_id="founder_tier_blitz", offer="founder_tier", avenue="flippers", angle="scarcity_seat_number",
        subject="s", body="b", facts_used=[], source_refs=[], recommended_channel="email",
        confidence_score=90, contact_email="Prospect@Example.com",
        created_at="2026-01-01T00:00:00+00:00",
    ))
    store.append_draft(fresh_db, store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id="OPP-NEW", buyer_entity_id=1,
        cell_id="founder_tier_blitz", offer="founder_tier", avenue="flippers", angle="scarcity_seat_number",
        subject="s", body="b", facts_used=[], source_refs=[], recommended_channel="email",
        confidence_score=90, contact_email="prospect@example.com",
        created_at="2026-06-01T00:00:00+00:00",
    ))
    assert store.find_opportunity_thread_id_by_email(fresh_db, "PROSPECT@EXAMPLE.COM") == "OPP-NEW"
    assert store.find_opportunity_thread_id_by_email(fresh_db, "nobody@example.com") is None
    assert store.find_opportunity_thread_id_by_email(fresh_db, "") is None


# Acceptance item 6: classify 10 seeded replies.
def test_ten_seeded_replies_classify_and_route_correctly(not_suppressed_db, mock_claude, monkeypatch):
    monkeypatch.setattr("src.services.email_suppression.suppress_contact", MagicMock())

    for i, fixture in enumerate(REPLIES):
        thread_id = f"{fixture['opportunity_thread_id']}-{i}"
        payload = dict(fixture, opportunity_thread_id=thread_id)
        _seed_parent_draft(not_suppressed_db, thread_id)

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
    _seed_parent_draft(not_suppressed_db, thread_id)
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


# PR #180 finding: a failed suppression write must never be silently treated
# as a completed opt-out — that's a compliance gap, not a best-effort nicety.
# The fix removed the try/except around suppress_contact() so the exception
# propagates all the way to worker.py's _process_one, which leaves the event
# unacked for Redis Streams to redeliver instead of acking a false success.
def test_unsubscribe_suppression_failure_propagates_and_does_not_complete(not_suppressed_db, mock_claude, monkeypatch):
    def _boom(db, email, source):
        raise RuntimeError("simulated suppression write failure")

    monkeypatch.setattr("src.services.email_suppression.suppress_contact", _boom)

    thread_id = "OPP-TEST-UNSUB-FAIL"
    _seed_parent_draft(not_suppressed_db, thread_id)
    mock_claude.side_effect = [classify_result("HOSTILE", "UNSUBSCRIBE")]

    with pytest.raises(RuntimeError, match="simulated suppression write failure"):
        reply.run_reply(
            {
                "opportunity_thread_id": thread_id, "from_address": "unsub-fail@example.com",
                "subject": "Re:", "body_text": "unsubscribe me", "received_at": store.now().isoformat(),
            },
            db=not_suppressed_db,
        )

    # The graph never reached persist, so no reply record exists claiming a
    # false "suppressed"/"completed" outcome, and the thread was never
    # incorrectly marked closed.
    assert store.read_replies(opportunity_thread_id=thread_id) == []
    assert opportunity_state.current_status(thread_id) != "closed"


def test_booking_request_reply_triggers_call_booked_event(fresh_db, mock_claude, monkeypatch):
    from src.agents.cora import queue

    thread_id = "OPP-TEST-BOOKING"
    _seed_parent_draft(fresh_db, thread_id)
    mock_claude.side_effect = [
        classify_result("INTERESTED", "BOOKING_REQUEST"),
        compose_result("Re:", "Great — here's the booking link."),
    ]

    fake_row = {"id": 1, "opportunity_thread_id": thread_id, "confidence_score": 90, "county_id": "hillsborough"}
    monkeypatch.setattr(
        "src.agents.cora.tools.read_tools.get_buyer_entity_by_opportunity_thread_id",
        lambda db, tid: fake_row,
    )

    result = reply.run_reply(
        {
            "opportunity_thread_id": thread_id, "from_address": "prospect@example.com",
            "subject": "Re:", "body_text": "Yes let's talk this week", "received_at": store.now().isoformat(),
        },
        db=fresh_db,
    )
    assert result["terminal_status"] == "completed"
    assert result["subtype"] == "BOOKING_REQUEST"

    published = queue.read_batch("test-consumer", count=10, block_ms=200)
    assert len(published) == 1
    assert published[0].event_type == "call.booked"
    assert published[0].payload["opportunity_thread_id"] == thread_id
    assert published[0].payload["buyer_entity"]["opportunity_thread_id"] == thread_id
    assert published[0].payload["scheduled_for"] is None
    queue.ack(published[0].message_id)


def test_non_booking_reply_never_publishes_call_booked(not_suppressed_db, mock_claude):
    from src.agents.cora import queue

    thread_id = "OPP-TEST-NONBOOKING"
    _seed_parent_draft(not_suppressed_db, thread_id)
    mock_claude.side_effect = [
        classify_result("INTERESTED", None),
        compose_result("Re:", "Tell me more."),
    ]
    reply.run_reply(
        {
            "opportunity_thread_id": thread_id, "from_address": "prospect@example.com",
            "subject": "Re:", "body_text": "tell me more", "received_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )
    published = queue.read_batch("test-consumer-2", count=10, block_ms=200)
    assert published == []


def test_unresolvable_buyer_entity_skips_call_booked_without_crashing(fresh_db, mock_claude, monkeypatch):
    from src.agents.cora import queue

    thread_id = "OPP-TEST-BOOKING-UNRESOLVABLE"
    _seed_parent_draft(fresh_db, thread_id)
    mock_claude.side_effect = [
        classify_result("INTERESTED", "BOOKING_REQUEST"),
        compose_result("Re:", "Great — here's the booking link."),
    ]

    monkeypatch.setattr(
        "src.agents.cora.tools.read_tools.get_buyer_entity_by_opportunity_thread_id",
        lambda db, tid: None,  # no matching buyer_entities row
    )

    result = reply.run_reply(
        {
            "opportunity_thread_id": thread_id, "from_address": "prospect@example.com",
            "subject": "Re:", "body_text": "Yes let's talk", "received_at": store.now().isoformat(),
        },
        db=fresh_db,
    )
    assert result["terminal_status"] == "completed"  # persisting the reply must still succeed
    published = queue.read_batch("test-consumer-3", count=10, block_ms=200)
    assert published == []


@pytest.mark.integration
def test_real_claude_classification_on_seeded_replies(fresh_db):
    """Real Claude classification against all 10 seeded replies — no mocking."""
    correct = 0
    for i, fixture in enumerate(REPLIES):
        thread_id = f"{fixture['opportunity_thread_id']}-real-{i}"
        payload = dict(fixture, opportunity_thread_id=thread_id)
        _seed_parent_draft(fresh_db, thread_id)
        result = reply.run_reply(payload, db=fresh_db)
        assert result["terminal_status"] == "completed", (fixture, result)
        if result.get("intent") == fixture["expected_intent"]:
            correct += 1
    assert correct >= 8  # best-effort — real LLM classification, not a hard 10/10 guarantee
