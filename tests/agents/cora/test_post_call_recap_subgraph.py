from __future__ import annotations

from sqlalchemy import text

from src.agents.cora import store
from src.agents.cora.subgraphs import post_call_recap
from tests.agents.cora.conftest import compose_result


def _seed_conversation(db, thread_id: str) -> None:
    store.append_draft(db, store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id=thread_id, buyer_entity_id=1,
        cell_id="founder_tier_blitz", offer="founder_tier", avenue="flippers",
        angle="scarcity_seat_number", subject="Founding seat", body="Noticed your purchases.",
        facts_used=[], source_refs=[], recommended_channel="email", confidence_score=90,
    ))


FAKE_BUYER_ENTITY = {
    "id": 1, "canonical_name": "Test Buyer LLC", "entity_type": "llc",
    "confidence_score": 90, "is_whale": True, "county_id": "hillsborough",
    "opportunity_thread_id": "OPP-POSTCALL-1",
}


def test_post_call_recap_persists_a_normal_draft(not_suppressed_db, mock_claude, monkeypatch):
    thread_id = "OPP-POSTCALL-1"
    _seed_conversation(not_suppressed_db, thread_id)
    monkeypatch.setattr(
        "src.agents.cora.tools.read_tools.get_buyer_entity_by_opportunity_thread_id",
        lambda db, tid: FAKE_BUYER_ENTITY,
    )
    monkeypatch.setattr(
        "src.agents.cora.tools.read_tools.get_contact_channel",
        lambda db, bid: {"email": "prospect@example.com", "phone": None},
    )
    mock_claude.return_value = compose_result("Re: great talking with you", "Following up as promised.")

    result = post_call_recap.run_post_call_recap(
        {
            "opportunity_thread_id": thread_id, "transcript_text": "Prospect agreed to review pricing.",
            "call_outcome": "interested", "duration_seconds": 240,
            "completed_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )

    assert result["terminal_status"] == "completed"
    draft_id = result["draft_id"]
    assert draft_id

    record = store.read_drafts(not_suppressed_db, opportunity_thread_id=thread_id, cell_id="post_call_recap")
    assert len(record) == 1
    assert record[0]["status"] == "draft"
    assert record[0]["subject"] == "Re: great talking with you"
    assert record[0]["body"] == "Following up as promised."
    assert record[0]["facts_used"][0]["fact_key"] == "call_outcome"
    assert record[0]["facts_used"][0]["value"] == "interested"


def test_post_call_recap_unresolvable_buyer_entity_fails_without_crashing(not_suppressed_db, mock_claude, monkeypatch):
    monkeypatch.setattr(
        "src.agents.cora.tools.read_tools.get_buyer_entity_by_opportunity_thread_id",
        lambda db, tid: None,
    )

    result = post_call_recap.run_post_call_recap(
        {
            "opportunity_thread_id": "OPP-POSTCALL-MISSING", "transcript_text": None,
            "call_outcome": None, "duration_seconds": None, "completed_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )

    assert result["terminal_status"] == "failed"
    assert result["reject_reason"] == "unresolvable_buyer_entity"
    mock_claude.assert_not_called()


def test_post_call_recap_draft_is_attributed_to_the_buyers_own_venture(
    not_suppressed_db, mock_claude, monkeypatch
):
    """Same CLONE-v2.2/CL4 issue as outreach.py: a completed-call recap for a
    second venture's buyer must not be recorded under the primary venture."""
    venture_key = "test_postcall_second_venture"
    county_id = f"{venture_key}_county"
    not_suppressed_db.execute(
        text("INSERT INTO ventures (venture_key, display_name, brand_name, is_active) "
             "VALUES (:k, 'Second Venture', 'Second Venture', true) "
             "ON CONFLICT (venture_key) DO NOTHING"),
        {"k": venture_key},
    )
    not_suppressed_db.execute(
        text("INSERT INTO counties (county_id, display_name, venture_key, zip_prefixes, is_active) "
             "VALUES (:c, 'Second County', :k, '[]'::jsonb, true) "
             "ON CONFLICT (county_id) DO NOTHING"),
        {"c": county_id, "k": venture_key},
    )
    not_suppressed_db.flush()

    thread_id = "OPP-POSTCALL-SECOND-VENTURE"
    _seed_conversation(not_suppressed_db, thread_id)
    monkeypatch.setattr(
        "src.agents.cora.tools.read_tools.get_buyer_entity_by_opportunity_thread_id",
        lambda db, tid: dict(FAKE_BUYER_ENTITY, opportunity_thread_id=thread_id, county_id=county_id),
    )
    monkeypatch.setattr(
        "src.agents.cora.tools.read_tools.get_contact_channel",
        lambda db, bid: {"email": "prospect@example.com", "phone": None},
    )
    mock_claude.return_value = compose_result("Re: great talking with you", "Following up as promised.")

    result = post_call_recap.run_post_call_recap(
        {
            "opportunity_thread_id": thread_id, "transcript_text": "Prospect agreed to review pricing.",
            "call_outcome": "interested", "duration_seconds": 240,
            "completed_at": store.now().isoformat(),
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "completed"

    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=thread_id, cell_id="post_call_recap")[0]
    assert draft["venture_key"] == venture_key


def test_post_call_recap_respects_suppression(suppressed_db, mock_claude, monkeypatch):
    thread_id = "OPP-POSTCALL-SUPPRESSED"
    _seed_conversation(suppressed_db, thread_id)
    monkeypatch.setattr(
        "src.agents.cora.tools.read_tools.get_buyer_entity_by_opportunity_thread_id",
        lambda db, tid: dict(FAKE_BUYER_ENTITY, opportunity_thread_id=thread_id),
    )
    monkeypatch.setattr(
        "src.agents.cora.tools.read_tools.get_contact_channel",
        lambda db, bid: {"email": "opted-out@example.com", "phone": None},
    )

    result = post_call_recap.run_post_call_recap(
        {
            "opportunity_thread_id": thread_id, "transcript_text": "call happened",
            "call_outcome": "interested", "duration_seconds": 100,
            "completed_at": store.now().isoformat(),
        },
        db=suppressed_db,
    )

    assert result["terminal_status"] == "rejected"
    assert result["reject_reason"] == "suppressed"
    mock_claude.assert_not_called()
