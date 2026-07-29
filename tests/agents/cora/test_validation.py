from __future__ import annotations

from src.agents.cora import store
from src.agents.cora.kill_switch import CORA_GLOBAL_FEATURE
from src.agents.cora.validation import validate_can_draft
from src.core.redis_client import get_redis
from tests.agents.cora.fixtures.whales import WHALES, facts_for


def test_kill_switch_active_rejects(not_suppressed_db):
    get_redis().set(f"kill_switch_override:{CORA_GLOBAL_FEATURE}", "red", ex=60)
    whale = WHALES[0]
    result = validate_can_draft(
        buyer_entity=whale, cell_id="founder_tier_blitz", facts_used=facts_for(whale),
        recommended_channel="email", db=not_suppressed_db, email="x@example.com",
    )
    assert result.allowed is False
    assert result.reject_reason == "kill_switch_active"


def test_low_confidence_rejects(not_suppressed_db):
    whale = WHALES[8]  # confidence_score=40, below UNVERIFIED_FLOOR=70
    result = validate_can_draft(
        buyer_entity=whale, cell_id="founder_tier_blitz", facts_used=facts_for(whale),
        recommended_channel="email", db=not_suppressed_db, email="x@example.com",
    )
    assert result.allowed is False
    assert result.reject_reason == "low_confidence"


def test_empty_facts_rejects(not_suppressed_db):
    whale = WHALES[0]
    result = validate_can_draft(
        buyer_entity=whale, cell_id="founder_tier_blitz", facts_used=[],
        recommended_channel="email", db=not_suppressed_db, email="x@example.com",
    )
    assert result.allowed is False
    assert result.reject_reason == "facts_missing"


def test_fact_missing_observed_at_rejects(not_suppressed_db):
    whale = WHALES[0]
    facts = [{"fact_key": "total_purchase_count", "value": 5, "source_ref": "test"}]  # no observed_at
    result = validate_can_draft(
        buyer_entity=whale, cell_id="founder_tier_blitz", facts_used=facts,
        recommended_channel="email", db=not_suppressed_db, email="x@example.com",
    )
    assert result.allowed is False
    assert result.reject_reason == "facts_missing"


def test_stale_facts_rejects(not_suppressed_db):
    whale = WHALES[0]
    result = validate_can_draft(
        buyer_entity=whale, cell_id="founder_tier_blitz", facts_used=facts_for(whale, stale=True),
        recommended_channel="email", db=not_suppressed_db, email="x@example.com",
    )
    assert result.allowed is False
    assert result.reject_reason == "stale_facts"


def test_suppressed_email_rejects(suppressed_db):
    whale = WHALES[0]
    result = validate_can_draft(
        buyer_entity=whale, cell_id="founder_tier_blitz", facts_used=facts_for(whale),
        recommended_channel="email", db=suppressed_db, email="opted-out@example.com",
    )
    assert result.allowed is False
    assert result.reject_reason == "suppressed"


def test_duplicate_actionable_draft_rejects(not_suppressed_db):
    whale = WHALES[0]
    store.append_draft(not_suppressed_db, store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id=whale["opportunity_thread_id"],
        buyer_entity_id=whale["id"], cell_id="founder_tier_blitz", offer="founder_tier",
        avenue="flippers", angle="scarcity_seat_number", subject="s", body="b",
        facts_used=facts_for(whale), source_refs=[], recommended_channel="email",
        confidence_score=whale["confidence_score"],
    ))
    result = validate_can_draft(
        buyer_entity=whale, cell_id="founder_tier_blitz", facts_used=facts_for(whale),
        recommended_channel="email", db=not_suppressed_db, email="x@example.com",
    )
    assert result.allowed is False
    assert result.reject_reason == "duplicate_actionable"


def test_is_followup_bypasses_duplicate_check(not_suppressed_db):
    whale = WHALES[0]
    store.append_draft(not_suppressed_db, store.OutboundDraftRecord(
        draft_id=store.new_draft_id(), opportunity_thread_id=whale["opportunity_thread_id"],
        buyer_entity_id=whale["id"], cell_id="founder_tier_blitz", offer="founder_tier",
        avenue="flippers", angle="scarcity_seat_number", subject="s", body="b",
        facts_used=facts_for(whale), source_refs=[], recommended_channel="email",
        confidence_score=whale["confidence_score"],
    ))
    result = validate_can_draft(
        buyer_entity=whale, cell_id="founder_tier_blitz", facts_used=facts_for(whale),
        recommended_channel="email", db=not_suppressed_db, email="x@example.com", is_followup=True,
    )
    assert result.allowed is True


def test_happy_path_allowed(not_suppressed_db):
    whale = WHALES[1]
    result = validate_can_draft(
        buyer_entity=whale, cell_id="founder_tier_blitz", facts_used=facts_for(whale),
        recommended_channel="email", db=not_suppressed_db, email="x@example.com",
    )
    assert result.allowed is True
    assert result.reject_reason is None
