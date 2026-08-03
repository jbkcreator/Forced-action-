from __future__ import annotations

from datetime import timedelta

import pytest

from config.cora_cell_grid import get_cell
from src.agents.cora import store
from src.agents.cora.subgraphs import outreach
from tests.agents.cora.conftest import compose_result
from tests.agents.cora.fixtures.whales import WHALES, facts_for

CELL_ID = "founder_tier_blitz"


def _run(whale, db, mock_claude, *, contact_email="prospect@example.com", **extra):
    mock_claude.return_value = compose_result(
        "Founding seat at Forced Action",
        f"Noticed your {whale['total_purchase_count']} purchases — reach out if interested.",
    )
    return outreach.run_outreach(
        {
            "buyer_entity": whale,
            "cell_id": CELL_ID,
            "facts_used": facts_for(whale),
            "contact_email": contact_email,
            "contact_phone": None,
            **extra,
        },
        db=db,
    )


# Acceptance item 1: 10 outreach drafts (9 valid whales + the 1 deliberately below-threshold one).
def test_ten_whales_each_produce_a_correct_or_correctly_rejected_outcome(not_suppressed_db, mock_claude):
    completed = 0
    for whale in WHALES:
        result = _run(whale, not_suppressed_db, mock_claude)
        if whale["confidence_score"] < 70:
            assert result["terminal_status"] == "rejected"
            assert result["reject_reason"] == "low_confidence"
        else:
            assert result["terminal_status"] == "completed", result
            completed += 1
    assert completed == 9  # every whale except the deliberately-below-floor one (WHALES[8])


# Acceptance item 3: correct offer x avenue x angle tags.
def test_draft_carries_correct_cell_tags(not_suppressed_db, mock_claude):
    whale = WHALES[0]
    result = _run(whale, not_suppressed_db, mock_claude)
    cell = get_cell(CELL_ID)
    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["offer"] == cell["offer"]
    assert draft["avenue"] == cell["avenue"]
    assert draft["angle"] == cell["angle"]
    assert draft["draft_id"] == result["draft_id"]


# Acceptance item 4: correct opportunity_thread_id end-to-end.
def test_draft_carries_correct_opportunity_thread_id(not_suppressed_db, mock_claude):
    whale = WHALES[1]
    _run(whale, not_suppressed_db, mock_claude)
    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["opportunity_thread_id"] == whale["opportunity_thread_id"]


# Acceptance item 5: working applicable links (booking real, payment None — documented).
def test_draft_link_resolution(not_suppressed_db, mock_claude, monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "demo_calendly_url", "https://calendly.com/test-rep", raising=False)

    whale = dict(WHALES[2])
    mock_claude.return_value = compose_result("subj", "body")
    result = outreach.run_outreach(
        {
            "buyer_entity": whale, "cell_id": "hard_money_intro_lenders",
            "facts_used": facts_for(whale), "contact_email": "x@example.com", "contact_phone": None,
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "completed"
    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["booking_link"] == "https://calendly.com/test-rep"
    assert draft["payment_link"] is None


# Acceptance item 7: suppressed target -> no actionable draft.
def test_suppressed_target_produces_no_draft(suppressed_db, mock_claude):
    whale = WHALES[3]
    result = _run(whale, suppressed_db, mock_claude)
    assert result["terminal_status"] == "rejected"
    assert result["reject_reason"] == "suppressed"
    assert store.read_drafts(suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"]) == []


# Acceptance item 8: below-threshold Hunter record rejected.
def test_below_threshold_confidence_rejected(not_suppressed_db, mock_claude):
    whale = WHALES[8]
    result = _run(whale, not_suppressed_db, mock_claude)
    assert result["terminal_status"] == "rejected"
    assert result["reject_reason"] == "low_confidence"


# Acceptance item 9: duplicate event -> no duplicate output.
def test_duplicate_draft_attempt_is_rejected_not_duplicated(not_suppressed_db, mock_claude):
    whale = WHALES[4]
    first = _run(whale, not_suppressed_db, mock_claude)
    assert first["terminal_status"] == "completed"
    second = _run(whale, not_suppressed_db, mock_claude)
    assert second["terminal_status"] == "rejected"
    assert second["reject_reason"] == "duplicate_actionable"
    assert len(store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])) == 1


# Acceptance item 12: draft older than 72h is treated as expired at read time.
def test_draft_older_than_72h_is_expired(not_suppressed_db, mock_claude):
    from sqlalchemy import text

    whale = WHALES[5]
    result = _run(whale, not_suppressed_db, mock_claude)
    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert store.is_draft_expired(draft) is False

    backdated_at = store.parse_dt(draft["created_at"]) - timedelta(hours=73)
    not_suppressed_db.execute(
        text("UPDATE outbound_drafts SET created_at = :created_at WHERE draft_id = :draft_id"),
        {"created_at": backdated_at, "draft_id": result["draft_id"]},
    )
    refreshed = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert store.is_draft_expired(refreshed) is True

    expired_count = store.expire_stale_drafts(not_suppressed_db)
    assert expired_count == 1
    final = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert final["status"] == "expired"


# Reject path: empty facts_used never reaches compose.
def test_missing_facts_rejected_before_compose(not_suppressed_db, mock_claude):
    whale = WHALES[6]
    result = outreach.run_outreach(
        {
            "buyer_entity": whale, "cell_id": CELL_ID, "facts_used": [],
            "contact_email": "x@example.com", "contact_phone": None,
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "rejected"
    assert result["reject_reason"] == "facts_missing"
    mock_claude.assert_not_called()


def test_invalid_cell_id_fails(not_suppressed_db, mock_claude):
    whale = WHALES[7]
    result = outreach.run_outreach(
        {
            "buyer_entity": whale, "cell_id": "not_a_real_cell", "facts_used": facts_for(whale),
            "contact_email": "x@example.com", "contact_phone": None,
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "failed"
    assert result["reject_reason"] == "invalid_cell_id"


@pytest.mark.integration
def test_real_claude_draft_grounds_facts_no_invention(fresh_db):
    """Best-effort no-hallucination check against the REAL Claude API — not mocked."""
    whale = WHALES[0]
    facts = facts_for(whale)
    result = outreach.run_outreach(
        {
            "buyer_entity": whale, "cell_id": CELL_ID, "facts_used": facts,
            "contact_email": "real-claude-test@example.com", "contact_phone": None,
        },
        db=fresh_db,
    )
    assert result["terminal_status"] == "completed", result
    draft = store.read_drafts(fresh_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["subject"]
    assert draft["body"]
    # Best-effort containment: at least one fact value shows up verbatim in the copy.
    combined = f"{draft['subject']} {draft['body']}"
    assert any(str(f["value"]) in combined for f in facts)
