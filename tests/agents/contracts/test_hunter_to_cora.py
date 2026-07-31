import pytest
from pydantic import ValidationError

from src.agents.contracts.hunter_to_cora import (
    HunterToCoraHandoff,
    is_handoff_citable,
    reject_handoff,
    validate_handoff,
)

_BASE_ROW = {
    "opportunity_thread_id": "OPP-2026-00042",
    "confidence_score": 85,
    "entity_type": "LLC",
    "total_purchase_count": 4,
    "total_cash_volume": 1_200_000,
    "contact_channel": "phone",
    "contact_confidence": 90,
    "why_now": "3 purchases in the trailing 18 months -- most recent: Warranty Deed on 2026-07-01",
    "whale_flagged_at": None,
}


def test_valid_row_passes():
    handoff = validate_handoff(_BASE_ROW)
    assert handoff.opportunity_thread_id == "OPP-2026-00042"
    assert is_handoff_citable(handoff) is True


def test_low_confidence_is_not_citable_but_still_structurally_valid():
    row = {**_BASE_ROW, "confidence_score": 40}
    handoff = validate_handoff(row)
    assert is_handoff_citable(handoff) is False  # gating.UNVERIFIED_FLOOR = 70


def test_bad_thread_id_format_rejected():
    row = {**_BASE_ROW, "opportunity_thread_id": "not-a-real-id"}
    with pytest.raises(ValidationError):
        validate_handoff(row)


def test_missing_why_now_and_missing_auction_date_rejected():
    row = {**_BASE_ROW, "why_now": None}
    with pytest.raises(ValidationError):
        validate_handoff(row)


def test_auction_deed_date_satisfies_why_now_requirement_for_cell_2_rows():
    """Cell #2 (auction fast-follow) rows never carry why_now -- verified
    directly against read_tools.get_recent_auction_fast_follow_whales()'s
    SELECT, which has no why_now column. latest_auction_deed_date must
    satisfy the temporal-catalyst requirement instead."""
    row = {**_BASE_ROW, "why_now": None, "latest_auction_deed_date": "2026-07-25"}
    handoff = validate_handoff(row)
    assert handoff.why_now is None
    assert handoff.latest_auction_deed_date == "2026-07-25"


def test_reject_handoff_writes_audit_row_and_slack_noops_without_config(fresh_db, caplog):
    reject_handoff(fresh_db, _BASE_ROW, ["confidence_score: 40 < 70 (UNVERIFIED_FLOOR)"])
    fresh_db.commit()

    from sqlalchemy import text
    row = fresh_db.execute(
        text("SELECT boundary, reference_id FROM handoff_rejections WHERE reference_id = :r ORDER BY id DESC LIMIT 1"),
        {"r": "OPP-2026-00042"},
    ).mappings().first()
    assert row["boundary"] == "hunter_to_cora"
