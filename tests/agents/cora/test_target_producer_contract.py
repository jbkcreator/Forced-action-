from unittest.mock import patch

from src.agents.cora.tools.read_tools import get_contact_channel


def test_get_contact_channel_returns_confidence(fresh_db):
    result = get_contact_channel(fresh_db, buyer_entity_id=-1)  # no such id — the None-path
    assert set(result.keys()) >= {"email", "phone", "contact_confidence"}
    assert result["email"] is None
    assert result["phone"] is None
    assert result["contact_confidence"] is None


from src.agents.cora.ingestion.target_producer import _produce_from_rows


def _row(thread_id: str, confidence: int, why_now: str = "3 purchases in the trailing 18 months") -> dict:
    return {
        "opportunity_thread_id": thread_id,
        "confidence_score": confidence,
        "total_purchase_count": 3,
        "total_cash_volume": 900_000,
        "why_now": why_now,
        "whale_flagged_at": None,
        "fallback_score": 0.5,
    }


def test_low_confidence_row_is_rejected_not_published(fresh_db):
    """A row below UNVERIFIED_FLOOR must never reach queue.publish (i.e.
    never surface in a Cora draft) -- the exact Hunter constitution rule
    gating.py states but nothing previously enforced against real
    get_ranked_whales() output."""
    with patch("src.agents.cora.ingestion.target_producer.store.has_duplicate_actionable_draft", return_value=False), \
         patch("src.agents.cora.ingestion.target_producer.get_buyer_entity_by_opportunity_thread_id",
               return_value={"id": 1, "entity_type": "LLC"}), \
         patch("src.agents.cora.ingestion.target_producer.get_contact_channel",
               return_value={"email": None, "phone": "+15551234567", "contact_confidence": 80}), \
         patch("src.agents.cora.ingestion.target_producer.queue.publish") as mock_publish:
        produced = _produce_from_rows(fresh_db, "founder_tier_blitz", [_row("OPP-2026-00050", confidence=40)])

    assert produced == []
    mock_publish.assert_not_called()

    from sqlalchemy import text
    row = fresh_db.execute(
        text("SELECT boundary FROM handoff_rejections WHERE reference_id = :r ORDER BY id DESC LIMIT 1"),
        {"r": "OPP-2026-00050"},
    ).mappings().first()
    assert row["boundary"] == "hunter_to_cora"


def test_citable_row_is_published(fresh_db):
    with patch("src.agents.cora.ingestion.target_producer.store.has_duplicate_actionable_draft", return_value=False), \
         patch("src.agents.cora.ingestion.target_producer.get_buyer_entity_by_opportunity_thread_id",
               return_value={"id": 2, "entity_type": "LLC"}), \
         patch("src.agents.cora.ingestion.target_producer.get_contact_channel",
               return_value={"email": None, "phone": "+15551234567", "contact_confidence": 80}), \
         patch("src.agents.cora.ingestion.target_producer.queue.publish", return_value="msg-1") as mock_publish:
        produced = _produce_from_rows(fresh_db, "founder_tier_blitz", [_row("OPP-2026-00051", confidence=85)])

    assert produced == ["OPP-2026-00051"]
    mock_publish.assert_called_once()
