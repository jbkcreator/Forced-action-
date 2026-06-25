"""Tests for Phase 3 A1: Loss Autopsy Engine."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.loss_autopsy import run_loss_autopsy


@pytest.fixture()
def db():
    """Minimal mock session that satisfies the autopsy service."""
    session = MagicMock()
    # Idempotency check: no existing autopsy
    session.execute.return_value.scalar_one_or_none.return_value = None
    # Context queries return empty mappings
    session.execute.return_value.mappings.return_value.first.return_value = None
    session.execute.return_value.mappings.return_value.all.return_value = []
    return session


_TOOL_INPUT = {
    "primary_rejection_reason": "PRICING_TOO_HIGH",
    "competitor_rate_delta": 0.015,
    "underwriting_blocker": None,
    "cora_behavior_adjustment": "Reduce price by 10% for Gold leads in Hillsborough.",
}


def test_run_loss_autopsy_writes_row(db):
    with patch(
        "src.services.loss_autopsy.call_claude_with_usage",
        return_value={
            "tool_input": _TOOL_INPUT,
            "text": "",
            "model": "sonnet",
            "input_tokens": 400,
            "output_tokens": 80,
            "cost_usd": 0.002400,
        },
    ):
        result = run_loss_autopsy(
            property_id=1001,
            trigger_reason="CLOSED_LOST",
            db=db,
            deal_outcome_id=42,
        )

    assert result is not None
    assert result.trigger_reason == "CLOSED_LOST"
    assert result.primary_rejection_reason == "PRICING_TOO_HIGH"
    assert float(result.competitor_rate_delta) == pytest.approx(0.015)
    assert result.underwriting_blocker is None
    assert "Reduce price" in result.cora_behavior_adjustment
    db.add.assert_called_once_with(result)
    db.flush.assert_called_once()


def test_run_loss_autopsy_idempotency(db):
    """Second call for same deal_outcome_id returns None without calling Claude."""
    # Simulate existing row
    db.execute.return_value.scalar_one_or_none.return_value = "some-existing-uuid"

    with patch("src.services.loss_autopsy.call_claude_with_usage") as mock_claude:
        result = run_loss_autopsy(
            property_id=1001,
            trigger_reason="CLOSED_LOST",
            db=db,
            deal_outcome_id=42,
        )

    assert result is None
    mock_claude.assert_not_called()


def test_run_loss_autopsy_invalid_trigger(db):
    result = run_loss_autopsy(
        property_id=1001,
        trigger_reason="INVALID_REASON",
        db=db,
    )
    assert result is None
    db.add.assert_not_called()


def test_run_loss_autopsy_claude_failure_returns_none(db):
    with patch(
        "src.services.loss_autopsy.call_claude_with_usage",
        side_effect=RuntimeError("API timeout"),
    ):
        result = run_loss_autopsy(
            property_id=1001,
            trigger_reason="DECLINED",
            db=db,
            deal_outcome_id=99,
        )

    assert result is None
    db.add.assert_not_called()


def test_run_loss_autopsy_ghosted_sla_no_deal_outcome(db):
    """GHOSTED_SLA autopsies have no deal_outcome_id — no idempotency check needed."""
    with patch(
        "src.services.loss_autopsy.call_claude_with_usage",
        return_value={
            "tool_input": _TOOL_INPUT,
            "text": "",
            "model": "sonnet",
            "input_tokens": 300,
            "output_tokens": 60,
            "cost_usd": 0.0018,
        },
    ):
        result = run_loss_autopsy(
            property_id=2002,
            trigger_reason="GHOSTED_SLA",
            db=db,
        )

    assert result is not None
    assert result.trigger_reason == "GHOSTED_SLA"
    assert result.deal_outcome_id is None
