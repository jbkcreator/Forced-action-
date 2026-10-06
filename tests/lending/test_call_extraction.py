"""Call-field extraction: validation and the two anti-invention guards. The model call is patched."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from src.lending.call_extraction import EXTRACTION_TASK_TYPE, TOOL_NAME, CallExtraction, extract_call_fields

TODAY = date(2026, 10, 5)
TRANSCRIPT = (
    "Caller: Hi, calling about the Main Street loan. Borrower: Rates are too high right now. "
    "I still need to send the operating agreement. My partner Dana decides. "
    "Call me back Thursday. Honestly, we already closed with another lender."
)


def _extract(tool_input, transcript=TRANSCRIPT):
    with patch("src.services.claude_router.call_claude_with_usage",
               return_value={"tool_input": tool_input}) as call:
        return extract_call_fields(transcript, today=TODAY), call


def test_fields_are_extracted_and_cleaned():
    result, call = _extract({
        "objection": "Rates are too high",
        "missing_file_items": ["operating agreement", " operating agreement ", ""],
        "next_action": "Call back Thursday",
        "deadline": "2026-10-08",
        "referral_names": ["Dana"],
        "decision_maker": "Partner Dana",
        "kill_reason_verbatim": "we already closed with another lender",
    })
    assert result == CallExtraction(
        objection="Rates are too high", missing_file_items=["operating agreement"],
        next_action="Call back Thursday", deadline=date(2026, 10, 8), referral_names=["Dana"],
        decision_maker="Partner Dana", kill_reason_verbatim="we already closed with another lender",
    )
    kwargs = call.call_args.kwargs
    assert kwargs["task_type"] == EXTRACTION_TASK_TYPE
    assert kwargs["tool_choice"] == {"type": "tool", "name": TOOL_NAME}


def test_kill_reason_not_in_the_transcript_is_dropped():
    result, _ = _extract({"kill_reason_verbatim": "I found a cheaper bank"})
    assert result.kill_reason_verbatim is None


def test_past_or_far_deadline_is_dropped():
    assert _extract({"deadline": "2025-01-01"})[0].deadline is None
    assert _extract({"deadline": "2028-01-01"})[0].deadline is None


def test_malformed_deadline_keeps_the_rest():
    result, _ = _extract({"deadline": "next week", "objection": "Timing"})
    assert (result.deadline, result.objection) == (None, "Timing")


def test_blank_strings_become_none():
    result, _ = _extract({"objection": "   ", "decision_maker": ""})
    assert (result.objection, result.decision_maker) == (None, None)


def test_empty_transcript_skips_the_model():
    with patch("src.services.claude_router.call_claude_with_usage") as call:
        assert extract_call_fields("  ", today=TODAY) == CallExtraction()
    call.assert_not_called()


def test_blocked_model_call_raises_instead_of_storing_empty():
    with pytest.raises(RuntimeError):
        _extract(None)


def test_vendor_cost_pause_blocks_extraction_before_any_model_call():
    db = MagicMock()
    pause = MagicMock(reason="cost spike")
    with patch("src.services.claude_router.get_active_pause", return_value=pause) as get_pause, \
            patch("src.services.claude_router._log_usage"), \
            patch("src.services.claude_router._build_client") as build_client:
        with pytest.raises(RuntimeError):
            extract_call_fields(TRANSCRIPT, today=TODAY, db=db)
    get_pause.assert_called_once_with(db, "claude", EXTRACTION_TASK_TYPE)
    build_client.return_value.messages.create.assert_not_called()


def test_qualification_fields_are_extracted():
    result, call = _extract({
        "completed_projects_3y": 2, "credit_band": "at_or_above_640", "has_liquidity": True,
        "deal_status": "actively_looking", "property_address": "", "target_market": " Tampa Bay ",
    })
    assert (result.completed_projects_3y, result.credit_band, result.has_liquidity) == (2, "at_or_above_640", True)
    assert (result.deal_status, result.property_address, result.target_market) == ("actively_looking", None, "Tampa Bay")
    schema = call.call_args.kwargs["tools"][0]["input_schema"]["properties"]
    assert schema["credit_band"]["enum"] == ["at_or_above_640", "below_640"]


@pytest.mark.parametrize("field_name,value", [
    ("credit_band", "720"),
    ("credit_band", "good"),
    ("deal_status", "maybe"),
    ("completed_projects_3y", -1),
    ("completed_projects_3y", "several"),
    ("completed_projects_3y", 5000),
])
def test_values_outside_the_allowed_set_are_dropped(field_name, value):
    result, _ = _extract({field_name: value, "objection": "Timing"})
    assert getattr(result, field_name) is None
    assert result.objection == "Timing"


def test_exact_credit_score_is_never_requested():
    _, call = _extract({"objection": "Timing"})
    system = call.call_args.kwargs["system"]
    assert "exact credit score" in system and "640" in system


def test_overlong_field_fails_validation():
    with pytest.raises(ValidationError):
        _extract({"objection": "x" * 400})
