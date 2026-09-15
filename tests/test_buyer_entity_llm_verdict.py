"""
Pure unit tests for _parse_llm_verdict() (WP-4, WI-5) -- no DB, no network.

Covers the optional 3rd reasoning line added to the LLM tie-break response
contract: backward-compatible with the original 2-line (verdict,
confidence) response, and appends the reason to explanation only when
present.

Run:
    pytest tests/test_buyer_entity_llm_verdict.py -v
"""
from __future__ import annotations

from src.services.buyer_entity_resolution import _parse_llm_verdict


def test_two_line_response_still_parses():
    """The original 2-line contract (no reasoning line) must still parse
    identically -- full backward compatibility."""
    verdict = _parse_llm_verdict("SAME\n78")
    assert verdict is not None
    assert verdict.is_match is True
    assert verdict.confidence == 78
    assert "78" in verdict.explanation
    assert ":" not in verdict.explanation.split("confidence=78")[-1]


def test_three_line_response_appends_reason():
    """A 3rd line is appended to explanation for human audit, without
    changing is_match/confidence."""
    verdict = _parse_llm_verdict(
        "SAME\n78\nsame surname, plausible initial variant"
    )
    assert verdict is not None
    assert verdict.is_match is True
    assert verdict.confidence == 78
    assert "same surname, plausible initial variant" in verdict.explanation


def test_different_verdict_zeroes_confidence_regardless_of_reason_line():
    verdict = _parse_llm_verdict("DIFFERENT\n15\nshared surname only, different first names")
    assert verdict is not None
    assert verdict.is_match is False
    assert verdict.confidence == 0
    assert "DIFFERENT" in verdict.explanation
    assert "shared surname only" in verdict.explanation


def test_empty_third_line_is_ignored():
    """A blank 3rd line (model returned nothing after the confidence
    number) must not corrupt the explanation with an empty quoted string."""
    verdict = _parse_llm_verdict("SAME\n90\n")
    assert verdict is not None
    assert verdict.explanation == "LLM returned SAME confidence=90"


def test_unparseable_response_returns_none():
    assert _parse_llm_verdict("I am not sure") is None
    assert _parse_llm_verdict("SAME") is None
    assert _parse_llm_verdict("") is None
