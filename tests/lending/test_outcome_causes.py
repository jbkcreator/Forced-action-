"""Cause tags on unfunded outcomes."""
from __future__ import annotations

import pytest

from config.call_outcome_causes import UNFUNDED_CAUSES
from src.lending.outcome_causes import MissingCause, UnknownCause, require_cause


def test_the_six_causes_from_the_brief():
    assert set(UNFUNDED_CAUSES) == {
        "contactability", "timing", "fit", "borrower_choice", "lender_execution", "our_execution",
    }


def test_valid_cause_is_normalized():
    assert require_cause("  Our_Execution ") == "our_execution"


@pytest.mark.parametrize("value", [None, "", "   "])
def test_unfunded_outcome_needs_a_cause(value):
    with pytest.raises(MissingCause):
        require_cause(value)


def test_unknown_cause_is_refused():
    with pytest.raises(UnknownCause):
        require_cause("bad_source")
