"""Borrower lead score 0–10 (Runbook § 4.3): each criterion, all, none, and unknown inputs."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.lending.borrower_score import (
    EXPERIENCE,
    LOAN_SIZE,
    TARGET_STATE,
    UNDER_CONTRACT,
    BorrowerFacts,
    facts_from_lendingflow_lead,
    facts_from_web_lead,
    projects_from_band,
    score_borrower,
)

ALL = (UNDER_CONTRACT, EXPERIENCE, TARGET_STATE, LOAN_SIZE)


@pytest.mark.parametrize("facts, criterion, points", [
    (BorrowerFacts(under_contract=True), UNDER_CONTRACT, 4),
    (BorrowerFacts(completed_projects_36m=3), EXPERIENCE, 3),
    (BorrowerFacts(property_state="FL"), TARGET_STATE, 2),
    (BorrowerFacts(property_state=" ga "), TARGET_STATE, 2),
    (BorrowerFacts(loan_amount=300_000), LOAN_SIZE, 1),
])
def test_each_criterion_alone(facts, criterion, points):
    result = score_borrower(facts)
    assert result.score == points and result.earned == ((criterion, points),)
    assert set(result.missing) == set(ALL) - {criterion}


def test_all_four_score_ten():
    result = score_borrower(BorrowerFacts(True, 5, "GA", Decimal("750000")))
    assert result.score == 10 and result.missing == ()


def test_known_but_unmet_inputs_score_zero_and_are_not_missing():
    result = score_borrower(BorrowerFacts(False, 2, "TX", 299_999))
    assert result.score == 0 and result.earned == () and result.missing == ()


def test_nothing_known_scores_zero_with_everything_missing():
    result = score_borrower(BorrowerFacts())
    assert result.score == 0 and result.missing == ALL


@pytest.mark.parametrize("band, expected", [
    ("None yet", 0), ("1 to 2", 1), ("3 to 5", 3),  # the live form's spelling
    ("3-5", 3), ("6+", 6), ("1-2", 1), ("None", 0), ("0", 0), ("", None), (None, None), ("not sure", None),
    ("nonetheless 4", 4),
])
def test_project_bands(band, expected):
    assert projects_from_band(band) == expected


def test_a_lendingflow_lead_scores_state_and_amount_only():
    result = score_borrower(facts_from_lendingflow_lead({"property_state": "FL", "loan_amount": 500_000}))
    assert result.score == 3 and set(result.missing) == {UNDER_CONTRACT, EXPERIENCE}


def test_a_web_lead_scores_experience_only():
    result = score_borrower(facts_from_web_lead({"completed_projects_3y": "3-5", "property_city": "Tampa"}))
    assert result.score == 3 and set(result.missing) == {UNDER_CONTRACT, TARGET_STATE, LOAN_SIZE}
