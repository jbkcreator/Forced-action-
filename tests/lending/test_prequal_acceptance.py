"""Split T-27 / Runbook §8 Test 2: real lender matrix -> 3 qualifying profiles with ranked lenders,
a 4th non-qualifying profile with structured failure codes. Placeholders never shown as confirmed."""
from __future__ import annotations

from dataclasses import replace

import pytest

from config.lender_matrix import LENDER_MATRIX
from src.lending.contracts import LoanType
from src.lending.prequal_acceptance import (
    COST_BASIS,
    SUBJECT_TO_CONFIRMATION,
    TEST2_PROFILES,
    RankedLender,
    evaluate_profile,
)

QUALIFYING = ("1_fix_and_flip", "2_experienced_builder", "3_credit_challenged_investor")
NON_QUALIFYING = "4_non_qualifying"


def _outcomes():
    return {p.name: evaluate_profile(p, matrix=LENDER_MATRIX) for p in TEST2_PROFILES}


def _profile(name):
    return next(p for p in TEST2_PROFILES if p.name == name)


def test_four_profiles_three_qualify_one_does_not():
    outcomes = _outcomes()
    assert set(outcomes) == {*QUALIFYING, NON_QUALIFYING}
    assert all(outcomes[n].qualifies for n in QUALIFYING) and not outcomes[NON_QUALIFYING].qualifies


def test_qualifying_profiles_rank_fitting_lenders_and_mark_every_placeholder():
    outcomes = _outcomes()
    for name in QUALIFYING:
        outcome = outcomes[name]
        assert outcome.limits and outcome.ranking
        assert all(isinstance(row, RankedLender) for row in outcome.ranking)
        assert [r.rank for r in outcome.ranking] == list(range(1, len(outcome.ranking) + 1))
        for row in outcome.ranking:
            assert row.status == SUBJECT_TO_CONFIRMATION  # every lender is unverified today
            assert row.cost_basis == COST_BASIS


def test_backflip_ranks_first_when_it_fits():
    """Josh A2: Backflip rows first whenever they fit, then cheapest (unknown cost last)."""
    outcomes = _outcomes()
    for name in QUALIFYING:
        assert outcomes[name].ranking[0].lender_key.startswith("generic_backflip")


def test_credit_challenged_profile_is_in_the_640_679_band():
    profile = _profile("3_credit_challenged_investor")
    assert profile.lead.credit_band == "640-679" and profile.borrower.credit_band_min_fico < 680


def test_non_qualifying_profile_outputs_only_specific_failure_codes_and_no_letter():
    outcome = _outcomes()[NON_QUALIFYING]
    assert outcome.qualifies is False and outcome.limits == [] and outcome.ranking == []
    assert outcome.failure_codes == ["credit_below_min"]


def test_lenders_that_do_not_offer_the_loan_type_are_not_counted_as_failures():
    outcome = _outcomes()[NON_QUALIFYING]
    offering = {r.key for r in LENDER_MATRIX if LoanType.FIX_AND_FLIP in r.loan_types}
    assert set(outcome.failure_codes_by_lender) == offering
    assert set(outcome.loan_type_not_offered) == {r.key for r in LENDER_MATRIX} - offering


def test_a_confirmed_lender_is_not_marked_subject_to_confirmation():
    confirmed = tuple(replace(r, verified=True, field_status={}) for r in LENDER_MATRIX)
    outcome = evaluate_profile(_profile("1_fix_and_flip"), matrix=confirmed)
    assert outcome.ranking and all(row.status == "confirmed" for row in outcome.ranking)


def test_demo_writes_three_watermarked_pdfs_and_a_json_report(tmp_path):
    import importlib.util
    import json
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "scripts" / "lending_prequal_demo.py"
    spec = importlib.util.spec_from_file_location("lending_prequal_demo", path)
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)

    demo.main(["--out", str(tmp_path)])
    pdfs = sorted(p.name for p in tmp_path.glob("*.pdf"))
    assert len(pdfs) == 3 and not any(p.startswith("4_") for p in pdfs)
    report = json.loads((tmp_path / "test2_report.json").read_text(encoding="utf-8"))
    by_name = {r["name"]: r for r in report}
    assert all(by_name[n]["qualifies"] for n in QUALIFYING) and not by_name[NON_QUALIFYING]["qualifies"]
    assert by_name[NON_QUALIFYING]["failure_codes"] == ["credit_below_min"]
    assert by_name["1_fix_and_flip"]["ranking"][0]["rank"] == 1
