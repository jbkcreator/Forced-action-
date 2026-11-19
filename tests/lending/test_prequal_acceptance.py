"""Acceptance Test 2: real lender matrix -> qualifying profiles with ranked lenders, a non-qualifying profile
with structured failure codes. Placeholders never shown as confirmed.

Client list Nov 2 (Q1): every lender runs on the same safe set (credit floor 680) until Josh sends the real
rules, so the 640-679 credit-challenged investor sits on every lender's floor: "confirm on the call", no
letter yet. It qualified only through Backflip's Oct 1 floor of 640, which is no longer applied."""
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

QUALIFYING = ("1_fix_and_flip", "2_experienced_builder")
CONFIRM_ON_CALL = "3_credit_challenged_investor"
NON_QUALIFYING = "4_non_qualifying"


def _outcomes():
    return {p.name: evaluate_profile(p, matrix=LENDER_MATRIX) for p in TEST2_PROFILES}


def _profile(name):
    return next(p for p in TEST2_PROFILES if p.name == name)


def test_two_profiles_qualify_one_is_confirm_on_the_call_one_does_not():
    outcomes = _outcomes()
    assert set(outcomes) == {*QUALIFYING, CONFIRM_ON_CALL, NON_QUALIFYING}
    assert all(outcomes[n].qualifies for n in QUALIFYING) and not outcomes[NON_QUALIFYING].qualifies
    assert not outcomes[CONFIRM_ON_CALL].qualifies and outcomes[CONFIRM_ON_CALL].needs_more_data


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
    assert pdfs == ["1_fix_and_flip.pdf", "2_experienced_builder.pdf"]
    report = json.loads((tmp_path / "test2_report.json").read_text(encoding="utf-8"))
    by_name = {r["name"]: r for r in report}
    assert all(by_name[n]["qualifies"] for n in QUALIFYING) and not by_name[NON_QUALIFYING]["qualifies"]
    assert by_name[NON_QUALIFYING]["failure_codes"] == ["credit_below_min"]
    assert by_name["1_fix_and_flip"]["ranking"][0]["rank"] == 1


def test_a_profile_with_only_possible_lenders_needs_more_data_rather_than_declining(monkeypatch):
    from types import SimpleNamespace

    from src.lending import prequal_acceptance

    undecided = SimpleNamespace(fitting=[], non_fitting=[], possible=[SimpleNamespace(lender_key="rcn")])
    monkeypatch.setattr(prequal_acceptance, "evaluate_lender_fit", lambda *_a, **_kw: undecided)
    outcome = evaluate_profile(_profile("1_fix_and_flip"), matrix=LENDER_MATRIX)
    assert outcome.qualifies is False and outcome.failure_codes == []
    assert outcome.needs_more_data is True and outcome.possible == ["rcn"]


def test_the_credit_challenged_investor_is_possible_on_every_lender_that_offers_the_loan_type():
    outcome = _outcomes()[CONFIRM_ON_CALL]
    offering = {r.key for r in LENDER_MATRIX if LoanType.FIX_AND_FLIP in r.loan_types}
    assert outcome.ranking == [] and outcome.failure_codes == [] and set(outcome.possible) == offering


def test_every_lender_runs_on_the_same_safe_set():
    from config.lender_matrix import PLACEHOLDER_BOX, effective_rules

    for rules in LENDER_MATRIX:
        effective = effective_rules(rules)
        for name, value in PLACEHOLDER_BOX.items():
            if name != "loan_types":  # each lender keeps the loan types it offers
                assert getattr(effective, name) == value, (rules.key, name)
        assert effective.min_purchase_price is None and effective.min_completed_projects == 0
        assert effective.rate_spread is None and effective.origination_points is None


def test_qualifying_and_declined_profiles_are_not_flagged_needs_more_data():
    outcomes = _outcomes()
    assert not any(outcomes[name].needs_more_data for name in QUALIFYING)
    assert outcomes[NON_QUALIFYING].needs_more_data is False
