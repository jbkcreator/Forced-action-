"""T-04 acceptance tests — Lender Box + LendingFlow intake contracts.

Acceptance criteria (Chunk1 split):
  - Types are importable.
  - Fake evaluator unit test passes.
  - No logic present.

Run:
    pytest tests/lending/test_contracts.py -v
"""
from __future__ import annotations

import json
from decimal import Decimal
from uuid import uuid4
from datetime import datetime, timezone

import pytest

from src.lending.contracts import (
    BorrowerProfile,
    LenderFit,
    LenderFitEvaluator,
    LenderFitResult,
    LenderMiss,
    LendingFlowLeadCreated,
    LoanRequest,
    LoanType,
    RoutingTag,
)
from src.lending.fakes import FakeLenderFitEvaluator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class TestLoanType:
    def test_all_four_values_present(self):
        assert LoanType.FIX_AND_FLIP.value == "FIX_AND_FLIP"
        assert LoanType.GROUND_UP_CONSTRUCTION.value == "GROUND_UP_CONSTRUCTION"
        assert LoanType.DSCR_RENTAL.value == "DSCR_RENTAL"
        assert LoanType.BRIDGE.value == "BRIDGE"

    def test_is_str_enum(self):
        assert isinstance(LoanType.FIX_AND_FLIP, str)


class TestRoutingTag:
    def test_full_machine_and_nurture(self):
        assert RoutingTag.FULL_MACHINE.value == "FULL_MACHINE"
        assert RoutingTag.NURTURE.value == "NURTURE"


# ---------------------------------------------------------------------------
# BorrowerProfile
# ---------------------------------------------------------------------------

class TestBorrowerProfile:
    def test_all_none_is_valid(self):
        """Profile is built incrementally; all fields optional."""
        p = BorrowerProfile()
        assert p.credit_band_min_fico is None
        assert p.completed_projects is None

    def test_frozen(self):
        p = BorrowerProfile(completed_projects=2)
        with pytest.raises(Exception):
            p.completed_projects = 3  # type: ignore[misc]

    def test_no_numeric_score_field(self):
        """credit_band_min_fico is a band lower bound, never an exact score.
        The field must exist; there must be no 'fico_score' or 'credit_score'."""
        fields = BorrowerProfile.model_fields
        assert "credit_band_min_fico" in fields
        assert "fico_score" not in fields
        assert "credit_score" not in fields

    def test_ground_up_builds_field_present(self):
        p = BorrowerProfile(completed_projects=3, completed_ground_up_builds=1)
        assert p.completed_ground_up_builds == 1


# ---------------------------------------------------------------------------
# LoanRequest
# ---------------------------------------------------------------------------

class TestLoanRequest:
    def _minimal(self):
        return LoanRequest(
            loan_type=LoanType.FIX_AND_FLIP,
            loan_amount=Decimal("250000"),
            state="FL",
        )

    def test_minimal_construct(self):
        req = self._minimal()
        assert req.loan_type is LoanType.FIX_AND_FLIP
        assert req.loan_amount == Decimal("250000")
        assert req.property_address is None

    def test_money_is_decimal(self):
        req = self._minimal()
        assert isinstance(req.loan_amount, Decimal)

    def test_frozen(self):
        req = self._minimal()
        with pytest.raises(Exception):
            req.state = "GA"  # type: ignore[misc]

    def test_optional_fields_default_none(self):
        req = self._minimal()
        for field in ("property_type", "purchase_price", "rehab_budget",
                      "arv", "property_address", "target_close_date"):
            assert getattr(req, field) is None


# ---------------------------------------------------------------------------
# LenderFitResult
# ---------------------------------------------------------------------------

class TestLenderFitResult:
    def test_empty_result(self):
        r = LenderFitResult()
        assert r.fitting == []
        assert r.non_fitting == []
        assert r.missing_fields == []
        assert r.lender_fit_score is None

    def test_fitting_and_non_fitting_are_separate_lists(self):
        r = LenderFitResult(
            fitting=[LenderFit(lender_key="rcn", lender_name="RCN Capital")],
            non_fitting=[LenderMiss(lender_key="easy", lender_name="Easy Street",
                                    reasons=["FICO 620 below 640 floor"])],
        )
        assert len(r.fitting) == 1
        assert len(r.non_fitting) == 1
        assert r.non_fitting[0].reasons == ["FICO 620 below 640 floor"]

    def test_reasons_are_strings(self):
        miss = LenderMiss(
            lender_key="kiavi",
            lender_name="Kiavi Affiliate",
            reasons=["Loan amount $350K below $500K construction minimum"],
        )
        assert all(isinstance(r, str) for r in miss.reasons)

    def test_frozen(self):
        r = LenderFitResult()
        with pytest.raises(Exception):
            r.lender_fit_score = Decimal("80")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# LendingFlowLeadCreated
# ---------------------------------------------------------------------------

class TestLendingFlowLeadCreated:
    def _event(self, **kwargs):
        defaults = dict(
            lead_id=uuid4(),
            phone="+17275550001",
            email="test@example.com",
            created_at=datetime.now(timezone.utc),
        )
        defaults.update(kwargs)
        return LendingFlowLeadCreated(**defaults)

    def test_construct_minimal(self):
        ev = self._event()
        assert ev.credit_band_min_fico is None
        assert ev.loan_amount is None

    def test_phone_not_in_repr(self):
        ev = self._event()
        assert "+17275550001" not in repr(ev)

    def test_email_not_in_repr(self):
        ev = self._event()
        assert "test@example.com" not in repr(ev)

    def test_json_round_trip(self):
        ev = self._event(
            credit_band_min_fico=640,
            loan_amount=Decimal("300000"),
            state="FL",
            loan_type=LoanType.FIX_AND_FLIP,
        )
        data = json.loads(ev.model_dump_json())
        ev2 = LendingFlowLeadCreated.model_validate(data)
        assert ev2.lead_id == ev.lead_id
        assert ev2.loan_type is LoanType.FIX_AND_FLIP
        assert ev2.loan_amount == Decimal("300000")

    def test_frozen(self):
        ev = self._event()
        with pytest.raises(Exception):
            ev.state = "GA"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# FakeLenderFitEvaluator
# ---------------------------------------------------------------------------

class TestFakeLenderFitEvaluator:
    def test_satisfies_protocol(self):
        assert isinstance(FakeLenderFitEvaluator(), LenderFitEvaluator)

    def test_default_result_has_one_fitting_lender(self):
        ev = FakeLenderFitEvaluator()
        result = ev.evaluate(BorrowerProfile(), LoanRequest(
            loan_type=LoanType.FIX_AND_FLIP,
            loan_amount=Decimal("200000"),
            state="FL",
        ))
        assert len(result.fitting) == 1

    def test_custom_result_returned_unchanged(self):
        custom = LenderFitResult(missing_fields=["arv"])
        ev = FakeLenderFitEvaluator(custom)
        result = ev.evaluate(BorrowerProfile(), LoanRequest(
            loan_type=LoanType.BRIDGE,
            loan_amount=Decimal("150000"),
            state="GA",
        ))
        assert result is custom

    def test_different_inputs_same_fake_result(self):
        """Fake ignores inputs — callers control the result via the constructor."""
        ev = FakeLenderFitEvaluator()
        r1 = ev.evaluate(BorrowerProfile(completed_projects=0), LoanRequest(
            loan_type=LoanType.DSCR_RENTAL, loan_amount=Decimal("100000"), state="FL",
        ))
        r2 = ev.evaluate(BorrowerProfile(completed_projects=10), LoanRequest(
            loan_type=LoanType.GROUND_UP_CONSTRUCTION, loan_amount=Decimal("900000"), state="FL",
        ))
        assert r1 is r2

    def test_no_logic_in_evaluate(self):
        """Confirm there is no conditional logic inside the fake's evaluate().

        The Fake is not a linter check on the protocol — it just returns its
        fixed result.  This test drives a pathological input (amounts of 0) and
        confirms no exception is raised, because the Fake does not inspect values.
        """
        ev = FakeLenderFitEvaluator()
        result = ev.evaluate(
            BorrowerProfile(credit_band_min_fico=0),
            LoanRequest(loan_type=LoanType.BRIDGE, loan_amount=Decimal("0"), state="XX"),
        )
        assert isinstance(result, LenderFitResult)
