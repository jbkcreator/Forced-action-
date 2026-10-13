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
    BindingCap,
    BorrowerProfile,
    DeclineReason,
    FieldStatus,
    LenderFit,
    LenderFitEvaluator,
    LenderFitResult,
    LenderMiss,
    LenderPossible,
    LendingFlowLeadCreated,
    LoanRequest,
    LoanType,
    RoutingTag,
    SizingFlag,
)
from src.lending.fakes import FakeLenderFitEvaluator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# New enumerations (G1, G2, G3 gap-fix)
# ---------------------------------------------------------------------------

class TestFieldStatus:
    def test_three_values(self):
        assert FieldStatus.CONFIRMED.value == "confirmed"
        assert FieldStatus.PLACEHOLDER.value == "placeholder"
        assert FieldStatus.CONFIRM_AT_SUBMISSION.value == "confirm_at_submission"

    def test_is_str_enum(self):
        assert isinstance(FieldStatus.CONFIRMED, str)


class TestDeclineReason:
    def test_joshes_exact_list(self):
        """Every code Josh listed (Oct 8 addendum) must be present."""
        expected = {
            "credit_below_min", "experience_below_min", "ltv_over_max",
            "ltc_over_max", "loan_too_small", "loan_too_large",
            "property_type_excluded", "state_not_covered", "owner_occupied",
            "seat_cap_reached", "borrower_unresponsive", "borrower_withdrew",
            "docs_incomplete", "other",
        }
        actual = {r.value for r in DeclineReason}
        assert expected == actual

    def test_is_str_enum(self):
        assert isinstance(DeclineReason.LOAN_TOO_SMALL, str)


class TestBindingCap:
    def test_four_caps(self):
        assert {c.value for c in BindingCap} == {"requested", "ltc", "arv", "max_loan"}


class TestSizingFlag:
    def test_two_flags(self):
        assert SizingFlag.UPSIDE_DOWN.value == "upside_down"
        assert SizingFlag.ARV_OVER_3X.value == "arv_over_3x"


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

class TestLenderFit:
    def test_subject_to_confirmation_default_false(self):
        fit = LenderFit(lender_key="rcn_capital", lender_name="RCN Capital")
        assert fit.subject_to_confirmation is False

    def test_subject_to_confirmation_set(self):
        fit = LenderFit(
            lender_key="generic_backflip",
            lender_name="Backflip (Generic)",
            subject_to_confirmation=True,
        )
        assert fit.subject_to_confirmation is True


class TestLenderMissReasonCodes:
    def test_reason_codes_default_empty(self):
        miss = LenderMiss(lender_key="rcn", lender_name="RCN", reasons=["too small"])
        assert miss.reason_codes == []

    def test_reason_codes_set(self):
        miss = LenderMiss(
            lender_key="rcn",
            lender_name="RCN",
            reasons=["Loan amount below minimum"],
            reason_codes=[DeclineReason.LOAN_TOO_SMALL],
        )
        assert DeclineReason.LOAN_TOO_SMALL in miss.reason_codes


class TestLenderPossible:
    def test_construct(self):
        p = LenderPossible(
            lender_key="easy_street",
            lender_name="Easy Street Capital",
            why_unknown=["credit band unknown — caller did not capture above/below 640"],
        )
        assert p.lender_key == "easy_street"
        assert len(p.why_unknown) == 1

    def test_frozen(self):
        p = LenderPossible(lender_key="x", lender_name="X", why_unknown=["unknown"])
        with pytest.raises(Exception):
            p.lender_key = "y"  # type: ignore[misc]


class TestLenderFitResult:
    def test_empty_result(self):
        r = LenderFitResult()
        assert r.fitting == []
        assert r.possible == []
        assert r.non_fitting == []
        assert r.missing_fields == []
        assert r.lender_fit_score is None

    def test_three_buckets_independent(self):
        r = LenderFitResult(
            fitting=[LenderFit(lender_key="rcn", lender_name="RCN Capital")],
            possible=[LenderPossible(lender_key="easy", lender_name="Easy Street",
                                     why_unknown=["credit unknown"])],
            non_fitting=[LenderMiss(lender_key="kiavi", lender_name="Kiavi",
                                    reasons=["FICO 620 below 640 floor"],
                                    reason_codes=[DeclineReason.CREDIT_BELOW_MIN])],
        )
        assert len(r.fitting) == 1
        assert len(r.possible) == 1
        assert len(r.non_fitting) == 1
        assert r.non_fitting[0].reason_codes == [DeclineReason.CREDIT_BELOW_MIN]

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
            created_at=datetime.now(timezone.utc),
        )
        defaults.update(kwargs)
        return LendingFlowLeadCreated(**defaults)

    def test_construct_minimal_no_email(self):
        """G4 (D-1): email is Optional; a lead without one is valid."""
        ev = self._event()
        assert ev.email is None
        assert ev.credit_band_min_fico is None
        assert ev.loan_amount is None

    def test_construct_with_email(self):
        ev = self._event(email="test@example.com")
        assert ev.email == "test@example.com"

    def test_vendor_lead_id_default_none(self):
        """G4 (D-1): vendor_lead_id is Optional; absent when schema pending."""
        ev = self._event()
        assert ev.vendor_lead_id is None

    def test_needs_review_default_false(self):
        ev = self._event()
        assert ev.needs_review is False

    def test_needs_review_set_when_no_vendor_id(self):
        """G4 (D-1): producer must set needs_review=True when vendor id missing."""
        ev = self._event(needs_review=True)
        assert ev.needs_review is True
        assert ev.vendor_lead_id is None  # both can coexist

    def test_with_vendor_lead_id(self):
        ev = self._event(vendor_lead_id="LF-12345", needs_review=False)
        assert ev.vendor_lead_id == "LF-12345"
        assert ev.needs_review is False

    def test_phone_not_in_repr(self):
        ev = self._event()
        assert "+17275550001" not in repr(ev)

    def test_email_not_in_repr(self):
        ev = self._event(email="test@example.com")
        assert "test@example.com" not in repr(ev)

    def test_json_round_trip(self):
        ev = self._event(
            email="test@example.com",
            vendor_lead_id="LF-99",
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
        assert ev2.vendor_lead_id == "LF-99"

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
