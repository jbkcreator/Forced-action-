"""T-07 seam to the T-05 Lender Box engine: which leads get a letter, and with which loan bounds."""
from decimal import Decimal

from config.lender_matrix import LenderRules
from src.lending.contracts import LoanType
from src.lending.prequal import FitLimits, PrequalLead
from src.lending.prequal_fit import evaluate_prequal_fit, get_fit_evaluator

PLACEHOLDER_BOX = LenderRules(
    key="placeholder_box",
    name="Placeholder box",
    verified=True,
    loan_types=frozenset({LoanType.FIX_AND_FLIP}),
    min_loan_amount=Decimal("75000"),
    max_loan_amount=Decimal("2000000"),
    credit_floor=680,
    approved_states=frozenset({"FL", "GA", "TX"}),
)
UNVERIFIED = LenderRules(key="unverified", name="Unverified", verified=False)


def _lead(**overrides) -> PrequalLead:
    values = dict(credit_band="740+", loan_amount=500_000, property_state="FL", loan_type="FIX_AND_FLIP")
    values.update(overrides)
    return PrequalLead(**values)


def test_fitting_lender_gives_its_loan_bounds():
    assert evaluate_prequal_fit(_lead(), (PLACEHOLDER_BOX,)) == [FitLimits(75_000, 2_000_000)]


def test_no_fit_when_state_not_covered():
    assert evaluate_prequal_fit(_lead(property_state="NY"), (PLACEHOLDER_BOX,)) == []


def test_no_fit_when_credit_band_below_floor():
    assert evaluate_prequal_fit(_lead(credit_band="640-679"), (PLACEHOLDER_BOX,)) == []


def test_no_fit_for_other_loan_type():
    assert evaluate_prequal_fit(_lead(loan_type="DSCR_RENTAL"), (PLACEHOLDER_BOX,)) == []


def test_unknown_loan_type_gets_no_fit_instead_of_an_error():
    assert evaluate_prequal_fit(_lead(loan_type="SOMETHING_ELSE"), (PLACEHOLDER_BOX,)) == []


def test_lowercase_state_and_loan_type_are_read():
    assert evaluate_prequal_fit(_lead(property_state=" fl ", loan_type="fix_and_flip"), (PLACEHOLDER_BOX,))


def test_lender_without_bounds_does_not_limit_the_range():
    open_box = LenderRules(key="open", name="Open", verified=True)
    [limits] = evaluate_prequal_fit(_lead(), (open_box,))
    assert limits.min_amount == 0 and limits.max_amount > 2_000_000


def test_seam_stays_closed_while_every_lender_is_unverified():
    assert get_fit_evaluator((UNVERIFIED,)) is None


def test_seam_opens_once_one_lender_is_usable():
    evaluator = get_fit_evaluator((UNVERIFIED, PLACEHOLDER_BOX))
    assert evaluator is not None
    assert evaluator(_lead()) == [FitLimits(75_000, 2_000_000)]


def test_live_matrix_keeps_letters_waiting_today():
    from config.lender_matrix import LENDER_MATRIX
    if not any(rules.verified for rules in LENDER_MATRIX):
        assert get_fit_evaluator() is None
