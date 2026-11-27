"""R1 1.3 D3: the money model gives no number while the client's inputs are missing, and the formula once they exist."""
from __future__ import annotations

from decimal import Decimal

import config.lending_money_model as mm


def test_no_value_while_points_and_close_rates_are_unset() -> None:
    assert mm.expected_fee_per_hour(loan_amount=300_000, product="flip", stage="SUBMITTED", backflip_fits=True) is None


def test_formula_once_inputs_exist(monkeypatch) -> None:
    monkeypatch.setitem(mm.POINTS, ("backflip", "flip"), Decimal(2))
    monkeypatch.setitem(mm.CLOSE_RATE_60D, "SUBMITTED", Decimal("0.5"))
    # $300k at 2 points = $6,000 fee; half of SUBMITTED files fund in 60 days; equal hours (1).
    assert mm.expected_fee_per_hour(loan_amount=300_000, product="flip", stage="SUBMITTED",
                                    backflip_fits=True) == Decimal(3000)
    assert mm.expected_fee_per_hour(loan_amount=None, product="flip", stage="SUBMITTED", backflip_fits=True) is None
