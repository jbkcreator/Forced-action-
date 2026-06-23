"""B1 / M9 Revenue Engine — pure-logic unit tests (no DB).

Behavior 1: MRR normalization (§12.7).
"""

import pytest

from src.services.revenue_engine import normalize_mrr_cents, classify_movement


class TestNormalizeMrrCents:
    def test_monthly_is_unchanged(self):
        assert normalize_mrr_cents(29900, "monthly") == 29900

    def test_annual_is_divided_by_twelve(self):
        # $3,600/yr -> $300/mo run-rate
        assert normalize_mrr_cents(360000, "annual") == 30000

    def test_one_time_is_excluded(self):
        assert normalize_mrr_cents(9900, "one_time") == 0

    def test_trial_is_zero(self):
        assert normalize_mrr_cents(0, "trial") == 0


class TestClassifyMovement:
    def test_zero_to_positive_is_new(self):
        assert classify_movement(0, 29900) == "new"

    def test_increase_is_expansion(self):
        assert classify_movement(29900, 49900) == "expansion"

    def test_decrease_still_paying_is_contraction(self):
        assert classify_movement(49900, 29900) == "contraction"

    def test_positive_to_zero_is_churn(self):
        assert classify_movement(29900, 0) == "churn"

    def test_no_change_is_none(self):
        assert classify_movement(29900, 29900) is None

    def test_zero_to_zero_is_none(self):
        assert classify_movement(0, 0) is None
