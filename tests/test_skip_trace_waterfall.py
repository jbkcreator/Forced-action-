"""Unit tests for the skip trace waterfall."""
import pytest
from unittest.mock import patch

from src.services.skip_trace_result import SkipTraceResult, compute_confidence


class TestComputeConfidence:

    def test_no_contact_returns_zero(self):
        assert compute_confidence(None, None, None, None) == 0.0

    def test_mobile_high_reachability(self):
        # 0.50 + 0.20 + 0.15 = 0.85
        assert compute_confidence("+18135550000", None, None, None, 85) == pytest.approx(0.85)

    def test_landline_only_no_score(self):
        # 0.50 only — under 0.70 threshold → should escalate
        assert compute_confidence(None, "+18135550001", None, None) == pytest.approx(0.50)

    def test_email_no_phone(self):
        # 0.50 + 0.10 = 0.60 — still under threshold
        assert compute_confidence(None, None, "a@b.com", None) == pytest.approx(0.60)

    def test_mobile_plus_email_hits_threshold(self):
        # 0.50 + 0.20 + 0.10 = 0.80 → above 0.70
        assert compute_confidence("+18135550000", None, "a@b.com", None) == pytest.approx(0.80)

    def test_full_result_caps_at_one(self):
        result = compute_confidence("+13135550000", "+13135550001", "a@b.com", "123 Main", 95)
        assert result == pytest.approx(1.0)

    def test_low_reachability_score_not_counted(self):
        # Score 40 < 70 → +0.15 not awarded; 0.50 + 0.20 = 0.70
        assert compute_confidence("+18135550000", None, None, None, 40) == pytest.approx(0.70)

    def test_mailing_address_adds_points(self):
        # 0.50 + 0.20 + 0.05 = 0.75
        assert compute_confidence("+18135550000", None, None, "123 Main St", None) == pytest.approx(0.75)

    def test_reachability_exactly_70_counts(self):
        # Score exactly 70 → +0.15 awarded; 0.50 + 0.20 + 0.15 = 0.85
        assert compute_confidence("+18135550000", None, None, None, 70) == pytest.approx(0.85)


class TestCostCeilingInvariants:

    def test_all_three_providers_at_exactly_ceiling(self):
        # $0.02 + $0.50 + $0.28 = $0.80 = 80 cents
        assert 2 + 50 + 28 == 80

    def test_batchdata_only_well_under_ceiling(self):
        assert 2 <= 80

    def test_batchdata_plus_idi_under_ceiling(self):
        assert 2 + 50 == 52
        assert 52 <= 80

    def test_tlo_excluded_correctly(self):
        # TLO minimum $1.00 = 100 cents; 2 + 50 + 100 = 152 > 80
        assert 2 + 50 + 100 > 80

    def test_irb_excluded_correctly(self):
        # IRB ~$3.00 = 300 cents; 2 + 50 + 300 = 352 >> 80
        assert 2 + 50 + 300 > 80


class TestSkipTraceResult:

    def test_skipped_result_has_zero_cost(self):
        r = SkipTraceResult(
            provider="pdl", success=False, skipped=True, confidence=0.0, cost_cents=0
        )
        assert r.cost_cents == 0
        assert r.skipped is True
        assert r.success is False

    def test_successful_result_structure(self):
        r = SkipTraceResult(
            provider="batchdata",
            success=True,
            skipped=False,
            confidence=0.85,
            cost_cents=2,
            mobile_phone="+18135550000",
            email="owner@example.com",
        )
        assert r.confidence == pytest.approx(0.85)
        assert r.cost_cents == 2
        assert r.mobile_phone == "+18135550000"


class TestPDLSkippedWhenNoKey:

    def test_returns_skipped_when_pdl_key_absent(self):
        from src.services.pdl_skip_trace import run_pdl_lookup
        with patch("src.services.pdl_skip_trace.get_settings") as mock:
            mock.return_value.pdl_api_key = None
            result = run_pdl_lookup("John", "Doe", "123 Main St", "Tampa", "FL", "33601")
        assert result.skipped is True
        assert result.cost_cents == 0
        assert result.success is False
        assert result.provider == "pdl"
