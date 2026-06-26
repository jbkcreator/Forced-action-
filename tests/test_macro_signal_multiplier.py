"""Tests for A7 FRED macro-signal → CDS distress multiplier integration.

All tests use mocked DB sessions — no real Postgres required.

Run:
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest tests/test_macro_signal_multiplier.py -v
"""
from __future__ import annotations

import json
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.services.macro_signal_multiplier_service import (
    _load_rules,
    apply_macro_multiplier,
    get_latest_mortgage_rate_context,
    get_macro_distress_multipliers,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_session(rate: float | None = None) -> MagicMock:
    """Return a mock Session whose macro_signals query returns the given rate."""
    session = MagicMock()
    if rate is None:
        session.execute.return_value.mappings.return_value.first.return_value = None
    else:
        row = {"value": str(rate), "observed_at": date(2026, 6, 18), "source_series_id": "MORTGAGE30US"}
        session.execute.return_value.mappings.return_value.first.return_value = row
    return session


def _patch_rules(rules: dict):
    """Context manager that replaces the cached _load_rules return value."""
    _load_rules.cache_clear()
    return patch(
        "src.services.macro_signal_multiplier_service._load_rules",
        return_value=rules,
    )


_DEFAULT_RULES = {
    "mortgage_30y_fixed": {
        "source": "fred",
        "series_id": "MORTGAGE30US",
        "signal_key": "mortgage_30y_fixed",
        "high_rate_threshold": 6.5,
        "max_multiplier": 1.15,
        "affected_signals": {
            "foreclosures": 1.10,
            "tax_delinquencies": 1.08,
            "loan_lane_refi_risk": 1.15,
        },
    }
}


# ---------------------------------------------------------------------------
# Test 1: No macro data → neutral multipliers (empty dict)
# ---------------------------------------------------------------------------

class TestNoMacroData:
    def test_no_row_returns_empty_dict(self):
        session = _mock_session(rate=None)
        with _patch_rules(_DEFAULT_RULES):
            result = get_macro_distress_multipliers(session)
        assert result == {}

    def test_no_row_means_apply_is_noop(self):
        """apply_macro_multiplier with empty multipliers leaves base unchanged."""
        assert apply_macro_multiplier("foreclosures", 68.0, {}) == 68.0
        assert apply_macro_multiplier("tax_delinquencies", 70.0, {}) == 70.0


# ---------------------------------------------------------------------------
# Test 2: Rate below threshold → neutral multipliers
# ---------------------------------------------------------------------------

class TestRateBelowThreshold:
    def test_rate_below_returns_empty_dict(self):
        session = _mock_session(rate=5.9)
        with _patch_rules(_DEFAULT_RULES):
            result = get_macro_distress_multipliers(session)
        assert result == {}

    def test_just_below_threshold_is_neutral(self):
        session = _mock_session(rate=6.49)
        with _patch_rules(_DEFAULT_RULES):
            result = get_macro_distress_multipliers(session)
        assert result == {}


# ---------------------------------------------------------------------------
# Test 3: Rate at threshold → multiplier applies
# ---------------------------------------------------------------------------

class TestRateAtThreshold:
    def test_rate_exactly_at_threshold_activates(self):
        session = _mock_session(rate=6.5)
        with _patch_rules(_DEFAULT_RULES):
            result = get_macro_distress_multipliers(session)
        assert "foreclosures" in result
        assert "tax_delinquencies" in result
        assert result["foreclosures"] == pytest.approx(1.10)
        assert result["tax_delinquencies"] == pytest.approx(1.08)

    def test_apply_multiplier_at_threshold(self):
        mults = {"foreclosures": 1.10}
        boosted = apply_macro_multiplier("foreclosures", 68.0, mults)
        assert boosted == pytest.approx(68.0 * 1.10)

    def test_apply_tax_delinquency_multiplier(self):
        mults = {"tax_delinquencies": 1.08}
        boosted = apply_macro_multiplier("tax_delinquencies", 70.0, mults)
        assert boosted == pytest.approx(70.0 * 1.08)


# ---------------------------------------------------------------------------
# Test 4: Rate above threshold → multiplier applies but is clamped to max
# ---------------------------------------------------------------------------

class TestRateAboveThreshold:
    def test_rate_above_threshold_activates(self):
        session = _mock_session(rate=7.5)
        with _patch_rules(_DEFAULT_RULES):
            result = get_macro_distress_multipliers(session)
        assert result["foreclosures"] == pytest.approx(1.10)
        assert result["tax_delinquencies"] == pytest.approx(1.08)

    def test_configured_mult_clamped_to_max(self):
        """If config has a multiplier > max_multiplier, it is clamped."""
        rules = {
            "mortgage_30y_fixed": {
                **_DEFAULT_RULES["mortgage_30y_fixed"],
                "max_multiplier": 1.05,
                "affected_signals": {"foreclosures": 1.99},  # way over max
            }
        }
        session = _mock_session(rate=7.0)
        with _patch_rules(rules):
            result = get_macro_distress_multipliers(session)
        assert result["foreclosures"] == pytest.approx(1.05)

    def test_apply_result_clamped_to_100(self):
        """apply_macro_multiplier never exceeds 100 regardless of base or mult."""
        clamped = apply_macro_multiplier("foreclosures", 95.0, {"foreclosures": 1.15})
        assert clamped == pytest.approx(100.0)

    def test_apply_result_never_below_zero(self):
        clamped = apply_macro_multiplier("foreclosures", -10.0, {"foreclosures": 1.10})
        assert clamped == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Test 5 & 6: Only configured signal types are affected
# ---------------------------------------------------------------------------

class TestSignalSelectivity:
    def test_only_affected_signals_in_result(self):
        session = _mock_session(rate=7.0)
        with _patch_rules(_DEFAULT_RULES):
            result = get_macro_distress_multipliers(session)
        assert set(result.keys()) == {"foreclosures", "tax_delinquencies", "loan_lane_refi_risk"}

    def test_non_configured_signal_unchanged(self):
        mults = {"foreclosures": 1.10, "tax_delinquencies": 1.08}
        # probate is not in the affected_signals config
        assert apply_macro_multiplier("probate", 70.0, mults) == pytest.approx(70.0)

    def test_bankruptcy_unchanged(self):
        mults = {"foreclosures": 1.10}
        assert apply_macro_multiplier("bankruptcy", 55.0, mults) == pytest.approx(55.0)

    def test_code_violations_unchanged(self):
        mults = {"foreclosures": 1.10, "tax_delinquencies": 1.08}
        assert apply_macro_multiplier("code_violations", 75.0, mults) == pytest.approx(75.0)

    def test_multiplier_one_is_noop(self):
        """A multiplier of exactly 1.0 is treated as neutral."""
        assert apply_macro_multiplier("foreclosures", 68.0, {"foreclosures": 1.0}) == pytest.approx(68.0)


# ---------------------------------------------------------------------------
# Test 7: Missing or malformed config falls back safely
# ---------------------------------------------------------------------------

class TestConfigFallback:
    def test_empty_config_returns_neutral(self):
        session = _mock_session(rate=7.0)
        with _patch_rules({}):
            result = get_macro_distress_multipliers(session)
        assert result == {}

    def test_missing_affected_signals_returns_neutral(self):
        rules = {
            "mortgage_30y_fixed": {
                "high_rate_threshold": 6.5,
                "max_multiplier": 1.15,
                "affected_signals": {},
            }
        }
        session = _mock_session(rate=7.0)
        with _patch_rules(rules):
            result = get_macro_distress_multipliers(session)
        assert result == {}

    def test_db_exception_returns_neutral(self):
        session = MagicMock()
        session.execute.side_effect = Exception("DB unavailable")
        with _patch_rules(_DEFAULT_RULES):
            result = get_macro_distress_multipliers(session)
        assert result == {}

    def test_missing_config_file_returns_neutral(self, tmp_path):
        """_load_rules returns {} if config file is missing."""
        _load_rules.cache_clear()
        with patch(
            "src.services.macro_signal_multiplier_service._CONFIG_PATH",
            tmp_path / "nonexistent.json",
        ):
            _load_rules.cache_clear()
            from src.services.macro_signal_multiplier_service import _load_rules as lr
            lr.cache_clear()
            result = lr()
        assert result == {}

    def test_malformed_json_returns_neutral(self, tmp_path):
        """_load_rules returns {} if JSON is not a dict."""
        bad = tmp_path / "macro_signal_rules.json"
        bad.write_text("[1, 2, 3]")
        with patch("src.services.macro_signal_multiplier_service._CONFIG_PATH", bad):
            _load_rules.cache_clear()
            from src.services.macro_signal_multiplier_service import _load_rules as lr
            lr.cache_clear()
            result = lr()
        assert result == {}


# ---------------------------------------------------------------------------
# Test 8: Existing CDS scoring path — multiplier integrates correctly
# ---------------------------------------------------------------------------

class TestCDSIntegration:
    def test_neutral_multipliers_leave_base_unchanged(self):
        """When no macro data: base weight is untouched through the whole chain."""
        base = 68.0
        result = apply_macro_multiplier("foreclosures", base, {})
        assert result == pytest.approx(base)

    def test_active_multiplier_boosts_base(self):
        base = 68.0
        mults = {"foreclosures": 1.10}
        result = apply_macro_multiplier("foreclosures", base, mults)
        assert result > base
        assert result == pytest.approx(68.0 * 1.10)

    def test_get_latest_mortgage_rate_context_returns_none_when_empty(self):
        session = _mock_session(rate=None)
        ctx = get_latest_mortgage_rate_context(session)
        assert ctx is None

    def test_get_latest_mortgage_rate_context_returns_value(self):
        session = _mock_session(rate=6.72)
        ctx = get_latest_mortgage_rate_context(session)
        assert ctx is not None
        assert ctx["value"] == pytest.approx(6.72)
        assert ctx["series_id"] == "MORTGAGE30US"

    def test_full_chain_no_data(self):
        """Full chain: no DB row → get_macro_distress_multipliers → apply → unchanged."""
        session = _mock_session(rate=None)
        with _patch_rules(_DEFAULT_RULES):
            mults = get_macro_distress_multipliers(session)
        assert apply_macro_multiplier("foreclosures", 68.0, mults) == pytest.approx(68.0)
        assert apply_macro_multiplier("tax_delinquencies", 70.0, mults) == pytest.approx(70.0)

    def test_full_chain_high_rate(self):
        """Full chain: rate above threshold → boosted weights for configured signals."""
        session = _mock_session(rate=7.1)
        with _patch_rules(_DEFAULT_RULES):
            mults = get_macro_distress_multipliers(session)
        foreclosure_boosted = apply_macro_multiplier("foreclosures", 68.0, mults)
        tax_boosted         = apply_macro_multiplier("tax_delinquencies", 70.0, mults)
        probate_unchanged   = apply_macro_multiplier("probate", 70.0, mults)

        assert foreclosure_boosted == pytest.approx(68.0 * 1.10)
        assert tax_boosted         == pytest.approx(70.0 * 1.08)
        assert probate_unchanged   == pytest.approx(70.0)
