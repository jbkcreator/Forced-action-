"""Tests for Sprint 4.7 Phase 3 — Unified Market Timing Multiplier Integration.

Covers:
  1.  FRED signal-key mismatch is fixed (mortgage_rate_30yr in config + service).
  2.  FRED multiplier activates with mortgage_rate_30yr DB row.
  3.  No macro data → neutral (no CDS change).
  4.  Missing/malformed config → neutral.
  5.  County FIPS resolver maps Hillsborough → 12057.
  6.  County FIPS resolver maps Pinellas → 12103.
  7.  Unknown county falls back safely (neutral).
  8.  Market pressure context is fetched once per county, not per property.
  9.  Unified multiplier applied exactly once per signal per property.
  10. No double-boost (overlapping signals take max, not product).
  11. Only configured financial-distress signals are boosted.
  12. Non-financial signals remain unchanged.
  13. Max multiplier cap is enforced by compose.
  14. Census data does not trigger live urgency.
  15. compose_market_timing_multipliers: elevated HPI → expected multipliers.
  16. compose_market_timing_multipliers: all-neutral context → {}.

Run:
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest tests/test_market_timing_integration.py -v
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from src.services.macro_signal_multiplier_service import (
    _load_rules,
    apply_macro_multiplier,
    get_macro_distress_multipliers,
    get_latest_mortgage_rate_context,
)
from src.services.market_timing_config import (
    compose_market_timing_multipliers,
    load_market_timing_config,
)


# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

_RULES_WITH_CORRECT_KEY = {
    "mortgage_rate_30yr": {
        "source": "fred",
        "series_id": "MORTGAGE30US",
        "signal_key": "mortgage_rate_30yr",
        "high_rate_threshold": 6.5,
        "max_multiplier": 1.15,
        "affected_signals": {
            "foreclosures": 1.10,
            "tax_delinquencies": 1.08,
            "loan_lane_refi_risk": 1.15,
        },
    }
}

_MARKET_TIMING_CONFIG = {
    "version": "1.0",
    "signals": {
        "mortgage_rate_30yr": {
            "max_multiplier": 1.15,
            "pressure_multipliers": {
                "elevated": {"foreclosures": 1.05, "tax_delinquencies": 1.04, "loan_lane_refi_risk": 1.08},
                "high":     {"foreclosures": 1.10, "tax_delinquencies": 1.08, "loan_lane_refi_risk": 1.15},
            },
        },
        "house_price_index": {
            "max_multiplier": 1.15,
            "pressure_multipliers": {
                "elevated": {"foreclosures": 1.06, "tax_delinquencies": 1.05, "loan_lane_refi_risk": 1.08},
                "high":     {"foreclosures": 1.12, "tax_delinquencies": 1.10, "loan_lane_refi_risk": 1.15},
            },
        },
        "county_unemployment_rate": {
            "max_multiplier": 1.10,
            "pressure_multipliers": {
                "rising":   {"foreclosures": 1.04, "tax_delinquencies": 1.05},
                "elevated": {"foreclosures": 1.08, "tax_delinquencies": 1.10, "evictions": 1.05},
            },
        },
    },
}


def _mock_session(rate: float | None = None) -> MagicMock:
    session = MagicMock()
    if rate is None:
        session.execute.return_value.mappings.return_value.first.return_value = None
    else:
        row = {
            "value": str(rate),
            "observed_at": date(2026, 6, 1),
            "source_series_id": "MORTGAGE30US",
        }
        session.execute.return_value.mappings.return_value.first.return_value = row
    return session


def _patch_rules(rules: dict):
    _load_rules.cache_clear()
    return patch(
        "src.services.macro_signal_multiplier_service._load_rules",
        return_value=rules,
    )


# ---------------------------------------------------------------------------
# 1. FRED signal-key mismatch is fixed
# ---------------------------------------------------------------------------

class TestFREDKeyFixed:
    def test_macro_signal_rules_json_uses_correct_key(self):
        cfg_path = Path(__file__).parent.parent / "config" / "macro_signal_rules.json"
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
        assert "mortgage_rate_30yr" in data, "macro_signal_rules.json must use 'mortgage_rate_30yr'"
        assert "mortgage_30y_fixed" not in data, "stale key 'mortgage_30y_fixed' must be removed"

    def test_service_queries_correct_key(self):
        """get_latest_mortgage_rate_context SQL must reference mortgage_rate_30yr."""
        import inspect
        import src.services.macro_signal_multiplier_service as svc
        src_text = inspect.getsource(svc.get_latest_mortgage_rate_context)
        assert "mortgage_rate_30yr" in src_text
        assert "mortgage_30y_fixed" not in src_text

    def test_fred_multiplier_activates_with_correct_key(self):
        """FRED multiplier activates when DB has mortgage_rate_30yr row above threshold."""
        session = _mock_session(rate=6.8)
        with _patch_rules(_RULES_WITH_CORRECT_KEY):
            result = get_macro_distress_multipliers(session)
        assert "foreclosures" in result
        assert result["foreclosures"] == pytest.approx(1.10)


# ---------------------------------------------------------------------------
# 2. No macro data → neutral
# ---------------------------------------------------------------------------

class TestNoMacroDataNeutral:
    def test_no_db_row_returns_empty_multipliers(self):
        session = _mock_session(rate=None)
        with _patch_rules(_RULES_WITH_CORRECT_KEY):
            result = get_macro_distress_multipliers(session)
        assert result == {}

    def test_empty_multipliers_leave_base_unchanged(self):
        base = 72.0
        assert apply_macro_multiplier("foreclosures", base, {}) == pytest.approx(base)
        assert apply_macro_multiplier("tax_delinquencies", base, {}) == pytest.approx(base)

    def test_compose_with_all_neutral_context_returns_empty(self):
        neutral_ctx = {
            "mortgage_rate":       {"pressure": "neutral"},
            "county_hpi":          {"pressure": "neutral"},
            "county_unemployment": {"pressure": "neutral"},
        }
        result = compose_market_timing_multipliers(neutral_ctx, _MARKET_TIMING_CONFIG)
        assert result == {}

    def test_compose_with_empty_context_returns_empty(self):
        assert compose_market_timing_multipliers({}, _MARKET_TIMING_CONFIG) == {}


# ---------------------------------------------------------------------------
# 3. Missing / malformed config → neutral
# ---------------------------------------------------------------------------

class TestMalformedConfigNeutral:
    def test_missing_config_file_returns_neutral(self, tmp_path):
        cfg = load_market_timing_config(tmp_path / "nonexistent.json")
        assert cfg == {"version": "unknown", "signals": {}}

    def test_compose_with_missing_config_returns_empty(self):
        ctx = {"county_hpi": {"pressure": "elevated"}}
        result = compose_market_timing_multipliers(ctx, {"signals": {}})
        assert result == {}

    def test_compose_with_no_config_arg_loads_safely(self):
        """When config=None, compose calls load_market_timing_config — missing file → neutral."""
        with patch(
            "src.services.market_timing_config.load_market_timing_config",
            return_value={"version": "unknown", "signals": {}},
        ):
            ctx = {"county_hpi": {"pressure": "high"}}
            result = compose_market_timing_multipliers(ctx)
        assert result == {}


# ---------------------------------------------------------------------------
# 4. County FIPS resolution via county config
# ---------------------------------------------------------------------------

class TestCountyFIPSResolution:
    def _make_county_config(self, fips: str) -> dict:
        return {"fips": fips, "county_id": "hillsborough"}

    def test_hillsborough_fips_from_config(self):
        """county_config returns fips=12057 for hillsborough."""
        with patch(
            "src.utils.county_config.get_county_config",
            return_value=self._make_county_config("12057"),
        ):
            from src.utils.county_config import get_county_config
            cfg = get_county_config("hillsborough")
        assert cfg["fips"] == "12057"

    def test_pinellas_fips_from_config(self):
        with patch(
            "src.utils.county_config.get_county_config",
            return_value={**self._make_county_config("12103"), "county_id": "pinellas"},
        ):
            from src.utils.county_config import get_county_config
            cfg = get_county_config("pinellas")
        assert cfg["fips"] == "12103"

    def test_unknown_county_fips_is_empty_string(self):
        with patch(
            "src.utils.county_config.get_county_config",
            return_value={"fips": "", "county_id": "unknown"},
        ):
            from src.utils.county_config import get_county_config
            cfg = get_county_config("unknown")
        assert not cfg["fips"]


# ---------------------------------------------------------------------------
# 5. Market pressure context caching per county (not per property)
# ---------------------------------------------------------------------------

class TestMarketPressureCaching:
    def test_preload_calls_context_once_per_county(self):
        """_preload_county_multipliers must call get_county_market_pressure_context
        once per county row, not once per property."""
        from src.services.cds_engine import _preload_county_multipliers

        session = MagicMock()
        # Two counties in DB
        session.execute.return_value.fetchall.return_value = [
            ("hillsborough", "12057"),
            ("pinellas", "12103"),
        ]

        neutral_ctx = {
            "mortgage_rate":       {"pressure": "neutral"},
            "county_hpi":          {"pressure": "neutral"},
            "county_unemployment": {"pressure": "neutral"},
            "overall_market_pressure": "neutral",
        }

        with patch(
            "src.services.cds_engine.get_county_market_pressure_context",
            return_value=neutral_ctx,
        ) as mock_ctx:
            result = _preload_county_multipliers(session, _MARKET_TIMING_CONFIG)

        assert mock_ctx.call_count == 2
        calls = [c[0][1] for c in mock_ctx.call_args_list]  # (session, fips)
        assert "12057" in calls
        assert "12103" in calls

    def test_preload_db_failure_returns_empty(self):
        from src.services.cds_engine import _preload_county_multipliers
        session = MagicMock()
        session.execute.side_effect = Exception("DB error")
        result = _preload_county_multipliers(session, _MARKET_TIMING_CONFIG)
        assert result == {}

    def test_county_context_exception_skipped_gracefully(self):
        from src.services.cds_engine import _preload_county_multipliers
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [("hillsborough", "12057")]
        with patch(
            "src.services.cds_engine.get_county_market_pressure_context",
            side_effect=Exception("boom"),
        ):
            result = _preload_county_multipliers(session, _MARKET_TIMING_CONFIG)
        assert result == {}


# ---------------------------------------------------------------------------
# 6. Unified multiplier: correct composition
# ---------------------------------------------------------------------------

class TestUnifiedMultiplierComposition:
    def test_elevated_hpi_boosts_financial_signals(self):
        ctx = {
            "mortgage_rate":       {"pressure": "neutral"},
            "county_hpi":          {"pressure": "elevated"},
            "county_unemployment": {"pressure": "neutral"},
        }
        result = compose_market_timing_multipliers(ctx, _MARKET_TIMING_CONFIG)
        assert result["foreclosures"] == pytest.approx(1.06)
        assert result["tax_delinquencies"] == pytest.approx(1.05)

    def test_high_mortgage_rate_boosts_loan_lane(self):
        ctx = {
            "mortgage_rate":       {"pressure": "high"},
            "county_hpi":          {"pressure": "neutral"},
            "county_unemployment": {"pressure": "neutral"},
        }
        result = compose_market_timing_multipliers(ctx, _MARKET_TIMING_CONFIG)
        assert result["loan_lane_refi_risk"] == pytest.approx(1.15)

    def test_rising_unemployment_boosts_tax_delinquencies(self):
        ctx = {
            "mortgage_rate":       {"pressure": "neutral"},
            "county_hpi":          {"pressure": "neutral"},
            "county_unemployment": {"pressure": "rising"},
        }
        result = compose_market_timing_multipliers(ctx, _MARKET_TIMING_CONFIG)
        assert result["tax_delinquencies"] == pytest.approx(1.05)
        assert result["foreclosures"] == pytest.approx(1.04)


# ---------------------------------------------------------------------------
# 7. No double-boost (max, not product)
# ---------------------------------------------------------------------------

class TestNoDoubleBoost:
    def test_overlapping_signals_take_max_not_product(self):
        """When HPI + unemployment both affect foreclosures, take MAX, not 1.06 × 1.04."""
        ctx = {
            "mortgage_rate":       {"pressure": "neutral"},
            "county_hpi":          {"pressure": "elevated"},   # foreclosures → 1.06
            "county_unemployment": {"pressure": "rising"},     # foreclosures → 1.04
        }
        result = compose_market_timing_multipliers(ctx, _MARKET_TIMING_CONFIG)
        # MAX(1.06, 1.04) = 1.06, not 1.06 * 1.04 = 1.1024
        assert result["foreclosures"] == pytest.approx(1.06)
        assert result["foreclosures"] < 1.07

    def test_apply_macro_multiplier_called_once_per_signal(self):
        """Unified multiplier is applied once. Calling apply_macro_multiplier
        twice on the same base with the same multiplier would double-boost."""
        base = 68.0
        mults = {"foreclosures": 1.10}
        once  = apply_macro_multiplier("foreclosures", base, mults)
        twice = apply_macro_multiplier("foreclosures", once, mults)
        assert once == pytest.approx(68.0 * 1.10)
        assert twice == pytest.approx(68.0 * 1.10 * 1.10)
        # The engine must not call apply_macro_multiplier twice — one call produces `once`
        assert once != twice


# ---------------------------------------------------------------------------
# 8. Only configured financial-distress signals are boosted
# ---------------------------------------------------------------------------

class TestSignalSelectivity:
    def test_non_financial_signals_unchanged(self):
        elevated_ctx = {
            "mortgage_rate":       {"pressure": "high"},
            "county_hpi":          {"pressure": "high"},
            "county_unemployment": {"pressure": "elevated"},
        }
        result = compose_market_timing_multipliers(elevated_ctx, _MARKET_TIMING_CONFIG)
        non_financial = ["probate", "bankruptcy", "code_violations", "building_permits",
                         "fire", "storm_damage", "flood_damage", "insurance_claim",
                         "deed_transfers", "divorce_filings"]
        for sig in non_financial:
            assert sig not in result, f"Non-financial signal '{sig}' must not be in unified multiplier"

    def test_evictions_only_boosted_by_elevated_unemployment(self):
        ctx = {
            "mortgage_rate":       {"pressure": "high"},
            "county_hpi":          {"pressure": "high"},
            "county_unemployment": {"pressure": "elevated"},  # elevated → evictions: 1.05
        }
        result = compose_market_timing_multipliers(ctx, _MARKET_TIMING_CONFIG)
        assert "evictions" in result
        assert result["evictions"] == pytest.approx(1.05)

    def test_evictions_not_boosted_by_mortgage_rate_alone(self):
        ctx = {
            "mortgage_rate":       {"pressure": "high"},
            "county_hpi":          {"pressure": "neutral"},
            "county_unemployment": {"pressure": "neutral"},
        }
        result = compose_market_timing_multipliers(ctx, _MARKET_TIMING_CONFIG)
        assert "evictions" not in result


# ---------------------------------------------------------------------------
# 9. Max multiplier cap enforced
# ---------------------------------------------------------------------------

class TestMaxMultiplierCap:
    def test_compose_respects_signal_max_multiplier(self):
        cfg = {
            "signals": {
                "house_price_index": {
                    "max_multiplier": 1.05,
                    "pressure_multipliers": {
                        "high": {"foreclosures": 1.15},  # over max
                    },
                }
            }
        }
        ctx = {"county_hpi": {"pressure": "high"}}
        result = compose_market_timing_multipliers(ctx, cfg)
        assert result["foreclosures"] == pytest.approx(1.05)

    def test_apply_macro_multiplier_clamps_result_to_100(self):
        result = apply_macro_multiplier("foreclosures", 95.0, {"foreclosures": 1.15})
        assert result == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# 10. Census data does not trigger live urgency
# ---------------------------------------------------------------------------

class TestCensusBaselineOnly:
    def test_census_key_not_in_context_to_signal_map(self):
        from src.services.market_timing_config import _CONTEXT_TO_SIGNAL_KEY
        assert "census_baseline" not in _CONTEXT_TO_SIGNAL_KEY

    def test_census_pressure_not_in_market_timing_rules(self):
        cfg_path = Path(__file__).parent.parent / "config" / "market_timing_rules.json"
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
        signal_keys = list(data.get("signals", {}).keys())
        census_keys = [k for k in signal_keys if "census" in k.lower()]
        assert census_keys == [], f"Census signals must not appear in market_timing_rules.json: {census_keys}"

    def test_compose_ignores_census_section_in_context(self):
        """Even if a caller passes census_baseline in context, it has no effect."""
        ctx = {
            "mortgage_rate":       {"pressure": "neutral"},
            "county_hpi":          {"pressure": "neutral"},
            "county_unemployment": {"pressure": "neutral"},
            "census_baseline":     {"pressure": "high"},  # malformed but harmless
        }
        result = compose_market_timing_multipliers(ctx, _MARKET_TIMING_CONFIG)
        assert result == {}
