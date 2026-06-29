"""Tests for src/services/market_timing_config.py (Sprint 4.7 Phase 2).

All tests are pure unit tests — no DB, no file I/O against the real config.
"""
import json
from pathlib import Path

import pytest

from src.services.market_timing_config import (
    MULTIPLIER_MAX,
    MULTIPLIER_MIN,
    get_multipliers_for_pressure,
    load_market_timing_config,
    validate_market_timing_config,
)

# ---------------------------------------------------------------------------
# Minimal valid config fixture reused across tests
# ---------------------------------------------------------------------------

_VALID_CONFIG = {
    "version": "1.0",
    "signals": {
        "house_price_index": {
            "source": "fhfa",
            "signal_key": "house_price_index",
            "geography_scope": "metro",
            "geography_id": "county_msa_proxy",
            "max_multiplier": 1.15,
            "pressure_multipliers": {
                "elevated": {"foreclosures": 1.06, "tax_delinquencies": 1.05},
                "high":     {"foreclosures": 1.12, "tax_delinquencies": 1.10},
            },
        },
        "county_unemployment_rate": {
            "source": "bls",
            "signal_key": "county_unemployment_rate",
            "geography_scope": "county",
            "geography_id": "county_fips",
            "max_multiplier": 1.10,
            "pressure_multipliers": {
                "rising":   {"foreclosures": 1.04, "tax_delinquencies": 1.05},
                "elevated": {"foreclosures": 1.08, "tax_delinquencies": 1.10, "evictions": 1.05},
            },
        },
    },
}


# ---------------------------------------------------------------------------
# 1. load_market_timing_config: happy path
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_loads_valid_json_file(self, tmp_path):
        cfg_file = tmp_path / "market_timing_rules.json"
        cfg_file.write_text(json.dumps(_VALID_CONFIG), encoding="utf-8")
        result = load_market_timing_config(cfg_file)
        assert result["version"] == "1.0"
        assert "house_price_index" in result["signals"]

    def test_returns_default_when_file_missing(self, tmp_path):
        result = load_market_timing_config(tmp_path / "nonexistent.json")
        assert result == {"version": "unknown", "signals": {}}

    def test_returns_default_on_malformed_json(self, tmp_path):
        bad_file = tmp_path / "market_timing_rules.json"
        bad_file.write_text("{ not valid json }", encoding="utf-8")
        result = load_market_timing_config(bad_file)
        assert result == {"version": "unknown", "signals": {}}


# ---------------------------------------------------------------------------
# 2. get_multipliers_for_pressure: neutral / unknown paths
# ---------------------------------------------------------------------------

class TestGetMultipliersNeutral:
    def test_unknown_signal_key_returns_empty(self):
        result = get_multipliers_for_pressure("nonexistent_signal", "elevated", _VALID_CONFIG)
        assert result == {}

    def test_neutral_pressure_returns_empty_when_no_rule(self):
        # "neutral" is not in _VALID_CONFIG pressure_multipliers for house_price_index
        result = get_multipliers_for_pressure("house_price_index", "neutral", _VALID_CONFIG)
        assert result == {}

    def test_unknown_pressure_label_returns_empty(self):
        result = get_multipliers_for_pressure("house_price_index", "unknown", _VALID_CONFIG)
        assert result == {}


# ---------------------------------------------------------------------------
# 3. get_multipliers_for_pressure: correct values returned
# ---------------------------------------------------------------------------

class TestGetMultipliersValues:
    def test_elevated_hpi_returns_correct_multipliers(self):
        result = get_multipliers_for_pressure("house_price_index", "elevated", _VALID_CONFIG)
        assert result == {"foreclosures": 1.06, "tax_delinquencies": 1.05}

    def test_high_unemployment_returns_correct_multipliers(self):
        result = get_multipliers_for_pressure(
            "county_unemployment_rate", "elevated", _VALID_CONFIG
        )
        assert result["foreclosures"] == pytest.approx(1.08)
        assert result["tax_delinquencies"] == pytest.approx(1.10)
        assert result["evictions"] == pytest.approx(1.05)

    def test_multiplier_capped_at_signal_max(self):
        # Config with a multiplier above the signal's max_multiplier
        cfg = {
            "signals": {
                "house_price_index": {
                    "max_multiplier": 1.10,
                    "pressure_multipliers": {
                        "high": {"foreclosures": 1.15},  # exceeds max_multiplier
                    },
                }
            }
        }
        result = get_multipliers_for_pressure("house_price_index", "high", cfg)
        assert result["foreclosures"] == pytest.approx(1.10)  # clamped to max


# ---------------------------------------------------------------------------
# 4. validate_market_timing_config
# ---------------------------------------------------------------------------

class TestValidateConfig:
    def test_valid_config_returns_no_errors(self):
        errors = validate_market_timing_config(_VALID_CONFIG)
        assert errors == []

    def test_invalid_multiplier_range_is_flagged(self):
        cfg = {
            "signals": {
                "house_price_index": {
                    "max_multiplier": 1.15,
                    "pressure_multipliers": {
                        "elevated": {"foreclosures": 3.0},  # out of [1.0, 2.0]
                    },
                }
            }
        }
        errors = validate_market_timing_config(cfg)
        assert any("out of" in e for e in errors)

    def test_unknown_pressure_label_is_flagged(self):
        cfg = {
            "signals": {
                "house_price_index": {
                    "max_multiplier": 1.15,
                    "pressure_multipliers": {
                        "catastrophic": {"foreclosures": 1.10},  # not a valid label
                    },
                }
            }
        }
        errors = validate_market_timing_config(cfg)
        assert any("unknown pressure label" in e for e in errors)
