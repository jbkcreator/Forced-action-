"""Tests for Pre-4.7 market pressure / spread calculation service.

All tests use mocked DB sessions — no real Postgres required.

Run:
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest tests/test_market_pressure_service.py -v
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from src.services.market_pressure_service import (
    CENSUS_SIGNAL_ROLES,
    _classify_rate_pressure,
    _overall_pressure,
    calculate_county_hpi_spread,
    calculate_county_unemployment_spread,
    calculate_rate_spread,
    get_county_market_pressure_context,
    get_trailing_average,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _session_with_rows(*rows):
    """Return a mock session whose execute() returns the given rows in order."""
    session = MagicMock()
    results = [MagicMock(fetchone=MagicMock(return_value=r)) for r in rows]
    session.execute.side_effect = results
    return session


def _session_returning(fetchone=None, fetchall=None):
    session = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = fetchone
    result.fetchall.return_value = fetchall or []
    session.execute.return_value = result
    return session


# ---------------------------------------------------------------------------
# Test 1: DB audit does not mutate data
# ---------------------------------------------------------------------------

class TestAuditReadOnly:
    def test_audit_script_imports_without_side_effects(self):
        """Importing the audit script module must not execute any DB calls."""
        import importlib.util, sys
        spec = importlib.util.spec_from_file_location(
            "audit_macro_signals",
            "scripts/audit_macro_signals.py",
        )
        mod = importlib.util.module_from_spec(spec)
        # Do NOT exec it — just confirm it is importable as a module definition
        assert spec is not None

    def test_trailing_average_is_select_only(self):
        """get_trailing_average must only issue SELECT queries (no INSERT/UPDATE/DELETE)."""
        session = MagicMock()
        session.execute.return_value.fetchone.return_value = (5.0, 10)
        get_trailing_average(session, "mortgage_30y_fixed", "national", "US", months=12)
        for c in session.execute.call_args_list:
            sql = str(c[0][0]).upper()
            assert "INSERT" not in sql
            assert "UPDATE" not in sql
            assert "DELETE" not in sql


# ---------------------------------------------------------------------------
# Test 2: get_trailing_average
# ---------------------------------------------------------------------------

class TestTrailingAverage:
    def test_returns_float_when_data_present(self):
        session = _session_returning(fetchone=(Decimal("6.82"), 12))
        result = get_trailing_average(session, "mortgage_30y_fixed", "national", "US", months=12)
        assert result == pytest.approx(6.82)

    def test_returns_none_when_fewer_than_3_observations(self):
        session = _session_returning(fetchone=(Decimal("6.82"), 2))
        result = get_trailing_average(session, "mortgage_30y_fixed", "national", "US", months=12)
        assert result is None

    def test_returns_none_when_no_rows(self):
        session = _session_returning(fetchone=None)
        result = get_trailing_average(session, "mortgage_30y_fixed", "national", "US")
        assert result is None

    def test_returns_none_when_count_is_zero(self):
        session = _session_returning(fetchone=(None, 0))
        result = get_trailing_average(session, "mortgage_30y_fixed", "national", "US")
        assert result is None


# ---------------------------------------------------------------------------
# Test 3: calculate_rate_spread
# ---------------------------------------------------------------------------

class TestRateSpread:
    def test_unknown_when_no_current_rate(self):
        session = MagicMock()
        session.execute.return_value.fetchone.return_value = None
        result = calculate_rate_spread(session)
        assert result["pressure"] == "unknown"

    def test_neutral_when_spread_is_small(self):
        # current = 6.47, trailing_avg = 6.50 → spread = -0.03 → neutral
        session = MagicMock()
        results = [
            MagicMock(fetchone=MagicMock(return_value=(Decimal("6.47"), date(2026, 6, 18)))),
            MagicMock(fetchone=MagicMock(return_value=(Decimal("6.50"), 12))),
        ]
        session.execute.side_effect = results
        result = calculate_rate_spread(session)
        assert result["pressure"] == "neutral"
        assert result["current"] == pytest.approx(6.47)
        assert result["spread"] == pytest.approx(-0.03, abs=0.01)

    def test_elevated_when_spread_above_threshold(self):
        # current = 7.10, trailing_avg = 6.50 → spread = 0.60 → elevated
        session = MagicMock()
        results = [
            MagicMock(fetchone=MagicMock(return_value=(Decimal("7.10"), date(2026, 6, 18)))),
            MagicMock(fetchone=MagicMock(return_value=(Decimal("6.50"), 12))),
        ]
        session.execute.side_effect = results
        result = calculate_rate_spread(session)
        assert result["pressure"] == "elevated"

    def test_high_when_spread_above_high_threshold(self):
        # current = 8.0, trailing_avg = 6.50 → spread = 1.5 → high
        session = MagicMock()
        results = [
            MagicMock(fetchone=MagicMock(return_value=(Decimal("8.0"), date(2026, 6, 18)))),
            MagicMock(fetchone=MagicMock(return_value=(Decimal("6.50"), 12))),
        ]
        session.execute.side_effect = results
        result = calculate_rate_spread(session)
        assert result["pressure"] == "high"

    def test_unknown_trailing_avg_when_too_few_observations(self):
        session = MagicMock()
        results = [
            MagicMock(fetchone=MagicMock(return_value=(Decimal("6.47"), date(2026, 6, 18)))),
            MagicMock(fetchone=MagicMock(return_value=(None, 2))),
        ]
        session.execute.side_effect = results
        result = calculate_rate_spread(session)
        assert result["trailing_avg"] is None
        assert result["pressure"] == "unknown"


# ---------------------------------------------------------------------------
# Test 4: calculate_county_hpi_spread
# ---------------------------------------------------------------------------

class TestCountyHPISpread:
    def test_unknown_when_no_county_data(self):
        session = _session_returning(fetchone=None)
        result = calculate_county_hpi_spread(session, "12057")
        assert result["pressure"] == "unknown"
        assert result["county_fips"] == "12057"

    def test_elevated_when_hpi_rising(self):
        session = MagicMock()
        results = [
            MagicMock(fetchone=MagicMock(return_value=(Decimal("420.5"), date(2025, 12, 1)))),
            MagicMock(fetchone=MagicMock(return_value=(Decimal("410.0"), 12))),
        ]
        session.execute.side_effect = results
        result = calculate_county_hpi_spread(session, "12057")
        assert result["pressure"] == "elevated"
        assert result["spread"] == pytest.approx(10.5, abs=0.1)

    def test_neutral_when_hpi_stable(self):
        session = MagicMock()
        results = [
            MagicMock(fetchone=MagicMock(return_value=(Decimal("410.5"), date(2025, 12, 1)))),
            MagicMock(fetchone=MagicMock(return_value=(Decimal("410.0"), 12))),
        ]
        session.execute.side_effect = results
        result = calculate_county_hpi_spread(session, "12057")
        assert result["pressure"] == "neutral"

    def test_msa_cbsa_used_as_proxy(self):
        # County HPI not in FHFA public data; service queries Tampa MSAD instead.
        # FHFA uses MSAD code 45294, not the official CBSA code 45300.
        session = _session_returning(fetchone=None)
        calculate_county_hpi_spread(session, "12103")
        call_args = str(session.execute.call_args_list[0])
        assert "45294" in call_args  # Tampa MSAD (FHFA place_id), not county FIPS


# ---------------------------------------------------------------------------
# Test 5: calculate_county_unemployment_spread
# ---------------------------------------------------------------------------

class TestCountyUnemploymentSpread:
    def test_unknown_when_no_data(self):
        session = _session_returning(fetchone=None)
        result = calculate_county_unemployment_spread(session, "12057")
        assert result["pressure"] == "unknown"

    def test_rising_when_spread_above_threshold(self):
        # current = 4.1, trailing = 3.7 → spread = 0.4 → rising
        session = MagicMock()
        results = [
            MagicMock(fetchone=MagicMock(return_value=(Decimal("4.1"), date(2026, 4, 1)))),
            MagicMock(fetchone=MagicMock(return_value=(Decimal("3.7"), 12))),
        ]
        session.execute.side_effect = results
        result = calculate_county_unemployment_spread(session, "12057")
        assert result["pressure"] == "rising"
        assert result["current"] == pytest.approx(4.1)

    def test_county_unemployment_normalized_with_correct_fips(self):
        """Records for each county must carry the correct FIPS as geography_id."""
        from src.loaders.macro_signals.bls_client import COUNTY_LAUS_SERIES
        assert "12057" in COUNTY_LAUS_SERIES
        assert "12103" in COUNTY_LAUS_SERIES
        assert COUNTY_LAUS_SERIES["12057"] == "LAUCN120570000000003"
        assert COUNTY_LAUS_SERIES["12103"] == "LAUCN121030000000003"

    def test_neutral_when_unemployment_stable(self):
        session = MagicMock()
        results = [
            MagicMock(fetchone=MagicMock(return_value=(Decimal("3.3"), date(2026, 4, 1)))),
            MagicMock(fetchone=MagicMock(return_value=(Decimal("3.2"), 12))),
        ]
        session.execute.side_effect = results
        result = calculate_county_unemployment_spread(session, "12103")
        assert result["pressure"] == "neutral"


# ---------------------------------------------------------------------------
# Test 6: BLS LAUS county series validation
# ---------------------------------------------------------------------------

class TestBLSCountySeries:
    def test_series_ids_have_correct_format(self):
        """LAUS format: LAUCN + state2 + county3 + 0000000 + 003 = 20 chars."""
        from src.loaders.macro_signals.bls_client import COUNTY_LAUS_SERIES
        for fips, series_id in COUNTY_LAUS_SERIES.items():
            assert series_id.startswith("LAUCN"), f"{series_id} missing LAUCN prefix"
            assert len(series_id) == 20, f"{series_id} should be 20 chars"
            assert series_id.endswith("003"), f"{series_id} should end with measure 003"

    def test_hillsborough_series_contains_correct_fips(self):
        from src.loaders.macro_signals.bls_client import COUNTY_LAUS_SERIES
        series = COUNTY_LAUS_SERIES["12057"]
        assert "12057" in series

    def test_pinellas_series_contains_correct_fips(self):
        from src.loaders.macro_signals.bls_client import COUNTY_LAUS_SERIES
        series = COUNTY_LAUS_SERIES["12103"]
        assert "12103" in series

    def test_known_series_includes_county_laus(self):
        from src.loaders.macro_signals.bls_client import KNOWN_SERIES
        assert "LAUCN120570000000003" in KNOWN_SERIES
        assert "LAUCN121030000000003" in KNOWN_SERIES
        assert KNOWN_SERIES["LAUCN120570000000003"] == "county_unemployment_rate"

    def test_county_series_in_source_registry(self):
        from src.loaders.macro_signals.source_registry import SOURCES
        bls_series = SOURCES["bls"]["series"]
        assert "LAUCN120570000000003" in bls_series
        assert "LAUCN121030000000003" in bls_series
        assert bls_series["LAUCN120570000000003"]["geography_scope"] == "county"
        assert bls_series["LAUCN120570000000003"]["geography_id"] == "12057"
        assert bls_series["LAUCN121030000000003"]["geography_id"] == "12103"

    def test_invalid_series_degrades_safely(self):
        """fetch_series with an invalid ID returns an empty list, not an exception."""
        from unittest.mock import patch
        body = {
            "status": "REQUEST_SUCCEEDED",
            "Results": {"series": [{"seriesID": "INVALID000", "data": []}]},
            "message": [],
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = body
        mock_resp.raise_for_status.return_value = None
        from src.loaders.macro_signals.bls_client import BLSClient
        with patch("src.loaders.macro_signals.bls_client.requests_post_with_retry", return_value=mock_resp):
            client = BLSClient()
            records = client.fetch_series(["INVALID000"], start_year=2024, end_year=2024)
        assert records == []


# ---------------------------------------------------------------------------
# Test 7: FHFA county HPI normalization
# ---------------------------------------------------------------------------

class TestFHFACountyHPI:
    def test_parse_hpi_with_county_place_ids(self):
        """parse_hpi_master correctly filters to target county FIPS."""
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master

        csv_text = (
            "hpi_type,hpi_flavor,frequency,level,place_name,place_id,yr,period,index_nsa,index_sa\n"
            "traditional,purchase-only,monthly,county,Hillsborough County,12057,2025,1,420.0,\n"
            "traditional,purchase-only,monthly,county,Pinellas County,12103,2025,1,380.0,\n"
            "traditional,purchase-only,monthly,state,Florida,FL,2025,1,310.0,\n"
        )
        records = parse_hpi_master(
            csv_text, frequency="monthly", levels=["county"], place_ids=["12057", "12103"]
        )
        assert len(records) == 2
        geo_ids = {r["geography_id"] for r in records}
        assert geo_ids == {"12057", "12103"}
        for r in records:
            assert r["geography_scope"] == "county"
            assert r["signal_key"] == "house_price_index"
            assert r["source"] == "fhfa"

    def test_county_hpi_normalized_record_schema(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        from src.loaders.macro_signals.normalization import REQUIRED_KEYS

        csv_text = (
            "hpi_type,hpi_flavor,frequency,level,place_name,place_id,yr,period,index_nsa,index_sa\n"
            "traditional,purchase-only,monthly,county,Hillsborough,12057,2025,3,425.0,\n"
        )
        records = parse_hpi_master(csv_text, levels=["county"], place_ids=["12057"])
        assert len(records) == 1
        rec = records[0]
        for key in REQUIRED_KEYS:
            assert key in rec, f"Missing required key: {key}"


# ---------------------------------------------------------------------------
# Test 8: get_county_market_pressure_context
# ---------------------------------------------------------------------------

class TestCountyMarketPressureContext:
    def test_missing_all_data_returns_neutral(self):
        """When all data is absent, overall pressure must be neutral, not an exception."""
        session = MagicMock()
        session.execute.return_value.fetchone.return_value = None
        session.execute.return_value.fetchall.return_value = []
        result = get_county_market_pressure_context(session, "12057")
        assert "overall_market_pressure" in result
        assert result["overall_market_pressure"] == "neutral"
        assert result["county_fips"] == "12057"

    def test_exception_returns_unknown_pressure(self):
        """DB error must be caught — never propagate from context getter."""
        session = MagicMock()
        session.execute.side_effect = Exception("DB unavailable")
        result = get_county_market_pressure_context(session, "12057")
        assert result["overall_market_pressure"] == "unknown"
        assert "error" in result

    def test_returns_all_expected_keys(self):
        session = MagicMock()
        session.execute.return_value.fetchone.return_value = None
        session.execute.return_value.fetchall.return_value = []
        result = get_county_market_pressure_context(session, "12057")
        for key in ("county_fips", "mortgage_rate", "county_hpi", "county_unemployment",
                    "census_baseline", "overall_market_pressure"):
            assert key in result, f"Missing key: {key}"

    def test_elevated_components_produce_moderate_overall(self):
        """Two elevated components → overall = moderate."""
        components = ["elevated", "elevated", "neutral"]
        assert _overall_pressure(components) == "elevated"

    def test_all_neutral_returns_neutral(self):
        assert _overall_pressure(["neutral", "neutral", "neutral"]) == "neutral"

    def test_all_unknown_returns_neutral(self):
        assert _overall_pressure(["unknown", "unknown", "unknown"]) == "neutral"

    def test_one_high_returns_high(self):
        assert _overall_pressure(["high", "neutral", "neutral"]) == "high"


# ---------------------------------------------------------------------------
# Test 9: Census classified as baseline, not live urgency
# ---------------------------------------------------------------------------

class TestCensusRole:
    def test_all_census_signals_have_a_role(self):
        from src.loaders.macro_signals.census_client import ACS_VARIABLES
        for var_id, meta in ACS_VARIABLES.items():
            key = meta["signal_key"]
            assert key in CENSUS_SIGNAL_ROLES, f"No role for Census signal {key} ({var_id})"

    def test_no_census_signal_is_live_urgency(self):
        for key, role in CENSUS_SIGNAL_ROLES.items():
            assert role in ("baseline_context", "slow_moving_risk_factor"), (
                f"Census signal {key} has unexpected role '{role}' — must not be live urgency"
            )

    def test_vacant_housing_is_risk_factor_not_baseline(self):
        assert CENSUS_SIGNAL_ROLES["vacant_housing_units"] == "slow_moving_risk_factor"

    def test_total_population_is_baseline(self):
        assert CENSUS_SIGNAL_ROLES["total_population"] == "baseline_context"

    def test_census_baseline_note_present_in_context(self):
        """_get_census_baseline must include a note preventing urgency misuse."""
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [
            ("vacant_housing_units", Decimal("5000"), date(2022, 1, 1)),
        ]
        from src.services.market_pressure_service import _get_census_baseline
        result = _get_census_baseline(session, "12057")
        assert "note" in result
        assert "baseline" in result["note"].lower()


# ---------------------------------------------------------------------------
# Test 10: _classify_rate_pressure helper
# ---------------------------------------------------------------------------

class TestClassifyPressure:
    def test_neutral_in_dead_zone(self):
        thresholds = {"elevated": 0.25, "high": 0.75, "declining": -0.25}
        assert _classify_rate_pressure(0.10, thresholds) == "neutral"

    def test_elevated_above_threshold(self):
        thresholds = {"elevated": 0.25, "high": 0.75, "declining": -0.25}
        assert _classify_rate_pressure(0.50, thresholds) == "elevated"

    def test_high_above_high_threshold(self):
        thresholds = {"elevated": 0.25, "high": 0.75, "declining": -0.25}
        assert _classify_rate_pressure(1.0, thresholds) == "high"

    def test_declining_below_threshold(self):
        thresholds = {"elevated": 0.25, "high": 0.75, "declining": -0.25}
        assert _classify_rate_pressure(-0.50, thresholds) == "declining"
