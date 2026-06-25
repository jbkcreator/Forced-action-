"""Unit tests for A7 macro signal loaders.

No DB dependency. No real HTTP calls — all network I/O is mocked.

Run:
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest tests/macro_signals/ -v
"""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.loaders.macro_signals.normalization import REQUIRED_KEYS, normalize_record
from src.loaders.macro_signals.source_registry import SOURCES


# ---------------------------------------------------------------------------
# normalization
# ---------------------------------------------------------------------------

class TestNormalization:
    def _base(self, **overrides) -> dict:
        kwargs = dict(
            source="fred",
            signal_key="mortgage_rate_30yr",
            source_series_id="MORTGAGE30US",
            value=6.87,
            observed_at=date(2024, 1, 4),
            frequency="weekly",
            geography_scope="national",
            geography_id="US",
            unit="percent",
            raw_payload={"date": "2024-01-04", "value": "6.87"},
        )
        kwargs.update(overrides)
        return normalize_record(**kwargs)

    def test_all_required_keys_present(self):
        rec = self._base()
        for key in REQUIRED_KEYS:
            assert key in rec, f"Missing required key: {key}"

    def test_date_converted_to_iso_string(self):
        rec = self._base(observed_at=date(2024, 3, 15))
        assert rec["observed_at"] == "2024-03-15"

    def test_string_date_passed_through(self):
        rec = self._base(observed_at="2024-03-15")
        assert rec["observed_at"] == "2024-03-15"

    def test_value_is_float(self):
        rec = self._base(value=6)
        assert isinstance(rec["value"], float)

    def test_extra_kwargs_included(self):
        rec = self._base(index_sa=6.90, hpi_type="traditional")
        assert rec["index_sa"] == 6.90
        assert rec["hpi_type"] == "traditional"


# ---------------------------------------------------------------------------
# source_registry
# ---------------------------------------------------------------------------

class TestSourceRegistry:
    def test_fred_present(self):
        assert "fred" in SOURCES

    def test_bls_present(self):
        assert "bls" in SOURCES

    def test_fhfa_present(self):
        assert "fhfa" in SOURCES

    def test_census_present(self):
        assert "census_acs5" in SOURCES

    def test_mortgage30us_in_fred_series(self):
        assert "MORTGAGE30US" in SOURCES["fred"]["series"]

    def test_fred_key_required(self):
        assert SOURCES["fred"]["key_required"] is True

    def test_fhfa_no_key_required(self):
        assert SOURCES["fhfa"]["key_required"] is False

    def test_bls_no_key_required(self):
        assert SOURCES["bls"]["key_required"] is False

    def test_all_sources_have_required_fields(self):
        required = {"name", "base_url", "key_required", "free", "series"}
        for source_id, meta in SOURCES.items():
            missing = required - meta.keys()
            assert not missing, f"{source_id} missing fields: {missing}"


# ---------------------------------------------------------------------------
# FREDClient
# ---------------------------------------------------------------------------

class TestFREDClient:
    def _mock_response(self, observations: list[dict]) -> MagicMock:
        resp = MagicMock()
        resp.json.return_value = {"observations": observations}
        resp.raise_for_status = MagicMock()
        return resp

    def _make_client(self) -> "FREDClient":
        from src.loaders.macro_signals.fred_client import FREDClient
        return FREDClient(api_key="test_key")

    def test_skips_dot_values(self):
        from src.loaders.macro_signals.fred_client import FREDClient
        obs = [
            {"date": "2024-01-04", "value": "6.87"},
            {"date": "2024-01-11", "value": "."},
            {"date": "2024-01-18", "value": "6.92"},
        ]
        with patch("src.loaders.macro_signals.fred_client.requests_get_with_retry",
                   return_value=self._mock_response(obs)):
            client = FREDClient(api_key="test_key")
            records = client.fetch_observations("MORTGAGE30US")

        assert len(records) == 2
        values = [r["value"] for r in records]
        assert 6.87 in values
        assert 6.92 in values

    def test_converts_date_to_iso_string(self):
        from src.loaders.macro_signals.fred_client import FREDClient
        obs = [{"date": "2024-03-14", "value": "6.74"}]
        with patch("src.loaders.macro_signals.fred_client.requests_get_with_retry",
                   return_value=self._mock_response(obs)):
            client = FREDClient(api_key="test_key")
            records = client.fetch_observations("MORTGAGE30US")

        assert records[0]["observed_at"] == "2024-03-14"

    def test_value_cast_to_float(self):
        from src.loaders.macro_signals.fred_client import FREDClient
        obs = [{"date": "2024-01-04", "value": "6.87"}]
        with patch("src.loaders.macro_signals.fred_client.requests_get_with_retry",
                   return_value=self._mock_response(obs)):
            client = FREDClient(api_key="test_key")
            records = client.fetch_observations("MORTGAGE30US")

        assert isinstance(records[0]["value"], float)
        assert records[0]["value"] == pytest.approx(6.87)

    def test_all_required_keys_in_record(self):
        from src.loaders.macro_signals.fred_client import FREDClient
        obs = [{"date": "2024-01-04", "value": "6.87"}]
        with patch("src.loaders.macro_signals.fred_client.requests_get_with_retry",
                   return_value=self._mock_response(obs)):
            client = FREDClient(api_key="test_key")
            records = client.fetch_observations("MORTGAGE30US")

        for key in REQUIRED_KEYS:
            assert key in records[0], f"Missing key: {key}"

    def test_source_is_fred(self):
        from src.loaders.macro_signals.fred_client import FREDClient
        obs = [{"date": "2024-01-04", "value": "6.87"}]
        with patch("src.loaders.macro_signals.fred_client.requests_get_with_retry",
                   return_value=self._mock_response(obs)):
            client = FREDClient(api_key="test_key")
            records = client.fetch_observations("MORTGAGE30US")

        assert records[0]["source"] == "fred"

    def test_signal_key_from_registry(self):
        from src.loaders.macro_signals.fred_client import FREDClient
        obs = [{"date": "2024-01-04", "value": "6.87"}]
        with patch("src.loaders.macro_signals.fred_client.requests_get_with_retry",
                   return_value=self._mock_response(obs)):
            client = FREDClient(api_key="test_key")
            records = client.fetch_observations("MORTGAGE30US")

        assert records[0]["signal_key"] == "mortgage_rate_30yr"

    def test_empty_observations_returns_empty_list(self):
        from src.loaders.macro_signals.fred_client import FREDClient
        with patch("src.loaders.macro_signals.fred_client.requests_get_with_retry",
                   return_value=self._mock_response([])):
            client = FREDClient(api_key="test_key")
            records = client.fetch_observations("MORTGAGE30US")

        assert records == []

    def test_raises_without_api_key(self):
        from src.loaders.macro_signals.fred_client import FREDClient
        with patch("src.loaders.macro_signals.fred_client.settings") as mock_settings:
            mock_settings.fred_api_key = None
            with pytest.raises(ValueError, match="FRED_API_KEY"):
                FREDClient()

    def test_skips_non_numeric_value(self):
        from src.loaders.macro_signals.fred_client import FREDClient
        obs = [
            {"date": "2024-01-04", "value": "N/A"},
            {"date": "2024-01-11", "value": "7.01"},
        ]
        with patch("src.loaders.macro_signals.fred_client.requests_get_with_retry",
                   return_value=self._mock_response(obs)):
            client = FREDClient(api_key="test_key")
            records = client.fetch_observations("MORTGAGE30US")

        assert len(records) == 1
        assert records[0]["value"] == pytest.approx(7.01)


# ---------------------------------------------------------------------------
# BLSClient
# ---------------------------------------------------------------------------

class TestBLSClient:
    def _mock_response(self, series_data: list[dict]) -> MagicMock:
        resp = MagicMock()
        resp.json.return_value = {
            "status": "REQUEST_SUCCEEDED",
            "Results": {"series": series_data},
        }
        resp.raise_for_status = MagicMock()
        return resp

    def test_parses_monthly_records(self):
        from src.loaders.macro_signals.bls_client import BLSClient
        data = [
            {"year": "2024", "period": "M01", "value": "3.7", "footnotes": []},
            {"year": "2024", "period": "M02", "value": "3.9", "footnotes": []},
        ]
        series = [{"seriesID": "LNS14000000", "data": data}]
        with patch("src.loaders.macro_signals.bls_client.requests_post_with_retry",
                   return_value=self._mock_response(series)):
            client = BLSClient()
            records = client.fetch_series(["LNS14000000"], 2024, 2024)

        assert len(records) == 2

    def test_skips_annual_average_m13(self):
        from src.loaders.macro_signals.bls_client import BLSClient
        data = [
            {"year": "2024", "period": "M01", "value": "3.7", "footnotes": []},
            {"year": "2024", "period": "M13", "value": "3.8", "footnotes": []},  # annual avg
        ]
        series = [{"seriesID": "LNS14000000", "data": data}]
        with patch("src.loaders.macro_signals.bls_client.requests_post_with_retry",
                   return_value=self._mock_response(series)):
            client = BLSClient()
            records = client.fetch_series(["LNS14000000"], 2024, 2024)

        assert len(records) == 1
        assert records[0]["observed_at"] == "2024-01-01"

    def test_raises_on_api_error(self):
        from src.loaders.macro_signals.bls_client import BLSClient
        resp = MagicMock()
        resp.json.return_value = {"status": "REQUEST_FAILED", "message": ["Bad series ID"]}
        resp.raise_for_status = MagicMock()
        with patch("src.loaders.macro_signals.bls_client.requests_post_with_retry",
                   return_value=resp):
            client = BLSClient()
            with pytest.raises(RuntimeError, match="BLS API error"):
                client.fetch_series(["BADID"], 2024, 2024)

    def test_all_required_keys_in_record(self):
        from src.loaders.macro_signals.bls_client import BLSClient
        data = [{"year": "2024", "period": "M01", "value": "3.7", "footnotes": []}]
        series = [{"seriesID": "LNS14000000", "data": data}]
        with patch("src.loaders.macro_signals.bls_client.requests_post_with_retry",
                   return_value=self._mock_response(series)):
            client = BLSClient()
            records = client.fetch_series(["LNS14000000"], 2024, 2024)

        for key in REQUIRED_KEYS:
            assert key in records[0], f"Missing key: {key}"

    def test_source_is_bls(self):
        from src.loaders.macro_signals.bls_client import BLSClient
        data = [{"year": "2024", "period": "M01", "value": "3.7", "footnotes": []}]
        series = [{"seriesID": "LNS14000000", "data": data}]
        with patch("src.loaders.macro_signals.bls_client.requests_post_with_retry",
                   return_value=self._mock_response(series)):
            client = BLSClient()
            records = client.fetch_series(["LNS14000000"], 2024, 2024)

        assert records[0]["source"] == "bls"


# ---------------------------------------------------------------------------
# FHFAHPILoader
# ---------------------------------------------------------------------------

_SAMPLE_CSV = """\
hpi_type,hpi_flavor,frequency,level,place_name,place_id,yr,period,index_nsa,index_sa
traditional,purchase-only,monthly,USA or Census Division,United States,USA,2024,1,420.50,421.00
traditional,purchase-only,monthly,state,Florida,FL,2024,1,385.20,386.00
traditional,purchase-only,monthly,state,California,CA,2024,1,510.30,511.00
traditional,all-transactions,monthly,USA or Census Division,United States,USA,2024,1,415.00,
traditional,purchase-only,quarterly,USA or Census Division,United States,USA,2024,1,418.00,419.00
traditional,purchase-only,monthly,state,Florida,FL,2024,2,387.00,388.00
"""


class TestFHFAHPILoader:
    def test_parses_correct_records(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV)
        # Default: purchase-only, monthly — should get 4 rows
        assert len(records) == 4

    def test_filters_by_flavor(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV, hpi_flavor="all-transactions")
        assert len(records) == 1

    def test_filters_by_frequency(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV, frequency="quarterly")
        assert len(records) == 1

    def test_filters_by_level(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV, levels=["state"])
        assert len(records) == 3  # FL Jan, CA Jan, FL Feb

    def test_filters_by_place_id(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV, place_ids=["FL"])
        assert len(records) == 2

    def test_value_is_index_nsa(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV, levels=["USA or Census Division"])
        assert records[0]["value"] == pytest.approx(420.50)

    def test_index_sa_included_when_present(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV, levels=["USA or Census Division"])
        assert records[0]["index_sa"] == pytest.approx(421.00)

    def test_index_sa_none_when_missing(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV, hpi_flavor="all-transactions")
        assert records[0]["index_sa"] is None

    def test_monthly_date_construction(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV, levels=["USA or Census Division"])
        assert records[0]["observed_at"] == "2024-01-01"

    def test_all_required_keys_present(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV)
        for key in REQUIRED_KEYS:
            assert key in records[0], f"Missing key: {key}"

    def test_source_is_fhfa(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV)
        assert records[0]["source"] == "fhfa"

    def test_signal_key_is_house_price_index(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV)
        assert records[0]["signal_key"] == "house_price_index"

    def test_geography_scope_mapped(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        records = parse_hpi_master(_SAMPLE_CSV, levels=["state"])
        assert records[0]["geography_scope"] == "state"

    def test_min_year_filter(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import parse_hpi_master
        csv_with_old = _SAMPLE_CSV + "traditional,purchase-only,monthly,state,Florida,FL,2019,1,300.00,301.00\n"
        records = parse_hpi_master(csv_with_old, levels=["state"], min_year=2024)
        years = {r["observed_at"][:4] for r in records}
        assert "2019" not in years

    def test_fetch_hpi_uses_raw_csv(self):
        from src.loaders.macro_signals.fhfa_hpi_loader import fetch_hpi
        records = fetch_hpi(raw_csv=_SAMPLE_CSV, levels=["state"])
        assert len(records) == 3
