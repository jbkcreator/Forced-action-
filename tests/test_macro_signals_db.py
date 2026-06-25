"""Tests for A7 Macro-Signal DB Persistence Layer.

Unit tests (mock_db): service contract, skip/error handling in sync task.
Integration tests (fresh_db): real Postgres upsert, uniqueness, idempotency.

Run unit tests only (no DB required):
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest tests/test_macro_signals_db.py -v -k "not PG"

Run all (requires DATABASE_URL):
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest tests/test_macro_signals_db.py -v
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.loaders.macro_signals.normalization import REQUIRED_KEYS, normalize_record
from src.services.macro_signal_service import (
    get_latest_macro_signal,
    get_latest_macro_signals_by_source,
    upsert_macro_signal,
    upsert_macro_signals,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _sample_record(
    source="fred",
    signal_key="mortgage_30y_fixed",
    source_series_id="MORTGAGE30US",
    observed_at=date(2024, 1, 4),
    value=6.62,
    geography_scope="national",
    geography_id="US",
    unit="percent",
    frequency="weekly",
) -> dict:
    return normalize_record(
        source=source,
        signal_key=signal_key,
        source_series_id=source_series_id,
        value=value,
        observed_at=observed_at,
        frequency=frequency,
        geography_scope=geography_scope,
        geography_id=geography_id,
        unit=unit,
        raw_payload={"date": str(observed_at), "value": str(value)},
    )


# ---------------------------------------------------------------------------
# Test 9: Loader output matches service contract
# ---------------------------------------------------------------------------

class TestNormalizationContract:
    def test_required_keys_present_in_sample(self):
        rec = _sample_record()
        for key in REQUIRED_KEYS:
            assert key in rec, f"Missing required key: {key}"

    def test_observed_at_is_date_or_str(self):
        rec = _sample_record()
        assert isinstance(rec["observed_at"], (date, str))

    def test_value_is_numeric(self):
        rec = _sample_record()
        assert isinstance(rec["value"], (int, float, Decimal))

    def test_source_series_id_not_none(self):
        rec = _sample_record()
        assert rec["source_series_id"] is not None

    def test_string_observed_at_is_accepted(self):
        rec = _sample_record(observed_at=date(2024, 3, 7))
        rec["observed_at"] = "2024-03-07"
        # Service _to_row should parse it without raising
        from src.services.macro_signal_service import _to_row
        row = _to_row(rec)
        assert row["observed_at"] == date(2024, 3, 7)


# ---------------------------------------------------------------------------
# Test 10: No CDS scoring code touched
# ---------------------------------------------------------------------------

class TestCDSUntouched:
    def test_cds_engine_not_imported_by_service(self):
        import src.services.macro_signal_service as svc
        assert not hasattr(svc, "cds_engine"), "Service must not import cds_engine"

    def test_scoring_config_not_imported_by_service(self):
        import importlib, sys
        mod = sys.modules.get("src.services.macro_signal_service")
        assert mod is not None
        source = open(mod.__file__).read()
        assert "scoring" not in source.lower(), "Service must not reference scoring config"

    def test_sync_task_does_not_import_cds(self):
        import src.tasks.sync_macro_signals as task
        source = open(task.__file__).read()
        assert "cds_engine" not in source, "Sync task must not import cds_engine"


# ---------------------------------------------------------------------------
# Tests 7 & 8: Sync task skip/error behaviour (mock DB)
# ---------------------------------------------------------------------------

class TestSyncTaskBehaviour:
    def test_census_skipped_when_key_missing(self):
        from src.tasks.sync_macro_signals import _sync_census
        mock_session = MagicMock()
        with patch("src.tasks.sync_macro_signals.get_settings") as mock_settings:
            mock_settings.return_value.census_api_key = None
            result = _sync_census(mock_session)
        assert result.get("skipped") is True
        assert "CENSUS_API_KEY" in result.get("reason", "")

    def test_fred_skipped_when_key_missing(self):
        from src.tasks.sync_macro_signals import _sync_fred
        mock_session = MagicMock()
        with patch("src.tasks.sync_macro_signals.fetch_mortgage_rates", side_effect=ValueError("FRED_API_KEY is not set")):
            result = _sync_fred(mock_session)
        assert result.get("skipped") is True

    def test_one_source_failure_does_not_block_others(self):
        from src.tasks.sync_macro_signals import _SOURCE_RUNNERS, run_sync

        call_log: list[str] = []

        def _fail(session):
            raise RuntimeError("simulated failure")

        def _ok(session):
            call_log.append("ok")
            return {"inserted": 1, "updated": 0}

        patched_runners = {
            "fred":   _fail,
            "fhfa":   _ok,
            "bls":    _ok,
            "census": _ok,
        }

        with patch("src.tasks.sync_macro_signals._SOURCE_RUNNERS", patched_runners), \
             patch("src.tasks.sync_macro_signals.get_db_context") as mock_ctx:
            mock_session = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run_sync()

        assert "error" in results["fred"]
        assert results["fhfa"]["inserted"] == 1
        assert results["bls"]["inserted"] == 1
        assert results["census"]["inserted"] == 1
        assert len(call_log) == 3

    def test_sources_filter_respected(self):
        from src.tasks.sync_macro_signals import run_sync

        def _ok(session):
            return {"inserted": 5, "updated": 0}

        patched_runners = {
            "fred":   _ok,
            "fhfa":   _ok,
            "bls":    _ok,
            "census": _ok,
        }

        with patch("src.tasks.sync_macro_signals._SOURCE_RUNNERS", patched_runners), \
             patch("src.tasks.sync_macro_signals.get_db_context") as mock_ctx:
            mock_session = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run_sync(sources=["fred", "bls"])

        assert set(results.keys()) == {"fred", "bls"}

    def test_unknown_source_raises(self):
        from src.tasks.sync_macro_signals import run_sync
        with pytest.raises(ValueError, match="Unknown source"):
            run_sync(sources=["bogus"])


# ---------------------------------------------------------------------------
# Integration tests — require real Postgres (fresh_db fixture)
# Tests 1–6
# ---------------------------------------------------------------------------

class TestPGUpsertSingleRecord:
    """Test 1: Single record insert."""

    def test_insert_creates_row(self, fresh_db):
        rec = _sample_record()
        result = upsert_macro_signal(fresh_db, rec)
        assert result["inserted"] == 1
        assert result["updated"] == 0

    def test_inserted_row_is_queryable(self, fresh_db):
        rec = _sample_record(signal_key="mortgage_30y_fixed", geography_id="US")
        upsert_macro_signal(fresh_db, rec)
        found = get_latest_macro_signal(fresh_db, "mortgage_30y_fixed", geography_id="US")
        assert found is not None
        assert found["signal_key"] == "mortgage_30y_fixed"
        assert found["geography_id"] == "US"
        assert float(found["value"]) == pytest.approx(6.62)


class TestPGUpsertDuplicate:
    """Test 2: Duplicate upsert does not create duplicate rows."""

    def test_duplicate_does_not_create_second_row(self, fresh_db):
        rec = _sample_record(signal_key="cpi_all_urban", source="bls")
        upsert_macro_signal(fresh_db, rec)
        upsert_macro_signal(fresh_db, rec)

        from sqlalchemy import text as sa_text
        count = fresh_db.execute(
            sa_text("SELECT COUNT(*) FROM macro_signals WHERE signal_key = 'cpi_all_urban'")
        ).scalar()
        assert count == 1

    def test_second_upsert_is_classified_as_update(self, fresh_db):
        rec = _sample_record(signal_key="unemployment_rate", source="bls")
        upsert_macro_signal(fresh_db, rec)
        result2 = upsert_macro_signal(fresh_db, rec)
        assert result2["updated"] == 1
        assert result2["inserted"] == 0


class TestPGUpsertValueUpdate:
    """Test 3: Updated value and raw_payload overwrite existing row."""

    def test_value_refreshed_on_conflict(self, fresh_db):
        rec = _sample_record(signal_key="hpi_national", value=350.0)
        upsert_macro_signal(fresh_db, rec)

        rec_revised = _sample_record(signal_key="hpi_national", value=355.5)
        upsert_macro_signal(fresh_db, rec_revised)

        found = get_latest_macro_signal(fresh_db, "hpi_national")
        assert float(found["value"]) == pytest.approx(355.5)

    def test_raw_payload_refreshed_on_conflict(self, fresh_db):
        rec = _sample_record(signal_key="hpi_state", value=300.0)
        rec["raw_payload"] = {"original": True}
        upsert_macro_signal(fresh_db, rec)

        rec2 = _sample_record(signal_key="hpi_state", value=302.0)
        rec2["raw_payload"] = {"revised": True}
        upsert_macro_signal(fresh_db, rec2)

        from sqlalchemy import text as sa_text
        raw = fresh_db.execute(
            sa_text("SELECT raw_payload FROM macro_signals WHERE signal_key = 'hpi_state'")
        ).scalar()
        assert raw == {"revised": True}


class TestPGBatchUpsert:
    """Test 4: Batch upsert works."""

    def test_batch_insert_multiple_records(self, fresh_db):
        records = [
            _sample_record(signal_key="mortgage_30y_fixed", observed_at=date(2024, 1, d))
            for d in range(1, 6)
        ]
        result = upsert_macro_signals(fresh_db, records)
        assert result["inserted"] == 5
        assert result["updated"] == 0

    def test_empty_batch_returns_zeros(self, fresh_db):
        result = upsert_macro_signals(fresh_db, [])
        assert result == {"inserted": 0, "updated": 0}

    def test_batch_mixed_insert_and_update(self, fresh_db):
        rec = _sample_record(signal_key="rent_cpi", observed_at=date(2024, 6, 1))
        upsert_macro_signals(fresh_db, [rec])

        batch = [
            _sample_record(signal_key="rent_cpi", observed_at=date(2024, 6, 1), value=321.0),  # conflict
            _sample_record(signal_key="rent_cpi", observed_at=date(2024, 7, 1)),               # new
        ]
        result = upsert_macro_signals(fresh_db, batch)
        assert result["inserted"] == 1
        assert result["updated"] == 1


class TestPGLatestLookup:
    """Test 5: Latest signal lookup works."""

    def test_returns_most_recent_observation(self, fresh_db):
        for d in [date(2024, 1, 1), date(2024, 6, 1), date(2024, 3, 1)]:
            upsert_macro_signal(
                fresh_db,
                _sample_record(signal_key="mortgage_15y_fixed", observed_at=d, value=float(d.month)),
            )
        found = get_latest_macro_signal(fresh_db, "mortgage_15y_fixed")
        assert found["observed_at"] == date(2024, 6, 1)
        assert float(found["value"]) == pytest.approx(6.0)

    def test_geography_filter_applied(self, fresh_db):
        upsert_macro_signal(
            fresh_db,
            _sample_record(signal_key="median_income", geography_scope="county", geography_id="12057", value=70_000),
        )
        upsert_macro_signal(
            fresh_db,
            _sample_record(signal_key="median_income", geography_scope="county", geography_id="12103", value=65_000),
        )
        found = get_latest_macro_signal(fresh_db, "median_income", geography_id="12057")
        assert float(found["value"]) == pytest.approx(70_000)
        assert found["geography_id"] == "12057"

    def test_returns_none_when_not_found(self, fresh_db):
        found = get_latest_macro_signal(fresh_db, "nonexistent_signal_xyz")
        assert found is None

    def test_get_latest_by_source(self, fresh_db):
        upsert_macro_signals(
            fresh_db,
            [
                _sample_record(source="bls", signal_key="unemployment_rate_national"),
                _sample_record(source="bls", signal_key="cpi_all_urban"),
            ],
        )
        rows = get_latest_macro_signals_by_source(fresh_db, "bls")
        keys = {r["signal_key"] for r in rows}
        assert "unemployment_rate_national" in keys
        assert "cpi_all_urban" in keys

    def test_get_latest_by_source_empty_when_none(self, fresh_db):
        rows = get_latest_macro_signals_by_source(fresh_db, "nonexistent_source_xyz")
        assert rows == []


class TestPGIdempotentSync:
    """Test 6: Repeated sync does not duplicate rows."""

    def test_double_upsert_same_batch_is_idempotent(self, fresh_db):
        records = [
            _sample_record(signal_key="idempotent_test", observed_at=date(2024, 1, d))
            for d in range(1, 4)
        ]
        upsert_macro_signals(fresh_db, records)
        upsert_macro_signals(fresh_db, records)

        from sqlalchemy import text as sa_text
        count = fresh_db.execute(
            sa_text("SELECT COUNT(*) FROM macro_signals WHERE signal_key = 'idempotent_test'")
        ).scalar()
        assert count == 3

    def test_second_sync_classifies_all_as_updates(self, fresh_db):
        records = [
            _sample_record(signal_key="idempotent_update_test", observed_at=date(2024, 2, d))
            for d in range(1, 4)
        ]
        upsert_macro_signals(fresh_db, records)
        result2 = upsert_macro_signals(fresh_db, records)
        assert result2["inserted"] == 0
        assert result2["updated"] == 3
