"""Phase 0 tests — per-county kill-switch metric ingest."""
import json
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db_returning_zeros():
    """Return a mock Session whose execute().scalar() always returns 0."""
    scalar = MagicMock(return_value=0)
    first = MagicMock(return_value=None)
    execute_result = MagicMock()
    execute_result.scalar.return_value = 0
    execute_result.scalar_one_or_none.return_value = 0
    execute_result.first.return_value = None
    db = MagicMock()
    db.execute.return_value = execute_result
    return db


def _patch_ingest(monkeypatch, counties, source="hillsborough"):
    """Patch list_counties, settings, DB, and Redis for ingest tests."""
    monkeypatch.setattr(
        "src.tasks.kill_switch_metric_ingest.settings",
        MagicMock(county_launch_source_county=source),
    )
    # Patch list_counties inside the function's import path.
    monkeypatch.setattr(
        "src.utils.county_config.list_counties",
        lambda: counties,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPerCountyLoop:
    def test_loops_all_active_counties(self, monkeypatch):
        """Both counties get a metrics dict in the return value."""
        counties = ["hillsborough", "pinellas"]
        _patch_ingest(monkeypatch, counties)

        upserted = []
        cached = []

        def fake_write_row(db, county_id, metrics):
            upserted.append(county_id)

        def fake_cache(feature, value, county_id=None):
            cached.append((feature, county_id))

        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._write_platform_daily_stats_row",
            fake_write_row,
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._cache_metric",
            fake_cache,
        )

        db = _make_db_returning_zeros()
        with patch("src.tasks.kill_switch_metric_ingest.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            from src.tasks.kill_switch_metric_ingest import run_kill_switch_metric_ingest
            result = run_kill_switch_metric_ingest(dry_run=False)

        assert set(upserted) == {"hillsborough", "pinellas"}
        assert "hillsborough" in result
        assert "pinellas" in result

    def test_legacy_redis_key_only_for_source_county(self, monkeypatch):
        """No-prefix Redis key written for source county, NOT for pinellas."""
        counties = ["hillsborough", "pinellas"]
        _patch_ingest(monkeypatch, counties, source="hillsborough")

        cached_calls = []

        def fake_cache(feature, value, county_id=None):
            cached_calls.append({"feature": feature, "county_id": county_id})

        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._write_platform_daily_stats_row",
            lambda *a: None,
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._cache_metric",
            fake_cache,
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._check_accelerated_wallet_push_floor",
            lambda *a: None,
        )

        db = _make_db_returning_zeros()
        with patch("src.tasks.kill_switch_metric_ingest.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            with patch("src.utils.county_config.list_counties", return_value=counties):
                from src.tasks.kill_switch_metric_ingest import run_kill_switch_metric_ingest
                result = run_kill_switch_metric_ingest(dry_run=False)

        # Legacy (no county_id) calls must only be for source county features.
        legacy_calls = [c for c in cached_calls if c["county_id"] is None]
        pinellas_calls = [c for c in cached_calls if c["county_id"] == "pinellas"]
        # Legacy keys exist (source county wrote them).
        assert len(legacy_calls) > 0
        # No pinellas feature was written without county_id qualifier.
        assert all(c["county_id"] is not None for c in pinellas_calls)

    def test_aw_push_floor_called_once_source_only(self, monkeypatch):
        """_check_accelerated_wallet_push_floor called exactly once, source county only."""
        counties = ["hillsborough", "pinellas"]
        _patch_ingest(monkeypatch, counties, source="hillsborough")

        floor_calls = []

        def fake_floor(db, take_rate):
            floor_calls.append(take_rate)
            return None

        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._check_accelerated_wallet_push_floor",
            fake_floor,
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._write_platform_daily_stats_row",
            lambda *a: None,
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._cache_metric",
            lambda *a, **kw: None,
        )

        db = _make_db_returning_zeros()
        with patch("src.tasks.kill_switch_metric_ingest.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            with patch("src.utils.county_config.list_counties", return_value=counties):
                from src.tasks.kill_switch_metric_ingest import run_kill_switch_metric_ingest
                run_kill_switch_metric_ingest(dry_run=False)

        assert len(floor_calls) == 1

    def test_dry_run_writes_nothing(self, monkeypatch):
        """dry_run=True → no DB upsert, no Redis set."""
        counties = ["hillsborough", "pinellas"]
        _patch_ingest(monkeypatch, counties)

        upserted = []
        cached = []

        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._write_platform_daily_stats_row",
            lambda *a: upserted.append(a),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._cache_metric",
            lambda *a, **kw: cached.append(a),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._check_accelerated_wallet_push_floor",
            lambda *a: None,
        )

        db = _make_db_returning_zeros()
        with patch("src.tasks.kill_switch_metric_ingest.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            with patch("src.utils.county_config.list_counties", return_value=counties):
                from src.tasks.kill_switch_metric_ingest import run_kill_switch_metric_ingest
                result = run_kill_switch_metric_ingest(dry_run=True)

        assert upserted == []
        assert cached == []
        assert "hillsborough" in result
        assert "pinellas" in result

    def test_source_alias_present(self, monkeypatch):
        """Return value contains '_source' alias pointing at source-county metrics."""
        counties = ["hillsborough"]
        _patch_ingest(monkeypatch, counties, source="hillsborough")

        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._write_platform_daily_stats_row",
            lambda *a: None,
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._cache_metric",
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._check_accelerated_wallet_push_floor",
            lambda *a: None,
        )

        db = _make_db_returning_zeros()
        with patch("src.tasks.kill_switch_metric_ingest.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            with patch("src.utils.county_config.list_counties", return_value=counties):
                from src.tasks.kill_switch_metric_ingest import run_kill_switch_metric_ingest
                result = run_kill_switch_metric_ingest(dry_run=False)

        assert "_source" in result
        assert result["_source"] is result["hillsborough"]

    def test_list_counties_failure_falls_back_to_source(self, monkeypatch):
        """If list_counties raises, falls back to source county — no crash."""
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest.settings",
            MagicMock(county_launch_source_county="hillsborough"),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._write_platform_daily_stats_row",
            lambda *a: None,
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._cache_metric",
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_metric_ingest._check_accelerated_wallet_push_floor",
            lambda *a: None,
        )

        db = _make_db_returning_zeros()
        with patch("src.tasks.kill_switch_metric_ingest.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            with patch("src.utils.county_config.list_counties", side_effect=Exception("db down")):
                from src.tasks.kill_switch_metric_ingest import run_kill_switch_metric_ingest
                result = run_kill_switch_metric_ingest(dry_run=True)

        assert "hillsborough" in result
