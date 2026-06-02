"""Phase 2 tests — weekly kill-switch scorecard task."""
from unittest.mock import MagicMock, patch, call
import json
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(source="hillsborough", self_healing=True):
    s = MagicMock()
    s.county_launch_source_county = source
    s.cora_self_healing_enabled = self_healing
    return s


def _mock_db_context(db):
    ctx = MagicMock()
    ctx.__enter__ = lambda s: db
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx


def _db_with_no_data():
    """DB that returns None/empty for every query (no snapshot rows, no incidents)."""
    first_result = MagicMock()
    first_result.first.return_value = None
    fetchall_result = MagicMock()
    fetchall_result.fetchall.return_value = []
    execute_result = MagicMock()
    execute_result.first.return_value = None
    execute_result.fetchall.return_value = []
    db = MagicMock()
    db.execute.return_value = execute_result
    return db


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestScorecardCardWritten:
    def test_card_written_with_expected_type_and_date(self, monkeypatch):
        """Card is written with card_type='kill_switch_scorecard'."""
        db = _db_with_no_data()
        written_sql = []
        _orig_result = db.execute.return_value

        def spy_execute(stmt, params=None):
            written_sql.append(str(stmt))
            return _orig_result

        db.execute = spy_execute

        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=False)

        assert any("kill_switch_scorecard" in s for s in written_sql)
        assert "summary" in result
        assert "card" in result

    def test_dry_run_writes_no_card(self, monkeypatch):
        """dry_run=True → no INSERT into learning_cards."""
        db = _db_with_no_data()
        written_sql = []

        original_execute = db.execute

        def spy_execute(stmt, params=None):
            written_sql.append(str(stmt))
            return original_execute.return_value

        db.execute = spy_execute

        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)

        assert not any("INSERT" in s for s in written_sql)
        assert result["dry_run"] is True


class TestCountyBreakdown:
    def test_per_county_feature_breakdown(self, monkeypatch):
        """Both counties appear in the card with a features list."""
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough", "pinellas"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)

        counties = result["card"]["counties"]
        assert "hillsborough" in counties
        assert "pinellas" in counties
        assert len(counties["hillsborough"]["features"]) > 0
        assert len(counties["pinellas"]["features"]) > 0

    def test_kill_rec_pending_only_source_county(self, monkeypatch):
        """pinellas features have kill_rec_pending=None (Level 1 limit)."""
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(source="hillsborough"),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough", "pinellas"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)

        pinellas_features = result["card"]["counties"]["pinellas"]["features"]
        for f in pinellas_features:
            assert f["kill_rec_pending"] is None, f"pinellas feature {f['metric']} has non-None kill_rec_pending"

        # hillsborough features have kill_rec_pending as bool (no incident → False)
        hills_features = result["card"]["counties"]["hillsborough"]["features"]
        for f in hills_features:
            assert isinstance(f["kill_rec_pending"], bool), \
                f"hillsborough feature {f['metric']} kill_rec_pending should be bool"

    def test_level1_footnote_present(self, monkeypatch):
        """Level-1 limitation footnote appears when non-source counties exist."""
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough", "pinellas"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)

        footnotes = result["card"]["footnotes"]
        assert any("Level 1" in n or "source-county-only" in n for n in footnotes)


class TestEngineDisabledFootnote:
    def test_engine_disabled_footnote_when_self_healing_off(self, monkeypatch):
        """Footnote mentions self-healing disabled when env var is false."""
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(self_healing=False),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)

        footnotes = result["card"]["footnotes"]
        assert any("self-healing" in n and "disabled" in n for n in footnotes)

    def test_no_crash_when_engine_disabled(self, monkeypatch):
        """No exception when self-healing is disabled (no incident rows)."""
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(self_healing=False),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)
        assert result is not None


class TestUnsnapshottedMetrics:
    def test_unsnapshotted_metric_streak_na(self, monkeypatch):
        """Metrics not in _BASELINE_COLUMNS have red_streak=None."""
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            from src.services.kill_switch_scorecard_data import SNAPSHOTTED_METRICS
            result = run_weekly_scorecard(dry_run=True)

        features = result["card"]["counties"]["hillsborough"]["features"]
        for f in features:
            if f["metric"] not in SNAPSHOTTED_METRICS:
                assert f["red_streak"] is None, \
                    f"{f['metric']} should have red_streak=None (not snapshotted)"

    def test_unsnapshotted_footnote_present(self, monkeypatch):
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)

        footnotes = result["card"]["footnotes"]
        assert any("streak n/a" in n for n in footnotes)


class TestChannelsStub:
    def test_channels_section_is_stub(self, monkeypatch):
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough"],
        )
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: None,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)

        ch = result["card"]["channels"]
        assert ch["status"] == "n/a"
        assert "ad-spend" in ch["reason"]


class TestAlarmLine:
    def test_all_green_produces_no_alarm(self, monkeypatch):
        """When all features are green/unknown, summary is 'KS: all green'."""
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough"],
        )
        # Return each metric's exact green threshold — green for both
        # higher_is_better (>=green) AND lower_is_better (<=green) directions.
        from config.cora_guardrails import KILL_SWITCH

        def green_value(metric, county_id=None):
            cfg = KILL_SWITCH.get(metric)
            return float(cfg["green"]) if cfg else None

        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            green_value,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)

        # Should not contain "KS:" alarm prefix with county details
        assert result["summary"] == "KS: all green"

    def test_alarm_line_present_when_red(self, monkeypatch):
        """When a red feature exists, alarm line contains 'KS:'."""
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.get_settings",
            lambda: _make_settings(),
        )
        monkeypatch.setattr(
            "src.tasks.kill_switch_scorecard.list_counties",
            lambda: ["hillsborough"],
        )
        # Return low value → red for higher_is_better metrics
        monkeypatch.setattr(
            "src.services.kill_switch_scorecard_data.get_cached_metric",
            lambda *a, **kw: 1.0,
        )

        db = _db_with_no_data()
        with patch("src.tasks.kill_switch_scorecard.get_db_context",
                   return_value=_mock_db_context(db)):
            from src.tasks.kill_switch_scorecard import run_weekly_scorecard
            result = run_weekly_scorecard(dry_run=True)

        assert result["summary"].startswith("KS:")
        assert "🔴" in result["summary"]
