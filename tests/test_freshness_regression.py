"""
Aggregate freshness-regression alert — distinct from heartbeat_monitor.py's
existing per-source alerts. Diffs the CURRENT fleet-wide fresh/stale split
against the split 4 days ago (default FRESHNESS_REGRESSION_BASELINE_DAYS),
recomputed via compute_heartbeats()'s own `now=` parameter rather than a new
time-series table — scraper_run_stats' history is permanent, so calling the
same, already-tested function with an older `now` genuinely recomputes
historical freshness.

Pure-function coverage only (newly_stale_labels / FreshnessRegression /
check_freshness_regression), all via monkeypatched compute_heartbeats() —
no DB access, no live scraper_run_stats reads.
"""
from datetime import datetime, timezone

from src.tasks.heartbeat_monitor import (
    FRESHNESS_REGRESSION_MIN_NEWLY_STALE,
    FreshnessRegression,
    Heartbeat,
    check_freshness_regression,
    newly_stale_labels,
)


def _beat(source_type, is_stale, county_id=None):
    return Heartbeat(
        source_type=source_type, sla_minutes=1500, last_success_at=None,
        age_minutes=0, is_stale=is_stale, county_id=county_id,
    )


class TestNewlyStaleLabels:
    def test_fresh_to_stale_is_newly_stale(self):
        now = [_beat("permits", is_stale=True)]
        baseline = [_beat("permits", is_stale=False)]
        assert newly_stale_labels(now, baseline) == ["permits"]

    def test_stale_to_stale_is_not_newly_stale(self):
        """Already stale at baseline too -> not a NEW regression, an
        ongoing one; per-source heartbeat alerts already cover this."""
        now = [_beat("permits", is_stale=True)]
        baseline = [_beat("permits", is_stale=True)]
        assert newly_stale_labels(now, baseline) == []

    def test_fresh_to_fresh_is_not_newly_stale(self):
        now = [_beat("permits", is_stale=False)]
        baseline = [_beat("permits", is_stale=False)]
        assert newly_stale_labels(now, baseline) == []

    def test_absent_from_baseline_is_skipped_not_counted(self):
        """A label only present now (e.g. an off-day skip at baseline time)
        has nothing to compare against — must not be treated as a
        regression by default-counting it in."""
        now = [_beat("permits", is_stale=True)]
        baseline = []
        assert newly_stale_labels(now, baseline) == []

    def test_multi_county_labels_compared_independently(self):
        now = [
            _beat("violations", is_stale=True, county_id="pinellas"),
            _beat("violations", is_stale=False, county_id="hillsborough"),
        ]
        baseline = [
            _beat("violations", is_stale=False, county_id="pinellas"),
            _beat("violations", is_stale=False, county_id="hillsborough"),
        ]
        assert newly_stale_labels(now, baseline) == ["violations/pinellas"]


class TestFreshnessRegressionDataclass:
    def test_below_threshold_is_not_a_regression(self):
        r = FreshnessRegression(
            now_fresh_count=39, now_total_count=41, baseline_fresh_count=41,
            baseline_total_count=41, baseline_days=4,
            newly_stale=["permits", "foreclosures"],
        )
        assert len(r.newly_stale) == 2 < FRESHNESS_REGRESSION_MIN_NEWLY_STALE
        assert r.is_regression is False

    def test_at_threshold_is_a_regression(self):
        r = FreshnessRegression(
            now_fresh_count=38, now_total_count=41, baseline_fresh_count=41,
            baseline_total_count=41, baseline_days=4,
            newly_stale=["permits", "foreclosures", "sunbiz"],
        )
        assert len(r.newly_stale) == FRESHNESS_REGRESSION_MIN_NEWLY_STALE
        assert r.is_regression is True

    def test_alert_subject_and_body_name_every_newly_stale_source(self):
        r = FreshnessRegression(
            now_fresh_count=38, now_total_count=41, baseline_fresh_count=41,
            baseline_total_count=41, baseline_days=4,
            newly_stale=["permits", "foreclosures", "sunbiz"],
        )
        subject = r.alert_subject()
        body = r.alert_body()
        assert "3" in subject
        assert "38/41" in subject
        assert "41/41" in subject
        for label in r.newly_stale:
            assert label in body
        assert "AGGREGATE" in body


class TestCheckFreshnessRegression:
    def test_diffs_against_a_four_day_baseline_by_default(self, monkeypatch):
        calls = []

        def fake_compute_heartbeats(now=None):
            calls.append(now)
            return [_beat("permits", is_stale=False)]

        monkeypatch.setattr(
            "src.tasks.heartbeat_monitor.compute_heartbeats", fake_compute_heartbeats
        )
        now = datetime(2026, 9, 2, tzinfo=timezone.utc)
        beats_now = [_beat("permits", is_stale=True)]

        result = check_freshness_regression(beats_now, now=now)

        assert result.baseline_days == 4
        assert calls == [datetime(2026, 8, 29, tzinfo=timezone.utc)]
        assert result.newly_stale == ["permits"]
        assert result.now_fresh_count == 0
        assert result.now_total_count == 1
        assert result.baseline_fresh_count == 1
        assert result.baseline_total_count == 1

    def test_custom_baseline_days_is_honored(self, monkeypatch):
        calls = []

        def fake_compute_heartbeats(now=None):
            calls.append(now)
            return []

        monkeypatch.setattr(
            "src.tasks.heartbeat_monitor.compute_heartbeats", fake_compute_heartbeats
        )
        now = datetime(2026, 9, 2, tzinfo=timezone.utc)
        check_freshness_regression([], now=now, baseline_days=7)
        assert calls == [datetime(2026, 8, 26, tzinfo=timezone.utc)]
