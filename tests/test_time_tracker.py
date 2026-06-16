"""Unit tests for src/utils/time_tracker.py — no DB, tmp_path based."""

import json
import time

import pytest

from src.utils.time_tracker import TimeTracker


def _read_lines(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


class TestSpan:
    def test_span_writes_start_marker_then_timed_line(self, tmp_path):
        log = tmp_path / "t.jsonl"
        tracker = TimeTracker("unit", run_id="run1", log_path=log,
                              meta={"county": "x"})
        with tracker.span("phase_a", chunk=1):
            pass

        start, line = _read_lines(log)
        # start marker: lets a live tail show the phase currently running
        assert start["type"] == "start"
        assert start["name"] == "phase_a"
        assert start["meta"] == {"chunk": 1}
        assert "seconds" not in start
        # timed span line on completion
        assert line["type"] == "span"
        assert line["name"] == "phase_a"
        assert line["component"] == "unit"
        assert line["run_id"] == "run1"
        assert line["run_meta"] == {"county": "x"}
        assert line["meta"] == {"chunk": 1}
        assert isinstance(line["seconds"], float)
        assert "ts" in line

    def test_mid_block_meta_enrichment_lands_in_span_line_only(self, tmp_path):
        log = tmp_path / "t.jsonl"
        tracker = TimeTracker("unit", log_path=log)
        with tracker.span("preload") as s:
            s["rows"] = 428_079
        start, line = _read_lines(log)
        assert "rows" not in start["meta"]      # enrichment happened after start
        assert line["meta"]["rows"] == 428_079

    def test_duration_is_plausible(self, tmp_path):
        log = tmp_path / "t.jsonl"
        tracker = TimeTracker("unit", log_path=log)
        with tracker.span("sleepy"):
            time.sleep(0.02)
        line = _read_lines(log)[-1]
        assert line["seconds"] >= 0.015

    def test_span_written_even_when_block_raises(self, tmp_path):
        log = tmp_path / "t.jsonl"
        tracker = TimeTracker("unit", log_path=log)
        with pytest.raises(ValueError):
            with tracker.span("boom"):
                raise ValueError("host error")
        line = _read_lines(log)[-1]
        assert line["type"] == "span"
        assert line["name"] == "boom"   # timed and recorded despite the raise

    def test_non_json_meta_does_not_break(self, tmp_path):
        from decimal import Decimal
        log = tmp_path / "t.jsonl"
        tracker = TimeTracker("unit", log_path=log)
        with tracker.span("decimals", value=Decimal("3.14")):
            pass
        line = _read_lines(log)[-1]
        assert line["meta"]["value"] == "3.14"   # default=str fallback
        assert tracker.enabled is True


class TestEventAndSummary:
    def test_event_writes_marker_line(self, tmp_path):
        log = tmp_path / "t.jsonl"
        tracker = TimeTracker("unit", log_path=log)
        tracker.event("download_skipped", reason="--skip-download")
        (line,) = _read_lines(log)
        assert line["type"] == "event"
        assert line["meta"]["reason"] == "--skip-download"
        assert "seconds" not in line

    def test_summary_aggregates(self, tmp_path):
        log = tmp_path / "t.jsonl"
        tracker = TimeTracker("unit", log_path=log)
        for _ in range(3):
            with tracker.span("chunk"):
                time.sleep(0.01)
        with tracker.span("stmt1"):
            time.sleep(0.01)
        tracker.finish(updated=412, dry_run=True)

        lines = _read_lines(log)
        summary = lines[-1]
        assert summary["type"] == "summary"
        assert summary["meta"] == {"updated": 412, "dry_run": True}
        assert summary["wall_seconds"] > 0
        chunk = summary["spans"]["chunk"]
        assert chunk["calls"] == 3
        assert chunk["total_seconds"] >= 0.03 - 0.005
        assert chunk["max_seconds"] >= chunk["mean_seconds"] - 1e-6
        assert 0 < chunk["pct_wall"] <= 100
        assert summary["spans"]["stmt1"]["calls"] == 1
        # sorted by total desc — chunk (3 sleeps) before stmt1 (1 sleep)
        assert list(summary["spans"]) == ["chunk", "stmt1"]


class TestSafety:
    def test_disabled_tracker_is_a_noop(self, tmp_path):
        log = tmp_path / "t.jsonl"
        tracker = TimeTracker("unit", log_path=log, enabled=False)
        with tracker.span("x") as s:
            assert s == {}
        tracker.event("y")
        tracker.finish()
        assert not log.exists()

    def test_unwritable_path_never_raises_and_self_disables(self, tmp_path, caplog):
        # A file used as a directory makes mkdir/open fail on every platform
        blocker = tmp_path / "blocker"
        blocker.write_text("not a dir")
        bad = blocker / "sub" / "t.jsonl"

        tracker = TimeTracker("unit", log_path=bad)
        with caplog.at_level("WARNING"):
            with tracker.span("phase"):
                pass            # host block must run untouched
            tracker.event("e")
            tracker.finish()

        assert tracker.enabled is False
        warnings = [r for r in caplog.records if "TimeTracker disabled" in r.message]
        assert len(warnings) == 1   # one warning, then silent

    def test_host_flow_unaffected_after_disable(self, tmp_path):
        blocker = tmp_path / "blocker"
        blocker.write_text("not a dir")
        tracker = TimeTracker("unit", log_path=blocker / "x" / "t.jsonl")

        result = []
        with tracker.span("work"):
            result.append(1)    # first span disables the tracker
        with tracker.span("more") as s:
            result.append(2)
            s["k"] = "v"        # enriching a dead span is harmless
        assert result == [1, 2]
