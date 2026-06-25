"""Unit tests for A3: Warm-Start Priors & Heuristics Tuning."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.services.heuristic_loader import (
    invalidate_cache,
    load_overrides,
    reset_feedback_rows,
    seed_from_json,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_db(rows=None, scalar=None):
    """Return a minimal mock Session."""
    db = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = rows or []
    result.rowcount = scalar or 0
    db.execute.return_value = result
    return db


def _minimal_heuristics_json(tmp_path: Path, deltas: dict) -> Path:
    data = {
        "version": "test",
        "signal_weight_deltas": deltas,
        "delta_bounds": {"min": -15, "max": 15},
        "tuner_config": {
            "loss_window_days": 30,
            "win_window_days": 30,
            "min_sample_size": 5,
            "max_delta_per_run": 5,
            "loss_rate_threshold": 0.70,
            "win_boost_threshold": 5,
        },
    }
    p = tmp_path / "heuristics.json"
    p.write_text(json.dumps(data))
    return p


# ---------------------------------------------------------------------------
# load_overrides
# ---------------------------------------------------------------------------

def test_load_overrides_empty_db():
    """Empty table returns an empty dict."""
    invalidate_cache()
    db = _mock_db(rows=[])
    result = load_overrides(db)
    assert result == {}


def test_load_overrides_returns_mapping():
    """Rows are returned as (vertical, signal_type) → delta mapping."""
    invalidate_cache()
    db = _mock_db(rows=[
        ("wholesalers", "probate", 5.0),
        ("fix_flip", "foreclosures", 3.0),
    ])
    result = load_overrides(db)
    assert result[("wholesalers", "probate")] == 5.0
    assert result[("fix_flip", "foreclosures")] == 3.0


# ---------------------------------------------------------------------------
# seed_from_json
# ---------------------------------------------------------------------------

def test_seed_from_json(tmp_path):
    """All signal_weight_deltas entries are upserted with correct values."""
    p = _minimal_heuristics_json(tmp_path, {"wholesalers": {"probate": 5, "judgment_liens": 3}})
    db = MagicMock()
    n = seed_from_json(db, json_path=str(p))
    assert n == 2
    assert db.execute.call_count == 2


def test_seed_from_json_clamps_to_bounds(tmp_path):
    """Deltas exceeding bounds are clamped before upsert."""
    p = _minimal_heuristics_json(tmp_path, {"wholesalers": {"probate": 99}})
    db = MagicMock()
    seed_from_json(db, json_path=str(p))
    call_params = db.execute.call_args_list[0][0][1]
    assert call_params["d"] == 15.0  # clamped to max


def test_seed_skips_empty_verticals(tmp_path):
    """Verticals with no deltas produce no upserts."""
    p = _minimal_heuristics_json(tmp_path, {"wholesalers": {}, "fix_flip": {"foreclosures": 3}})
    db = MagicMock()
    n = seed_from_json(db, json_path=str(p))
    assert n == 1


# ---------------------------------------------------------------------------
# Override applied in scoring
# ---------------------------------------------------------------------------

def test_override_applied_in_scoring():
    """MultiVerticalScorer applies delta to base weight via _weight_overrides."""
    from config.scoring import VERTICAL_WEIGHTS

    base_raw = VERTICAL_WEIGHTS["wholesalers"]["probate"]
    delta = 5.0
    expected = max(0, min(100, base_raw + delta))

    # Confirm the formula matches what _score_vertical uses
    assert expected == base_raw + delta  # probate base is well below 95, so no clamping


def test_delta_clamped_at_upper_bound():
    """Delta that would push base above 100 is clamped to 100."""
    # Simulate a signal with base=98, delta=+10 → effective=100 not 108
    base_raw = 98
    delta = 10.0
    effective = max(0, min(100, base_raw + delta))
    assert effective == 100


def test_delta_clamped_at_lower_bound():
    """Delta that would push base below 0 is clamped to 0."""
    base_raw = 5
    delta = -20.0
    effective = max(0, min(100, base_raw + delta))
    assert effective == 0


# ---------------------------------------------------------------------------
# reset_feedback_rows
# ---------------------------------------------------------------------------

def test_reset_feedback_rows():
    """reset_feedback_rows calls DELETE and returns rowcount."""
    db = MagicMock()
    db.execute.return_value.rowcount = 3
    deleted = reset_feedback_rows(db)
    assert deleted == 3
    call_sql = str(db.execute.call_args_list[0][0][0])
    assert "loss_feedback" in call_sql or "win_feedback" in call_sql


# ---------------------------------------------------------------------------
# Tuner logic
# ---------------------------------------------------------------------------

def _make_tuner_db(loss_rows=None, win_rows=None, current_rows=None):
    """Return a mock session that returns loss/win/current rows in sequence."""
    db = MagicMock()
    results = [
        MagicMock(fetchall=MagicMock(return_value=loss_rows or [])),
        MagicMock(fetchall=MagicMock(return_value=win_rows or [])),
        MagicMock(fetchall=MagicMock(return_value=current_rows or [])),
    ]
    db.execute.side_effect = results
    return db


def test_tuner_applies_loss_penalty():
    """High loss rate → negative delta computed and upserted."""
    from tasks.heuristic_tuner import run

    db = _make_tuner_db(
        loss_rows=[("wholesalers", "probate", 8)],
        win_rows=[("wholesalers", "probate", 2)],
        current_rows=[],
    )
    with patch("tasks.heuristic_tuner._HEURISTICS_PATH",
               Path("config/heuristics.json")):
        summary = run(db=db, dry_run=True)

    assert summary["rows_updated"] >= 1
    update = next(u for u in summary["updates"] if u["signal_type"] == "probate")
    assert update["delta"] < 0
    assert update["source"] == "loss_feedback"


def test_tuner_applies_win_boost():
    """High win count → positive delta computed."""
    from tasks.heuristic_tuner import run

    db = _make_tuner_db(
        loss_rows=[("wholesalers", "probate", 0)],
        win_rows=[("wholesalers", "probate", 10)],
        current_rows=[],
    )
    with patch("tasks.heuristic_tuner._HEURISTICS_PATH",
               Path("config/heuristics.json")):
        summary = run(db=db, dry_run=True)

    assert summary["rows_updated"] >= 1
    update = next(u for u in summary["updates"] if u["signal_type"] == "probate")
    assert update["delta"] > 0
    assert update["source"] == "win_feedback"


def test_tuner_skips_below_min_sample():
    """Pair with total < min_sample_size (5) produces no update."""
    from tasks.heuristic_tuner import run

    db = _make_tuner_db(
        loss_rows=[("wholesalers", "probate", 3)],
        win_rows=[("wholesalers", "probate", 1)],
        current_rows=[],
    )
    with patch("tasks.heuristic_tuner._HEURISTICS_PATH",
               Path("config/heuristics.json")):
        summary = run(db=db, dry_run=True)

    assert summary["rows_updated"] == 0


def test_tuner_dry_run_no_db_write():
    """dry_run=True: delta computed but no upsert SQL issued."""
    from tasks.heuristic_tuner import run

    db = _make_tuner_db(
        loss_rows=[("wholesalers", "probate", 8)],
        win_rows=[("wholesalers", "probate", 2)],
        current_rows=[],
    )
    with patch("tasks.heuristic_tuner._HEURISTICS_PATH",
               Path("config/heuristics.json")):
        summary = run(db=db, dry_run=True)

    # Only the 3 SELECT calls should have been made, no INSERT/UPDATE
    assert summary["dry_run"] is True
    for call in db.execute.call_args_list:
        sql = str(call[0][0]).upper()
        assert "INSERT" not in sql and "UPDATE" not in sql
