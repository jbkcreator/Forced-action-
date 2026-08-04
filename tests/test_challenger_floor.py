"""
LEARN-v2.2 L3 challenger protected-floor — pure-function tests.

Covers the share math (compute_share) and the classify_cells membership rule
without a DB, per CLAUDE.md's preference for targeted tests.
"""
from __future__ import annotations

from types import SimpleNamespace

from config.challenger_floor import (
    CHALLENGER_FLOOR_PCT,
    MIN_TOTAL_SENDS,
    validate_challenger_config,
)
from src.services.challenger_floor import (
    classify_cells,
    compute_share,
    evaluate_floor,
)


def test_config_valid():
    assert validate_challenger_config() == []


def test_under_floor():
    # 60 challenger sends of 400 total = 15% — under the 30% floor.
    share, under, shortfall, note = compute_share(60, 400, CHALLENGER_FLOOR_PCT, MIN_TOTAL_SENDS)
    assert share == 15.0
    assert under is True
    assert shortfall == 15.0
    assert "under" in note.lower()


def test_at_or_above_floor():
    # 160 of 400 = 40% — not under.
    share, under, shortfall, note = compute_share(160, 400, CHALLENGER_FLOOR_PCT, MIN_TOTAL_SENDS)
    assert share == 40.0
    assert under is False
    assert shortfall is None


def test_exactly_on_floor_is_not_under():
    share, under, shortfall, _ = compute_share(30, 100, 30, MIN_TOTAL_SENDS)
    assert share == 30.0
    assert under is False
    assert shortfall is None


def test_insufficient_volume():
    share, under, shortfall, note = compute_share(5, 10, CHALLENGER_FLOOR_PCT, MIN_TOTAL_SENDS)
    assert share is None
    assert under is False
    assert shortfall is None
    assert "insufficient" in note.lower()


def test_zero_total_no_divide_by_zero():
    share, under, shortfall, note = compute_share(0, 0, CHALLENGER_FLOOR_PCT, MIN_TOTAL_SENDS)
    assert share is None
    assert under is False
    assert shortfall is None


class _FakeExec:
    """Minimal db.execute(...).fetchall() stub returning preset verdict rows."""

    def __init__(self, cell_ids):
        self._rows = [SimpleNamespace(cell_id=c) for c in cell_ids]

    def execute(self, *a, **k):
        return self

    def fetchall(self):
        return self._rows


def test_classify_verdict_row_is_incumbent(monkeypatch):
    stats = {
        "cell_a": SimpleNamespace(sends=10),   # has verdict row -> incumbent
        "cell_b": SimpleNamespace(sends=10),   # no verdict, low sends -> challenger
    }
    db = _FakeExec(["cell_a"])
    challengers, incumbents = classify_cells(db, "v1", stats)
    assert challengers == {"cell_b"}
    assert incumbents == {"cell_a"}


def test_classify_high_sends_no_verdict_is_challenger():
    # A well-sampled cell with no verdict row (dead zone: not auto_double, not
    # throttle) is still a CHALLENGER — send count alone never reclassifies it.
    stats = {
        "cell_a": SimpleNamespace(sends=1000),   # many sends, no verdict -> challenger
        "cell_b": SimpleNamespace(sends=5),      # few sends, no verdict -> challenger
    }
    db = _FakeExec([])  # no verdict rows
    challengers, incumbents = classify_cells(db, "v1", stats)
    assert challengers == {"cell_a", "cell_b"}
    assert incumbents == set()


class _FakeEvalDb:
    """db stub for evaluate_floor: verdict query returns preset cell_ids."""

    def __init__(self, verdict_cell_ids):
        self._rows = [SimpleNamespace(cell_id=c) for c in verdict_cell_ids]

    def execute(self, *a, **k):
        return self

    def fetchall(self):
        return self._rows


def test_evaluate_floor_no_challengers_not_under_floor(monkeypatch):
    import src.services.challenger_floor as svc

    stats = {
        "cell_a": SimpleNamespace(sends=500),
        "cell_b": SimpleNamespace(sends=500),
    }
    monkeypatch.setattr(svc, "_ladder_row", lambda db, key: SimpleNamespace())
    monkeypatch.setattr(svc, "_ineligible_for_auto_double", lambda row: None)
    monkeypatch.setattr(svc, "cell_reply_rates", lambda db, key, window_days: stats)

    # Every active cell has a verdict -> challenger cohort is empty.
    db = _FakeEvalDb(["cell_a", "cell_b"])
    report = evaluate_floor(db, "v1")

    assert report.challenger_cells == []
    assert report.under_floor is False
    assert report.challenger_share_pct == 0.0
    assert "challenger" in report.note.lower()
    assert report.total_sends == 1000
