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
    VERDICT_SAMPLE,
    validate_challenger_config,
)
from src.services.challenger_floor import classify_cells, compute_share


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


def test_classify_high_sends_is_incumbent():
    stats = {
        "cell_a": SimpleNamespace(sends=VERDICT_SAMPLE),   # judged by sample -> incumbent
        "cell_b": SimpleNamespace(sends=VERDICT_SAMPLE - 1),  # under sample, no verdict -> challenger
    }
    db = _FakeExec([])  # no verdict rows
    challengers, incumbents = classify_cells(db, "v1", stats)
    assert challengers == {"cell_b"}
    assert incumbents == {"cell_a"}
