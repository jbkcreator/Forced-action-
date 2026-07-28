"""Unit tests for A5: Pre-Decision Snapshot Service."""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from src.services.snapshot_service import (
    _compute_runner_ups,
    capture_snapshot,
    resolve_snapshot,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ALL_SCORES = {
    "wholesalers":      78.5,
    "fix_flip":         65.2,
    "restoration":      42.0,
    "roofing":          38.5,
    "public_adjusters": 55.0,
    "attorneys":        25.0,
}


def _mock_db(
    existing_id=None,
    vertical_scores=None,
    subscriber_row=None,
    pricing_row=None,
    lifecycle_graph=None,
    closer_row=None,
    rowcount=1,
):
    """Return a mock Session with deterministic execute side effects."""
    db = MagicMock()
    scores = vertical_scores if vertical_scores is not None else _ALL_SCORES

    distress_row = MagicMock()
    distress_row.__getitem__ = lambda self, k: {
        "vertical_scores": scores,
        "final_cds_score": 72.5,
        "lead_tier": "Gold",
        "urgency_level": "High",
        "distress_types": ["probate", "judgment_liens"],
    }[k]

    def _one_or_none_existing():
        m = MagicMock()
        m.scalar_one_or_none.return_value = existing_id
        return m

    def _one_or_none_subscriber():
        result = MagicMock()
        result.mappings.return_value.one_or_none.return_value = subscriber_row
        return result

    def _one_or_none_score():
        result = MagicMock()
        result.mappings.return_value.one_or_none.return_value = distress_row if scores else None
        return result

    def _one_or_none_pricing():
        result = MagicMock()
        result.mappings.return_value.one_or_none.return_value = pricing_row
        return result

    def _scalar_lifecycle():
        result = MagicMock()
        result.scalar_one_or_none.return_value = lifecycle_graph
        return result

    def _one_or_none_closer():
        result = MagicMock()
        result.mappings.return_value.one_or_none.return_value = closer_row
        return result

    db.execute.side_effect = [
        _one_or_none_existing(),    # idempotency check
        _one_or_none_score(),       # _gather_vertical_scores
        _one_or_none_subscriber(),  # subscriber_id lookup
        _one_or_none_pricing(),     # _gather_pricing
        _scalar_lifecycle(),             # _gather_lifecycle_graph
        _one_or_none_closer(),      # _gather_pitch_variant
    ]
    exec_result = MagicMock()
    exec_result.rowcount = rowcount
    db.execute.return_value = exec_result
    return db


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_capture_snapshot_writes_row():
    """Happy path: capture_snapshot calls db.add and db.flush."""
    db = _mock_db()
    result = capture_snapshot(property_id=123, db=db, deal_outcome_id=1, selected_vertical="wholesalers")
    db.add.assert_called_once()
    db.flush.assert_called()


def test_capture_snapshot_idempotent():
    """Second call for same deal_outcome_id returns existing row without re-insert."""
    existing_uuid = "550e8400-e29b-41d4-a716-446655440000"
    db = MagicMock()
    # idempotency check returns existing id
    idempotency_result = MagicMock()
    idempotency_result.scalar_one_or_none.return_value = existing_uuid
    db.execute.return_value = idempotency_result
    db.get.return_value = MagicMock(id=existing_uuid)

    result = capture_snapshot(property_id=123, db=db, deal_outcome_id=1)

    db.add.assert_not_called()
    db.get.assert_called_once()


def test_capture_snapshot_all_6_verticals():
    """all_vertical_scores on the created snapshot has exactly 6 keys."""
    added_snaps = []
    db = _mock_db()
    db.add.side_effect = lambda obj: added_snaps.append(obj)

    capture_snapshot(property_id=123, db=db, deal_outcome_id=1, selected_vertical="wholesalers")

    assert len(added_snaps) == 1
    snap = added_snaps[0]
    assert snap.all_vertical_scores is not None
    assert len(snap.all_vertical_scores) == 6


def test_capture_snapshot_runner_up_verticals():
    """runner_up_verticals has top-3 non-selected verticals sorted descending."""
    added_snaps = []
    db = _mock_db()
    db.add.side_effect = lambda obj: added_snaps.append(obj)

    capture_snapshot(property_id=123, db=db, deal_outcome_id=1, selected_vertical="wholesalers")

    snap = added_snaps[0]
    runners = snap.runner_up_verticals
    assert runners is not None
    assert len(runners) <= 3
    # First runner-up should be fix_flip (65.2) — highest after wholesalers (78.5)
    assert runners[0]["vertical"] == "fix_flip"
    assert runners[0]["score"] == 65.2
    # delta should be negative (below selected 78.5)
    assert runners[0]["delta_vs_selected"] < 0


def test_capture_snapshot_no_distress_score():
    """If distress_scores has no row, snapshot is still created with null score fields."""
    added_snaps = []
    db = _mock_db(vertical_scores={})
    db.add.side_effect = lambda obj: added_snaps.append(obj)

    result = capture_snapshot(property_id=999, db=db, deal_outcome_id=99, selected_vertical="fix_flip")

    # Should not raise — graceful degradation
    db.add.assert_called_once()
    snap = added_snaps[0]
    assert snap.all_vertical_scores is None or snap.all_vertical_scores == {}


def test_capture_snapshot_outcome_funded():
    """outcome_status='funded' sets resolved_at on the snapshot."""
    added_snaps = []
    db = _mock_db()
    db.add.side_effect = lambda obj: added_snaps.append(obj)

    capture_snapshot(property_id=123, db=db, deal_outcome_id=1,
                     selected_vertical="wholesalers", outcome_status="funded")

    snap = added_snaps[0]
    assert snap.outcome_status == "funded"
    assert snap.resolved_at is not None


def test_resolve_snapshot_updates_row():
    """resolve_snapshot executes an UPDATE with correct params."""
    db = MagicMock()
    resolve_snapshot(deal_outcome_id=42, outcome_status="lost", db=db)
    db.execute.assert_called_once()
    sql_str = str(db.execute.call_args[0][0])
    assert "UPDATE" in sql_str.upper()
    params = db.execute.call_args[0][1]
    assert params["status"] == "lost"
    assert params["did"] == 42


def test_resolve_snapshot_no_row():
    """resolve_snapshot is a silent no-op when no matching row exists."""
    db = MagicMock()
    # Should not raise even if rowcount == 0
    resolve_snapshot(deal_outcome_id=999, outcome_status="funded", db=db)
    db.execute.assert_called_once()


# ---------------------------------------------------------------------------
# Runner-up computation unit tests
# ---------------------------------------------------------------------------


def test_compute_runner_ups_sorted_descending():
    """Runner-ups are sorted by score descending."""
    scores = {"wholesalers": 80.0, "fix_flip": 60.0, "attorneys": 70.0,
              "roofing": 40.0, "restoration": 50.0, "public_adjusters": 65.0}
    runners = _compute_runner_ups(scores, "wholesalers")
    assert len(runners) == 3
    assert runners[0]["vertical"] == "attorneys"
    assert runners[1]["vertical"] == "public_adjusters"
    assert runners[2]["vertical"] == "fix_flip"


def test_compute_runner_ups_delta_correct():
    """delta_vs_selected is score - selected_score."""
    scores = {"wholesalers": 80.0, "fix_flip": 60.0, "attorneys": 70.0,
              "roofing": 40.0, "restoration": 50.0, "public_adjusters": 65.0}
    runners = _compute_runner_ups(scores, "wholesalers")
    assert runners[0]["delta_vs_selected"] == round(70.0 - 80.0, 2)  # -10.0


def test_compute_runner_ups_empty_scores():
    """Empty score dict returns empty list without error."""
    assert _compute_runner_ups({}, "wholesalers") == []
