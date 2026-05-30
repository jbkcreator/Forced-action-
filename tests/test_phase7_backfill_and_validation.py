"""Phase 7 tests — outcome backfill and churn_validation_report.

All DB I/O stubbed. No real DB required.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.tasks.churn_validation_report import _median, compute_report


_NOW = datetime(2026, 5, 30, 13, 0, 0, tzinfo=timezone.utc)


def _ns(**kwargs):
    return SimpleNamespace(**kwargs)


def _make_pred(
    sub_id=1,
    score=72,
    band="high",
    predicted_at=None,
    in_holdout=False,
    save_offer_sent_at=None,
    predicted_inactivity_at=None,
    realized_inactive_at=None,
    was_correct=None,
):
    return _ns(
        subscriber_id=sub_id,
        churn_risk_score=score,
        churn_risk_band=band,
        predicted_at=predicted_at or (_NOW - timedelta(days=6)),
        in_holdout=in_holdout,
        save_offer_sent_at=save_offer_sent_at,
        predicted_inactivity_at=predicted_inactivity_at or (_NOW - timedelta(days=3)),
        realized_inactive_at=realized_inactive_at,
        was_correct=was_correct,
    )


def _build_db(rows):
    db = MagicMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = rows
    db.execute.return_value = result
    return db


# ── _median helper ────────────────────────────────────────────────────────


def test_median_odd():
    assert _median([1, 3, 5]) == 3


def test_median_even():
    assert _median([1, 2, 3, 4]) == 2.5


# ── backfill integration (via churn_scoring._backfill_outcomes) ───────────


def test_backfill_sets_realized_outcome():
    """Subscriber who went inactive → was_correct=True on their prediction."""
    from src.tasks.churn_scoring import _backfill_outcomes

    pred = _ns(
        subscriber_id=1,
        predicted_at=_NOW - timedelta(days=6),
        predicted_inactivity_at=_NOW - timedelta(days=3),
        realized_inactive_at=None,
        was_correct=None,
    )

    call_count = [0]

    def _execute(*_a, **_kw):
        result = MagicMock()
        idx = call_count[0]
        call_count[0] += 1
        if idx == 0:
            # backfill query: return [pred]
            result.scalars.return_value.all.return_value = [pred]
        else:
            # last_debit query: return None (subscriber went inactive)
            result.scalar_one_or_none.return_value = None
        return result

    db = MagicMock()
    db.execute.side_effect = _execute

    count = _backfill_outcomes(db, _NOW)

    assert count == 1
    assert pred.was_correct is True
    assert pred.realized_inactive_at == _NOW


def test_backfill_marks_false_positive():
    """Flagged subscriber who stayed active (non-holdout) → was_correct=False."""
    from src.tasks.churn_scoring import _backfill_outcomes

    pred = _ns(
        subscriber_id=2,
        predicted_at=_NOW - timedelta(days=6),
        predicted_inactivity_at=_NOW - timedelta(days=3),
        realized_inactive_at=None,
        was_correct=None,
    )

    debit_time = _NOW - timedelta(days=2)  # subscriber debited after prediction

    call_count = [0]

    def _execute(*_a, **_kw):
        result = MagicMock()
        idx = call_count[0]
        call_count[0] += 1
        if idx == 0:
            result.scalars.return_value.all.return_value = [pred]
        else:
            result.scalar_one_or_none.return_value = debit_time
        return result

    db = MagicMock()
    db.execute.side_effect = _execute

    _backfill_outcomes(db, _NOW)

    assert pred.was_correct is False
    assert pred.realized_inactive_at == debit_time


def test_backfill_idempotent():
    """Rows already backfilled (realized_inactive_at is not None) are not re-touched."""
    from src.tasks.churn_scoring import _backfill_outcomes

    # _backfill_outcomes only queries rows WHERE realized_inactive_at IS NULL
    # Simulate this by returning an empty list (no unbackfilled rows)
    db = MagicMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = []
    db.execute.return_value = result

    count = _backfill_outcomes(db, _NOW)
    assert count == 0


# ── compute_report ────────────────────────────────────────────────────────


def test_report_handles_insufficient_sample():
    """< MIN_SAMPLE labeled rows → insufficient_sample=True, no noisy numbers."""
    db = _build_db([])  # empty — definitely below minimum
    report = compute_report(db)
    assert report["insufficient_sample"] is True
    assert "precision" not in report


def _make_fixture_rows(n_tp=20, n_fp=5, n_holdout_onset=8, n_holdout_ok=2, n_saved_onset=3, n_saved_ok=12):
    """Build a controlled fixture of backfilled rows for precision/recall/lift math."""
    rows = []
    sub_id = 1

    # True positives (high risk, went inactive, not holdout, offer sent)
    for _ in range(n_tp):
        rows.append(_make_pred(
            sub_id=sub_id,
            band="high",
            in_holdout=False,
            save_offer_sent_at=_NOW - timedelta(days=3),
            predicted_inactivity_at=_NOW - timedelta(days=3),
            realized_inactive_at=_NOW,
            was_correct=True,
        ))
        sub_id += 1

    # False positives (high risk, stayed active)
    for _ in range(n_fp):
        rows.append(_make_pred(
            sub_id=sub_id,
            band="high",
            in_holdout=False,
            save_offer_sent_at=_NOW - timedelta(days=3),
            predicted_inactivity_at=_NOW - timedelta(days=3),
            realized_inactive_at=_NOW - timedelta(days=2),
            was_correct=False,
        ))
        sub_id += 1

    # Holdout: went inactive
    for _ in range(n_holdout_onset):
        rows.append(_make_pred(
            sub_id=sub_id,
            band="high",
            in_holdout=True,
            save_offer_sent_at=None,
            predicted_inactivity_at=_NOW - timedelta(days=3),
            realized_inactive_at=_NOW,
            was_correct=True,
        ))
        sub_id += 1

    # Holdout: stayed active
    for _ in range(n_holdout_ok):
        rows.append(_make_pred(
            sub_id=sub_id,
            band="high",
            in_holdout=True,
            save_offer_sent_at=None,
            realized_inactive_at=_NOW - timedelta(days=2),
            was_correct=False,
        ))
        sub_id += 1

    # Saved arm (not holdout, offer sent): went inactive
    for _ in range(n_saved_onset):
        rows.append(_make_pred(
            sub_id=sub_id,
            band="high",
            in_holdout=False,
            save_offer_sent_at=_NOW - timedelta(days=3),
            predicted_inactivity_at=_NOW - timedelta(days=3),
            realized_inactive_at=_NOW,
            was_correct=True,
        ))
        sub_id += 1

    # Saved arm: stayed active
    for _ in range(n_saved_ok):
        rows.append(_make_pred(
            sub_id=sub_id,
            band="high",
            in_holdout=False,
            save_offer_sent_at=_NOW - timedelta(days=3),
            realized_inactive_at=_NOW - timedelta(days=2),
            was_correct=False,
        ))
        sub_id += 1

    return rows


def test_validation_precision_recall_math():
    """Known fixture → expected precision/recall values.

    Isolate to only non-holdout high-risk rows so the math is clear:
    TP=24, FP=6 → precision = 24/30 = 0.8.
    """
    rows = _make_fixture_rows(
        n_tp=24, n_fp=6,
        n_holdout_onset=0, n_holdout_ok=0,
        n_saved_onset=0, n_saved_ok=0,
    )
    assert len(rows) >= 30  # meets MIN_SAMPLE
    db = _build_db(rows)
    report = compute_report(db)

    assert report["insufficient_sample"] is False
    # tp=24, fp=6 → precision=24/30=0.8
    assert report["precision"] == pytest.approx(0.8, abs=0.01)


def test_lead_time_computation():
    """Flagged 3 days before onset → lead_time ≈ 3."""
    predicted_at = _NOW - timedelta(days=6)
    realized_at = _NOW - timedelta(days=3)
    # lead_time = (realized - predicted_at) in days = 3

    rows = [
        _make_pred(
            sub_id=1,
            band="high",
            predicted_at=predicted_at,
            in_holdout=False,
            save_offer_sent_at=_NOW - timedelta(days=4),
            predicted_inactivity_at=_NOW - timedelta(days=3),
            realized_inactive_at=realized_at,
            was_correct=True,
        )
    ] * 35  # enough for MIN_SAMPLE

    db = _build_db(rows)
    report = compute_report(db)

    assert report["median_lead_time_days"] == pytest.approx(3.0, abs=0.1)


def test_lift_uses_holdout_arm_only():
    """Lift computed from holdout vs saved arm rates."""
    rows = _make_fixture_rows(n_holdout_onset=8, n_holdout_ok=2, n_saved_onset=2, n_saved_ok=13)
    db = _build_db(rows)
    report = compute_report(db)

    assert report["insufficient_sample"] is False
    assert report["holdout_n"] > 0
    assert report["saved_n"] > 0
    # holdout onset rate = 8/10 = 0.8; saved onset rate = 2/15 ≈ 0.133
    # lift = 0.8 - 0.133 > 0 (positive lift = save offer works)
    assert report["lift"] is not None
    assert report["lift"] > 0
