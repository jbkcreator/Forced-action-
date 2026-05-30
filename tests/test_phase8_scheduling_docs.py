"""Phase 8 tests — scheduling and documentation checks.

Validates crontab entries and config/churn.py importability.
No DB required.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


_CRONTAB = Path(__file__).parent.parent / "scripts" / "cron" / "crontab.txt"


def _read_crontab() -> str:
    return _CRONTAB.read_text(encoding="utf-8")


# ── crontab ───────────────────────────────────────────────────────────────


def test_crontab_churn_scoring_present():
    """churn_scoring is scheduled in crontab.txt."""
    content = _read_crontab()
    assert "churn_scoring" in content, "churn_scoring not found in crontab"


def test_crontab_churn_validation_report_present():
    """churn_validation_report is scheduled in crontab.txt."""
    content = _read_crontab()
    assert "churn_validation_report" in content


def test_crontab_ordering_churn_before_proactive():
    """The active churn_scoring cron line appears before proactive_save in crontab."""
    lines = _read_crontab().splitlines()
    active = [ln for ln in lines if ln.strip() and not ln.strip().startswith("#")]
    churn_idx = next((i for i, ln in enumerate(active) if "churn_scoring" in ln), None)
    save_idx = next((i for i, ln in enumerate(active) if "proactive_save" in ln), None)
    assert churn_idx is not None, "No active churn_scoring cron line found"
    assert save_idx is not None, "No active proactive_save cron line found"
    assert churn_idx < save_idx, (
        "churn_scoring must appear before proactive_save in active cron lines "
        "(load-bearing ordering dependency)"
    )


def test_crontab_proactive_save_at_15():
    """proactive_save is scheduled at hour 15 UTC (after churn_scoring at 13)."""
    content = _read_crontab()
    # Find the cron line for proactive_save (not churn_scoring)
    save_lines = [ln for ln in content.splitlines() if "proactive_save" in ln and not ln.strip().startswith("#")]
    assert save_lines, "No active proactive_save cron line found"
    # Each active line should start with a minute/hour pattern; hour should be 15
    for line in save_lines:
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            assert parts[1] == "15", f"proactive_save hour is {parts[1]}, expected 15"


# ── config/churn.py ───────────────────────────────────────────────────────


def test_config_churn_importable():
    """config/churn.py loads without error."""
    import config.churn as churn
    assert churn


def test_config_churn_weights_sum():
    """WEIGHTS sum to 1.0 (within float tolerance)."""
    from config.churn import WEIGHTS
    total = sum(WEIGHTS.values())
    assert abs(total - 1.0) < 0.001, f"WEIGHTS sum to {total}, expected 1.0"


def test_config_churn_horizon():
    """HORIZON_DAYS = 3 (precision-biased default)."""
    from config.churn import HORIZON_DAYS
    assert HORIZON_DAYS == 3


def test_config_churn_fire_bands():
    """FIRE_BANDS contains exactly high and very_high."""
    from config.churn import FIRE_BANDS
    assert FIRE_BANDS == {"high", "very_high"}


# ── smoke: declining subscriber end-to-end ────────────────────────────────


@pytest.mark.scenario_platform
def test_scenario_declining_subscriber_end_to_end():
    """Smoke: declining subscriber → churn_scoring flags → proactive_save fires early.

    This is a lightweight integration check using all-mocked collaborators.
    It verifies the pipeline contract: scoring at day ~2 (before old day-5 trigger).
    """
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch

    NOW = datetime(2026, 5, 30, 15, 0, 0, tzinfo=timezone.utc)
    sub_id = 42

    # churn_scoring sees: 3 days since last debit (high risk), predicts onset at day 4
    predicted_onset = NOW + timedelta(days=1)   # 1 day out → within HORIZON_DAYS

    # proactive_save reads the latest churn_predictions row: band=high, onset 1d out,
    # not in holdout. Second read is MAX(save_offer_sent_at) → None (never offered).
    prediction = SimpleNamespace(
        churn_risk_band="high",
        predicted_inactivity_at=predicted_onset,
        in_holdout=False,
        save_offer_sent_at=None,
    )

    call_count = [0]

    def _execute(*_a, **_kw):
        result = MagicMock()
        idx = call_count[0]
        call_count[0] += 1
        if idx == 0:
            result.scalar_one_or_none.return_value = prediction
        else:
            result.scalar_one_or_none.return_value = None  # MAX(save_offer_sent_at)
        return result

    mock_db = MagicMock()
    mock_db.execute.side_effect = _execute

    from src.tasks.proactive_save import _identify_risk

    sub = SimpleNamespace(
        id=sub_id, tier="wallet", status="active",
        grace_expires_at=None, created_at=NOW - timedelta(days=90),
    )

    with patch("src.tasks.proactive_save.datetime") as mock_dt:
        mock_dt.now.return_value = NOW
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        trigger = _identify_risk(sub, mock_db)

    assert trigger == "churn_risk", f"Expected churn_risk trigger, got: {trigger}"
