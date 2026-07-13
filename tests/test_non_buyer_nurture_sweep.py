"""
Tests for the non-buyer nurture daily sweep task. Thin wiring only — DB and
Instantly are mocked; the real logic is tested in test_non_buyer_nurture.py.
"""
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import src.core.database  # noqa: F401 — force real-settings import before tests patch get_settings
from src.tasks import non_buyer_nurture_sweep


@contextmanager
def _fake_db_context(db):
    yield db


def _patched(campaign_id="camp_shared", candidates=None, enroll_result=None, reconciled=0):
    settings = MagicMock()
    settings.non_buyer_nurture_campaign_id = campaign_id
    db = MagicMock()
    return (
        patch("config.settings.get_settings", return_value=settings),
        patch("src.core.database.get_db_context", return_value=_fake_db_context(db)),
        patch("src.services.non_buyer_nurture.find_candidates", return_value=candidates or []),
        patch("src.services.non_buyer_nurture.reconcile_conversions", return_value=reconciled),
        patch("src.services.non_buyer_nurture.enroll", return_value=enroll_result or {"enrolled": 0, "retried": 0}),
        db,
    )


def test_sweep_enrolls_found_candidates_and_runs_backstop():
    candidates = [{"email": "a@example.com"}, {"email": "b@example.com"}]
    p_settings, p_dbctx, p_find, p_recon, p_enroll, db = _patched(
        candidates=candidates, enroll_result={"enrolled": 2, "retried": 0}, reconciled=3)

    with p_settings, p_dbctx, p_find, p_recon as mock_recon, p_enroll as mock_enroll:
        result = non_buyer_nurture_sweep.run()

    mock_recon.assert_called_once_with(db)
    mock_enroll.assert_called_once_with(db, candidates, campaign_id="camp_shared")
    assert result["enrolled"] == 2
    assert result["reconciled"] == 3


def test_sweep_dry_run_does_not_enroll():
    candidates = [{"email": "a@example.com"}]
    p_settings, p_dbctx, p_find, p_recon, p_enroll, db = _patched(candidates=candidates)

    with p_settings, p_dbctx, p_find, p_recon as mock_recon, p_enroll as mock_enroll:
        result = non_buyer_nurture_sweep.run(dry_run=True)

    mock_recon.assert_not_called()
    mock_enroll.assert_not_called()
    assert result["dry_run"] is True
    assert result["candidates"] == 1


def test_sweep_skips_when_campaign_not_configured():
    p_settings, p_dbctx, p_find, p_recon, p_enroll, db = _patched(campaign_id=None)

    with p_settings, p_dbctx, p_find as mock_find, p_recon, p_enroll as mock_enroll:
        result = non_buyer_nurture_sweep.run()

    mock_find.assert_not_called()
    mock_enroll.assert_not_called()
    assert "skipped" in result
