"""tests/scripts/test_run_stage_monitor_worker.py"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_main_runs_all_three_sweeps():
    from scripts.run_stage_monitor_worker import main

    with patch("scripts.run_stage_monitor_worker.create_engine"), patch(
        "scripts.run_stage_monitor_worker.sessionmaker"
    ) as mock_sessionmaker, patch(
        "src.agents.reply_concierge.stage_monitor.sweep_stalled_files", return_value=1
    ) as mock_stall, patch(
        "src.agents.reply_concierge.stage_monitor.sweep_status_touches", return_value=2
    ) as mock_touch, patch(
        "src.agents.reply_concierge.stage_monitor.sweep_document_chases", return_value=3
    ) as mock_chase:
        mock_session = MagicMock()
        mock_sessionmaker.return_value.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_sessionmaker.return_value.return_value.__exit__ = MagicMock(return_value=False)
        main(dry_run=False)

    mock_stall.assert_called_once()
    mock_touch.assert_called_once()
    mock_chase.assert_called_once()
