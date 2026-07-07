"""
Unit tests for src/connectors/runner.py — no DB required.

get_db_context and record_scraper_stats are both mocked: run_connector's job
is orchestration (open session, time the call, record stats, return an exit
code), not the DB/stats mechanics themselves — those are already covered by
their own modules.
"""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from src.connectors.runner import ConnectorRunResult, run_connector


@contextmanager
def _fake_db_context(session):
    yield session


class TestRunConnectorSuccess:
    def test_success_returns_zero_and_records_stats(self):
        fake_session = MagicMock()
        result = ConnectorRunResult(total_read=10, staged=7, quarantined=2, skipped=1, errors=0)
        work_fn = MagicMock(return_value=result)

        with patch("src.connectors.runner.get_db_context", return_value=_fake_db_context(fake_session)), \
             patch("src.connectors.runner.record_scraper_stats") as mock_record:
            exit_code = run_connector("foreclosure_outcomes", "hillsborough", work_fn)

        assert exit_code == 0
        work_fn.assert_called_once_with(fake_session, "hillsborough")
        mock_record.assert_called_once()
        kwargs = mock_record.call_args.kwargs
        assert kwargs["source_type"] == "foreclosure_outcomes"
        assert kwargs["total_scraped"] == 10
        assert kwargs["matched"] == 7
        assert kwargs["unmatched"] == 2
        assert kwargs["skipped"] == 1
        assert kwargs["run_success"] is True
        assert kwargs["error_type"] is None
        assert kwargs["county_id"] == "hillsborough"

    def test_unregistered_source_type_raises_before_opening_session(self):
        work_fn = MagicMock()
        with patch("src.connectors.runner.get_db_context") as mock_ctx, \
             patch("src.connectors.runner.record_scraper_stats") as mock_record:
            with pytest.raises(KeyError):
                run_connector("nonexistent_source_xyz", "hillsborough", work_fn)
        mock_ctx.assert_not_called()
        mock_record.assert_not_called()
        work_fn.assert_not_called()


class TestRunConnectorFailure:
    def test_work_fn_exception_is_caught_and_recorded(self):
        fake_session = MagicMock()
        work_fn = MagicMock(side_effect=RuntimeError("source portal timed out"))

        with patch("src.connectors.runner.get_db_context", return_value=_fake_db_context(fake_session)), \
             patch("src.connectors.runner.record_scraper_stats") as mock_record:
            exit_code = run_connector("tax_deed_outcomes", "hillsborough", work_fn)

        assert exit_code == 1
        kwargs = mock_record.call_args.kwargs
        assert kwargs["run_success"] is False
        assert kwargs["error_type"] == "connector_error"
        assert "source portal timed out" in kwargs["error_message"]
        # Failure with no partial result still records zeroed counts, not a crash.
        assert kwargs["total_scraped"] == 0

    def test_no_unhandled_traceback_escapes(self):
        # The whole point of run_connector: a blown-up work_fn must not propagate
        # an unhandled exception up to the cron process.
        work_fn = MagicMock(side_effect=ValueError("boom"))
        with patch("src.connectors.runner.get_db_context", return_value=_fake_db_context(MagicMock())), \
             patch("src.connectors.runner.record_scraper_stats"):
            exit_code = run_connector("appraiser_sale_outcomes", "pinellas", work_fn)
        assert exit_code == 1


class TestRunConnectorDryRun:
    def test_dry_run_reports_success_without_persisting(self):
        fake_session = MagicMock()
        result = ConnectorRunResult(total_read=3, staged=3, quarantined=0, skipped=0, errors=0)
        work_fn = MagicMock(return_value=result)

        with patch("src.connectors.runner.get_db_context", return_value=_fake_db_context(fake_session)), \
             patch("src.connectors.runner.record_scraper_stats") as mock_record:
            exit_code = run_connector("dor_sale_outcomes", "hillsborough", work_fn, dry_run=True)

        assert exit_code == 0
        kwargs = mock_record.call_args.kwargs
        assert kwargs["run_success"] is True
        assert kwargs["total_scraped"] == 3
