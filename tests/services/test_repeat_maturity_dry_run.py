"""
Regression test: run_monitors(dry_run=True) must never post to Slack.

Before the fix, run_monitors() always called _post_alert() regardless of
dry-run; the CLI's subsequent rollback undid only the DB writes, so a
"dry run" still sent real, permanent Slack messages (and could duplicate
them on a later run since the idempotency log row was rolled back).
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from src.services.repeat_maturity_engine import MonitorAlert, run_monitors

_ALERT = MonitorAlert(
    monitor_type="loan_maturity",
    buyer_entity_id=1,
    canonical_name="ZTEST ENTITY",
    source_event_id=None,
    property_address=None,
    event_date=date(2026, 1, 1),
    days_since=0,
    total_purchase_count=1,
    total_cash_volume=0.0,
    buyer_type=None,
    financing_signal=None,
)


@patch("src.services.repeat_maturity_engine._check_portfolio_expansion", return_value=[])
@patch("src.services.repeat_maturity_engine._check_dscr_day120", return_value=[])
@patch("src.services.repeat_maturity_engine._check_next_project", return_value=[])
@patch("src.services.repeat_maturity_engine._check_loan_maturity", return_value=[_ALERT])
@patch("src.services.repeat_maturity_engine._record_fired")
@patch("src.services.repeat_maturity_engine._post_alert")
def test_dry_run_skips_slack_and_idempotency_log(
    mock_post_alert, mock_record_fired, *_checkers,
):
    alerts = run_monitors(MagicMock(), today=date(2026, 1, 1), dry_run=True)

    assert alerts == [_ALERT]
    mock_post_alert.assert_not_called()
    mock_record_fired.assert_not_called()


@patch("src.services.repeat_maturity_engine._check_portfolio_expansion", return_value=[])
@patch("src.services.repeat_maturity_engine._check_dscr_day120", return_value=[])
@patch("src.services.repeat_maturity_engine._check_next_project", return_value=[])
@patch("src.services.repeat_maturity_engine._check_loan_maturity", return_value=[_ALERT])
@patch("src.services.repeat_maturity_engine._record_fired")
@patch("src.services.repeat_maturity_engine._post_alert", return_value="1234.5678")
def test_live_run_posts_slack_and_records(
    mock_post_alert, mock_record_fired, *_checkers,
):
    alerts = run_monitors(MagicMock(), today=date(2026, 1, 1), dry_run=False)

    assert alerts == [_ALERT]
    mock_post_alert.assert_called_once_with(_ALERT)
    mock_record_fired.assert_called_once()
