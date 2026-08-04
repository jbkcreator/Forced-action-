"""Tests for the agent_pnl_monthly cron task."""
from datetime import date
from unittest.mock import MagicMock, patch

from sqlalchemy import text


def test_run_upserts_all_six_seats(fresh_db):
    """run() must upsert one agent_pnl row for each seat."""
    with patch("src.services.email.send_email") as mock_email:
        from src.tasks.agent_pnl_monthly import run
        run(session=fresh_db, period_month=date(2099, 8, 1))

    rows = fresh_db.execute(text(
        "SELECT seat FROM agent_pnl WHERE period_month = '2099-08-01'"
    )).fetchall()
    seats = {r.seat for r in rows}
    assert seats == {"vera", "cora", "hunter", "relay", "dev_shop", "lifecycle"}


def test_run_sends_email(fresh_db):
    """run() must call send_email exactly once with a non-empty body."""
    with patch("src.services.email.send_email") as mock_email:
        from src.tasks.agent_pnl_monthly import run
        run(session=fresh_db, period_month=date(2099, 9, 1))

    assert mock_email.call_count == 1
    call = mock_email.call_args
    body = call.kwargs.get("body_text") or (call.args[2] if len(call.args) > 2 else "")
    assert len(body) > 50


def test_run_is_idempotent(fresh_db):
    """Running twice for the same month must not raise; row count stays at 6."""
    with patch("src.services.email.send_email"):
        from src.tasks.agent_pnl_monthly import run
        run(session=fresh_db, period_month=date(2099, 10, 1))
        run(session=fresh_db, period_month=date(2099, 10, 1))

    rows = fresh_db.execute(text(
        "SELECT seat FROM agent_pnl WHERE period_month = '2099-10-01'"
    )).fetchall()
    assert len(rows) == 6
