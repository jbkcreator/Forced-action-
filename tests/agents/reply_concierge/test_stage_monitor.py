"""tests/agents/reply_concierge/test_stage_monitor.py"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.agents.reply_concierge import stage_monitor


def _mock_db():
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = []
    return db


class TestSweepStalledFiles:
    def test_no_stalled_files_returns_zero(self):
        db = _mock_db()
        assert stage_monitor.sweep_stalled_files(db) == 0

    def test_flags_stalled_file_and_posts_exceptions(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "opportunity_id": "opp-1", "person_id": "p-1",
            "backflip_stage": "docs_requested",
            "last_stage_change_at": datetime.now(timezone.utc) - timedelta(days=10),
        }
        db.execute.return_value.fetchall.return_value = [row]
        count = stage_monitor.sweep_stalled_files(db)
        assert count == 1
        insert_calls = [
            call for call in db.execute.call_args_list
            if "INSERT INTO relay_approval_queue" in str(call.args[0])
        ]
        assert len(insert_calls) == 1
        assert insert_calls[0].args[1]["lane"] == "EXCEPTIONS"
        db.execute.assert_any_call(
            stage_monitor._MARK_STALLED_SQL,
            {"opportunity_id": "opp-1"},
        )
