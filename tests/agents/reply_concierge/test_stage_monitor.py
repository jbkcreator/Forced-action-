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


class TestSweepStatusTouches:
    def test_no_files_due_returns_zero(self):
        db = _mock_db()
        assert stage_monitor.sweep_status_touches(db) == 0

    def test_sends_touch_for_overdue_file(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "opportunity_id": "opp-1", "person_id": "p-1",
            "backflip_stage": "under_review",
            "last_borrower_touch_at": datetime.now(timezone.utc) - timedelta(days=8),
            "contact_email": "borrower@example.com",
        }
        db.execute.return_value.fetchall.return_value = [row]
        with patch(
            "src.services.fa_max_file_state.send_governed_email", return_value=True,
        ) as mock_send, patch(
            "src.services.fa_max_file_state.touch_borrower"
        ) as mock_touch:
            count = stage_monitor.sweep_status_touches(db)
        assert count == 1
        mock_send.assert_called_once()
        assert mock_send.call_args.kwargs["contact_email"] == "borrower@example.com"
        mock_touch.assert_called_once_with(db, opportunity_id="opp-1")

    def test_never_touched_file_is_due_immediately(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "opportunity_id": "opp-2", "person_id": "p-2",
            "backflip_stage": "submitted",
            "last_borrower_touch_at": None,
            "contact_email": "new@example.com",
        }
        db.execute.return_value.fetchall.return_value = [row]
        with patch(
            "src.services.fa_max_file_state.send_governed_email", return_value=True,
        ), patch("src.services.fa_max_file_state.touch_borrower"):
            count = stage_monitor.sweep_status_touches(db)
        assert count == 1

    def test_recently_touched_file_is_skipped(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "opportunity_id": "opp-3", "person_id": "p-3",
            "backflip_stage": "under_review",
            "last_borrower_touch_at": datetime.now(timezone.utc) - timedelta(days=1),
            "contact_email": "borrower3@example.com",
        }
        db.execute.return_value.fetchall.return_value = [row]
        with patch("src.services.fa_max_file_state.send_governed_email") as mock_send:
            count = stage_monitor.sweep_status_touches(db)
        assert count == 0
        mock_send.assert_not_called()

    def test_missing_contact_email_is_skipped(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "opportunity_id": "opp-4", "person_id": "p-4",
            "backflip_stage": "under_review",
            "last_borrower_touch_at": datetime.now(timezone.utc) - timedelta(days=8),
            "contact_email": None,
        }
        db.execute.return_value.fetchall.return_value = [row]
        with patch("src.services.fa_max_file_state.send_governed_email") as mock_send:
            count = stage_monitor.sweep_status_touches(db)
        assert count == 0
        mock_send.assert_not_called()

    def test_governance_block_does_not_count_as_sent(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "opportunity_id": "opp-5", "person_id": "p-5",
            "backflip_stage": "under_review",
            "last_borrower_touch_at": datetime.now(timezone.utc) - timedelta(days=8),
            "contact_email": "suppressed@example.com",
        }
        db.execute.return_value.fetchall.return_value = [row]
        with patch(
            "src.services.fa_max_file_state.send_governed_email", return_value=False,
        ), patch("src.services.fa_max_file_state.touch_borrower") as mock_touch:
            count = stage_monitor.sweep_status_touches(db)
        assert count == 0
        mock_touch.assert_not_called()


class TestSweepDocumentChases:
    def test_no_outstanding_requests_returns_zero(self):
        db = _mock_db()
        assert stage_monitor.sweep_document_chases(db) == 0

    def test_sends_followup_after_threshold(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "id": 1, "opportunity_id": "opp-1", "person_id": "p-1",
            "document_name": "Bank Statement", "first_chase_sent_at":
                datetime.now(timezone.utc) - timedelta(days=4),
            "followup_chase_sent_at": None, "escalated_at": None,
            "contact_email": "borrower@example.com",
        }
        db.execute.return_value.fetchall.return_value = [row]
        with patch(
            "src.services.fa_max_file_state.send_governed_email", return_value=True,
        ) as mock_send:
            count = stage_monitor.sweep_document_chases(db)
        assert count == 1
        assert mock_send.call_args.kwargs["lane"] == "RELATIONSHIPS"
        db.execute.assert_any_call(
            stage_monitor._MARK_FOLLOWUP_SENT_SQL, {"id": 1},
        )

    def test_followup_skipped_when_no_contact_email(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "id": 4, "opportunity_id": "opp-4", "person_id": "p-4",
            "document_name": "Bank Statement", "first_chase_sent_at":
                datetime.now(timezone.utc) - timedelta(days=3),
            "followup_chase_sent_at": None, "escalated_at": None,
            "contact_email": None,
        }
        db.execute.return_value.fetchall.return_value = [row]
        with patch("src.services.fa_max_file_state.send_governed_email") as mock_send:
            count = stage_monitor.sweep_document_chases(db)
        assert count == 0
        mock_send.assert_not_called()

    def test_escalates_after_followup_threshold(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "id": 2, "opportunity_id": "opp-2", "person_id": "p-2",
            "document_name": "Tax Return", "first_chase_sent_at":
                datetime.now(timezone.utc) - timedelta(days=7),
            "followup_chase_sent_at": datetime.now(timezone.utc) - timedelta(days=4),
            "escalated_at": None, "contact_email": "borrower2@example.com",
        }
        db.execute.return_value.fetchall.return_value = [row]
        count = stage_monitor.sweep_document_chases(db)
        assert count == 1
        insert_calls = [
            call for call in db.execute.call_args_list
            if "INSERT INTO relay_approval_queue" in str(call.args[0])
        ]
        assert len(insert_calls) == 1
        assert insert_calls[0].args[1]["lane"] == "EXCEPTIONS"
        db.execute.assert_any_call(
            stage_monitor._MARK_ESCALATED_SQL, {"id": 2},
        )

    def test_not_yet_due_is_skipped(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "id": 3, "opportunity_id": "opp-3", "person_id": "p-3",
            "document_name": "ID", "first_chase_sent_at":
                datetime.now(timezone.utc) - timedelta(hours=1),
            "followup_chase_sent_at": None, "escalated_at": None,
            "contact_email": "borrower3@example.com",
        }
        db.execute.return_value.fetchall.return_value = [row]
        with patch("src.services.fa_max_file_state.send_governed_email") as mock_send:
            count = stage_monitor.sweep_document_chases(db)
        assert count == 0
        mock_send.assert_not_called()


class TestSendFirstChaseTouch:
    """The immediate, on-request-detection touch (Part B) -- distinct from
    sweep_document_chases()'s later follow-up/escalation steps."""

    def test_sends_and_marks_first_chase_sent(self):
        db = _mock_db()
        with patch(
            "src.services.fa_max_file_state.send_governed_email", return_value=True,
        ) as mock_send:
            sent = stage_monitor.send_first_chase_touch(
                db, opportunity_id="opp-1", person_id="p-1",
                document_name="Bank Statement", contact_email="borrower@example.com",
            )
        assert sent is True
        assert mock_send.call_args.kwargs["contact_email"] == "borrower@example.com"
        assert mock_send.call_args.kwargs["lane"] == "RELATIONSHIPS"
        db.execute.assert_any_call(
            stage_monitor._MARK_FIRST_CHASE_SENT_SQL,
            {"idempotency_key": "docreq:opp-1:Bank Statement"},
        )

    def test_skips_when_no_contact_email(self):
        db = _mock_db()
        with patch("src.services.fa_max_file_state.send_governed_email") as mock_send:
            sent = stage_monitor.send_first_chase_touch(
                db, opportunity_id="opp-2", person_id="p-2",
                document_name="ID", contact_email=None,
            )
        assert sent is False
        mock_send.assert_not_called()

    def test_governance_block_does_not_mark_sent(self):
        db = _mock_db()
        with patch(
            "src.services.fa_max_file_state.send_governed_email", return_value=False,
        ):
            sent = stage_monitor.send_first_chase_touch(
                db, opportunity_id="opp-3", person_id="p-3",
                document_name="ID", contact_email="suppressed@example.com",
            )
        assert sent is False
        mark_calls = [
            call for call in db.execute.call_args_list
            if call.args and call.args[0] is stage_monitor._MARK_FIRST_CHASE_SENT_SQL
        ]
        assert mark_calls == []
