"""tests/agents/reply_concierge/test_stage_monitor.py"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.agents.reply_concierge import stage_monitor
from src.services.fa_max_send_governance import GovernanceBlocked


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


_DB_PROHIBITED_TERMS = re.compile(
    r"(ssn|social.security|credit.score|fico|income|bank.statement|tax.return"
    r"|debt.to.income|interest.rate|loan.rate|loan.term|commitment)",
    re.IGNORECASE,
)


class TestBorrowerFacingCopyIsGeneric:
    """Finding 1c: relay_approval_queue carries a live CHECK constraint
    rejecting any fa_max_lending payload matching _DB_PROHIBITED_TERMS, and
    real document names ('Bank Statement', 'Tax Return', 'Proof of income',
    'Commitment letter') all match it. No borrower-facing template may name
    the document.
    """

    def test_chase_copy_never_names_a_document_and_clears_both_regexes(self):
        from src.services import fa_max_send_governance as governance

        copy = [
            stage_monitor._DOC_CHASE_FIRST_SUBJECT,
            stage_monitor._DOC_CHASE_FIRST_BODY,
            stage_monitor._DOC_CHASE_FOLLOWUP_SUBJECT,
            stage_monitor._DOC_CHASE_FOLLOWUP_BODY,
        ] + [
            stage_monitor._STATUS_TOUCH_BODY.format(stage_label=label)
            for label in list(stage_monitor._STAGE_LABELS.values()) + ["moving forward"]
        ]
        for text_value in copy:
            assert not _DB_PROHIBITED_TERMS.search(text_value), text_value
            governance.validate_safe_payload({"body": text_value})

    def test_first_chase_subject_and_body_do_not_interpolate_document_name(self):
        db = _mock_db()
        with patch(
            "src.services.fa_max_file_state.send_governed_email", return_value=True,
        ) as mock_send:
            stage_monitor.send_first_chase_touch(
                db, opportunity_id="opp-1", person_id="p-1",
                document_name="Bank Statement (last 2 months)",
                contact_email="borrower@example.com",
            )
        kwargs = mock_send.call_args.kwargs
        assert "Bank Statement" not in kwargs["subject"]
        assert "Bank Statement" not in kwargs["body"]

    def test_followup_subject_and_body_do_not_interpolate_document_name(self):
        db = _mock_db()
        with patch(
            "src.services.fa_max_file_state.send_governed_email", return_value=True,
        ) as mock_send:
            stage_monitor._send_chase_followup(db, {
                "id": 9, "opportunity_id": "opp-9", "person_id": "p-9",
                "document_name": "Tax Return", "contact_email": "b@example.com",
            })
        kwargs = mock_send.call_args.kwargs
        assert "Tax Return" not in kwargs["subject"]
        assert "Tax Return" not in kwargs["body"]


class TestEscalationPayload:
    def test_redacts_document_name_and_passes_governance(self):
        """Finding 1b: _escalate_chase inserts raw (not via
        send_governed_email), so it needs its own validate_safe_payload call;
        the document name is redacted because the CHECK constraint applies to
        every fa_max_lending payload regardless of lane.
        """
        from src.services import fa_max_send_governance as governance

        db = _mock_db()
        stage_monitor._escalate_chase(db, {
            "id": 11, "opportunity_id": "opp-11", "person_id": "p-11",
            "document_name": "Bank Statement",
        })
        insert_calls = [
            call for call in db.execute.call_args_list
            if "INSERT INTO relay_approval_queue" in str(call.args[0])
        ]
        assert len(insert_calls) == 1
        payload = json.loads(insert_calls[0].args[1]["payload"])
        assert payload["document_name_redacted"] is True
        assert payload["document_request_id"] == 11
        assert "Bank Statement" not in insert_calls[0].args[1]["payload"]
        assert not _DB_PROHIBITED_TERMS.search(insert_calls[0].args[1]["payload"])
        governance.validate_safe_payload(payload)

    def test_skips_insert_when_payload_is_blocked(self):
        db = _mock_db()
        with patch(
            "src.services.fa_max_send_governance.validate_safe_payload",
            side_effect=GovernanceBlocked("prohibited_financial_content:payload.x"),
        ):
            stage_monitor._escalate_chase(db, {
                "id": 12, "opportunity_id": "opp-12", "person_id": "p-12",
                "document_name": "Bank Statement",
            })
        insert_calls = [
            call for call in db.execute.call_args_list
            if "INSERT INTO relay_approval_queue" in str(call.args[0])
        ]
        assert insert_calls == []


class TestSweepQueryGuards:
    def test_doc_request_sweep_excludes_terminal_stages(self):
        """Finding 3: a declined borrower must not keep getting chased."""
        sql = str(stage_monitor._OUTSTANDING_DOC_REQUESTS_SQL)
        assert "fs.backflip_stage NOT IN ('funded', 'declined')" in sql

    def test_limited_sweeps_are_deterministically_ordered(self):
        """Finding 5: LIMIT 200 without ORDER BY starves rows past the 200th."""
        status_sql = str(stage_monitor._STATUS_TOUCH_CANDIDATES_SQL)
        assert "ORDER BY fs.last_borrower_touch_at NULLS FIRST" in status_sql
        assert status_sql.index("ORDER BY") < status_sql.index("LIMIT")

        doc_sql = str(stage_monitor._OUTSTANDING_DOC_REQUESTS_SQL)
        assert "ORDER BY dr.requested_at" in doc_sql
        assert doc_sql.index("ORDER BY") < doc_sql.index("LIMIT")


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
