"""tests/services/test_fa_max_file_state.py"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services import fa_max_file_state as svc


def _mock_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


class TestGetFileState:
    def test_returns_none_when_no_row(self):
        db = _mock_db()
        assert svc.get_file_state(db, opportunity_id="opp-1") is None

    def test_returns_dict_when_row_present(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {
            "opportunity_id": "opp-1", "person_id": "p-1",
            "backflip_stage": "submitted", "contact_email": None,
            "last_stage_change_at": None,
            "last_borrower_touch_at": None, "expected_next_stage": None,
            "stall_flagged_at": None,
        }
        db.execute.return_value.fetchone.return_value = row
        result = svc.get_file_state(db, opportunity_id="opp-1")
        assert result["backflip_stage"] == "submitted"


class TestEnsureFileState:
    def test_creates_row_with_explicit_contact_email(self):
        db = _mock_db()
        with patch.object(
            svc, "get_file_state",
            side_effect=[None, {"backflip_stage": "submitted", "contact_email": "b@example.com"}],
        ):
            result = svc.ensure_file_state(
                db, opportunity_id="opp-1", person_id="p-1", contact_email="b@example.com",
            )
        assert result["contact_email"] == "b@example.com"
        db.commit.assert_called()
        insert_kwargs = db.execute.call_args_list[0].args[1]
        assert insert_kwargs["contact_email"] == "b@example.com"

    def test_creates_row_with_best_effort_email_from_source_reference(self):
        db = _mock_db()
        person_row = MagicMock()
        person_row._mapping = {"source_reference": "borrower@example.com"}
        # First execute().fetchone(): the fa_max_persons lookup inside
        # _best_effort_contact_email. Second: none (used only for the lookup).
        db.execute.return_value.fetchone.side_effect = [person_row]
        with patch.object(
            svc, "get_file_state",
            side_effect=[None, {"backflip_stage": "submitted", "contact_email": "borrower@example.com"}],
        ):
            result = svc.ensure_file_state(db, opportunity_id="opp-1", person_id="p-1")
        assert result["contact_email"] == "borrower@example.com"

    def test_creates_row_with_null_email_when_source_reference_is_not_an_email(self):
        db = _mock_db()
        person_row = MagicMock()
        person_row._mapping = {"source_reference": "backflip-contact-99182"}
        db.execute.return_value.fetchone.side_effect = [person_row]
        with patch.object(
            svc, "get_file_state",
            side_effect=[None, {"backflip_stage": "submitted", "contact_email": None}],
        ):
            result = svc.ensure_file_state(db, opportunity_id="opp-1", person_id="p-1")
        assert result["contact_email"] is None

    def test_idempotent_when_row_exists(self):
        db = _mock_db()
        existing = {"opportunity_id": "opp-1", "backflip_stage": "under_review"}
        with patch.object(svc, "get_file_state", return_value=existing):
            result = svc.ensure_file_state(db, opportunity_id="opp-1", person_id="p-1")
        assert result == existing
        db.execute.assert_not_called()


class TestUpdateBackflipStage:
    def test_rejects_unknown_stage(self):
        db = _mock_db()
        with pytest.raises(ValueError, match="unknown backflip stage"):
            svc.update_backflip_stage(
                db, opportunity_id="opp-1", to_stage="not_a_real_stage",
                actor="test", source="manual",
            )

    def test_manual_source_logs_slack_channel(self):
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = ("p-1",)
        with patch("src.services.state_engine.write_interaction") as mock_write:
            svc.update_backflip_stage(
                db, opportunity_id="opp-1", to_stage="under_review",
                actor="manual:slack", source="manual",
            )
        assert mock_write.call_args.kwargs["channel"] == "slack"
        assert mock_write.call_args.kwargs["direction"] == "inbound"

    def test_email_parsed_source_logs_email_channel(self):
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = ("p-1",)
        with patch("src.services.state_engine.write_interaction") as mock_write:
            svc.update_backflip_stage(
                db, opportunity_id="opp-1", to_stage="under_review",
                actor="backflip_email", source="email_parsed",
            )
        assert mock_write.call_args.kwargs["channel"] == "email"

    def test_declined_cascades_to_coarse_fsm(self):
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = ("p-1",)
        with patch(
            "src.services.state_engine.get_opportunity_state",
            return_value={"current_stage": "submitted", "state_version": 3},
        ), patch("src.services.state_engine.transition") as mock_transition, patch(
            "src.services.state_engine.write_interaction"
        ):
            svc.update_backflip_stage(
                db, opportunity_id="opp-1", to_stage="declined",
                actor="manual:slack", source="manual",
            )
        mock_transition.assert_called_once()
        assert mock_transition.call_args.kwargs["to_state"] == "declined"


class TestRecordDocumentRequest:
    def test_inserts_row(self):
        db = _mock_db()
        result = svc.record_document_request(
            db, opportunity_id="opp-1", person_id="p-1",
            document_name="bank statement", source="manual",
            idempotency_key="doc:opp-1:bank statement:2026-09-21",
        )
        assert result["document_name"] == "bank statement"
        db.commit.assert_called()


class TestRecordDocumentReceived:
    def test_marks_received(self):
        db = _mock_db()
        db.execute.return_value.rowcount = 1
        count = svc.record_document_received(
            db, opportunity_id="opp-1", document_name="bank statement",
        )
        assert count == 1


class TestRecordTerms:
    def test_writes_terms_and_notifies_money_via_raw_insert(self):
        db = _mock_db()
        with patch(
            "src.services.state_engine.get_opportunity_state",
            return_value={"current_stage": "submitted", "state_version": 1},
        ), patch("src.services.state_engine.transition") as mock_transition:
            svc.record_terms(
                db, opportunity_id="opp-1", actor="email_parser",
                loan_amount_cents=25_000_000, maturity_months=12,
                backflip_ref="BF-99182",
            )
        mock_transition.assert_called_once()
        assert mock_transition.call_args.kwargs["to_state"] == "term_sheet"
        insert_calls = [
            call for call in db.execute.call_args_list
            if "INSERT INTO relay_approval_queue" in str(call.args[0])
        ]
        assert len(insert_calls) == 1
        assert insert_calls[0].args[1]["lane"] == "MONEY"


class TestTouchBorrower:
    def test_updates_last_touch(self):
        db = _mock_db()
        svc.touch_borrower(db, opportunity_id="opp-1")
        db.execute.assert_called_once()
        db.commit.assert_called_once()


class TestSendGovernedEmail:
    @staticmethod
    def _db_with_insert_returning_id():
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = (1,)
        return db

    def test_sends_when_consent_and_suppression_both_pass(self):
        db = self._db_with_insert_returning_id()
        with patch(
            "src.services.fa_max_send_governance.require_consent",
            return_value=MagicMock(allowed=True),
        ), patch(
            "src.services.fa_max_send_governance.suppression_reason", return_value=None,
        ):
            sent = svc.send_governed_email(
                db, opportunity_id="opp-1", person_id="p-1",
                contact_email="borrower@example.com", subject="Update",
                body="Still under review.", lane="RELATIONSHIPS",
                agent_name="stage_monitor", idempotency_key="test-key-1",
            )
        assert sent is True
        insert_calls = [
            call for call in db.execute.call_args_list
            if "INSERT INTO relay_approval_queue" in str(call.args[0])
        ]
        assert len(insert_calls) == 1
        assert insert_calls[0].args[1]["recipient"] == "borrower@example.com"

    def test_blocks_on_missing_consent(self):
        db = _mock_db()
        with patch(
            "src.services.fa_max_send_governance.require_consent",
            return_value=MagicMock(allowed=False, reason="consent_absent"),
        ):
            sent = svc.send_governed_email(
                db, opportunity_id="opp-1", person_id="p-1",
                contact_email="borrower@example.com", subject="Update",
                body="Still under review.", lane="RELATIONSHIPS",
                agent_name="stage_monitor", idempotency_key="test-key-2",
            )
        assert sent is False
        insert_calls = [
            call for call in db.execute.call_args_list
            if "INSERT INTO relay_approval_queue" in str(call.args[0])
        ]
        assert len(insert_calls) == 0

    def test_blocks_on_suppression(self):
        db = _mock_db()
        with patch(
            "src.services.fa_max_send_governance.require_consent",
            return_value=MagicMock(allowed=True),
        ), patch(
            "src.services.fa_max_send_governance.suppression_reason",
            return_value="email_opt_out",
        ):
            sent = svc.send_governed_email(
                db, opportunity_id="opp-1", person_id="p-1",
                contact_email="borrower@example.com", subject="Update",
                body="Still under review.", lane="RELATIONSHIPS",
                agent_name="stage_monitor", idempotency_key="test-key-3",
            )
        assert sent is False

    def test_blocks_prohibited_document_name_in_borrower_copy(self):
        """Finding 1a: relay_approval_queue's live CHECK constraint rejects any
        fa_max_lending payload naming a prohibited financial term, so the
        content check must happen before the INSERT, not as an IntegrityError.
        """
        db = self._db_with_insert_returning_id()
        with patch(
            "src.services.fa_max_send_governance.require_consent",
            return_value=MagicMock(allowed=True),
        ), patch(
            "src.services.fa_max_send_governance.suppression_reason", return_value=None,
        ):
            sent = svc.send_governed_email(
                db, opportunity_id="opp-1", person_id="p-1",
                contact_email="borrower@example.com",
                subject="Action needed: Bank Statement",
                body="We need your Bank Statement to keep your file moving.",
                lane="RELATIONSHIPS", agent_name="stage_monitor",
                idempotency_key="test-key-unsafe",
            )
        assert sent is False
        insert_calls = [
            call for call in db.execute.call_args_list
            if "INSERT INTO relay_approval_queue" in str(call.args[0])
        ]
        assert insert_calls == []

    def test_safe_copy_still_sends(self):
        db = self._db_with_insert_returning_id()
        with patch(
            "src.services.fa_max_send_governance.require_consent",
            return_value=MagicMock(allowed=True),
        ), patch(
            "src.services.fa_max_send_governance.suppression_reason", return_value=None,
        ):
            sent = svc.send_governed_email(
                db, opportunity_id="opp-1", person_id="p-1",
                contact_email="borrower@example.com",
                subject="Action needed: a document is still outstanding",
                body="We're waiting on one more document to keep your file moving.",
                lane="RELATIONSHIPS", agent_name="stage_monitor",
                idempotency_key="test-key-safe",
            )
        assert sent is True

    def test_stamps_autonomy_tier_a(self):
        """Finding 2: every other FA Max insert stamps 'A'; tier scopes
        fa_max_autonomy's send-count/edit-rate evidence.
        """
        db = self._db_with_insert_returning_id()
        with patch(
            "src.services.fa_max_send_governance.require_consent",
            return_value=MagicMock(allowed=True),
        ), patch(
            "src.services.fa_max_send_governance.suppression_reason", return_value=None,
        ):
            svc.send_governed_email(
                db, opportunity_id="opp-1", person_id="p-1",
                contact_email="borrower@example.com", subject="Update",
                body="Still under review.", lane="RELATIONSHIPS",
                agent_name="stage_monitor", idempotency_key="test-key-tier",
            )
        insert_sql = next(
            str(call.args[0]) for call in db.execute.call_args_list
            if "INSERT INTO relay_approval_queue" in str(call.args[0])
        )
        assert "'pending', :agent, 'A', :pid" in insert_sql

    def test_deduped_insert_returns_false(self):
        """Finding 6: ON CONFLICT DO NOTHING no-op must not read as a send, or
        the caller re-stamps first_chase_sent_at / last_borrower_touch_at.
        """
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = None
        with patch(
            "src.services.fa_max_send_governance.require_consent",
            return_value=MagicMock(allowed=True),
        ), patch(
            "src.services.fa_max_send_governance.suppression_reason", return_value=None,
        ):
            sent = svc.send_governed_email(
                db, opportunity_id="opp-1", person_id="p-1",
                contact_email="borrower@example.com", subject="Update",
                body="Still under review.", lane="RELATIONSHIPS",
                agent_name="stage_monitor", idempotency_key="test-key-dup",
            )
        assert sent is False
