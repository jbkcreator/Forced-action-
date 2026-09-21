"""tests/agents/reply_concierge/test_backflip_stage_ingest.py"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.agents.reply_concierge.backflip_email_parser import ParsedBackflipEvent
from src.agents.reply_concierge.backflip_stage_ingest import (
    apply_parsed_event,
    resolve_opportunity_by_backflip_ref,
)


def _mock_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


class TestResolveOpportunityByBackflipRef:
    def test_returns_none_when_not_found(self):
        db = _mock_db()
        assert resolve_opportunity_by_backflip_ref(db, "BF-00000") is None

    def test_returns_ids_when_found(self):
        db = _mock_db()
        row = MagicMock()
        row._mapping = {"opportunity_id": "opp-1", "person_id": "p-1"}
        db.execute.return_value.fetchone.return_value = row
        result = resolve_opportunity_by_backflip_ref(db, "BF-10293")
        assert result == {"opportunity_id": "opp-1", "person_id": "p-1"}


class TestApplyParsedEvent:
    def test_stage_change_applies_via_file_state(self):
        db = _mock_db()
        event = ParsedBackflipEvent(event_type="stage_change", backflip_ref="BF-1", stage="under_review")
        with patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value={"opportunity_id": "opp-1", "person_id": "p-1"},
        ), patch(
            "src.services.fa_max_file_state.ensure_file_state"
        ), patch(
            "src.services.fa_max_file_state.update_backflip_stage"
        ) as mock_update:
            applied = apply_parsed_event(db, event, source="email_parsed", actor="backflip_email")
        assert applied is True
        mock_update.assert_called_once_with(
            db, opportunity_id="opp-1", to_stage="under_review",
            actor="backflip_email", source="email_parsed",
        )

    def test_document_request_applies_via_file_state(self):
        db = _mock_db()
        event = ParsedBackflipEvent(
            event_type="document_request", backflip_ref="BF-1", document_name="Bank Statement",
        )
        with patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value={"opportunity_id": "opp-1", "person_id": "p-1"},
        ), patch(
            "src.services.fa_max_file_state.ensure_file_state"
        ), patch(
            "src.services.fa_max_file_state.record_document_request"
        ) as mock_record:
            applied = apply_parsed_event(db, event, source="email_parsed", actor="backflip_email")
        assert applied is True
        mock_record.assert_called_once()
        assert mock_record.call_args.kwargs["document_name"] == "Bank Statement"

    def test_terms_applies_via_file_state(self):
        db = _mock_db()
        event = ParsedBackflipEvent(
            event_type="terms", backflip_ref="BF-1",
            loan_amount_cents=25_000_000, maturity_months=12,
        )
        with patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value={"opportunity_id": "opp-1", "person_id": "p-1"},
        ), patch(
            "src.services.fa_max_file_state.ensure_file_state"
        ), patch(
            "src.services.fa_max_file_state.record_terms"
        ) as mock_record:
            applied = apply_parsed_event(db, event, source="email_parsed", actor="backflip_email")
        assert applied is True
        mock_record.assert_called_once_with(
            db, opportunity_id="opp-1", actor="backflip_email",
            loan_amount_cents=25_000_000, maturity_months=12, backflip_ref="BF-1",
        )

    def test_returns_false_when_ref_unresolved(self):
        db = _mock_db()
        event = ParsedBackflipEvent(event_type="stage_change", backflip_ref="BF-999", stage="under_review")
        with patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value=None,
        ):
            applied = apply_parsed_event(db, event, source="email_parsed", actor="backflip_email")
        assert applied is False
