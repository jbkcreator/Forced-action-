"""tests/api/test_fa_max_file_update_slack_command.py"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)


def _signed_headers():
    # Signature verification is patched in every test below — these are
    # placeholder headers so the endpoint's header-reading code doesn't KeyError.
    return {"x-slack-request-timestamp": "9999999999", "x-slack-signature": "v0=test"}


class TestFaMaxFileUpdateCommand:
    def test_rejects_bad_signature(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=False):
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U123", "text": "BF-1 under_review"},
                headers=_signed_headers(),
            )
        assert response.status_code == 401

    def test_rejects_unauthorized_user(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), patch(
            "src.api.admin_router._relay_approver_authorized", return_value=False
        ):
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U999", "text": "BF-1 under_review"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "Not authorized" in response.json()["text"]

    def test_stage_update_happy_path(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), patch(
            "src.api.admin_router._relay_approver_authorized", return_value=True
        ), patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value={"opportunity_id": "opp-1", "person_id": "p-1"},
        ), patch(
            "src.services.fa_max_file_state.ensure_file_state"
        ), patch(
            "src.services.fa_max_file_state.update_backflip_stage"
        ) as mock_update:
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U123", "text": "BF-1 under_review"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "under_review" in response.json()["text"]
        mock_update.assert_called_once()
        assert mock_update.call_args.kwargs["to_stage"] == "under_review"
        assert mock_update.call_args.kwargs["source"] == "manual"

    def test_document_request_happy_path(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), patch(
            "src.api.admin_router._relay_approver_authorized", return_value=True
        ), patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value={"opportunity_id": "opp-1", "person_id": "p-1"},
        ), patch(
            "src.services.fa_max_file_state.ensure_file_state"
        ), patch(
            "src.services.fa_max_file_state.record_document_request"
        ) as mock_record, patch(
            "src.services.fa_max_file_state.get_file_state",
            return_value={"contact_email": "borrower@example.com"},
        ), patch(
            "src.agents.reply_concierge.stage_monitor.send_first_chase_touch"
        ) as mock_first_touch:
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U123", "text": "BF-1 doc:Bank Statement"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        mock_record.assert_called_once()
        assert mock_record.call_args.kwargs["document_name"] == "Bank Statement"
        mock_first_touch.assert_called_once()
        assert mock_first_touch.call_args.kwargs["document_name"] == "Bank Statement"
        assert mock_first_touch.call_args.kwargs["contact_email"] == "borrower@example.com"

    def test_document_received_happy_path(self):
        """Finding 4: record_document_received() had no caller, so every
        request walked first-touch -> follow-up -> escalation even after the
        borrower complied. This is its one manual closure path.
        """
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), patch(
            "src.api.admin_router._relay_approver_authorized", return_value=True
        ), patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value={"opportunity_id": "opp-1", "person_id": "p-1"},
        ), patch(
            "src.services.fa_max_file_state.record_document_received", return_value=1,
        ) as mock_received:
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U123", "text": "BF-1 received:Bank Statement"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "received" in response.json()["text"]
        mock_received.assert_called_once()
        assert mock_received.call_args.kwargs["document_name"] == "Bank Statement"
        assert mock_received.call_args.kwargs["opportunity_id"] == "opp-1"

    def test_document_received_no_matching_request(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), patch(
            "src.api.admin_router._relay_approver_authorized", return_value=True
        ), patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value={"opportunity_id": "opp-1", "person_id": "p-1"},
        ), patch(
            "src.services.fa_max_file_state.record_document_received", return_value=0,
        ):
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U123", "text": "BF-1 received:Nope"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "No outstanding request" in response.json()["text"]

    def test_document_received_requires_a_name(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), patch(
            "src.api.admin_router._relay_approver_authorized", return_value=True
        ), patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value={"opportunity_id": "opp-1", "person_id": "p-1"},
        ), patch(
            "src.services.fa_max_file_state.record_document_received"
        ) as mock_received:
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U123", "text": "BF-1 received:   "},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "Usage" in response.json()["text"]
        mock_received.assert_not_called()

    def test_unknown_stage_returns_usage(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), patch(
            "src.api.admin_router._relay_approver_authorized", return_value=True
        ), patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value={"opportunity_id": "opp-1", "person_id": "p-1"},
        ):
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U123", "text": "BF-1 not_a_stage"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "Usage" in response.json()["text"]

    def test_unresolved_ref_returns_error(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), patch(
            "src.api.admin_router._relay_approver_authorized", return_value=True
        ), patch(
            "src.agents.reply_concierge.backflip_stage_ingest.resolve_opportunity_by_backflip_ref",
            return_value=None,
        ):
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U123", "text": "BF-999 under_review"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "No opportunity found" in response.json()["text"]
