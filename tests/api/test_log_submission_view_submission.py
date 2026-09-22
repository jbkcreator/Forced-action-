"""tests/api/test_log_submission_view_submission.py"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)


def _signed_headers():
    return {"x-slack-request-timestamp": "9999999999", "x-slack-signature": "v0=test"}


def _view_submission_payload(callback_id: str, private_metadata: dict, values: dict) -> str:
    payload = {
        "type": "view_submission",
        "user": {"id": "U123"},
        "view": {
            "callback_id": callback_id,
            "private_metadata": json.dumps(private_metadata),
            "state": {"values": values},
        },
    }
    return "payload=" + json.dumps(payload)


class TestLogSubmissionViewSubmission:
    def test_existing_borrower_creates_opportunity_and_sets_backflip_ref(self):
        raw = _view_submission_payload(
            "fa_max_log_submission_submit",
            {"mode": "search"},
            {
                "borrower_search_block": {"borrower_search": {"selected_option": {"value": "p-existing-1"}}},
                "opportunity_type_block": {"opportunity_type": {"selected_option": {"value": "rehab"}}},
                "loan_amount_block": {"loan_amount": {"value": "250000"}},
                "backflip_ref_block": {"backflip_ref": {"value": "BF-5521"}},
            },
        )
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
             patch(
                 "src.services.state_engine.create_fa_max_opportunity", return_value="opp-1",
             ) as mock_create, \
             patch("src.services.state_engine.transition") as mock_transition, \
             patch("src.services.fa_max_file_state.ensure_file_state") as mock_ensure, \
             patch(
                 "src.services.fa_max_file_state.record_terms",
             ) as mock_record_terms:
            response = client.post(
                "/api/admin/slack/interact", data=raw,
                headers={**_signed_headers(), "content-type": "application/x-www-form-urlencoded"},
            )
        assert response.status_code == 200
        mock_create.assert_called_once()
        assert mock_create.call_args.kwargs["person_id"] == "p-existing-1"
        assert mock_create.call_args.kwargs["opportunity_type"] == "rehab"
        mock_transition.assert_called_once()
        assert mock_transition.call_args.kwargs["to_state"] == "submitted"
        assert mock_transition.call_args.kwargs["validate_allowed_next"] is False
        assert "reason" in mock_transition.call_args.kwargs["context"]
        mock_ensure.assert_called_once()
        mock_record_terms.assert_called_once()
        assert mock_record_terms.call_args.kwargs["backflip_ref"] == "BF-5521"

    def test_new_borrower_creates_person_then_opportunity(self):
        raw = _view_submission_payload(
            "fa_max_log_submission_submit",
            {"mode": "new_borrower"},
            {
                "new_full_name_block": {"new_full_name": {"value": "John Smith"}},
                "new_email_block": {"new_email": {"value": "john@example.com"}},
                "new_phone_block": {"new_phone": {"value": ""}},
                "new_property_address_block": {"new_property_address": {"value": "123 Main St"}},
                "opportunity_type_block": {"opportunity_type": {"selected_option": {"value": "acquisition"}}},
                "loan_amount_block": {"loan_amount": {"value": ""}},
                "backflip_ref_block": {"backflip_ref": {"value": ""}},
            },
        )
        fake_person_row = MagicMock()
        fake_person_row.person_id = "p-new-1"
        from src.api.admin_router import get_db

        mock_db = MagicMock()
        mock_db.execute.return_value.fetchone.return_value = fake_person_row

        def _override():
            yield mock_db

        app.dependency_overrides[get_db] = _override
        try:
            with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
                 patch(
                     "src.services.state_engine.create_fa_max_opportunity", return_value="opp-2",
                 ) as mock_create, \
                 patch("src.services.state_engine.transition"), \
                 patch("src.services.fa_max_file_state.ensure_file_state"):
                response = client.post(
                    "/api/admin/slack/interact", data=raw,
                    headers={**_signed_headers(), "content-type": "application/x-www-form-urlencoded"},
                )
        finally:
            app.dependency_overrides.pop(get_db, None)
        assert response.status_code == 200
        assert mock_create.call_args.kwargs["person_id"] == "p-new-1"

    def test_missing_required_full_name_returns_validation_error(self):
        raw = _view_submission_payload(
            "fa_max_log_submission_submit",
            {"mode": "new_borrower"},
            {
                "new_full_name_block": {"new_full_name": {"value": ""}},
                "opportunity_type_block": {"opportunity_type": {"selected_option": {"value": "acquisition"}}},
            },
        )
        with patch("src.api.admin_router._verify_slack_signature", return_value=True):
            response = client.post(
                "/api/admin/slack/interact", data=raw,
                headers={**_signed_headers(), "content-type": "application/x-www-form-urlencoded"},
            )
        assert response.status_code == 200
        assert response.json().get("response_action") == "errors"
