"""tests/api/test_log_submission_slack_wiring.py"""
from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)


def _signed_headers():
    return {"x-slack-request-timestamp": "9999999999", "x-slack-signature": "v0=test"}


class TestLogSubmissionSlashCommand:
    def test_opens_modal_when_authorized(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
             patch("src.api.admin_router._relay_approver_authorized", return_value=True), \
             patch("src.api.admin_router.open_log_submission_modal", return_value=True) as mock_open:
            response = client.post(
                "/api/admin/slack/fa-max-log-submission",
                data={"user_id": "U123", "trigger_id": "trig-1", "text": ""},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        mock_open.assert_called_once_with("trig-1")

    def test_unauthorized_user_does_not_open_modal(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
             patch("src.api.admin_router._relay_approver_authorized", return_value=False), \
             patch("src.api.admin_router.open_log_submission_modal") as mock_open:
            response = client.post(
                "/api/admin/slack/fa-max-log-submission",
                data={"user_id": "U999", "trigger_id": "trig-1"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        mock_open.assert_not_called()


class TestBlockSuggestionSearch:
    def test_returns_matches_as_slack_options(self):
        raw_payload = (
            'payload={"type": "block_suggestion", "action_id": "borrower_search", '
            '"value": "Jane"}'
        )
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
             patch(
                 "src.services.fa_max_person_search.search_fa_max_persons",
                 return_value=[{
                     "person_id": "p-1", "full_name": "Jane Doe",
                     "email": "jane@example.com", "phone": None, "last_stage": "qualifying",
                 }],
             ):
            response = client.post(
                "/api/admin/slack/interact",
                data=raw_payload,
                headers={**_signed_headers(), "content-type": "application/x-www-form-urlencoded"},
            )
        assert response.status_code == 200
        body = response.json()
        assert body["options"][0]["value"] == "p-1"
        assert "Jane Doe" in body["options"][0]["text"]["text"]

    def test_no_matches_returns_empty_options(self):
        raw_payload = (
            'payload={"type": "block_suggestion", "action_id": "borrower_search", '
            '"value": "zzz"}'
        )
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
             patch("src.services.fa_max_person_search.search_fa_max_persons", return_value=[]):
            response = client.post(
                "/api/admin/slack/interact",
                data=raw_payload,
                headers={**_signed_headers(), "content-type": "application/x-www-form-urlencoded"},
            )
        assert response.status_code == 200
        assert response.json()["options"] == []
