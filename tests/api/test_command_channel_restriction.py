"""tests/api/test_command_channel_restriction.py"""
from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from src.api.main import app
from src.api.admin_router import _reject_if_wrong_command_channel

client = TestClient(app)


def _signed_headers():
    return {"x-slack-request-timestamp": "9999999999", "x-slack-signature": "v0=test"}


class TestRejectIfWrongCommandChannel:
    def test_allows_when_setting_unset(self):
        with patch("src.api.admin_router.get_settings") as mock_settings:
            mock_settings.return_value.fa_max_slack_cc_channel = None
            result = _reject_if_wrong_command_channel("C_ANYWHERE")
        assert result is None

    def test_allows_when_channel_matches(self):
        with patch("src.api.admin_router.get_settings") as mock_settings:
            mock_settings.return_value.fa_max_slack_cc_channel = "C_COMMAND_CENTER"
            result = _reject_if_wrong_command_channel("C_COMMAND_CENTER")
        assert result is None

    def test_rejects_when_channel_does_not_match(self):
        with patch("src.api.admin_router.get_settings") as mock_settings:
            mock_settings.return_value.fa_max_slack_cc_channel = "C_COMMAND_CENTER"
            result = _reject_if_wrong_command_channel("C_SOME_OTHER_CHANNEL")
        assert result is not None
        assert "command center" in result["text"].lower()


class TestFileUpdateCommandRespectsChannelRestriction:
    def test_rejected_outside_command_center_channel(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
             patch("src.api.admin_router.get_settings") as mock_settings:
            mock_settings.return_value.fa_max_slack_cc_channel = "C_COMMAND_CENTER"
            response = client.post(
                "/api/admin/slack/fa-max-file-update",
                data={"user_id": "U123", "text": "BF-1 under_review", "channel_id": "C_WRONG"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "command center" in response.json()["text"].lower()


class TestLogSubmissionCommandRespectsChannelRestriction:
    def test_rejected_outside_command_center_channel(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
             patch("src.api.admin_router.get_settings") as mock_settings, \
             patch("src.api.admin_router.open_log_submission_modal") as mock_open:
            mock_settings.return_value.fa_max_slack_cc_channel = "C_COMMAND_CENTER"
            response = client.post(
                "/api/admin/slack/fa-max-log-submission",
                data={"user_id": "U123", "trigger_id": "trig-1", "channel_id": "C_WRONG"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "command center" in response.json()["text"].lower()
        mock_open.assert_not_called()
