"""tests/api/test_fa_max_backflip_files_slack_command.py"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)


def _signed_headers():
    return {"x-slack-request-timestamp": "9999999999", "x-slack-signature": "v0=test"}


def _fake_row(full_name, backflip_ref, current_stage):
    row = MagicMock()
    row.full_name = full_name
    row.backflip_ref = backflip_ref
    row.current_stage = current_stage
    return row


class TestFaMaxBackflipFilesCommand:
    def test_rejects_bad_signature(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=False):
            response = client.post(
                "/api/admin/slack/fa-max-backflip-files",
                data={"user_id": "U123"},
                headers=_signed_headers(),
            )
        assert response.status_code == 401

    def test_rejects_unauthorized_user(self):
        with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
             patch("src.api.admin_router._relay_approver_authorized", return_value=False):
            response = client.post(
                "/api/admin/slack/fa-max-backflip-files",
                data={"user_id": "U999"},
                headers=_signed_headers(),
            )
        assert response.status_code == 200
        assert "Not authorized" in response.json()["text"]

    def test_lists_open_files_with_backflip_ref(self):
        from src.api.admin_router import get_db

        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = [
            _fake_row("Jane Doe", "bl1234", "under_review"),
            _fake_row("John Smith", "BF-2001", "submitted"),
        ]

        def _override():
            yield mock_db

        app.dependency_overrides[get_db] = _override
        try:
            with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
                 patch("src.api.admin_router._relay_approver_authorized", return_value=True):
                response = client.post(
                    "/api/admin/slack/fa-max-backflip-files",
                    data={"user_id": "U123"},
                    headers=_signed_headers(),
                )
        finally:
            app.dependency_overrides.pop(get_db, None)
        assert response.status_code == 200
        text = response.json()["text"]
        assert "*Open Backflip Files* (2)" in text
        assert "```" in text
        assert "Borrower" in text and "Ref" in text and "Stage" in text
        assert "Jane Doe" in text and "bl1234" in text and "under_review" in text
        assert "John Smith" in text and "BF-2001" in text and "submitted" in text
        assert "/fa-max-file-update <ref> ...`" in text

    def test_empty_result_returns_friendly_message(self):
        from src.api.admin_router import get_db

        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        def _override():
            yield mock_db

        app.dependency_overrides[get_db] = _override
        try:
            with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
                 patch("src.api.admin_router._relay_approver_authorized", return_value=True):
                response = client.post(
                    "/api/admin/slack/fa-max-backflip-files",
                    data={"user_id": "U123"},
                    headers=_signed_headers(),
                )
        finally:
            app.dependency_overrides.pop(get_db, None)
        assert response.status_code == 200
        assert "No open files" in response.json()["text"]

    def test_query_filters_to_open_outcome_with_a_ref(self):
        from src.api.admin_router import get_db

        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        def _override():
            yield mock_db

        app.dependency_overrides[get_db] = _override
        try:
            with patch("src.api.admin_router._verify_slack_signature", return_value=True), \
                 patch("src.api.admin_router._relay_approver_authorized", return_value=True):
                client.post(
                    "/api/admin/slack/fa-max-backflip-files",
                    data={"user_id": "U123"},
                    headers=_signed_headers(),
                )
        finally:
            app.dependency_overrides.pop(get_db, None)
        sql = str(mock_db.execute.call_args.args[0])
        assert "backflip_ref IS NOT NULL" in sql
        assert "outcome = 'open'" in sql
