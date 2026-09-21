"""tests/agents/cora/ingestion/test_reply_mailbox_poller_backflip.py

Unit-tests only the new branch's dispatch decision, not Gmail API
mechanics (already covered by the poller's own existing test suite).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.agents.cora.ingestion.reply_mailbox_poller import _process_candidate_message


def _mock_service(subject: str, body_text: str, from_address: str):
    service = MagicMock()
    service.users.return_value.messages.return_value.get.return_value.execute.return_value = {
        "payload": {
            "headers": [
                {"name": "From", "value": from_address},
                {"name": "Subject", "value": subject},
            ],
            "mimeType": "text/plain",
            "body": {"data": _b64(body_text)},
        }
    }
    return service


def _b64(text: str) -> str:
    import base64
    return base64.urlsafe_b64encode(text.encode()).decode()


class TestBackflipNotificationBranch:
    def test_backflip_sender_routes_to_stage_ingest(self):
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = None  # no fa_max_persons/thread match
        service = _mock_service(
            "Application BF-10293 — Now Under Review",
            "Your application BF-10293 has moved to Under Review.",
            "notifications@backflip.com",
        )
        with patch(
            "src.agents.cora.ingestion.reply_mailbox_poller.find_opportunity_thread_id_by_email",
            return_value=None,
        ), patch(
            "config.settings.get_settings"
        ) as mock_settings, patch(
            "src.agents.reply_concierge.backflip_stage_ingest.apply_parsed_event",
            return_value=True,
        ) as mock_apply:
            mock_settings.return_value.fa_max_backflip_notification_sender_domain = "backflip.com"
            result = _process_candidate_message(service, "msg-1", db)
        assert result is True
        mock_apply.assert_called_once()
        applied_event = mock_apply.call_args.args[1]
        assert applied_event.stage == "under_review"

    def test_non_backflip_sender_unmatched_falls_through_unchanged(self):
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = None
        service = _mock_service("Random newsletter", "Nothing relevant", "news@example.com")
        with patch(
            "src.agents.cora.ingestion.reply_mailbox_poller.find_opportunity_thread_id_by_email",
            return_value=None,
        ), patch("config.settings.get_settings") as mock_settings:
            mock_settings.return_value.fa_max_backflip_notification_sender_domain = "backflip.com"
            result = _process_candidate_message(service, "msg-2", db)
        assert result is False  # unchanged prior behavior: unmatched sender, not queued
