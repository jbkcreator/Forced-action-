"""tests/services/relay/test_log_submission_modal.py"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.services.relay.slack_post import (
    _build_log_submission_modal,
    _build_log_submission_new_entry_view,
    open_log_submission_modal,
)


class TestBuildLogSubmissionModal:
    def test_initial_view_has_external_select_search(self):
        view = _build_log_submission_modal()
        assert view["type"] == "modal"
        assert view["callback_id"] == "fa_max_log_submission_submit"
        block_types = {b.get("block_id") for b in view["blocks"]}
        assert "borrower_search_block" in block_types
        search_block = next(b for b in view["blocks"] if b["block_id"] == "borrower_search_block")
        assert search_block["element"]["type"] == "external_select"
        assert search_block["element"]["min_query_length"] == 2

    def test_initial_view_has_new_borrower_button(self):
        view = _build_log_submission_modal()
        action_ids = {
            el.get("action_id")
            for b in view["blocks"] if b.get("type") == "actions"
            for el in b.get("elements", [])
        }
        assert "log_submission_new_borrower" in action_ids


class TestBuildNewEntryView:
    def test_new_entry_view_has_required_fields(self):
        view = _build_log_submission_new_entry_view({})
        block_ids = {b["block_id"] for b in view["blocks"] if "block_id" in b}
        assert block_ids >= {
            "new_full_name_block", "new_email_block", "new_phone_block",
            "new_property_address_block", "opportunity_type_block",
            "loan_amount_block", "backflip_ref_block",
        }

    def test_new_entry_view_preserves_prior_private_metadata(self):
        view = _build_log_submission_new_entry_view({"note": "kept"})
        assert json.loads(view["private_metadata"])["note"] == "kept"
        assert json.loads(view["private_metadata"])["mode"] == "new_borrower"


class TestOpenLogSubmissionModal:
    def test_calls_views_open_with_built_view(self):
        with patch("src.services.relay.slack_post.get_settings") as mock_settings, \
             patch("slack_sdk.WebClient") as mock_client_cls:
            mock_settings.return_value.fa_max_slack_bot_token.get_secret_value.return_value = "xoxb-test"
            result = open_log_submission_modal("trigger-123")
        assert result is True
        mock_client_cls.return_value.views_open.assert_called_once()
        call_kwargs = mock_client_cls.return_value.views_open.call_args.kwargs
        assert call_kwargs["trigger_id"] == "trigger-123"
        assert call_kwargs["view"]["callback_id"] == "fa_max_log_submission_submit"

    def test_no_token_returns_false(self):
        with patch("src.services.relay.slack_post.get_settings") as mock_settings:
            mock_settings.return_value.fa_max_slack_bot_token = None
            result = open_log_submission_modal("trigger-123")
        assert result is False
