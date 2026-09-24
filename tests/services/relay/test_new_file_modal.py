"""tests/services/relay/test_new_file_modal.py"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.services.relay.slack_post import (
    _build_new_file_modal,
    _build_new_file_new_entry_view,
    open_new_file_modal,
    open_new_file_new_entry_view,
)


class TestBuildNewFileModal:
    def test_initial_view_has_external_select_search(self):
        view = _build_new_file_modal()
        assert view["type"] == "modal"
        assert view["callback_id"] == "fa_max_new_file_submit"
        block_types = {b.get("block_id") for b in view["blocks"]}
        assert "borrower_search_block" in block_types
        search_block = next(b for b in view["blocks"] if b["block_id"] == "borrower_search_block")
        assert search_block["element"]["type"] == "external_select"
        assert search_block["element"]["min_query_length"] == 2

    def test_initial_view_has_new_borrower_button(self):
        view = _build_new_file_modal()
        action_ids = {
            el.get("action_id")
            for b in view["blocks"] if b.get("type") == "actions"
            for el in b.get("elements", [])
        }
        assert "new_file_new_borrower" in action_ids

    def test_initial_view_has_deal_detail_blocks(self):
        # The search view's own submit button is live (existing-borrower
        # path submits directly), so it must carry the same deal fields the
        # submit handler reads -- opportunity_type is required by
        # fa_max_opportunities' CHECK constraint. property_address is here
        # too (not just on the new-borrower view) since a repeat existing
        # borrower's new loan can be for a different property.
        view = _build_new_file_modal()
        block_ids = {b["block_id"] for b in view["blocks"] if "block_id" in b}
        assert block_ids >= {
            "opportunity_type_block", "loan_amount_block", "backflip_ref_block",
            "new_property_address_block",
        }
        opp = next(b for b in view["blocks"] if b.get("block_id") == "opportunity_type_block")
        assert opp["element"]["type"] == "static_select"
        assert opp["element"]["action_id"] == "opportunity_type"
        assert not opp.get("optional")


class TestBuildNewEntryView:
    def test_new_entry_view_has_required_fields(self):
        view = _build_new_file_new_entry_view({})
        block_ids = {b["block_id"] for b in view["blocks"] if "block_id" in b}
        assert block_ids >= {
            "new_full_name_block", "new_email_block", "new_phone_block",
            "new_property_address_block", "opportunity_type_block",
            "loan_amount_block", "backflip_ref_block",
        }

    def test_new_entry_view_preserves_prior_private_metadata(self):
        view = _build_new_file_new_entry_view({"note": "kept"})
        assert json.loads(view["private_metadata"])["note"] == "kept"
        assert json.loads(view["private_metadata"])["mode"] == "new_borrower"


class TestOpenNewFileModal:
    def test_calls_views_open_with_built_view(self):
        with patch("src.services.relay.slack_post.get_settings") as mock_settings, \
             patch("slack_sdk.WebClient") as mock_client_cls:
            mock_settings.return_value.fa_max_slack_bot_token.get_secret_value.return_value = "xoxb-test"
            result = open_new_file_modal("trigger-123")
        assert result is True
        mock_client_cls.return_value.views_open.assert_called_once()
        call_kwargs = mock_client_cls.return_value.views_open.call_args.kwargs
        assert call_kwargs["trigger_id"] == "trigger-123"
        assert call_kwargs["view"]["callback_id"] == "fa_max_new_file_submit"

    def test_no_token_returns_false(self):
        with patch("src.services.relay.slack_post.get_settings") as mock_settings:
            mock_settings.return_value.fa_max_slack_bot_token = None
            result = open_new_file_modal("trigger-123")
        assert result is False


class TestOpenNewFileNewEntryView:
    def test_calls_views_update_in_place_preserving_metadata(self):
        with patch("src.services.relay.slack_post.get_settings") as mock_settings, \
             patch("slack_sdk.WebClient") as mock_client_cls:
            mock_settings.return_value.fa_max_slack_bot_token.get_secret_value.return_value = "xoxb-test"
            result = open_new_file_new_entry_view("V1", "h1", {"note": "kept"})
        assert result is True
        mock_client_cls.return_value.views_open.assert_not_called()
        kwargs = mock_client_cls.return_value.views_update.call_args.kwargs
        assert kwargs["view_id"] == "V1"
        assert kwargs["hash"] == "h1"
        meta = json.loads(kwargs["view"]["private_metadata"])
        assert meta == {"note": "kept", "mode": "new_borrower"}

    def test_missing_view_id_or_hash_returns_false(self):
        with patch("src.services.relay.slack_post.get_settings") as mock_settings, \
             patch("slack_sdk.WebClient") as mock_client_cls:
            mock_settings.return_value.fa_max_slack_bot_token.get_secret_value.return_value = "xoxb-test"
            assert open_new_file_new_entry_view("", "h1", {}) is False
            assert open_new_file_new_entry_view("V1", "", {}) is False
        mock_client_cls.return_value.views_update.assert_not_called()
