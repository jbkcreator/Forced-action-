"""Unit tests for WP-T2-12 card-thread fallback responder.

Tests the classify step, catalog dispatch, and reply formatting in isolation.
The DB-backed catalog queries are integration-tested via the classify+dispatch
path with a mocked DB session.
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.services.relay.thread_fallback_responder import (
    Bucket,
    ClassifyResult,
    _parse_classify_response,
    _validate_classify,
    _coalesce_roles,
    format_count_by_color_result,
    format_top_uncalled_deal_result,
    format_source_staleness_result,
    format_deal_status_result,
    build_redirect_text,
    build_other_ack_text,
    build_social_reply,
)


# ---------------------------------------------------------------------------
# _parse_classify_response — strict JSON parse of Haiku output
# ---------------------------------------------------------------------------

class TestParseClassifyResponse:
    def test_simple_lookup_with_params(self):
        raw = json.dumps({
            "bucket": "simple_lookup",
            "lookup_id": "count_by_color",
            "params": {"color": "green", "today": True},
        })
        result = _parse_classify_response(raw)
        assert result.bucket == Bucket.SIMPLE_LOOKUP
        assert result.lookup_id == "count_by_color"
        assert result.params == {"color": "green", "today": True}

    def test_cc_query_bucket(self):
        raw = json.dumps({"bucket": "cc_query", "lookup_id": None, "params": {}})
        result = _parse_classify_response(raw)
        assert result.bucket == Bucket.CC_QUERY
        assert result.lookup_id is None

    def test_other_bucket(self):
        raw = json.dumps({"bucket": "other", "lookup_id": None, "params": {}})
        result = _parse_classify_response(raw)
        assert result.bucket == Bucket.OTHER

    def test_unknown_lookup_id_downgrades_to_cc_query(self):
        """An unknown lookup_id (not in catalog) must redirect, not error."""
        raw = json.dumps({
            "bucket": "simple_lookup",
            "lookup_id": "made_up_lookup",
            "params": {},
        })
        result = _parse_classify_response(raw)
        assert result.bucket == Bucket.CC_QUERY

    def test_simple_lookup_missing_required_param_address_downgrades(self):
        """deal_status without address param must downgrade to cc_query."""
        raw = json.dumps({
            "bucket": "simple_lookup",
            "lookup_id": "deal_status",
            "params": {},
        })
        result = _parse_classify_response(raw)
        assert result.bucket == Bucket.CC_QUERY

    def test_invalid_json_becomes_other(self):
        result = _parse_classify_response("not json at all")
        assert result.bucket == Bucket.OTHER

    def test_empty_string_becomes_other(self):
        result = _parse_classify_response("")
        assert result.bucket == Bucket.OTHER

    def test_top_uncalled_deal_no_required_params(self):
        raw = json.dumps({
            "bucket": "simple_lookup",
            "lookup_id": "top_uncalled_deal",
            "params": {},
        })
        result = _parse_classify_response(raw)
        assert result.bucket == Bucket.SIMPLE_LOOKUP
        assert result.lookup_id == "top_uncalled_deal"

    def test_count_by_color_all_colors_no_params(self):
        raw = json.dumps({
            "bucket": "simple_lookup",
            "lookup_id": "count_by_color",
            "params": {"color": None, "today": False},
        })
        result = _parse_classify_response(raw)
        assert result.bucket == Bucket.SIMPLE_LOOKUP
        assert result.params["color"] is None

    def test_source_staleness_named_source(self):
        raw = json.dumps({
            "bucket": "simple_lookup",
            "lookup_id": "source_staleness",
            "params": {"source": "Tracerfy"},
        })
        result = _parse_classify_response(raw)
        assert result.bucket == Bucket.SIMPLE_LOOKUP
        assert result.params["source"] == "Tracerfy"


# ---------------------------------------------------------------------------
# Format helpers — pure functions, no DB
# ---------------------------------------------------------------------------

class TestFormatCountByColor:
    def test_single_color(self):
        rows = [{"gyr_color": "green", "cnt": 12}]
        text = format_count_by_color_result(rows, color_filter="green")
        assert "12" in text
        assert "green" in text.lower()

    def test_all_colors(self):
        rows = [
            {"gyr_color": "green", "cnt": 5},
            {"gyr_color": "yellow", "cnt": 3},
            {"gyr_color": "red", "cnt": 8},
        ]
        text = format_count_by_color_result(rows, color_filter=None)
        assert "5" in text
        assert "3" in text
        assert "8" in text

    def test_empty_result(self):
        text = format_count_by_color_result([], color_filter="green")
        assert "0" in text or "none" in text.lower() or "no " in text.lower()


class TestFormatTopUncalledDeal:
    def test_deal_found(self):
        row = {
            "opportunity_id": "abc-123",
            "gyr_color": "green",
            "expected_revenue_cents": 1800000,
            "current_stage": "submitted",
            "address": "4021 Bayshore Blvd",
        }
        text = format_top_uncalled_deal_result(row)
        assert "4021 Bayshore" in text or "abc-123" in text
        assert "$18,000" in text or "18,000" in text

    def test_no_deal(self):
        text = format_top_uncalled_deal_result(None)
        assert "no " in text.lower() or "none" in text.lower()


class TestFormatSourceStaleness:
    def test_stale_sources(self):
        rows = [{"source": "Tracerfy", "last_seen_days_ago": 5}]
        text = format_source_staleness_result(rows, source_filter=None)
        assert "Tracerfy" in text
        assert "5" in text

    def test_all_fresh(self):
        text = format_source_staleness_result([], source_filter=None)
        assert "fresh" in text.lower() or "no stale" in text.lower() or "all" in text.lower()

    def test_named_source_not_stale(self):
        text = format_source_staleness_result([], source_filter="BatchData")
        assert "BatchData" in text
        assert "fresh" in text.lower() or "no " in text.lower() or "not stale" in text.lower()


class TestFormatDealStatus:
    def test_deal_found(self):
        row = {
            "opportunity_id": "abc-123",
            "gyr_color": "yellow",
            "current_stage": "pre_approval",
            "outcome": "open",
            "updated_at": "2026-09-20T10:00:00",
            "address": "4021 Bayshore Blvd",
        }
        text = format_deal_status_result(row, address_query="4021 Bayshore")
        assert "yellow" in text.lower()
        assert "pre_approval" in text or "pre-approval" in text.lower()

    def test_not_found(self):
        text = format_deal_status_result(None, address_query="999 Nowhere St")
        assert "999 Nowhere" in text or "not found" in text.lower()


class TestRedirectAndAckText:
    def test_redirect_mentions_cc(self):
        text = build_redirect_text(cc_channel_id="C0BLD6BG6TS")
        assert "C0BLD6BG6TS" in text or "command center" in text.lower()

    def test_other_ack_mentions_followup(self):
        text = build_other_ack_text()
        assert "follow" in text.lower() or "noted" in text.lower()


class TestSocialReply:
    def test_thanks_variant(self):
        text = build_social_reply("thanks so much!")
        assert "anytime" in text.lower()

    def test_praise_variant(self):
        text = build_social_reply("nice work team")
        assert "anytime" in text.lower()

    def test_greeting_variant(self):
        text = build_social_reply("good morning team")
        assert "hey" in text.lower() or "👋" in text

    def test_social_reply_hints_capabilities(self):
        """Both variants nudge the operator toward what they can ask."""
        for msg in ("thanks!", "morning"):
            text = build_social_reply(msg).lower()
            assert "deal" in text or "source" in text


# ---------------------------------------------------------------------------
# Bucket validation (catalog membership)
# ---------------------------------------------------------------------------

VALID_LOOKUP_IDS = {"count_by_color", "top_uncalled_deal", "source_staleness", "deal_status"}

class TestCatalogMembership:
    @pytest.mark.parametrize("lookup_id", list(VALID_LOOKUP_IDS))
    def test_valid_catalog_entries_accepted(self, lookup_id: str):
        params: dict[str, Any] = {}
        if lookup_id == "deal_status":
            params = {"address": "123 Main St"}
        raw = json.dumps({"bucket": "simple_lookup", "lookup_id": lookup_id, "params": params})
        result = _parse_classify_response(raw)
        assert result.bucket == Bucket.SIMPLE_LOOKUP
        assert result.lookup_id == lookup_id

    def test_unknown_lookup_id_always_redirects(self):
        for bad_id in ("evaluate_deal", "run_report", "query_db", "scoreboard"):
            raw = json.dumps({"bucket": "simple_lookup", "lookup_id": bad_id, "params": {}})
            result = _parse_classify_response(raw)
            assert result.bucket == Bucket.CC_QUERY, f"{bad_id!r} should redirect to CC"


# ---------------------------------------------------------------------------
# Channel-level path (WP-T2-12 channel extension) — handle_channel_message
# ---------------------------------------------------------------------------

def _classify_stub(bucket_value: str, lookup_id=None, params=None):
    """Fake call_claude_with_usage response via the forced-tool_use path."""
    return {
        "text": "",
        "tool_input": {
            "bucket": bucket_value,
            "lookup_id": lookup_id,
            "params": params or {},
        },
        "input_tokens": 10,
        "output_tokens": 5,
        "cost_usd": 0.0001,
    }


class TestHandleChannelMessage:
    """The channel path answers real questions, stays silent on 'other',
    posts via post_note, and audits with relay_item_id=NULL."""

    def _run(self, text, classify_resp):
        event = {
            "type": "message", "user": "U_APPROVER", "channel": "C_RELATIONSHIPS",
            "ts": "1790000000.000100", "text": text,
        }
        with patch(
            "src.services.relay.thread_fallback_responder.call_claude_with_usage",
            return_value=classify_resp,
        ), patch(
            "src.core.database.get_db_context"
        ) as mock_db_ctx, patch(
            "src.services.relay.thread_fallback_responder._write_audit_log"
        ) as mock_audit, patch(
            "src.services.relay.thread_fallback_responder._run_catalog_lookup",
            return_value="Open opportunities: 5 green.",
        ), patch(
            "src.services.relay.slack_post.post_note"
        ) as mock_post:
            mock_db_ctx.return_value.__enter__.return_value = MagicMock()
            from src.services.relay.thread_fallback_responder import handle_channel_message
            handle_channel_message(
                event=event, venture_key="fa_max_lending",
                lane="RELATIONSHIPS", channel="C_RELATIONSHIPS",
            )
            return mock_post, mock_audit

    def test_simple_lookup_posts_answer(self):
        post, audit = self._run(
            "how many green deals?", _classify_stub("simple_lookup", "count_by_color", {"color": "green"})
        )
        post.assert_called_once()
        kwargs = post.call_args.kwargs
        assert kwargs["channel"] == "C_RELATIONSHIPS"
        assert kwargs["venture_key"] == "fa_max_lending"
        assert kwargs["thread_ts"] == "1790000000.000100"
        assert "green" in kwargs["text"].lower()

    def test_cc_query_posts_redirect(self):
        post, _ = self._run("evaluate this deal", _classify_stub("cc_query"))
        post.assert_called_once()
        # Redirect wording is stable regardless of whether a CC channel is set.
        assert "pipeline-intelligence" in post.call_args.kwargs["text"].lower()

    def test_other_stays_silent(self):
        """Gibberish / off-topic noise must NOT get a channel reply."""
        post, audit = self._run("asdfghjkl", _classify_stub("other"))
        post.assert_not_called()
        # Still audited — silence is logged, not invisible.
        audit.assert_called_once()

    def test_social_gets_friendly_reply(self):
        """Greetings/thanks are answered (not silent) so the bot feels present."""
        post, audit = self._run("good morning team", _classify_stub("social"))
        post.assert_called_once()
        assert post.call_args.kwargs["text"]  # non-empty friendly reply
        audit.assert_called_once()

    def test_audit_relay_item_id_is_none(self):
        _, audit = self._run("how many reds?", _classify_stub("simple_lookup", "count_by_color", {"color": "red"}))
        assert audit.call_args.kwargs["relay_item_id"] is None
        assert audit.call_args.kwargs["lane"] == "RELATIONSHIPS"

    def test_tool_use_path_preferred_over_text(self):
        """When tool_input is present it wins, even if text is garbage."""
        resp = _classify_stub("simple_lookup", "count_by_color", {"color": "green"})
        resp["text"] = "```json broken fence```"
        post, _ = self._run("how many greens?", resp)
        post.assert_called_once()

    def test_text_fallback_when_no_tool_input(self):
        """If the model returns text (no tool_use), the JSON fallback still parses."""
        resp = {
            "text": json.dumps({"bucket": "cc_query", "lookup_id": None, "params": {}}),
            "tool_input": None,
            "input_tokens": 1, "output_tokens": 1, "cost_usd": 0.0,
        }
        post, _ = self._run("evaluate this deal", resp)
        post.assert_called_once()
        assert "pipeline-intelligence" in post.call_args.kwargs["text"].lower()


# ---------------------------------------------------------------------------
# Follow-up context — _coalesce_roles enforces the Anthropic messages contract
# ---------------------------------------------------------------------------

class TestCoalesceRoles:
    def test_alternating_kept_as_is(self):
        msgs = [
            {"role": "user", "content": "how many greens?"},
            {"role": "assistant", "content": "5 green."},
            {"role": "user", "content": "DATA: what about reds?"},
        ]
        assert _coalesce_roles(msgs) == msgs

    def test_consecutive_user_turns_merged(self):
        msgs = [
            {"role": "user", "content": "how many greens?"},
            {"role": "user", "content": "DATA: what about reds?"},
        ]
        out = _coalesce_roles(msgs)
        assert len(out) == 1
        assert out[0]["role"] == "user"
        assert "greens" in out[0]["content"] and "reds" in out[0]["content"]

    def test_leading_assistant_dropped(self):
        msgs = [
            {"role": "assistant", "content": "stale bot note"},
            {"role": "user", "content": "DATA: how many greens?"},
        ]
        out = _coalesce_roles(msgs)
        assert out[0]["role"] == "user"
        assert len(out) == 1


class TestChannelHistoryWiring:
    """A threaded reply pulls prior turns; a top-level message does not."""

    def _run(self, event, history_turns):
        with patch(
            "src.services.relay.thread_fallback_responder.call_claude_with_usage",
            return_value=_classify_stub("simple_lookup", "count_by_color", {"color": "red"}),
        ) as mock_llm, patch(
            "src.core.database.get_db_context"
        ) as mock_db_ctx, patch(
            "src.services.relay.thread_fallback_responder._write_audit_log"
        ), patch(
            "src.services.relay.thread_fallback_responder._run_catalog_lookup",
            return_value="3 red.",
        ), patch(
            "src.services.relay.slack_post.post_note"
        ), patch(
            "src.services.relay.slack_post.fetch_thread_history",
            return_value=history_turns,
        ) as mock_hist:
            mock_db_ctx.return_value.__enter__.return_value = MagicMock()
            from src.services.relay.thread_fallback_responder import handle_channel_message
            handle_channel_message(
                event=event, venture_key="fa_max_lending",
                lane="RELATIONSHIPS", channel="C_REL",
            )
            return mock_llm, mock_hist

    def test_threaded_reply_fetches_and_passes_history(self):
        event = {
            "type": "message", "user": "U1", "channel": "C_REL",
            "ts": "1790000000.000200", "thread_ts": "1790000000.000100",
            "text": "what about reds?",
        }
        prior = [
            {"role": "user", "content": "how many greens?"},
            {"role": "assistant", "content": "5 green."},
        ]
        mock_llm, mock_hist = self._run(event, prior)
        mock_hist.assert_called_once()
        # The prior turns are prepended ahead of the current DATA: message.
        sent = mock_llm.call_args.kwargs["messages"]
        assert sent[0]["content"] == "how many greens?"
        assert sent[-1]["content"].startswith("DATA:")

    def test_top_level_message_no_history_fetch(self):
        event = {
            "type": "message", "user": "U1", "channel": "C_REL",
            "ts": "1790000000.000100", "text": "how many reds?",
        }
        mock_llm, mock_hist = self._run(event, [])
        mock_hist.assert_not_called()
        sent = mock_llm.call_args.kwargs["messages"]
        assert len(sent) == 1
        assert sent[0]["content"].startswith("DATA:")

    def test_history_fetch_failure_degrades_to_single_shot(self):
        """A conversations_replies error must not break the reply — history=[]"""
        event = {
            "type": "message", "user": "U1", "channel": "C_REL",
            "ts": "1790000000.000200", "thread_ts": "1790000000.000100",
            "text": "what about reds?",
        }
        # fetch_thread_history itself swallows errors and returns []; simulate that.
        mock_llm, _ = self._run(event, [])
        sent = mock_llm.call_args.kwargs["messages"]
        assert len(sent) == 1  # no history, still classifies the current message


# ---------------------------------------------------------------------------
# Routing seam — admin_router._handle_relay_thread_action WP-T2-12 branches
# ---------------------------------------------------------------------------

class TestChannelRouting:
    """The channel path fires only for an authorized approver posting in a
    mapped FA Max lane channel. Everything else is dropped."""

    def _dispatch(self, event, *, item=None, lane_map=None, authorized=True):
        from src.api import admin_router
        with patch(
            "src.services.relay.queue.get_item_by_slack_message_ts", return_value=item
        ), patch.object(
            admin_router, "_fa_max_channel_lane_map",
            return_value=(lane_map if lane_map is not None else {"C_REL": "RELATIONSHIPS"}),
        ), patch.object(
            admin_router, "_relay_approver_authorized", return_value=authorized
        ), patch(
            "src.services.relay.thread_fallback_responder.handle_channel_message"
        ) as mock_channel, patch(
            "src.services.relay.thread_fallback_responder.handle_thread_fallback_reply"
        ) as mock_card:
            admin_router._handle_relay_thread_action({"event": event})
            return mock_channel, mock_card

    def test_authorized_in_mapped_channel_answers(self):
        ch, _ = self._dispatch({
            "type": "message", "user": "U1", "channel": "C_REL",
            "ts": "1.1", "text": "how many greens?",
        })
        ch.assert_called_once()
        assert ch.call_args.kwargs["lane"] == "RELATIONSHIPS"

    def test_unauthorized_user_dropped(self):
        ch, _ = self._dispatch({
            "type": "message", "user": "U_STRANGER", "channel": "C_REL",
            "ts": "1.1", "text": "how many greens?",
        }, authorized=False)
        ch.assert_not_called()

    def test_unmapped_channel_dropped(self):
        ch, _ = self._dispatch({
            "type": "message", "user": "U1", "channel": "C_RANDOM",
            "ts": "1.1", "text": "how many greens?",
        })
        ch.assert_not_called()

    def test_bot_message_ignored(self):
        ch, card = self._dispatch({
            "type": "message", "user": "U1", "channel": "C_REL",
            "ts": "1.1", "text": "0 green.", "bot_id": "B1",
        })
        ch.assert_not_called()
        card.assert_not_called()

    def test_subtype_message_ignored(self):
        ch, _ = self._dispatch({
            "type": "message", "user": "U1", "channel": "C_REL",
            "ts": "1.1", "text": "joined", "subtype": "channel_join",
        })
        ch.assert_not_called()

    def test_missing_user_ignored(self):
        ch, _ = self._dispatch({
            "type": "message", "channel": "C_REL", "ts": "1.1", "text": "hi",
        })
        ch.assert_not_called()

    def test_card_thread_noncommand_uses_card_path_not_channel(self):
        item = MagicMock(venture_key="fa_max_lending", revision_count=0, id=5)
        ch, card = self._dispatch({
            "type": "message", "user": "U1", "channel": "C_REL",
            "ts": "2.2", "thread_ts": "1.1", "text": "why this one?",
        }, item=item)
        card.assert_called_once()
        ch.assert_not_called()


# ---------------------------------------------------------------------------
# Anti-hallucination — answers come ONLY from the DB, never the LLM
# ---------------------------------------------------------------------------

class TestNoHallucination:
    """The LLM classifies; the DB answers. A data reply must equal the actual
    rows, and no reply may invent a number the DB didn't return."""

    def test_reply_number_equals_db_count(self):
        from src.services.relay import thread_fallback_responder as tfr
        db = MagicMock()
        with patch.object(
            tfr, "_execute_catalog_query",
            return_value={"rows": [{"gyr_color": "red", "cnt": 7}], "count": 1},
        ):
            reply = tfr._run_catalog_lookup(
                ClassifyResult(bucket=Bucket.SIMPLE_LOOKUP, lookup_id="count_by_color",
                               params={"color": "red"}),
                db,
            )
        # The 7 is the DB's, not the model's.
        assert "7 red" in reply

    def test_db_error_yields_honest_message_not_a_number(self):
        from src.services.relay import thread_fallback_responder as tfr
        db = MagicMock()
        with patch.object(
            tfr, "_execute_catalog_query", return_value={"error": "connection reset"},
        ):
            reply = tfr._run_catalog_lookup(
                ClassifyResult(bucket=Bucket.SIMPLE_LOOKUP, lookup_id="count_by_color",
                               params={"color": "green"}),
                db,
            )
        # A DB error must NOT read as a confirmed zero — it must surface as an
        # honest "couldn't retrieve" reply, distinct from a real empty result.
        assert "0 open opportunities" not in reply
        assert "Couldn't retrieve" in reply
        assert not any(ch.isdigit() for ch in reply)

    def test_db_error_reply_differs_from_confirmed_zero(self):
        """The error reply and a genuine empty result must be distinguishable."""
        from src.services.relay import thread_fallback_responder as tfr
        db = MagicMock()

        with patch.object(
            tfr, "_execute_catalog_query", return_value={"error": "timeout"},
        ):
            error_reply = tfr._run_catalog_lookup(
                ClassifyResult(bucket=Bucket.SIMPLE_LOOKUP, lookup_id="count_by_color",
                               params={"color": "green"}),
                db,
            )
        with patch.object(
            tfr, "_execute_catalog_query", return_value={"rows": [], "count": 0},
        ):
            zero_reply = tfr._run_catalog_lookup(
                ClassifyResult(bucket=Bucket.SIMPLE_LOOKUP, lookup_id="count_by_color",
                               params={"color": "green"}),
                db,
            )
        assert error_reply != zero_reply
        assert "0 open opportunities" in zero_reply

    def test_catalog_query_never_raises(self):
        """Even a raising DB session returns a safe string, not an exception."""
        from src.services.relay import thread_fallback_responder as tfr
        db = MagicMock()
        db.execute.side_effect = RuntimeError("db down")
        reply = tfr._run_catalog_lookup(
            ClassifyResult(bucket=Bucket.SIMPLE_LOOKUP, lookup_id="deal_status",
                           params={"address": "123 Main"}),
            db,
        )
        assert isinstance(reply, str) and reply

    def test_redirect_and_ack_carry_no_data(self):
        """The only LLM-adjacent replies are static templates — no numbers."""
        redirect = build_redirect_text(cc_channel_id="C123")
        ack = build_other_ack_text()
        for txt in (redirect, ack):
            assert not any(ch.isdigit() for ch in txt.replace("C123", ""))

    def test_invalid_bucket_string_becomes_other(self):
        res = _validate_classify({"bucket": "definitely_not_a_bucket"})
        assert res.bucket == Bucket.OTHER

    def test_unknown_color_is_honest_zero_not_invented(self):
        """A nonsense color param returns the DB's empty result honestly."""
        text = format_count_by_color_result([], color_filter="purple")
        assert "0" in text and "purple" in text
