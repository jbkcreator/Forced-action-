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
    format_count_by_color_result,
    format_top_uncalled_deal_result,
    format_source_staleness_result,
    format_deal_status_result,
    build_redirect_text,
    build_other_ack_text,
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
