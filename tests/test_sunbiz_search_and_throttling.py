"""
Tests for the Sunbiz search-term sanitization fix and the circuit breaker
that distinguishes "Sunbiz is throttling this session" from "this specific
owner has no match / one-off Playwright hiccup."

Root cause (confirmed live against search.sunbiz.org): the site's own
client-side JS embeds the raw search term into a route path segment
(.../SearchResults/EntityName/{term}/Page1) without percent-encoding it. A
literal "/" or ":" in the term splits into an invalid extra path segment and
the server returns a generic "resource unavailable" page — indistinguishable
from a slow/unresponsive site from the caller's side (a #search-results
wait_for_selector timeout). This was previously misclassified as a Playwright
failure (stats["failed"]) for every owner name containing either character,
even when the entity's real registered name has nothing wrong with it
(e.g. "MATTAMY TAMPA/SARASOTA LLC").
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.scrappers.sunbiz.sunbiz_engine import (
    _CIRCUIT_BREAKER_THRESHOLD,
    _clean_search_name,
    _normalize,
    _run_playwright_batch,
)


# ── _normalize: "/" and ":" are word separators for comparison ──────────────

class TestNormalizeUrlBreakingChars:
    def test_slash_normalizes_to_space(self):
        assert _normalize("MATTAMY TAMPA/SARASOTA LLC") == "MATTAMY TAMPA SARASOTA LLC"

    def test_colon_normalizes_to_space(self):
        assert _normalize("SEFFNER GALATIANS 5:22 LLC") == "SEFFNER GALATIANS 5 22 LLC"

    def test_slash_and_punctuation_produce_same_normal_form_as_space_variant(self):
        """A Sunbiz result row's raw text (with the real '/') must normalize
        identically to the space-substituted search term we submit, or the
        exact-match comparison in _scrape_entity_detail silently fails."""
        result_row_text = "MAGNOLIA/DELEON ASSOCIATES, LTD."
        search_term = "MAGNOLIA DELEON ASSOCIATES LTD"
        assert _normalize(result_row_text) == _normalize(search_term)

    def test_existing_punctuation_stripping_still_works(self):
        assert _normalize("O'BRIEN & SONS, LLC.") == "OBRIEN AND SONS LLC"


# ── _clean_search_name: query-string construction ────────────────────────────

class TestCleanSearchName:
    def test_ttee_suffix_still_stripped(self):
        assert _clean_search_name("BKE REALTY INVESTMENTS LLC/TTEE") == "BKE REALTY INVESTMENTS LLC"

    def test_trustee_suffix_still_stripped(self):
        assert _clean_search_name("SOME HOLDINGS LLC/TRUSTEE") == "SOME HOLDINGS LLC"

    def test_tr_suffix_stripped(self):
        """Found from real production logs: 'GUNN PROPERTIES OF TAMPA INC / TR'
        — /TR is the same trust-role-annotation family as /TTEE."""
        assert _clean_search_name("GUNN PROPERTIES OF TAMPA INC / TR") == "GUNN PROPERTIES OF TAMPA INC"

    def test_co_suffix_still_stripped(self):
        assert _clean_search_name("WATERS XF LLC C/O ALTUS GROUP") == "WATERS XF LLC"

    def test_remaining_slash_replaced_with_space_not_left_raw(self):
        """Previously left untouched on the theory that guessing which side
        is the 'real' name was unsafe. Live-verified against search.sunbiz.org
        that a raw '/' 404s the route regardless, and the real entity is
        exact-matchable once '/' becomes a space — so replacing (not
        stripping one side) is the correct, non-lossy transform."""
        assert _clean_search_name("MATTAMY TAMPA/SARASOTA LLC") == "MATTAMY TAMPA SARASOTA LLC"
        assert _clean_search_name("ICON FL TAMPA INDUSTRIAL OWNER POOL 5 GA/FL LLC") == \
            "ICON FL TAMPA INDUSTRIAL OWNER POOL 5 GA FL LLC"

    def test_remaining_colon_replaced_with_space(self):
        assert _clean_search_name("SEFFNER GALATIANS 5:22 LLC") == "SEFFNER GALATIANS 5 22 LLC"

    def test_no_special_chars_unchanged(self):
        assert _clean_search_name("EMERALD GARDEN REAL ESTATE INC") == "EMERALD GARDEN REAL ESTATE INC"

    def test_suffix_strip_then_slash_replace_compose_correctly(self):
        """A name could in principle need both steps; confirm order doesn't
        leave a stray slash or double space behind."""
        cleaned = _clean_search_name("ALV/GAZIT TAMPA LLC/TTEE")
        assert "/" not in cleaned
        assert "  " not in cleaned


# ── circuit breaker: throttling vs per-owner bad luck ────────────────────────

def _mk_owners(n: int):
    return [SimpleNamespace(owner_name=f"TEST OWNER {i} LLC") for i in range(n)]


class _FakeAsyncPlaywrightContext:
    """Minimal async context manager standing in for `async with async_playwright() as pw:`."""

    def __init__(self, pw_instance):
        self._pw_instance = pw_instance

    async def __aenter__(self):
        return self._pw_instance

    async def __aexit__(self, *exc):
        return False


def _mk_fake_playwright():
    mock_page = AsyncMock()
    mock_context = AsyncMock()
    mock_context.new_page = AsyncMock(return_value=mock_page)
    mock_browser = AsyncMock()
    mock_browser.new_context = AsyncMock(return_value=mock_context)
    mock_pw_instance = AsyncMock()
    mock_pw_instance.chromium.launch = AsyncMock(return_value=mock_browser)
    return _FakeAsyncPlaywrightContext(mock_pw_instance)


def test_circuit_breaker_trips_and_aborts_remaining_after_failed_retry():
    """Sustained (consecutive) hard timeouts — even after one backoff+retry —
    must abort the rest of the batch rather than grinding through guaranteed
    failures, and must be flagged distinctly (rate_limited), not as a normal
    per-owner scraper_error."""
    owners = _mk_owners(20)
    stats = {
        "processed": 0, "enriched": 0, "skipped": 0, "failed": 0,
        "rate_limited": False, "remaining_unprocessed": 0,
    }

    async def run():
        with patch("playwright.async_api.async_playwright", return_value=_mk_fake_playwright()), \
             patch("src.scrappers.sunbiz.sunbiz_engine.apply_stealth_to_page", new=AsyncMock()), \
             patch("src.scrappers.sunbiz.sunbiz_engine._scrape_entity_detail",
                   new=AsyncMock(side_effect=TimeoutError("Page.wait_for_selector timeout"))), \
             patch("asyncio.sleep", new=AsyncMock()):
            await _run_playwright_batch(owners, dry_run=True, stats=stats, session=None, headless=True)

    asyncio.run(run())

    assert stats["rate_limited"] is True
    assert stats["remaining_unprocessed"] > 0
    # Aborted before processing all 20 — exactly threshold consecutive
    # failures plus the one retry attempt were made before giving up.
    assert stats["processed"] < 20
    assert stats["processed"] >= _CIRCUIT_BREAKER_THRESHOLD


def test_transient_blip_recovers_without_tripping_breaker():
    """A short run of failures below threshold, followed by a success, must
    NOT trip the breaker and must NOT abort the batch — this is ordinary
    per-owner noise (some names just have no Sunbiz match), not throttling."""
    owners = _mk_owners(5)
    stats = {
        "processed": 0, "enriched": 0, "skipped": 0, "failed": 0,
        "rate_limited": False, "remaining_unprocessed": 0,
    }

    call_count = {"n": 0}

    async def flaky(page, name):
        call_count["n"] += 1
        if call_count["n"] <= _CIRCUIT_BREAKER_THRESHOLD - 1:
            raise TimeoutError("transient")
        return None, None  # clean "not found" response afterwards

    async def run():
        with patch("playwright.async_api.async_playwright", return_value=_mk_fake_playwright()), \
             patch("src.scrappers.sunbiz.sunbiz_engine.apply_stealth_to_page", new=AsyncMock()), \
             patch("src.scrappers.sunbiz.sunbiz_engine._scrape_entity_detail", new=flaky), \
             patch("asyncio.sleep", new=AsyncMock()):
            await _run_playwright_batch(owners, dry_run=True, stats=stats, session=None, headless=True)

    asyncio.run(run())

    assert stats["rate_limited"] is False
    assert stats["processed"] == 5


def test_recovery_after_backoff_retry_resets_and_continues():
    """If the single post-backoff retry succeeds, the breaker resets and the
    batch keeps going instead of aborting on a one-time throttle blip."""
    owners = _mk_owners(_CIRCUIT_BREAKER_THRESHOLD + 3)
    stats = {
        "processed": 0, "enriched": 0, "skipped": 0, "failed": 0,
        "rate_limited": False, "remaining_unprocessed": 0,
    }

    call_count = {"n": 0}

    async def fails_then_recovers(page, name):
        call_count["n"] += 1
        if call_count["n"] <= _CIRCUIT_BREAKER_THRESHOLD:
            raise TimeoutError("throttled")
        return None, None

    async def run():
        with patch("playwright.async_api.async_playwright", return_value=_mk_fake_playwright()), \
             patch("src.scrappers.sunbiz.sunbiz_engine.apply_stealth_to_page", new=AsyncMock()), \
             patch("src.scrappers.sunbiz.sunbiz_engine._scrape_entity_detail", new=fails_then_recovers), \
             patch("asyncio.sleep", new=AsyncMock()):
            await _run_playwright_batch(owners, dry_run=True, stats=stats, session=None, headless=True)

    asyncio.run(run())

    assert stats["rate_limited"] is False
    assert stats["processed"] == _CIRCUIT_BREAKER_THRESHOLD + 3
