"""Tests for CourtListener bankruptcy fetch rate-limit safety.

The free-tier token allows only 50 requests/hour + 5/min. PR #245's unbounded
page-walk over the whole Middle District blew past that and earned multi-hour
bans. These tests lock in the guards that keep the request count small, without
touching the live API.
"""

from __future__ import annotations

from unittest.mock import patch

import src.scrappers.bankruptcy.bankruptcy_engine as bk


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _make_pages(n_pages, per_page=100):
    """Simulate an endless paginated result set (always has a 'next')."""
    calls = {"n": 0}

    def fake_get(url, params=None, headers=None, timeout=None, max_retry_delay=None):
        calls["n"] += 1
        # every page returns a full page and a next cursor → never self-terminates
        results = [
            {"docket_number": f"8:26-bk-{calls['n']:04d}{i}", "federal_dn_case_type": "bk",
             "case_name": f"In re: Debtor {i}"}
            for i in range(per_page)
        ]
        return _FakeResp({"results": results, "next": f"{bk.COURTLISTENER_API_URL}?cursor={calls['n']}"})

    return fake_get, calls


def test_pagination_stops_at_page_cap():
    fake_get, calls = _make_pages(999)
    with patch.object(bk, "requests_get_with_retry", side_effect=fake_get), \
         patch.object(bk.time, "sleep"):
        rows = bk.fetch_bankruptcy_filings(lookback_days=1)
    # Never walks more than the cap, no matter how many pages the API offers.
    assert calls["n"] == bk.COURTLISTENER_MAX_PAGES
    assert len(rows) == bk.COURTLISTENER_MAX_PAGES * 100


def test_retry_delay_cap_is_passed_through():
    seen = {}

    def fake_get(url, params=None, headers=None, timeout=None, max_retry_delay=None):
        seen["cap"] = max_retry_delay
        return _FakeResp({"results": [], "next": None})

    with patch.object(bk, "requests_get_with_retry", side_effect=fake_get):
        bk.fetch_bankruptcy_filings(lookback_days=1)
    assert seen["cap"] == bk.COURTLISTENER_MAX_RETRY_DELAY_SECONDS


def test_tampa_filter_keeps_only_8_prefix_bk():
    dockets = [
        {"docket_number": "8:26-bk-01234", "federal_dn_case_type": "bk", "case_name": "In re: Keep Me"},
        {"docket_number": "6:26-bk-09999", "federal_dn_case_type": "bk", "case_name": "In re: Orlando Div"},
        {"docket_number": "8:26-cv-00001", "federal_dn_case_type": "cv", "case_name": "Civil Case"},
    ]
    out = bk.filter_tampa_bankruptcies(dockets)
    assert len(out) == 1
    assert out[0]["Docket Number"] == "8:26-bk-01234"
    assert out[0]["Lead Name"] == "Keep Me"
