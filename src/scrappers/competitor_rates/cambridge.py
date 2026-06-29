"""Task 4.8 — Cambridge Home Loan DSCR rate-sheet adapter.

Static HTML page; no client-side rendering. The page does NOT advertise a
headline rate (only FICO/LTV/DSCR constraints), so rate_low is typically None.
Cambridge returns 403 to a bare requests UA — a browser User-Agent is required.
"""
from __future__ import annotations

import logging

from src.scrappers.competitor_rates.base import fetch_html, max_ltv_from, rate_low_from, soup_text
from src.services.competitor_benchmark import CompetitorRow

logger = logging.getLogger(__name__)

SOURCE_URL = "https://www.cambridgehomeloan.com/dscr-loan-florida/"


def parse_cambridge(html: str) -> CompetitorRow:
    text = soup_text(html)
    return CompetitorRow(
        lender_name="Cambridge Home Loan",
        product="dscr",
        region="florida",
        rate_low=rate_low_from(text),
        max_ltv=max_ltv_from(text),
    )


def fetch_cambridge() -> CompetitorRow:
    # ponytail: untested live path; parse_cambridge has the fixture test.
    return parse_cambridge(fetch_html(SOURCE_URL))
