"""Task 4.8 — Easy Street Capital adapter (own site, FL DSCR specialist).

FL DSCR / short-term-rental specialist. Multi-product page ("starting at 5.75%"
DSCR rental floor, "starting at 8.90%" bridge). rate_low_from takes the lowest
advertised rate = the DSCR floor. Static page, browser UA.
"""
from __future__ import annotations

import logging

from src.scrappers.competitor_rates.base import (
    fetch_html,
    max_ltv_from,
    min_fico_from,
    rate_low_from,
    soup_text,
)
from src.services.competitor_benchmark import CompetitorRow

logger = logging.getLogger(__name__)

SOURCE_URL = "https://easystreetcap.com/dscr-loans-florida/"


def parse_easy_street(html: str) -> CompetitorRow:
    text = soup_text(html)
    return CompetitorRow(
        lender_name="Easy Street Capital",
        product="dscr",
        region="florida",
        rate_low=rate_low_from(text),
        max_ltv=max_ltv_from(text),
        min_fico=min_fico_from(text),
    )


def fetch_easy_street() -> CompetitorRow:
    # ponytail: untested live path; parse_easy_street has the fixture test.
    return parse_easy_street(fetch_html(SOURCE_URL))
