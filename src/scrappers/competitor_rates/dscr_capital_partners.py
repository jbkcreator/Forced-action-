"""Task 4.8 — DSCR Capital Partners rate-sheet adapter.

The only built target that publishes a real rate matrix (FICO x LTV x rate),
refreshed weekly. Advertises a "from X%" best rate plus LTV bands in HTML
tables. Static page; plain fetch with a browser UA.
"""
from __future__ import annotations

import logging

from src.scrappers.competitor_rates.base import (
    RATE_RE,
    fetch_html,
    max_ltv_from,
    min_fico_from,
    prepay_from,
    soup_text,
)
from src.services.competitor_benchmark import CompetitorRow

logger = logging.getLogger(__name__)

SOURCE_URL = "https://dscrcapitalpartners.com/dscr-loan-rates/"


def parse_dscr_capital_partners(html: str) -> CompetitorRow:
    text = soup_text(html)
    rate = RATE_RE.search(text)
    return CompetitorRow(
        lender_name="DSCR Capital Partners",
        product="dscr",
        region=None,   # national rate sheet → FL-statewide
        rate_low=float(rate.group(1)) if rate else None,
        max_ltv=max_ltv_from(text),
        min_fico=min_fico_from(text),
        prepay=prepay_from(text),
    )


def fetch_dscr_capital_partners() -> CompetitorRow:
    # ponytail: untested live path; parse_dscr_capital_partners has the fixture test.
    return parse_dscr_capital_partners(fetch_html(SOURCE_URL))
