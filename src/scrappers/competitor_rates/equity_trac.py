"""Task 4.8 — Equity Trac adapter (own site, Tampa Bay private fund).

Hyper-local Tampa Bay hard-money fund. The page mixes a 30-yr rental teaser
("Starting at 4.95%") with the hard-money BRIDGE products ("8.25% 12 Month",
"8.75% 24 Month Bridge"). For the private/hard-money benchmark we use the
bridge floor and ignore the rental teaser (filter rates > 6%). Static page.
"""
from __future__ import annotations

import logging
import re

from src.scrappers.competitor_rates.base import (
    fetch_html,
    max_ltv_from,
    min_fico_from,
    soup_text,
)
from src.services.competitor_benchmark import CompetitorRow

logger = logging.getLogger(__name__)

SOURCE_URL = "https://www.equitytrac.com/loans"

_RATE_RE = re.compile(r"Starting at\s*(\d+\.\d+)\s*%", re.I)
_TERM_RE = re.compile(r"(\d+)\s*Month", re.I)
_RENTAL_FLOOR = 6.0   # rates at/below this are 30-yr rental teasers, not hard money


def parse_equity_trac(html: str) -> CompetitorRow:
    text = soup_text(html)
    bridge = [float(r) for r in _RATE_RE.findall(text) if float(r) > _RENTAL_FLOOR]
    terms = [int(t) for t in _TERM_RE.findall(text)]
    return CompetitorRow(
        lender_name="Equity Trac",
        product="private",
        region="tampa",
        rate_low=min(bridge) if bridge else None,
        max_ltv=max_ltv_from(text),
        min_fico=min_fico_from(text),
        term_months=min(terms) if terms else None,
    )


def fetch_equity_trac() -> CompetitorRow:
    # ponytail: untested live path; parse_equity_trac has the fixture test.
    return parse_equity_trac(fetch_html(SOURCE_URL))
