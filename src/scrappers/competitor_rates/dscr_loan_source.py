"""Task 4.8 — DSCR Loan Source rate-sheet adapter.

WordPress article. States leverage as a *down payment* ("minimum 20% down")
rather than an LTV, plus a separate cash-out "loan to value" figure. Max LTV is
the most aggressive of (100 - min_down) and any stated loan-to-value. No
advertised headline rate (only "1-2% higher than conventional").
"""
from __future__ import annotations

import logging
import re

from src.scrappers.competitor_rates.base import (
    RATE_RE,
    fetch_html,
    ltvs_in,
    min_dscr_from,
    min_fico_from,
    prepay_from,
    soup_text,
)
from src.services.competitor_benchmark import CompetitorRow

logger = logging.getLogger(__name__)

SOURCE_URL = "https://dscrloansource.com/what-investors-should-know-about-dscr-loans-in-tampa/"

_DOWN_RE = re.compile(r"(\d{1,2})\s*%\s*down", re.I)


def parse_dscr_loan_source(html: str) -> CompetitorRow:
    text = soup_text(html)
    candidates = [100 - int(d) for d in _DOWN_RE.findall(text)] + ltvs_in(text)
    rate = RATE_RE.search(text)
    return CompetitorRow(
        lender_name="DSCR Loan Source",
        product="dscr",
        region="tampa",
        rate_low=float(rate.group(1)) if rate else None,
        max_ltv=float(max(candidates)) if candidates else None,
        min_fico=min_fico_from(text),
        min_dscr=min_dscr_from(text),
        prepay=prepay_from(text),
    )


def fetch_dscr_loan_source() -> CompetitorRow:
    # ponytail: untested live path; parse_dscr_loan_source has the fixture test.
    return parse_dscr_loan_source(fetch_html(SOURCE_URL))
