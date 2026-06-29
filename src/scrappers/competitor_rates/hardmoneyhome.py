"""Task 4.8 — HardMoneyHome adapter (directory → many rows).

The Tampa listing page links ~50 per-lender profile pages, each publishing
structured terms: Available Rates (low-high), Max LTV, Points, Min FICO.
One profile → one CompetitorRow (product=private). Static pages, browser UA.

`parse_hardmoneyhome` also reads the city market-average paragraph as a single
aggregate row (kept for the "market avg" benchmark baseline).
"""
from __future__ import annotations

import logging
import re

from src.scrappers.competitor_rates.base import fetch_html, soup_text
from src.services.competitor_benchmark import CompetitorRow

logger = logging.getLogger(__name__)

LISTING_URL = "https://www.hardmoneyhome.com/hard-money-loans/tampa-fl"
SOURCE_URL = LISTING_URL  # registry display
PROFILE_URL = "https://www.hardmoneyhome.com/lenders/view/{slug}"
REGION = "tampa"

_NAME_RE = re.compile(r"(.+?)\s*-\s*Reviews", re.I)
_RATES_RE = re.compile(r"Available Rates:\s*([\d.]+)%?\s*-\s*([\d.]+)%", re.I)
_LTV_RE = re.compile(r"Max Loan-to-Value \(LTV\):\s*(\d{1,3})%", re.I)
_POINTS_RE = re.compile(r"Points Charged:\s*([\d.]+)%", re.I)
_FICO_RE = re.compile(r"Minimum FICO Score:\s*(\d{3})", re.I)
# city aggregate paragraph
_AVG_RATE_RE = re.compile(r"average\s+(?:around|approximately)?\s*(\d{1,2}\.\d)\s*%", re.I)
_AVG_LTV_RE = re.compile(r"(\d{2,3})\s*%\s+is the average loan[\s-]to[\s-]value", re.I)


def _f(m):
    return float(m.group(1)) if m else None


_STATE_ZIP_RE = re.compile(r",\s*([A-Z]{2})\s+\d{5}")
_ADDR_SUFFIX = {
    "Floor", "Fl", "Suite", "Ste", "Ave", "Avenue", "St", "Street", "Blvd",
    "Dr", "Drive", "Rd", "Road", "Way", "Lane", "Ln", "Pkwy", "Pike", "Ct",
    "Place", "Plaza", "N", "S", "E", "W", "#",
}


def _hq_location(text: str) -> str | None:
    # ponytail: heuristic — walk back from ", ST ZIP" taking trailing alpha
    # words until a street suffix/number. Ceiling: odd multi-word cities.
    m = _STATE_ZIP_RE.search(text)
    if not m:
        return None
    city: list[str] = []
    for w in reversed(text[: m.start()].split()):
        if any(c.isdigit() for c in w) or w.rstrip(".,") in _ADDR_SUFFIX:
            break
        city.insert(0, w.rstrip(".,"))
        if len(city) >= 3:
            break
    return f"{' '.join(city)}, {m.group(1)}" if city else m.group(1)


def profile_slugs(listing_html: str) -> list[str]:
    return sorted(set(re.findall(r"/lenders/view/([a-z0-9.-]+)", listing_html)))


def parse_hmh_profile(html: str) -> CompetitorRow:
    text = soup_text(html)
    name = _NAME_RE.search(text)
    rates = _RATES_RE.search(text)
    fico = _FICO_RE.search(text)
    return CompetitorRow(
        lender_name=name.group(1).strip() if name else "Unknown",
        product="private",
        region=REGION,
        rate_low=float(rates.group(1)) if rates else None,
        rate_high=float(rates.group(2)) if rates else None,
        max_ltv=_f(_LTV_RE.search(text)),
        points=_f(_POINTS_RE.search(text)),
        min_fico=int(fico.group(1)) if fico else None,
        hq_location=_hq_location(text),
    )


def parse_hardmoneyhome(html: str) -> CompetitorRow:
    """City market-average aggregate row (one row, not per-lender)."""
    text = soup_text(html)
    return CompetitorRow(
        lender_name="Tampa Hard Money (market avg)",
        product="private",
        region=REGION,
        rate_low=_f(_AVG_RATE_RE.search(text)),
        max_ltv=_f(_AVG_LTV_RE.search(text)),
    )


def fetch_hardmoneyhome() -> list[CompetitorRow]:
    # ponytail: untested live path; parsers have fixture tests.
    listing = fetch_html(LISTING_URL)
    rows = [parse_hardmoneyhome(listing)]  # market-avg aggregate
    for slug in profile_slugs(listing):
        try:
            rows.append(parse_hmh_profile(fetch_html(PROFILE_URL.format(slug=slug))))
        except Exception as exc:
            logger.warning("hmh profile %s failed: %s", slug, exc)
    return rows
