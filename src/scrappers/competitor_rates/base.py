"""Task 4.8 — shared helpers for competitor rate-sheet adapters."""
from __future__ import annotations

import re

from bs4 import BeautifulSoup

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}

# advertised headline rate, e.g. "from 5.99%" / "as low as 6.125%"
RATE_RE = re.compile(r"(?:from|as low as|starting at)\s+(\d{1,2}\.\d{1,3})\s*%", re.I)

# "80% LTV" / "75% loan to value". Digit lookbehind rejects decimal fragments
# like the "250" in "1.250% LTV".
_LTV_RE = re.compile(r"(?<!\d)(\d{2,3})\s*%\s*(?:LTV|loan[\s-]to[\s-]value)", re.I)


def soup_text(html: str) -> str:
    return BeautifulSoup(html, "html.parser").get_text(" ", strip=True)


def rate_low_from(text: str) -> float | None:
    rates = [float(r) for r in RATE_RE.findall(text)]
    return min(rates) if rates else None


_FICO_CTX_RE = re.compile(r"(\d{3})\D{0,14}(?:FICO|credit)", re.I)
_DSCR_RE = re.compile(r"(?:down to|minimum|as low as|min\.?)\s*(\d\.\d{1,2})\s*x", re.I)
_PREPAY_A_RE = re.compile(r"(\d+)\s*-?\s*(?:yr|year)s?\s+prepay", re.I)
_PREPAY_B_RE = re.compile(
    r"(?:prepay\w*|payoff penalt\w*|pre-?payment penalt\w*).{0,40}?(\d+)\s*-?\s*years?",
    re.I,
)


def min_fico_from(text: str) -> int | None:
    vals = [int(v) for v in _FICO_CTX_RE.findall(text) if 580 <= int(v) <= 850]
    return min(vals) if vals else None


def min_dscr_from(text: str) -> float | None:
    m = _DSCR_RE.search(text)
    return float(m.group(1)) if m else None


def prepay_from(text: str) -> str | None:
    m = _PREPAY_A_RE.search(text) or _PREPAY_B_RE.search(text)
    return f"{m.group(1)}yr" if m else None


def ltvs_in(text: str) -> list[int]:
    return [int(v) for v in _LTV_RE.findall(text) if int(v) <= 100]


def max_ltv_from(text: str) -> float | None:
    vals = ltvs_in(text)
    return float(max(vals)) if vals else None


def fetch_html(url: str) -> str:
    from src.utils.http_helpers import requests_get_with_retry

    return requests_get_with_retry(url, headers=BROWSER_HEADERS).text
