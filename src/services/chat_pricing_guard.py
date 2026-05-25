"""
Pricing hallucination guard for Concierge Chat responses.

Scans Claude's output for dollar amounts. Any amount not in the approved
pricing whitelist is replaced with a generic fallback and the violation is logged.
"""

import logging
import re

logger = logging.getLogger(__name__)

# Canonical approved prices from the revenue ladder (USD).
APPROVED_PRICES: frozenset[int] = frozenset({
    19,    # Weekend Bundle
    29,    # ZIP Booster
    39,    # Storm Bundle
    49,    # Wallet low
    89,    # Monthly Reload
    97,    # Data-Only
    99,    # Wallet mid (approx)
    147,   # (midpoint — kept for rounding)
    197,   # Territory Lock / month
    199,   # Wallet high
    299,   # AutoPilot Lite
    497,   # AutoPilot Pro
    1970,  # Annual Lock
    2000,  # Partner
})

# Matches "$NNN" or "$N,NNN" patterns (integer dollar amounts only).
_PRICE_RE = re.compile(r"\$(\d{1,2},?\d{3}|\d+)")

_FALLBACK = "pricing I can confirm"


def check_and_clean(text: str, session_id: str = "") -> str:
    """
    Return text with any unapproved dollar amounts replaced by the fallback phrase.
    Logs each violation.
    """
    def _replace(match: re.Match) -> str:
        raw = match.group(1).replace(",", "")
        try:
            amount = int(raw)
        except ValueError:
            return match.group(0)

        if amount in APPROVED_PRICES:
            return match.group(0)

        logger.warning(
            "chat_pricing_guard: unapproved price $%s in session %s — replaced",
            raw, session_id,
        )
        return _FALLBACK

    return _PRICE_RE.sub(_replace, text)
