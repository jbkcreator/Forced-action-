"""Task 4.8 — competitor benchmark config: adapter registry + flag thresholds."""
from __future__ import annotations

from src.scrappers.competitor_rates.cambridge import (
    SOURCE_URL as CAMBRIDGE_URL,
    fetch_cambridge,
)
from src.scrappers.competitor_rates.dscr_capital_partners import (
    SOURCE_URL as DSCR_CAPITAL_URL,
    fetch_dscr_capital_partners,
)
from src.scrappers.competitor_rates.dscr_loan_source import (
    SOURCE_URL as DSCR_LOAN_SOURCE_URL,
    fetch_dscr_loan_source,
)
from src.scrappers.competitor_rates.hardmoneyhome import (
    SOURCE_URL as HARDMONEYHOME_URL,
    fetch_hardmoneyhome,
)
from src.scrappers.competitor_rates.equity_trac import (
    SOURCE_URL as EQUITY_TRAC_URL,
    fetch_equity_trac,
)

# Each adapter: name (== --target value), live fetch fn, source URL, confidence.
ADAPTERS = [
    {"name": "cambridge", "fetch": fetch_cambridge,
     "source_url": CAMBRIDGE_URL, "confidence": "high"},
    {"name": "dscr_loan_source", "fetch": fetch_dscr_loan_source,
     "source_url": DSCR_LOAN_SOURCE_URL, "confidence": "high"},
    {"name": "dscr_capital_partners", "fetch": fetch_dscr_capital_partners,
     "source_url": DSCR_CAPITAL_URL, "confidence": "high"},
    {"name": "hardmoneyhome", "fetch": fetch_hardmoneyhome,
     "source_url": HARDMONEYHOME_URL, "confidence": "high"},
    {"name": "equity_trac", "fetch": fetch_equity_trac,
     "source_url": EQUITY_TRAC_URL, "confidence": "high"},
]
