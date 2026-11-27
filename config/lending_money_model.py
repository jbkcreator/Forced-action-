"""R1 1.3 money model: expected collected fee inside 60 days per Josh hour (D3). NOT used for ranking yet.

Every input is None until the client gives it (release1-task/CLIENT-QUESTIONS.md 8-10). No value here is invented:
``expected_fee_per_hour`` returns None while any input it needs is missing, and the morning list ranks by its fixed
tier order meanwhile.

- POINTS: points NDL earns per loan, by (lender group, product); lender group "backflip" when Backflip fits the deal,
  else "other" (client question 8; Backflip-first is already the routing rule, A2 10-09).
- CLOSE_RATE_60D: share of files at a stage that fund within 60 days. Not asked: computed from stage history once
  each stage has MIN_DEALS_PER_STAGE deals (client question 9 withdrawn).
- JOSH_HOURS: relative time per product. Default assumption (client question 10, to confirm): every product takes the
  same time, so per-hour ranking equals fee ranking.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

POINTS: dict[tuple[str, str], Optional[Decimal]] = {
    ("backflip", "flip"): None, ("backflip", "ground_up"): None, ("backflip", "dscr"): None,
    ("other", "flip"): None, ("other", "ground_up"): None, ("other", "dscr"): None,
}
CLOSE_RATE_60D: dict[str, Optional[Decimal]] = {}
MIN_DEALS_PER_STAGE = 20
JOSH_HOURS: dict[str, Decimal] = {"flip": Decimal(1), "ground_up": Decimal(1), "dscr": Decimal(1)}


def expected_fee_per_hour(*, loan_amount: Optional[int], product: str, stage: str, backflip_fits: bool
                          ) -> Optional[Decimal]:
    """loan amount x points x P(fund within 60 days | stage) / Josh hours, or None while any input is unknown."""
    points = POINTS.get(("backflip" if backflip_fits else "other", product))
    close_rate = CLOSE_RATE_60D.get(stage)
    hours = JOSH_HOURS.get(product)
    if loan_amount is None or points is None or close_rate is None or not hours:
        return None
    return Decimal(loan_amount) * points / Decimal(100) * close_rate / hours
