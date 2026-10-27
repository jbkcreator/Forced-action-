"""Deal leverage, shared by the lender-fit evaluator (T-05) and the R-1 router (Chunk 1 Addendum 1.2.4).

LTV = loan / ARV and LTC = loan / (purchase price + rehab budget), compared with a lender's caps (fractions,
e.g. 0.85). A missing or zero figure gives None: that check waits for the call (PENDING_CALL_FACTS).
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional, Union

Amount = Union[int, Decimal]


def ltv(loan: Amount, arv: Optional[Amount]) -> Optional[Decimal]:
    if not arv or arv <= 0:
        return None
    return Decimal(loan) / Decimal(arv)


def ltc(loan: Amount, purchase: Optional[Amount], rehab: Optional[Amount]) -> Optional[Decimal]:
    if purchase is None or rehab is None or purchase + rehab <= 0:
        return None
    return Decimal(loan) / Decimal(purchase + rehab)


def over_cap(ratio: Optional[Decimal], cap: Optional[Union[float, Decimal]]) -> bool:
    return ratio is not None and cap is not None and ratio > Decimal(str(cap))


def requested_vs_max(ratio: Decimal, cap: Union[float, Decimal]) -> str:
    """The decline wording Josh reads in Slack: ``Requested: 100.0%, Max: 85%``."""
    return f"Requested: {ratio:.1%}, Max: {Decimal(str(cap)):.0%}"
