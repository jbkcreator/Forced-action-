"""
Investor-transaction filter (SPEC Stage A).

A deed counts toward a partner's producer rank only when the buyer is a
probable investor — three-part test (spec line 385 / GRILL Q3):
  1. entity grantee  (LLC / Corporate / Trust suffix tokens)
  2. purchase-only arms-length deed  (sale_qualified + positive price; no quitclaim/gift)
  3. not homestead / owner-occupied  (HCPA proxy passed in by caller)

Mortgage rows are excluded automatically: when mortgage_amount is populated the
grantee is the lender (mortgagee), not the investor-buyer.
"""

from __future__ import annotations

from typing import Protocol, Optional


_ENTITY_TOKENS = {"llc", "corp", "corporation", "inc", "incorporated", "trust", "lp", "ltd"}
_EXCLUDED_DEED_TYPES = {"quit claim", "quitclaim", "gift"}


class _DeedLike(Protocol):
    grantee: Optional[str]
    sale_qualified: Optional[bool]
    sale_price: Optional[float]
    deed_type: Optional[str]
    mortgage_amount: Optional[float]


def _is_entity_grantee(grantee: Optional[str]) -> bool:
    if not grantee:
        return False
    tokens = {t.lower().rstrip(".,") for t in grantee.split()}
    return bool(tokens & _ENTITY_TOKENS)


def _is_arms_length_purchase(deed: _DeedLike) -> bool:
    if not deed.sale_qualified:
        return False
    if not deed.sale_price or deed.sale_price <= 0:
        return False
    if deed.deed_type:
        normalized = deed.deed_type.lower()
        if any(excl in normalized for excl in _EXCLUDED_DEED_TYPES):
            return False
    return True


def is_investor_transaction(deed: _DeedLike, homestead_exempt: bool = False) -> bool:
    """
    Return True when a deed represents a probable investor purchase.
    Pass homestead_exempt=True when the linked property carries an HCPA
    homestead exemption or owner-occupancy flag.
    """
    if homestead_exempt:
        return False
    # Mortgage rows: grantee is the lender, not the buyer.
    if deed.mortgage_amount is not None:
        return False
    if not _is_entity_grantee(deed.grantee):
        return False
    return _is_arms_length_purchase(deed)
