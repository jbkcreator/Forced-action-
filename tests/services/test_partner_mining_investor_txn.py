"""
Tests for investor_txn.is_investor_transaction.
Seam: public function, takes a Deed-like object, returns bool.
"""
from dataclasses import dataclass, field
from typing import Optional

import pytest

from src.services.partner_mining.investor_txn import is_investor_transaction


@dataclass
class _Deed:
    """Minimal Deed stand-in — only fields the filter reads."""
    grantee: Optional[str] = None
    sale_qualified: Optional[bool] = None
    sale_price: Optional[float] = None
    deed_type: Optional[str] = None
    mortgage_amount: Optional[float] = None


# ---------------------------------------------------------------------------
# entity grantee
# ---------------------------------------------------------------------------

def test_llc_grantee_qualifies_as_entity():
    deed = _Deed(grantee="SUN COAST PROPERTIES LLC", sale_qualified=True, sale_price=150_000)
    assert is_investor_transaction(deed) is True


def test_individual_grantee_does_not_qualify():
    deed = _Deed(grantee="JOHN SMITH", sale_qualified=True, sale_price=150_000)
    assert is_investor_transaction(deed) is False


def test_trust_grantee_qualifies_as_entity():
    deed = _Deed(grantee="SMITH FAMILY TRUST", sale_qualified=True, sale_price=150_000)
    assert is_investor_transaction(deed) is True


def test_corp_grantee_qualifies_as_entity():
    deed = _Deed(grantee="BAYVIEW CAPITAL CORP", sale_qualified=True, sale_price=150_000)
    assert is_investor_transaction(deed) is True


# ---------------------------------------------------------------------------
# purchase-only / arms-length
# ---------------------------------------------------------------------------

def test_unqualified_sale_excluded():
    deed = _Deed(grantee="SUNCOAST LLC", sale_qualified=False, sale_price=150_000)
    assert is_investor_transaction(deed) is False


def test_zero_price_excluded():
    deed = _Deed(grantee="SUNCOAST LLC", sale_qualified=True, sale_price=0)
    assert is_investor_transaction(deed) is False


def test_null_price_excluded():
    deed = _Deed(grantee="SUNCOAST LLC", sale_qualified=True, sale_price=None)
    assert is_investor_transaction(deed) is False


def test_quitclaim_deed_excluded():
    deed = _Deed(grantee="SUNCOAST LLC", sale_qualified=True, sale_price=150_000, deed_type="QUIT CLAIM DEED")
    assert is_investor_transaction(deed) is False


def test_gift_deed_excluded():
    deed = _Deed(grantee="SUNCOAST LLC", sale_qualified=True, sale_price=150_000, deed_type="GIFT DEED")
    assert is_investor_transaction(deed) is False


# ---------------------------------------------------------------------------
# mortgage rows are excluded (grantee = lender, not buyer)
# ---------------------------------------------------------------------------

def test_mortgage_type_row_excluded():
    """On mortgage rows the grantee is the lender — not an investor transaction."""
    deed = _Deed(grantee="WELLS FARGO BANK NA", sale_qualified=True, sale_price=200_000,
                 mortgage_amount=160_000)
    assert is_investor_transaction(deed) is False


# ---------------------------------------------------------------------------
# homestead proxy
# ---------------------------------------------------------------------------

def test_homestead_exempt_excluded():
    deed = _Deed(grantee="SUNCOAST LLC", sale_qualified=True, sale_price=150_000)
    assert is_investor_transaction(deed, homestead_exempt=True) is False
