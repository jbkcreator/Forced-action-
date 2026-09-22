"""
Tests for extract.extract_lender and extract.find_wholesaler_candidates.
Seam: public functions, take Deed-like objects, return Optional[str] / list.
"""
from dataclasses import dataclass
from datetime import date
from typing import Optional

import pytest

from src.services.partner_mining.extract import extract_lender, find_wholesaler_candidates


@dataclass
class _Deed:
    grantee: Optional[str] = None
    grantor: Optional[str] = None
    deed_type: Optional[str] = None
    doc_type: Optional[str] = None
    mortgage_amount: Optional[float] = None
    sale_price: Optional[float] = None
    sale_qualified: Optional[bool] = None
    record_date: Optional[date] = None
    instrument_number: str = "INST-001"
    property_id: int = 1


# ---------------------------------------------------------------------------
# extract_lender
# ---------------------------------------------------------------------------

def test_mortgage_row_returns_grantee_as_lender():
    deed = _Deed(
        grantee="WELLS FARGO BANK NA",
        deed_type="MORTGAGE",
        mortgage_amount=160_000,
    )
    assert extract_lender(deed) == "WELLS FARGO BANK NA"


def test_deed_of_trust_row_returns_grantee_as_lender():
    deed = _Deed(
        grantee="FIRST NATIONAL BANK",
        deed_type="DEED OF TRUST",
        mortgage_amount=200_000,
    )
    assert extract_lender(deed) == "FIRST NATIONAL BANK"


def test_non_mortgage_row_returns_none():
    deed = _Deed(
        grantee="SUNCOAST LLC",
        deed_type="WARRANTY DEED",
        mortgage_amount=None,
    )
    assert extract_lender(deed) is None


def test_mortgage_amount_present_but_no_grantee_returns_none():
    deed = _Deed(grantee=None, deed_type="MORTGAGE", mortgage_amount=100_000)
    assert extract_lender(deed) is None


def test_doc_type_mortgage_fallback():
    """Some rows use doc_type instead of deed_type for the mortgage signal."""
    deed = _Deed(
        grantee="ROCKET MORTGAGE LLC",
        deed_type=None,
        doc_type="MORTGAGE",
        mortgage_amount=250_000,
    )
    assert extract_lender(deed) == "ROCKET MORTGAGE LLC"


# ---------------------------------------------------------------------------
# find_wholesaler_candidates
# ---------------------------------------------------------------------------

def test_quick_reconveyance_within_window_flagged():
    """
    Party A buys parcel 1 on day 0 (as grantee).
    Party A sells parcel 1 within 120 days (as grantor).
    → Party A is a wholesaler candidate.
    """
    buy = _Deed(
        property_id=1,
        instrument_number="BUY-001",
        grantee="FAST FLIP LLC",
        grantor="ORIGINAL OWNER",
        record_date=date(2024, 1, 1),
        sale_price=100_000,
        sale_qualified=True,
    )
    sell = _Deed(
        property_id=1,
        instrument_number="SELL-001",
        grantee="END BUYER LLC",
        grantor="FAST FLIP LLC",
        record_date=date(2024, 2, 15),   # 45 days later
        sale_price=120_000,
        sale_qualified=True,
    )
    candidates = find_wholesaler_candidates([buy, sell])
    assert "FAST FLIP LLC" in candidates


def test_slow_hold_not_flagged():
    buy = _Deed(
        property_id=1,
        instrument_number="BUY-002",
        grantee="LONGTERM HOLD LLC",
        grantor="ORIGINAL OWNER",
        record_date=date(2023, 1, 1),
        sale_price=100_000,
        sale_qualified=True,
    )
    sell = _Deed(
        property_id=1,
        instrument_number="SELL-002",
        grantee="ANOTHER BUYER LLC",
        grantor="LONGTERM HOLD LLC",
        record_date=date(2024, 6, 1),   # > 120 days
        sale_price=130_000,
        sale_qualified=True,
    )
    candidates = find_wholesaler_candidates([buy, sell])
    assert "LONGTERM HOLD LLC" not in candidates


def test_no_re_conveyance_returns_empty():
    buy = _Deed(
        property_id=2,
        instrument_number="BUY-003",
        grantee="ONE BUYER LLC",
        grantor="SELLER",
        record_date=date(2024, 3, 1),
        sale_price=80_000,
        sale_qualified=True,
    )
    candidates = find_wholesaler_candidates([buy])
    assert candidates == []


def test_reconveyance_different_parcel_not_flagged():
    """Re-conveyance on a different property_id must not be mistaken for wholesale."""
    buy = _Deed(
        property_id=10,
        instrument_number="BUY-004",
        grantee="FLIP LLC",
        grantor="SELLER A",
        record_date=date(2024, 1, 1),
        sale_price=100_000,
        sale_qualified=True,
    )
    sell = _Deed(
        property_id=99,   # different parcel
        instrument_number="SELL-004",
        grantee="BUYER B LLC",
        grantor="FLIP LLC",
        record_date=date(2024, 2, 1),
        sale_price=120_000,
        sale_qualified=True,
    )
    candidates = find_wholesaler_candidates([buy, sell])
    assert "FLIP LLC" not in candidates
