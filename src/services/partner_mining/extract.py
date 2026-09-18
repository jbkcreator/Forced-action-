"""
Counterparty extraction from deed rows (SPEC Stage A).

extract_lender  — mortgage / deed-of-trust rows: grantee = mortgagee (lender).
find_wholesaler_candidates — detect quick re-conveyance of the same parcel
                             within the configured window (default 120 days).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Optional, Protocol, Sequence


_MORTGAGE_KEYWORDS = {"mortgage", "deed of trust"}


class _DeedLike(Protocol):
    property_id: int
    instrument_number: str
    grantee: Optional[str]
    grantor: Optional[str]
    deed_type: Optional[str]
    doc_type: Optional[str]
    mortgage_amount: Optional[float]
    record_date: Optional[date]


def _is_mortgage_row(deed: _DeedLike) -> bool:
    for field in (deed.deed_type, deed.doc_type):
        if field and any(kw in field.lower() for kw in _MORTGAGE_KEYWORDS):
            return True
    return False


def extract_lender(deed: _DeedLike) -> Optional[str]:
    """
    Return the lender name from a mortgage/deed-of-trust row.
    On these rows the grantee IS the mortgagee (lender).
    Returns None for non-mortgage rows or when grantee is absent.
    """
    if not _is_mortgage_row(deed):
        return None
    if not deed.mortgage_amount:
        return None
    return deed.grantee or None


def find_wholesaler_candidates(
    deeds: Sequence[_DeedLike],
    window_days: int = 120,
) -> list[str]:
    """
    Identify parties that buy a parcel and re-convey it within window_days.
    Returns a list of candidate names (grantee text), deduplicated.

    Detection logic:
      - Group deeds by property_id.
      - Per property, find any name that appears as grantee on one deed
        and as grantor on a later deed of the SAME property, where the gap
        between record dates is ≤ window_days.
    """
    # property_id → list of deeds with a record_date
    by_parcel: dict[int, list[_DeedLike]] = defaultdict(list)
    for deed in deeds:
        if deed.record_date:
            by_parcel[deed.property_id].append(deed)

    candidates: set[str] = set()

    for parcel_deeds in by_parcel.values():
        # Sort chronologically within each parcel.
        parcel_deeds.sort(key=lambda d: d.record_date)  # type: ignore[arg-type]

        # Map name → earliest date that name appeared as grantee on this parcel.
        first_buy: dict[str, date] = {}
        for deed in parcel_deeds:
            if deed.grantee:
                name = deed.grantee.strip().upper()
                if name not in first_buy:
                    first_buy[name] = deed.record_date  # type: ignore[assignment]

        # Now scan for the same name appearing as grantor within the window.
        for deed in parcel_deeds:
            if not deed.grantor:
                continue
            name = deed.grantor.strip().upper()
            buy_date = first_buy.get(name)
            if buy_date is None:
                continue
            sell_date: date = deed.record_date  # type: ignore[assignment]
            if buy_date >= sell_date:
                continue
            delta = (sell_date - buy_date).days
            if delta <= window_days:
                candidates.add(name)

    return list(candidates)
