"""
Resolve-or-quarantine helper for outcome connectors that read a genuinely new
raw file (no property_id already on the source row) — currently only the
Florida DOR statewide sales connector needs this.

Foreclosure, tax-deed, and property-appraiser outcome connectors read tables
that were already matched to a Property row by the existing loaders at
ingestion time (Foreclosure.property_id, TaxDeedAuction.property_id, etc.) —
they should look up that existing property_id directly and never call this
module. Calling this on already-matched data would be redundant re-matching
work and risks landing on a *worse* match than the one already on record.

This is a thin composition over BaseLoader's existing, already-generic
matching cascade and review-queue primitives (src/loaders/base.py) — it does
not reimplement parcel/address/owner matching.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.loaders.base import BaseLoader


class _GenericMatcher(BaseLoader):
    """
    Concrete BaseLoader subclass that exists only to reuse find_property_cascade /
    _classify_match / quarantine_unmatched outside a CSV-load context.
    Not a general-purpose loader — load_from_dataframe is intentionally unsupported.
    """

    def load_from_dataframe(self, df: pd.DataFrame, skip_duplicates: bool = True):
        raise NotImplementedError("_GenericMatcher is matching-only; it does not load DataFrames")


@dataclass
class ResolveResult:
    property_id: Optional[int]
    status: str                          # 'matched' | 'pending_review' | 'unmatched'
    match_method: Optional[str]
    match_confidence: Optional[float]    # 0.0-1.0


def resolve_or_quarantine(
    session: Session,
    county_id: str,
    source_type: str,
    raw_row: dict,
    *,
    parcel_id: Optional[str] = None,
    address: Optional[str] = None,
    owner_name: Optional[str] = None,
    zip_code: Optional[str] = None,
    city: Optional[str] = None,
    legal_desc: Optional[str] = None,
    instrument_number: Optional[str] = None,
    grantor: Optional[str] = None,
    address_string: Optional[str] = None,
) -> ResolveResult:
    """
    Run the standard parcel-matching cascade for a raw record with no known
    property_id. Matches land on the returned property_id; anything else is
    queued in UnmatchedRecord (match_status 'unmatched' or 'pending_review')
    via quarantine_unmatched — never dropped.
    """
    matcher = _GenericMatcher(session, county_id=county_id)

    prop, match_method, score = matcher.find_property_cascade(
        parcel_id=parcel_id,
        address=address,
        owner_name=owner_name,
        zip_code=zip_code,
        city=city,
        legal_desc=legal_desc,
    )
    status = matcher._classify_match(score or 0, match_method)
    confidence = (score / 100.0) if score is not None else None

    if status != "matched":
        matcher.quarantine_unmatched(
            source_type=source_type,
            raw_row=raw_row,
            county_id=county_id,
            instrument_number=instrument_number,
            grantor=grantor,
            address_string=address_string,
            match_status=status,
            match_confidence=confidence,
            candidate_property_id=(prop.id if prop else None),
            match_method=match_method,
        )
        return ResolveResult(
            property_id=None,
            status=status,
            match_method=match_method,
            match_confidence=confidence,
        )

    return ResolveResult(
        property_id=prop.id,
        status="matched",
        match_method=match_method,
        match_confidence=confidence,
    )
