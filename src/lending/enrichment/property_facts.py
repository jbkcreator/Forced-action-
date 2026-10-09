"""Forced Action facts for the card: link the lead's address to a property, then read what we hold.

Matching reuses ``BaseLoader.find_property_cascade`` with the parcel and owner stages unused (address
only), accepting only the auto-match tier (``config.matching`` 0.92): a wrong property would put another
person's Sunbiz and deed facts on a borrower's card. Coverage is Hillsborough and Pinellas; anything
else, or no match, returns no facts and the card says so. The card never invents a fact.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_enrichment import (
    COVERED_COUNTIES,
    COVERED_STATE,
    DEEDS_SHOWN,
    OFFICERS_SHOWN,
    PERMITS_SHOWN,
)
from config.matching import for_county
from src.loaders.base import BaseLoader

logger = logging.getLogger(__name__)


class _AddressMatchLoader(BaseLoader):
    """BaseLoader used only for its address matching cascade; never loads data."""

    def load_from_dataframe(self, df: pd.DataFrame, skip_duplicates: bool = True):
        raise NotImplementedError


@dataclass(frozen=True)
class PropertyMatch:
    property_id: int
    confidence: int
    county_id: str


@dataclass(frozen=True)
class PropertyFacts:
    sunbiz_standing: Optional[str] = None  # ACTIVE | INACTIVE | DISSOLVED, only when matched on Sunbiz
    officers: list[dict] = field(default_factory=list)
    deeds: list[dict] = field(default_factory=list)
    deed_count: int = 0
    permits: list[dict] = field(default_factory=list)
    permit_count: int = 0

    def to_json(self) -> dict:
        return {
            "sunbiz_standing": self.sunbiz_standing, "officers": self.officers, "deeds": self.deeds,
            "deed_count": self.deed_count, "permits": self.permits, "permit_count": self.permit_count,
        }


def match_property(
    db: Session, address: Optional[str], *, state: Optional[str], city: Optional[str] = None, zip_code: Optional[str] = None,
) -> Optional[PropertyMatch]:
    """The covered-county property for an address, or None (outside coverage, no match, or a tie)."""
    if not (address and address.strip()) or (state or COVERED_STATE).upper() != COVERED_STATE:
        return None
    best: Optional[PropertyMatch] = None
    tied = False
    for county in COVERED_COUNTIES:
        threshold = round(for_county(county).auto_match * 100)
        try:
            prop, _method, confidence = _AddressMatchLoader(db, county_id=county).find_property_cascade(
                address=address, city=city, zip_code=zip_code, addr_threshold=threshold,
            )
        except Exception as exc:  # class only: the address is borrower data
            logger.error("[enrichment] address match failed county=%s: %s", county, type(exc).__name__)
            continue
        if prop is None or confidence is None or confidence < threshold:
            continue
        candidate = PropertyMatch(prop.id, int(confidence), county)
        if best is None or candidate.confidence > best.confidence:
            best, tied = candidate, False
        elif candidate.confidence == best.confidence and candidate.property_id != best.property_id:
            tied = True
    return None if tied else best


_FACTS_SQL = text("""
SELECT o.entity_status, o.sunbiz_status, o.managing_members,
       (SELECT count(*) FROM deeds d WHERE d.property_id = :pid) AS deed_count,
       COALESCE((SELECT jsonb_agg(jsonb_build_object('record_date', x.record_date, 'deed_type', x.deed_type,
                                                       'sale_price', x.sale_price) ORDER BY x.record_date DESC NULLS LAST)
                 FROM (SELECT record_date, deed_type, sale_price FROM deeds WHERE property_id = :pid
                       ORDER BY record_date DESC NULLS LAST LIMIT :deeds) x), '[]'::jsonb) AS deeds,
       (SELECT count(*) FROM building_permits b WHERE b.property_id = :pid AND NOT b.is_enforcement_permit) AS permit_count,
       COALESCE((SELECT jsonb_agg(jsonb_build_object('permit_type', y.permit_type, 'issue_date', y.issue_date,
                                                       'status', y.status, 'job_value', y.job_value)
                                  ORDER BY y.issue_date DESC NULLS LAST)
                 FROM (SELECT permit_type, issue_date, status, job_value FROM building_permits
                       WHERE property_id = :pid AND NOT is_enforcement_permit
                       ORDER BY issue_date DESC NULLS LAST LIMIT :permits) y), '[]'::jsonb) AS permits
FROM properties p
LEFT JOIN owners o ON o.property_id = p.id
WHERE p.id = :pid
""")


def _officers(raw: Any) -> list[dict]:
    out: list[dict] = []
    for member in raw if isinstance(raw, list) else []:
        if isinstance(member, dict) and member.get("name"):
            out.append({"name": str(member["name"]), "title": member.get("title") or member.get("role")})
        elif isinstance(member, str) and member.strip():
            out.append({"name": member.strip(), "title": None})
    return out[:OFFICERS_SHOWN]


def load_property_facts(db: Session, property_id: int) -> Optional[PropertyFacts]:
    """One round trip. None when the property row is gone."""
    row = db.execute(_FACTS_SQL, {"pid": property_id, "deeds": DEEDS_SHOWN, "permits": PERMITS_SHOWN}).mappings().first()
    if row is None:
        return None
    sunbiz_matched = row["sunbiz_status"] == "matched" and bool(row["entity_status"])
    return PropertyFacts(
        sunbiz_standing=row["entity_status"] if sunbiz_matched else None,
        officers=_officers(row["managing_members"]) if sunbiz_matched else [],
        deeds=list(row["deeds"] or []),
        deed_count=int(row["deed_count"] or 0),
        permits=list(row["permits"] or []),
        permit_count=int(row["permit_count"] or 0),
    )
