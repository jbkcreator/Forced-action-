"""Pre-fill assembly — WP-7 WI-2.

Given a property (or a raw address when the tracked link is generic),
assemble the public-record facts a self-serve borrower sees on screen 1.

Hard allowlist by construction (§1.4 of the plan — owners.estimated_income /
credit_score_tier sit right next to the fields this reads): the payload is
built field-by-field from an explicit list, never by serializing a row.
ARV is never included — client-confirmed borrower-facing scope (plan §7 Q3).

One CTE-backed query, not one query per section. Every field is optional and
carries {value, source, as_of}; a missing field is absent from the payload,
never a null or a guess.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.loaders.base import BaseLoader

logger = logging.getLogger(__name__)

# Confidence floor below which an address match is not shown as "your
# property" — matches BaseLoader's own MATCHED tier (see config/matching.py).
_ADDRESS_MATCH_FLOOR = 75


class _AddressLookupLoader(BaseLoader):
    """Thin BaseLoader subclass that exists only to reuse
    find_property_cascade's address waterfall — WP-7 does not load anything."""

    def load_from_dataframe(self, df, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError("_AddressLookupLoader is read-only")


@dataclass
class PrefillPayload:
    property_id: Optional[int]
    fields: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"property_id": self.property_id, "fields": self.fields}


def _json_safe(value: Any) -> Any:
    """JSONB can't serialize Decimal/date directly via psycopg2's default
    encoder — coerce at the boundary rather than downstream at every call site."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _set(payload: PrefillPayload, key: str, value: Any, source: str, as_of: Optional[date] = None) -> None:
    if value is None or value == "":
        return
    payload.fields[key] = {
        "value": _json_safe(value),
        "source": source,
        "as_of": as_of.isoformat() if as_of else None,
    }


def find_property_by_address(db: Session, address: str, county_id: str = "hillsborough") -> tuple[Optional[int], Optional[int]]:
    """Resolve a raw address typed by a borrower (generic tracked link, or a
    below-confidence-floor per-property link) to a property_id.

    Returns (property_id, confidence) or (None, None) below the match floor —
    callers fall back to manual entry + lead capture (plan §7 Q6).
    """
    loader = _AddressLookupLoader(db, county_id=county_id)
    prop, method, confidence = loader.find_property_cascade(address=address)
    if prop is None or confidence is None or confidence < _ADDRESS_MATCH_FLOOR:
        return None, confidence
    return prop.id, confidence


_PREFILL_QUERY = text(
    """
    SELECT
        p.address, p.city, p.state, p.zip, p.beds, p.baths, p.sq_ft, p.year_built,
        o.owner_name, o.mailing_address,
        f.assessed_value_mkt,
        f.last_sale_price, f.last_sale_date,
        d.grantor AS last_deed_grantor, d.grantee AS last_deed_grantee,
        d.record_date AS last_deed_date, d.sale_price AS last_deed_price
    FROM properties p
    LEFT JOIN owners o ON o.property_id = p.id
    LEFT JOIN financials f ON f.property_id = p.id
    LEFT JOIN LATERAL (
        SELECT grantor, grantee, record_date, sale_price
        FROM deeds
        WHERE property_id = p.id
        ORDER BY record_date DESC
        LIMIT 1
    ) d ON true
    WHERE p.id = :property_id
    """
)

_PERMITS_QUERY = text(
    """
    SELECT permit_type, issue_date, status
    FROM building_permits
    WHERE property_id = :property_id AND is_enforcement_permit = false
    ORDER BY issue_date DESC
    LIMIT 5
    """
)


def assemble_prefill(db: Session, property_id: int) -> PrefillPayload:
    """Build the pre-fill payload for a known property_id.

    Allowlisted fields ONLY. Never includes: estimated_income,
    credit_score_tier, phone_*, email_*, internal ids, parcel_id, CDS scores,
    skip-trace data, or ARV (reserved but never rendered — plan §7 Q3).
    """
    row = db.execute(_PREFILL_QUERY, {"property_id": property_id}).mappings().first()
    payload = PrefillPayload(property_id=property_id)
    if row is None:
        logger.warning("assemble_prefill: property_id=%s not found", property_id)
        return payload

    _set(payload, "address", row["address"], "county_property_appraiser")
    _set(payload, "city", row["city"], "county_property_appraiser")
    _set(payload, "state", row["state"], "county_property_appraiser")
    _set(payload, "zip", row["zip"], "county_property_appraiser")
    _set(payload, "beds", row["beds"], "county_property_appraiser")
    _set(payload, "baths", row["baths"], "county_property_appraiser")
    _set(payload, "sq_ft", row["sq_ft"], "county_property_appraiser")
    _set(payload, "year_built", row["year_built"], "county_property_appraiser")

    _set(payload, "owner_name", row["owner_name"], "county_property_appraiser")
    _set(payload, "owner_mailing_address", row["mailing_address"], "county_property_appraiser")

    _set(payload, "tax_assessed_value", row["assessed_value_mkt"], "county_tax_roll")

    _set(payload, "last_sale_price", row["last_sale_price"], "county_deed_records", )
    _set(payload, "last_sale_date", row["last_sale_date"], "county_deed_records")

    if row["last_deed_date"] is not None:
        _set(
            payload, "last_deed_transfer",
            {
                "grantor": row["last_deed_grantor"],
                "grantee": row["last_deed_grantee"],
                "sale_price": row["last_deed_price"],
            },
            "county_deed_records",
            as_of=row["last_deed_date"],
        )

    permit_rows = db.execute(_PERMITS_QUERY, {"property_id": property_id}).mappings().all()
    if permit_rows:
        _set(
            payload, "open_permits",
            [
                {"type": r["permit_type"], "status": r["status"], "issue_date": r["issue_date"].isoformat() if r["issue_date"] else None}
                for r in permit_rows
            ],
            "county_permit_records",
        )

    # ARV deliberately never added — reserved field, plan §7 Q3.
    return payload
