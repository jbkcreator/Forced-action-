"""
Cross-trade exclusivity service for Lead Pack and Bundle deliveries.

Database-backed exclusivity is the authoritative source for lead locks
(replaces Redis `lead_hold`). A property sold to one Trade is locked from all
other Trades until `exclusive_until`.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def acquire_zip_lock(db: Session, zip_code: str, county_id: str) -> None:
    """
    Acquire a transaction-scoped PostgreSQL advisory lock for the zip:county key.

    Serializes concurrent deliveries that select-then-lock the same leads.
    Released automatically on commit/rollback. MUST be called BEFORE the lead
    selection query, not after.
    """
    key = f"{zip_code}:{county_id}"
    db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": key})


def get_exclusive_property_ids(
    db: Session,
    county_id: str,
    now: datetime,
    zip_code: Optional[str] = None,
    exclude_trade: Optional[str] = None,
) -> set[int]:
    """
    Return property_ids under active exclusivity for a county.

    - zip_code given      -> scope to that ZIP (delivery / single-ZIP feed).
    - zip_code None       -> county-wide (proof feeds, multi-ZIP subscriber feed).
    - exclude_trade given -> only ids sold to OTHER trades (feed gating; the
                             buyer keeps visibility of their own leads).

    Active means exclusive_until > now.
    """
    clauses = ["county_id = :county_id", "exclusive_until > :now"]
    params: dict = {"county_id": county_id, "now": now}
    if zip_code is not None:
        clauses.append("zip_code = :zip_code")
        params["zip_code"] = zip_code
    if exclude_trade is not None:
        clauses.append("sold_to_trade != :exclude_trade")
        params["exclude_trade"] = exclude_trade

    sql = "SELECT property_id FROM lead_exclusivity WHERE " + " AND ".join(clauses)
    result = db.execute(text(sql), params)
    return {row[0] for row in result.fetchall()}


def record_exclusivity(
    db: Session,
    property_ids: list[int],
    zip_code: str,
    county_id: str,
    trade: str,
    source: str,
    source_id: int,
    exclusive_until: datetime,
) -> None:
    """
    Upsert exclusivity rows for a delivery. Idempotent per (property_id, source):
    a re-sale of a property whose prior window already expired refreshes the row
    instead of raising on the unique constraint.
    """
    now = datetime.now(timezone.utc)
    for prop_id in property_ids:
        db.execute(
            text("""
                INSERT INTO lead_exclusivity (
                    property_id, zip_code, county_id, sold_to_trade,
                    source, source_id, exclusive_until, created_at
                ) VALUES (
                    :property_id, :zip_code, :county_id, :sold_to_trade,
                    :source, :source_id, :exclusive_until, :created_at
                )
                ON CONFLICT (property_id, source) DO UPDATE SET
                    zip_code        = EXCLUDED.zip_code,
                    county_id       = EXCLUDED.county_id,
                    sold_to_trade   = EXCLUDED.sold_to_trade,
                    source_id       = EXCLUDED.source_id,
                    exclusive_until = EXCLUDED.exclusive_until,
                    created_at      = EXCLUDED.created_at
            """),
            {
                "property_id": prop_id,
                "zip_code": zip_code,
                "county_id": county_id,
                "sold_to_trade": trade,
                "source": source,
                "source_id": source_id,
                "exclusive_until": exclusive_until,
                "created_at": now,
            },
        )


def clear_exclusivity_for_purchase(db: Session, purchase_id: int, source: str) -> int:
    """Remove exclusivity rows for a refunded purchase. Returns rows deleted."""
    result = db.execute(
        text("""
            DELETE FROM lead_exclusivity
            WHERE source = :source AND source_id = :source_id
        """),
        {"source": source, "source_id": purchase_id},
    )
    return result.rowcount


def purge_expired(db: Session, now: Optional[datetime] = None) -> int:
    """Delete rows whose exclusivity expired. Table-size hygiene only — reads
    already filter on exclusive_until > now, so this is not correctness."""
    now = now or datetime.now(timezone.utc)
    result = db.execute(
        text("DELETE FROM lead_exclusivity WHERE exclusive_until < :now"),
        {"now": now},
    )
    return result.rowcount
