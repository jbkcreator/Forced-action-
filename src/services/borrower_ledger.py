"""
Borrower ledger service — write helpers and read queries for borrower_ledger_events.

The ledger is an append-only event timeline keyed by buyer_entity_id (→ buyer_entities).
Every event traces back to a raw source record via (source_table, source_id).

Public API:
    record_event()       — upsert-safe single event insert (skips on conflict)
    get_timeline()       — ordered event list for one borrower
    get_summary()        — aggregate stats for one borrower (consumed by WP-6 / WP-9)
    get_recent_events()  — events of a specific type across all borrowers since a cutoff
"""
from __future__ import annotations

import json
import logging
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Write
# --------------------------------------------------------------------------- #

def record_event(
    session: Session,
    *,
    buyer_entity_id: int,
    event_type: str,
    event_date: date,
    source_table: str,
    source_id: int,
    property_id: Optional[int] = None,
    summary: Optional[str] = None,
    amount: Optional[Decimal] = None,
    meta: Optional[dict] = None,
) -> bool:
    """
    Insert one borrower ledger event. Skips silently when the same source,
    event type, and borrower identity already exists, so ingestion is safe to
    re-run while one deed can represent both a buyer acquisition and seller sale.

    Returns True if a row was inserted, False if it already existed.
    Does not commit — caller controls the transaction boundary.
    """
    result = session.execute(
        text("""
            INSERT INTO borrower_ledger_events
                (buyer_entity_id, event_type, event_date, property_id,
                 source_table, source_id, summary, amount, meta)
            VALUES
                (:buyer_entity_id, :event_type, :event_date, :property_id,
                 :source_table, :source_id, :summary, :amount, :meta)
            ON CONFLICT (source_table, source_id, event_type, buyer_entity_id) DO NOTHING
        """),
        {
            "buyer_entity_id": buyer_entity_id,
            "event_type": event_type,
            "event_date": event_date,
            "property_id": property_id,
            "source_table": source_table,
            "source_id": source_id,
            "summary": summary,
            "amount": amount,
            "meta": json.dumps(meta) if meta is not None else None,
        },
    )
    return result.rowcount == 1


# --------------------------------------------------------------------------- #
# Read
# --------------------------------------------------------------------------- #

def get_timeline(
    session: Session,
    buyer_entity_id: int,
    *,
    limit: int = 200,
    event_types: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    """
    Return the ordered event timeline for one borrower, newest first.
    Optionally filtered to specific event_types. Used by WP-6 triggers and
    the WP-9 dial list to build the talking-points context block.
    """
    type_filter = ""
    params: dict[str, Any] = {"entity_id": buyer_entity_id, "limit": limit}

    if event_types:
        type_filter = "AND ble.event_type = ANY(:event_types)"
        params["event_types"] = event_types

    rows = session.execute(
        text(f"""
            SELECT
                ble.id,
                ble.event_type,
                ble.event_date,
                p.address  AS property_address,
                p.county_id,
                ble.summary,
                ble.amount,
                ble.meta
            FROM borrower_ledger_events ble
            LEFT JOIN properties p ON p.id = ble.property_id
            WHERE ble.buyer_entity_id = :entity_id
              {type_filter}
            ORDER BY ble.event_date DESC, ble.id DESC
            LIMIT :limit
        """),
        params,
    ).mappings().all()

    return [dict(r) for r in rows]


def get_summary(session: Session, buyer_entity_id: int) -> dict[str, Any]:
    """
    Aggregate stats for one borrower — event counts by type, earliest and
    latest event dates, total deal volume, active property count.
    Consumed by WP-6 (to decide which trigger applies) and WP-9 (to build
    the opportunity score and talking points).
    """
    row = session.execute(
        text("""
            SELECT
                COUNT(*)                                                   AS total_events,
                MIN(event_date)                                            AS first_event_date,
                MAX(event_date)                                            AS last_event_date,
                COUNT(*) FILTER (WHERE event_type = 'deed_acquisition')   AS acquisitions,
                COUNT(*) FILTER (WHERE event_type = 'deed_sale')          AS sales,
                COUNT(*) FILTER (WHERE event_type = 'foreclosure_filed')  AS foreclosures,
                COUNT(*) FILTER (WHERE event_type = 'permit_filed')       AS permits,
                COUNT(*) FILTER (WHERE event_type = 'lien_filed')         AS liens,
                COUNT(*) FILTER (WHERE event_type = 'opportunity_opened') AS opportunities,
                COALESCE(SUM(amount) FILTER (
                    WHERE event_type = 'deed_acquisition' AND amount >= 1000
                ), 0)                                                      AS total_acquisition_volume,
                COUNT(DISTINCT property_id) FILTER (
                    WHERE property_id IS NOT NULL
                )                                                          AS distinct_properties
            FROM borrower_ledger_events
            WHERE buyer_entity_id = :entity_id
        """),
        {"entity_id": buyer_entity_id},
    ).mappings().one()

    return dict(row)


def get_recent_events(
    session: Session,
    event_types: list[str],
    since: date,
    *,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """
    Return events of the given type(s) across ALL borrowers since `since`.
    Used by WP-6 monitors to find new deeds, permits, or maturity signals
    added since the last sweep without scanning the full table.
    """
    rows = session.execute(
        text("""
            SELECT
                ble.id,
                ble.buyer_entity_id,
                be.canonical_name,
                ble.event_type,
                ble.event_date,
                ble.property_id,
                p.address  AS property_address,
                p.county_id,
                ble.summary,
                ble.amount,
                ble.meta
            FROM borrower_ledger_events ble
            JOIN buyer_entities be ON be.id = ble.buyer_entity_id
            LEFT JOIN properties p  ON p.id = ble.property_id
            WHERE ble.event_type = ANY(:event_types)
              AND ble.event_date  >= :since
            ORDER BY ble.event_date DESC, ble.id DESC
            LIMIT :limit
        """),
        {"event_types": event_types, "since": since, "limit": limit},
    ).mappings().all()

    return [dict(r) for r in rows]
