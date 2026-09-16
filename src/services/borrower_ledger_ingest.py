"""Incrementally materialize source records and transitions into the borrower ledger."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.borrower_ledger import record_event


@dataclass
class IngestStats:
    attempted: int = 0
    inserted: int = 0
    skipped: int = 0


_CANDIDATE_QUERY = text("""
    SELECT bel.buyer_entity_id, 'deed_acquisition' AS event_type,
           d.record_date AS event_date, 'deeds' AS source_table, d.id AS source_id,
           d.property_id, 'Acquired ' || COALESCE(d.instrument_number, '') AS summary,
           d.sale_price AS amount,
           json_build_object('grantee', d.grantee, 'deed_type', d.deed_type)::text AS meta_json
    FROM deeds d
    JOIN buyer_entity_links bel ON bel.source_table = 'deeds' AND bel.source_id = d.id
    WHERE d.record_date IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM borrower_ledger_events ble
          WHERE ble.source_table = 'deeds' AND ble.source_id = d.id
            AND ble.event_type = 'deed_acquisition'
            AND ble.buyer_entity_id = bel.buyer_entity_id
      )

    UNION ALL

    SELECT acquisition.buyer_entity_id, 'deed_sale' AS event_type,
           sale.record_date AS event_date, 'deeds' AS source_table, sale.id AS source_id,
           sale.property_id, 'Sold via ' || COALESCE(sale.instrument_number, '') AS summary,
           sale.sale_price AS amount,
           json_build_object('grantor', sale.grantor, 'grantee', sale.grantee,
                             'deed_type', sale.deed_type)::text AS meta_json
    FROM deeds sale
    JOIN LATERAL (
        SELECT prior_link.buyer_entity_id
        FROM deeds prior_deed
        JOIN buyer_entity_links prior_link
          ON prior_link.source_table = 'deeds' AND prior_link.source_id = prior_deed.id
        WHERE prior_deed.property_id = sale.property_id
          AND prior_deed.record_date < sale.record_date
        ORDER BY prior_deed.record_date DESC, prior_deed.id DESC
        LIMIT 1
    ) acquisition ON TRUE
    LEFT JOIN buyer_entity_links buyer
      ON buyer.source_table = 'deeds' AND buyer.source_id = sale.id
    WHERE sale.record_date IS NOT NULL
      AND acquisition.buyer_entity_id IS DISTINCT FROM buyer.buyer_entity_id
      AND NOT EXISTS (
          SELECT 1 FROM borrower_ledger_events ble
          WHERE ble.source_table = 'deeds' AND ble.source_id = sale.id
            AND ble.event_type = 'deed_sale'
            AND ble.buyer_entity_id = acquisition.buyer_entity_id
      )

    UNION ALL

    SELECT bel.buyer_entity_id, 'permit_filed' AS event_type,
           bp.issue_date AS event_date, 'building_permits' AS source_table, bp.id AS source_id,
           bp.property_id, 'Permit filed: ' || COALESCE(bp.permit_type, '') AS summary,
           NULL::numeric AS amount,
           json_build_object('permit_number', bp.permit_number, 'status', bp.status)::text AS meta_json
    FROM building_permits bp
    JOIN owners o ON o.property_id = bp.property_id
    JOIN buyer_entity_links bel ON bel.source_table = 'owners' AND bel.source_id = o.id
    WHERE bp.issue_date IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM borrower_ledger_events ble
          WHERE ble.source_table = 'building_permits' AND ble.source_id = bp.id
            AND ble.event_type = 'permit_filed'
            AND ble.buyer_entity_id = bel.buyer_entity_id
      )

    UNION ALL

    SELECT bel.buyer_entity_id, 'permit_closed' AS event_type,
           :as_of AS event_date, 'building_permits' AS source_table, bp.id AS source_id,
           bp.property_id, 'Permit closed: ' || COALESCE(bp.permit_type, '') AS summary,
           NULL::numeric AS amount,
           json_build_object('permit_number', bp.permit_number, 'status', bp.status)::text AS meta_json
    FROM building_permits bp
    JOIN owners o ON o.property_id = bp.property_id
    JOIN buyer_entity_links bel ON bel.source_table = 'owners' AND bel.source_id = o.id
    WHERE lower(trim(COALESCE(bp.status, ''))) IN ('complete', 'completed', 'closed', 'finaled')
      AND NOT EXISTS (
          SELECT 1 FROM borrower_ledger_events ble
          WHERE ble.source_table = 'building_permits' AND ble.source_id = bp.id
            AND ble.event_type = 'permit_closed'
            AND ble.buyer_entity_id = bel.buyer_entity_id
      )
""")


def sync_borrower_ledger(session: Session, *, as_of: date | None = None) -> IngestStats:
    """Append all currently visible acquisition, sale, and permit transitions."""
    as_of = as_of or date.today()
    rows = session.execute(_CANDIDATE_QUERY, {"as_of": as_of}).mappings().all()
    stats = IngestStats()
    for row in rows:
        stats.attempted += 1
        meta_json = row.meta_json
        inserted = record_event(
            session,
            buyer_entity_id=row.buyer_entity_id,
            event_type=row.event_type,
            event_date=row.event_date,
            source_table=row.source_table,
            source_id=row.source_id,
            property_id=row.property_id,
            summary=row.summary,
            amount=row.amount,
            meta=json.loads(meta_json) if meta_json else None,
        )
        stats.inserted += int(inserted)
        stats.skipped += int(not inserted)
    return stats
