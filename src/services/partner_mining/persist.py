"""
Persistence layer for WP-T2-9 partner mining (SPEC Stage F).

upsert_partner_rows writes ranked PartnerRow objects to fa_max_partners.
Matching key: (partner_class, canonical_name, county_id) — non-destructive
(dropouts keep their row, rank + status updated in place).

NOTE: person_id is required by the fa_max_partners FK. In v1, identity
resolution (Stage B) populates buyer_entity_id; the person_id is derived
from the fa_max_persons link set up by the WP-1 spine. Rows without a
resolved person are held as needs-enrichment and not yet written to
fa_max_partners — they require the enrichment bridge (Stage E).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from src.services.partner_mining.rank import PartnerRow

logger = logging.getLogger(__name__)


def upsert_partner_rows(
    db: "Session",
    rows: list[PartnerRow],
    *,
    county_id: str,
) -> int:
    """
    Upsert ranked partner rows into fa_max_partners.
    Returns the count of rows written.

    Rows that have no resolved buyer_entity_id (=0 stub) are logged as
    needs-enrichment and skipped — they cannot satisfy the person_id FK
    until Stage B identity resolution and Stage E enrichment bridge run.
    """
    written = 0
    skipped_enrichment = 0

    for row in rows:
        if not row.buyer_entity_id:
            skipped_enrichment += 1
            continue

        # Derive person_id from the buyer_entity → fa_max_persons link.
        person_id_row = db.execute(
            text("""
                SELECT p.person_id
                FROM fa_max_persons p
                JOIN buyer_entity_contact_anchors a ON a.person_id = p.person_id
                WHERE a.buyer_entity_id = :beid
                LIMIT 1
            """),
            {"beid": row.buyer_entity_id},
        ).fetchone()

        if not person_id_row:
            skipped_enrichment += 1
            continue

        person_id = person_id_row[0]

        db.execute(
            text("""
                INSERT INTO fa_max_partners
                    (person_id, partner_class, status, rank, source,
                     observed_transaction_count, last_observed_at,
                     county_id, buyer_entity_id)
                VALUES
                    (:person_id, :partner_class, :status, :rank, 'partner_mining',
                     :count, :last_observed, :county_id, :beid)
                ON CONFLICT (person_id, partner_class) DO UPDATE SET
                    status                    = EXCLUDED.status,
                    rank                      = EXCLUDED.rank,
                    observed_transaction_count = EXCLUDED.observed_transaction_count,
                    last_observed_at          = EXCLUDED.last_observed_at,
                    county_id                 = EXCLUDED.county_id,
                    buyer_entity_id           = EXCLUDED.buyer_entity_id
            """),
            {
                "person_id": person_id,
                "partner_class": row.partner_class,
                "status": row.status,
                "rank": row.rank,
                "count": row.observed_transaction_count,
                "last_observed": row.last_observed_at,
                "county_id": county_id,
                "beid": row.buyer_entity_id,
            },
        )
        written += 1

    if skipped_enrichment:
        logger.info(
            "[PartnerMining] %d rows need enrichment bridge (buyer_entity not yet resolved)",
            skipped_enrichment,
        )

    db.commit()
    return written
