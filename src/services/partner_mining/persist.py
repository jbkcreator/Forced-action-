"""
Persistence layer for WP-T2-9 partner mining (SPEC Stage F).

upsert_partner_rows writes ranked PartnerRow objects to fa_max_partners.
Matching key: (person_id, partner_class) — non-destructive (dropouts keep
their row, rank + ranking fields updated in place).

status is intentionally excluded from the conflict UPDATE — it is governed
by trg_fa_max_partner_state_guard and must only transition through the
state-engine contract, never overwritten directly by the sweep.
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

    Rows without a resolved buyer_entity_id (=0 stub) are skipped — they
    cannot satisfy the person_id FK until identity resolution runs.
    """
    written = 0
    skipped_enrichment = 0

    for row in rows:
        if not row.buyer_entity_id:
            skipped_enrichment += 1
            continue

        # Derive person_id from fa_max_persons.buyer_entity_id (WP-1 spine).
        person_id_row = db.execute(
            text("""
                SELECT person_id
                FROM fa_max_persons
                WHERE buyer_entity_id = :beid
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
                    (:person_id, :partner_class, 'identified', :rank, 'partner_mining',
                     :count, :last_observed, :county_id, :beid)
                ON CONFLICT (person_id, partner_class) DO UPDATE SET
                    rank                       = EXCLUDED.rank,
                    observed_transaction_count = EXCLUDED.observed_transaction_count,
                    last_observed_at           = EXCLUDED.last_observed_at,
                    county_id                  = EXCLUDED.county_id,
                    buyer_entity_id            = EXCLUDED.buyer_entity_id
            """),
            {
                "person_id": person_id,
                "partner_class": row.partner_class,
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
