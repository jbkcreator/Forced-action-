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
    resolved = [r for r in rows if r.buyer_entity_id]
    skipped_enrichment = len(rows) - len(resolved)

    if skipped_enrichment:
        logger.info(
            "[PartnerMining] %d rows need enrichment bridge (buyer_entity not yet resolved)",
            skipped_enrichment,
        )

    if not resolved:
        db.commit()
        return 0

    # Batch person_id lookup — one query for all buyer_entity_ids.
    entity_ids = list({r.buyer_entity_id for r in resolved})
    person_rows = db.execute(
        text("""
            SELECT buyer_entity_id, person_id
            FROM fa_max_persons
            WHERE buyer_entity_id = ANY(:eids)
        """),
        {"eids": entity_ids},
    ).fetchall()
    entity_to_person: dict[int, object] = {r.buyer_entity_id: r.person_id for r in person_rows}

    params_list = []
    for row in resolved:
        person_id = entity_to_person.get(row.buyer_entity_id)
        if not person_id:
            skipped_enrichment += 1
            continue
        params_list.append({
            "person_id": person_id,
            "partner_class": row.partner_class,
            "rank": row.rank,
            "count": row.observed_transaction_count,
            "first_observed": row.first_observed_at,
            "last_observed": row.last_observed_at,
            "county_id": county_id,
            "beid": row.buyer_entity_id,
        })

    if params_list:
        db.execute(
            text("""
                INSERT INTO fa_max_partners
                    (person_id, partner_class, status, rank, source,
                     observed_transaction_count, first_observed_at, last_observed_at,
                     county_id, buyer_entity_id)
                VALUES
                    (:person_id, :partner_class, 'identified', :rank, 'partner_mining',
                     :count, :first_observed, :last_observed, :county_id, :beid)
                ON CONFLICT (person_id, partner_class) DO UPDATE SET
                    rank                       = EXCLUDED.rank,
                    observed_transaction_count = EXCLUDED.observed_transaction_count,
                    first_observed_at          = COALESCE(fa_max_partners.first_observed_at,
                                                          EXCLUDED.first_observed_at),
                    last_observed_at           = EXCLUDED.last_observed_at,
                    county_id                  = EXCLUDED.county_id,
                    buyer_entity_id            = EXCLUDED.buyer_entity_id
            """),
            params_list,
        )

    db.commit()
    written = len(params_list)
    if skipped_enrichment:
        logger.info(
            "[PartnerMining] %d rows skipped (no fa_max_persons record for buyer_entity)",
            skipped_enrichment,
        )
    return written
