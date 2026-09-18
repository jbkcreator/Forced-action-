"""
Stage E — Contact enrichment bridge (SPEC Stage E / GRILL Q5).

enrich_top_partners bridges buyer_entity_id → owner_ids →
skip_trace_waterfall.run_cascade. Only active (top-25) rows are enriched
to keep cost bounded. Missing bridge → needs_enrichment=True on the row;
those rows retry on the next nightly sweep.

Cost is bounded by run_cascade's own ceiling ($0.80/lead, conf≥0.70 stop).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import text

from src.services.skip_trace_waterfall import run_cascade
from src.services.partner_mining.rank import PartnerRow

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_OWNER_IDS_SQL = """
    SELECT bel.source_id AS owner_id
    FROM buyer_entity_links bel
    WHERE bel.buyer_entity_id = :beid
      AND bel.source_table = 'owners'
"""


def enrich_top_partners(
    session: "Session",
    ranked_rows: list[PartnerRow],
    *,
    county_id: str,
) -> dict:
    """
    For each active (top-25) PartnerRow, resolve owner_ids via the
    buyer_entity_links graph and call run_cascade for those owners.

    Rows with no resolved buyer_entity_id (=0) or no linked owner rows
    are flagged needs_enrichment=True and retried on the next run.

    Returns:
        enriched           — rows where cascade was invoked
        needs_enrichment   — rows flagged for retry
        skipped_identified — 'identified' rows not enriched (by design)
    """
    enriched = 0
    needs_enrichment_count = 0
    skipped_identified = 0

    for row in ranked_rows:
        if row.status != "active":
            skipped_identified += 1
            continue

        if not row.buyer_entity_id:
            row.needs_enrichment = True
            needs_enrichment_count += 1
            continue

        owner_rows = session.execute(
            text(_OWNER_IDS_SQL), {"beid": row.buyer_entity_id}
        ).fetchall()
        owner_ids = [r.owner_id for r in owner_rows]

        if not owner_ids:
            row.needs_enrichment = True
            needs_enrichment_count += 1
            logger.debug(
                "[PartnerMining] no owner link for entity %d (%s) — needs_enrichment",
                row.buyer_entity_id, row.canonical_name,
            )
            continue

        try:
            run_cascade(
                county_id=county_id,
                owner_ids=owner_ids,
                today_only=False,
            )
            row.needs_enrichment = False
            enriched += 1
        except Exception as exc:
            logger.warning(
                "[PartnerMining] enrichment failed for %s: %s", row.canonical_name, exc
            )
            row.needs_enrichment = True
            needs_enrichment_count += 1

    return {
        "enriched": enriched,
        "needs_enrichment": needs_enrichment_count,
        "skipped_identified": skipped_identified,
    }
