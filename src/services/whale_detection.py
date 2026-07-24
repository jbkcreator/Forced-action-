"""
Whale detection (HUNTER-02, W1).

Flags a BuyerEntity as a whale — 3+ purchases in the trailing 18 months, OR
>$500K total cash volume — per Hunter's constitution standing run #3.
Pure aggregate query, no model of any kind: this never needed one.

Reuses BuyerEntity.total_cash_volume (populated by
src/services/buyer_entity_resolution.refresh_portfolio_aggregates, an
all-time sum) for the cash-volume half of the rule. The purchase-count half
is a genuinely different, rolling-window question an all-time count can't
answer, so it runs its own dedicated query here.

Also mints an Opportunity Thread ID (`OPP-YYYY-#####`) the moment an entity
first qualifies as a whale, per the dev-split plan §6b — Hunter (W1/W3) is
first in Phase 1 to need one. Minted once, never reassigned: an entity that
later drops out of whale status keeps its ID, since the ID identifies the
opportunity, not the current flag state.

Usage:
    from src.services.whale_detection import refresh_whale_flags
    refresh_whale_flags(session)                    # re-score every entity
    refresh_whale_flags(session, entity_ids=[1, 2])  # re-score just these
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

WHALE_MIN_PURCHASES = 3
WHALE_PURCHASE_WINDOW_DAYS = 548  # ~18 months
WHALE_MIN_CASH_VOLUME = 500_000

# Nominal-consideration floor ($1/$10/$0 family, trust, and corrective
# re-recordings of the same transaction) — same convention already used
# elsewhere in this repo (scoring_training_data.py, conversion_report.py):
# NULL is allowed through (an unrecorded price isn't necessarily nominal),
# but an explicit low price is excluded. Found via the founder spot-check:
# several "3+ purchase" whales turned out to be $1-$10 trust/family
# transfers, sometimes the SAME property recorded multiple times on the
# same day — not real purchases at all.
NOMINAL_CONSIDERATION_FLOOR = 1000

OPPORTUNITY_ID_PREFIX = "OPP-{year}-"
OPPORTUNITY_ID_SEQ_WIDTH = 5


def _mint_opportunity_ids(session: Session, count: int) -> list[str]:
    """Next `count` sequential OPP-YYYY-##### IDs for the current year.

    Single-writer context (nightly sweep / this connector only) — no
    SELECT-then-INSERT race to guard against, so a plain MAX+1 read is
    correct here without a dedicated sequence table.
    """
    if count == 0:
        return []
    prefix = OPPORTUNITY_ID_PREFIX.format(year=date.today().year)
    max_seq = session.execute(
        text("""
            SELECT COALESCE(MAX(CAST(SUBSTRING(opportunity_thread_id FROM :seq_start) AS INT)), 0)
            FROM buyer_entities
            WHERE opportunity_thread_id LIKE :pattern
        """),
        {"seq_start": len(prefix) + 1, "pattern": f"{prefix}%"},
    ).scalar()
    return [f"{prefix}{max_seq + i:0{OPPORTUNITY_ID_SEQ_WIDTH}d}" for i in range(1, count + 1)]


def refresh_whale_flags(session: Session, entity_ids: Optional[list[int]] = None) -> int:
    """
    Re-evaluate is_whale for every entity (or just entity_ids, if given) —
    continuously recalculated, per the constitution, not "set true and never
    reconsidered": an entity whose qualifying purchases age out of the
    18-month window and whose cash volume is under the floor is correctly
    demoted back to is_whale=false, not left stale.

    Single SQL statement via CTEs — never loops per-entity (a query-per-
    entity Python loop is exactly the pattern this repo's conventions
    forbid at any real row count, and this is meant to run over the full
    buyer_entities table on every nightly sweep).
    """
    entity_filter = "WHERE be.id = ANY(:entity_ids)" if entity_ids else ""
    result = session.execute(
        text(f"""
            WITH recent_purchases AS (
                SELECT bel.buyer_entity_id, COUNT(DISTINCT d.property_id) AS recent_count
                FROM buyer_entity_links bel
                JOIN deeds d ON d.id = bel.source_id AND bel.source_table = 'deeds'
                WHERE d.record_date >= (CURRENT_DATE - (:window_days * INTERVAL '1 day'))
                  AND (d.sale_price IS NULL OR d.sale_price >= :nominal_floor)
                GROUP BY bel.buyer_entity_id
            ),
            whale_status AS (
                SELECT be.id,
                       be.is_whale AS was_whale,
                       (COALESCE(rp.recent_count, 0) >= :min_purchases
                            OR be.total_cash_volume > :min_cash_volume) AS qualifies
                FROM buyer_entities be
                LEFT JOIN recent_purchases rp ON rp.buyer_entity_id = be.id
                {entity_filter}
            )
            UPDATE buyer_entities be
            SET is_whale = ws.qualifies,
                whale_flagged_at = CASE WHEN ws.qualifies THEN now() ELSE be.whale_flagged_at END
            FROM whale_status ws
            WHERE be.id = ws.id
            RETURNING be.id, ws.was_whale, ws.qualifies
        """),
        {
            "window_days": WHALE_PURCHASE_WINDOW_DAYS,
            "min_purchases": WHALE_MIN_PURCHASES,
            "min_cash_volume": WHALE_MIN_CASH_VOLUME,
            "nominal_floor": NOMINAL_CONSIDERATION_FLOOR,
            **({"entity_ids": entity_ids} if entity_ids else {}),
        },
    )
    rows = result.fetchall()

    newly_qualifying_ids = [r.id for r in rows if r.qualifies and not r.was_whale]
    opportunity_ids = _mint_opportunity_ids(session, len(newly_qualifying_ids))
    if newly_qualifying_ids:
        session.execute(
            text("""
                UPDATE buyer_entities SET opportunity_thread_id = :oid
                WHERE id = :id AND opportunity_thread_id IS NULL
            """),
            [{"id": eid, "oid": oid} for eid, oid in zip(newly_qualifying_ids, opportunity_ids)],
        )
        logger.info(
            "refresh_whale_flags: minted %d new opportunity thread ID(s): %s",
            len(newly_qualifying_ids), ", ".join(opportunity_ids),
        )

    session.commit()
    logger.info("refresh_whale_flags: re-evaluated %d entities.", len(rows))
    return len(rows)
