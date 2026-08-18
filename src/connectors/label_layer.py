"""
CDE-10 — Label layer: promote staged OutcomeCandidate rows into DealOutcome.

Consumes the unconsumed outcome_candidates queue (staged by the outcome
connectors) and writes DealOutcome rows through the same conventions and
side-effect seam the subscriber one-tap path uses (deal_outcome_effects.
record_outcome_side_effects — a guaranteed no-op for subscriber-less rows,
called anyway so every DealOutcome writer stays on the single seam, per
CDE-11). Rows land with subscriber_id NULL, confidence_tier
'public_record_inferred' (via outcome_confidence.default_tier_for_source),
outcome_source set to the staging connector's source_type, and a
deterministic source_ref for idempotency (same partial-unique-index upsert
idiom as the founder import, B0-01).

Event mapping — only TERMINAL market resolutions promote:
  closed_won : auction_sold_third_party, tax_deed_sold, qualified_sale
  closed_lost: auction_reverted_to_lender, tax_deed_redeemed
Cancelled auctions (both kinds) are not terminal — they routinely reschedule —
and unqualified_sale is a non-arms-length transfer, not a market outcome; all
three are marked consumed WITHOUT a DealOutcome so they never pollute the
learning loop but stay auditable in outcome_candidates. An event_type this
module does not recognize is left UNCONSUMED and counted as an error, so a
future vocabulary widening fails loud here instead of being silently guessed.

deal_size_bucket stays NULL on wins — candidate.amount is a sale price, not a
profit, and the bucket enum is a profit scale; a closed_lost row carries the
established 'skip' loss sentinel (same convention as B0-01).

CLI:
    python -m src.connectors.label_layer --county-id hillsborough
"""
from __future__ import annotations

import argparse
import hashlib
import logging
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.connectors.outcomes import (
    EVENT_TYPE_AUCTION_CANCELLED,
    EVENT_TYPE_AUCTION_REVERTED_TO_LENDER,
    EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
    EVENT_TYPE_DEED_FLIP,
    EVENT_TYPE_LIEN_SALE,
    EVENT_TYPE_LP_SOLD_PRE_AUCTION,
    EVENT_TYPE_PROBATE_SALE,
    EVENT_TYPE_QUALIFIED_SALE,
    EVENT_TYPE_TAX_DEED_CANCELLED,
    EVENT_TYPE_TAX_DEED_REDEEMED,
    EVENT_TYPE_TAX_DEED_SOLD,
    EVENT_TYPE_UNQUALIFIED_SALE,
)
from src.connectors.runner import ConnectorRunResult, run_connector
from src.services import outcome_confidence

logger = logging.getLogger(__name__)

SOURCE_TYPE = "outcome_label_layer"

PIPELINE_STAGE_BY_EVENT = {
    EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY: "closed_won",
    EVENT_TYPE_TAX_DEED_SOLD: "closed_won",
    EVENT_TYPE_QUALIFIED_SALE: "closed_won",
    # CDE-03/05/08 connectors (deed flip, lis-pendens, probate/lien) landed
    # after this map was first written and stage their own terminal-sale
    # events; each is a completed arms-length resale (property changed hands),
    # so all map to closed_won. Absent these, every row those live connectors
    # staged fell through to the "unrecognized event_type" branch and was left
    # unconsumed, re-failing every daily run.
    EVENT_TYPE_DEED_FLIP: "closed_won",
    EVENT_TYPE_LP_SOLD_PRE_AUCTION: "closed_won",
    EVENT_TYPE_PROBATE_SALE: "closed_won",
    EVENT_TYPE_LIEN_SALE: "closed_won",
    EVENT_TYPE_AUCTION_REVERTED_TO_LENDER: "closed_lost",
    EVENT_TYPE_TAX_DEED_REDEEMED: "closed_lost",
}

# Consumed without promotion: cancelled auctions reschedule (not terminal);
# an unqualified sale is a non-arms-length transfer (not a market outcome).
NON_TERMINAL_EVENTS = frozenset({
    EVENT_TYPE_AUCTION_CANCELLED,
    EVENT_TYPE_TAX_DEED_CANCELLED,
    EVENT_TYPE_UNQUALIFIED_SALE,
})


def source_ref_for(source_type: str, source_table: str, source_id: int,
                   event_date: date) -> str:
    """Deterministic deal_outcomes.source_ref for a candidate (md5 hex, 32 chars).

    Mirrors the candidate's own natural key (uq_outcome_candidate), so one
    staged outcome maps to exactly one DealOutcome across any number of runs.
    """
    basis = f"{source_type}|{source_table}|{source_id}|{event_date.isoformat()}"
    return hashlib.md5(basis.encode("utf-8")).hexdigest()


_SELECT_UNCONSUMED_SQL = text("""
SELECT id, property_id, county_id, source_type, source_table, source_id,
       event_type, event_date, amount
FROM outcome_candidates
WHERE consumed_at IS NULL AND county_id = :cid
ORDER BY id
""")

# confidence_tier/outcome_source are provenance — set at insert, never
# rewritten on a re-run. The measurable fields refresh in place so a
# connector's own upsert corrections (e.g. a revised winning bid) flow through.
_UPSERT_SQL = text("""
INSERT INTO deal_outcomes
    (subscriber_id, property_id, deal_size_bucket, deal_amount, deal_date,
     pipeline_stage, county_id, confidence_tier, outcome_source, source_ref,
     created_at)
VALUES
    (NULL, :pid, :bucket, :amount, :ddate,
     :stage, :county, :tier, :osource, :sref, NOW())
ON CONFLICT (source_ref) WHERE source_ref IS NOT NULL DO UPDATE SET
    property_id      = EXCLUDED.property_id,
    deal_size_bucket = EXCLUDED.deal_size_bucket,
    deal_amount      = EXCLUDED.deal_amount,
    deal_date        = EXCLUDED.deal_date,
    pipeline_stage   = EXCLUDED.pipeline_stage,
    county_id        = EXCLUDED.county_id
RETURNING id
""")

_MARK_CONSUMED_SQL = text("""
UPDATE outcome_candidates
SET consumed_at = NOW(), updated_at = NOW()
WHERE id = ANY(:ids)
""")


def _promote_one(session: Session, row) -> int:
    """Upsert one DealOutcome from a candidate row; return the deal_outcomes id."""
    stage = PIPELINE_STAGE_BY_EVENT[row.event_type]
    outcome_id = session.execute(_UPSERT_SQL, {
        "pid":     row.property_id,
        "bucket":  "skip" if stage == "closed_lost" else None,
        "amount":  row.amount,
        "ddate":   row.event_date,
        "stage":   stage,
        "county":  row.county_id,
        "tier":    outcome_confidence.default_tier_for_source(row.source_type),
        "osource": row.source_type,
        "sref":    source_ref_for(row.source_type, row.source_table,
                                  row.source_id, row.event_date),
    }).scalar_one()

    # The CDE-11 seam every DealOutcome writer routes through. No-op while
    # subscriber_id is NULL, but keeps pipeline rows on the same path as the
    # subscriber tap — any future seam behavior applies here automatically.
    from src.core.models import DealOutcome
    from src.services.deal_outcome_effects import record_outcome_side_effects
    record_outcome_side_effects(session.get(DealOutcome, outcome_id), None, session)

    return outcome_id


def promote_candidates(session: Session, county_id: str) -> ConnectorRunResult:
    """Promote every unconsumed candidate for a county.

    ConnectorRunResult mapping: total_read = candidates read, staged =
    DealOutcome rows written, skipped = non-terminal events consumed without
    promotion, errors = rows left unconsumed for retry (promotion failure or
    unrecognized event_type).
    """
    result = ConnectorRunResult()
    rows = session.execute(_SELECT_UNCONSUMED_SQL, {"cid": county_id}).fetchall()
    result.total_read = len(rows)

    consumed_ids: list[int] = []
    for row in rows:
        if row.event_type in NON_TERMINAL_EVENTS:
            consumed_ids.append(row.id)
            result.skipped += 1
            continue
        if row.event_type not in PIPELINE_STAGE_BY_EVENT:
            logger.error(
                "Unrecognized event_type %r on outcome_candidates.id=%s — "
                "left unconsumed; teach label_layer this event before it can promote.",
                row.event_type, row.id,
            )
            result.errors += 1
            continue
        try:
            # SAVEPOINT per row: a DB-level failure (constraint violation, bad
            # type coercion, etc.) aborts only this row's nested transaction,
            # not the whole run — otherwise one bad row would poison every
            # later statement in this session, including the final mark-
            # consumed UPDATE, silently rolling back rows already logged as
            # promoted.
            with session.begin_nested():
                _promote_one(session, row)
            consumed_ids.append(row.id)
            result.staged += 1
        except Exception:
            logger.exception(
                "Failed to promote outcome_candidates.id=%s (event=%s) — left unconsumed.",
                row.id, row.event_type,
            )
            result.errors += 1

    if consumed_ids:
        session.execute(_MARK_CONSUMED_SQL, {"ids": consumed_ids})

    logger.info(
        "[%s] county=%s read=%d promoted=%d skipped_non_terminal=%d errors=%d",
        SOURCE_TYPE, county_id, result.total_read, result.staged,
        result.skipped, result.errors,
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Promote staged OutcomeCandidate rows into DealOutcome (CDE-10)"
    )
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true",
                        help="Roll back after running — no rows persisted")
    args = parser.parse_args()

    return run_connector(SOURCE_TYPE, args.county_id, promote_candidates, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
