"""
Florida DOR statewide sales-outcome connector (CDE-07).

Mines already-ingested, already-matched dor_sales rows (src/core/models.py:
DorSale — property_id is resolved at ingestion via a strap join/parcel
transform, 99.8%+ matched for both hillsborough and pinellas) and stages
QUALIFIED arm's-length sales (QUAL_CD 01-06 per DOR's official vocabulary) as
qualified_sale OutcomeCandidates. Unlike the other three outcome connectors,
DOR rows arrive from a genuinely new statewide file, but matching already
happened at load time here — there is nothing left for this connector to
resolve.

Deliberately NOT staged:
- unqualified sales (QUAL_CD outside 01-06/98/99) — their only downstream
  fate would be consumed-without-promotion by the label layer; the raw rows
  stay queryable in dor_sales. Counted + logged only.
- pending decisions (QUAL_CD 98/99) — a re-posted roll finalizes the code in
  place (the ingestion loader upserts on the natural key); this connector's
  next full re-scan picks them up then.
- rows with property_id NULL (no SDF address/owner to re-match on; the
  ix_dor_sales_unresolved partial index on dor_sales is their retry frontier
  for a future re-ingestion, not something this connector can fix).

Cross-source dedup: the appraiser connector (CDE-06) already stages
qualified sales for the most recent sale per parcel. A DOR row for the same
property + sale MONTH is skipped and counted — the overlap count doubles as
a QA cross-check on the appraiser scraper (a collapsing overlap means one of
the two sources went quiet). Appraiser runs weekly and DOR lags months, so
the appraiser side always lands first in practice.

Sale dates are month-granular (sale_yr/sale_mo, no day) — event_date is the
first of the sale month.

CLI:
    python -m src.connectors.dor_sale_outcomes --county-id hillsborough
"""
from __future__ import annotations

import argparse
import logging
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.connectors.outcomes import (
    EVENT_TYPE_QUALIFIED_SALE,
    OutcomeCandidateData,
    upsert_outcome_candidates_bulk,
)
from src.connectors.runner import ConnectorRunResult, run_connector

logger = logging.getLogger(__name__)

SOURCE_TYPE = "dor_sale_outcomes"

# Florida statewide sale-qualification codes, per the official NAL/SDF/NAP
# Users Guide: 01-06 = qualified arm's-length; 98/99 = decision pending;
# everything else = a specific disqualification reason.
QUALIFIED_CODES = frozenset({"01", "02", "03", "04", "05", "06"})
PENDING_CODES = frozenset({"98", "99"})


def classify(qual_cd: str) -> str:
    """'qualified' | 'pending' | 'unqualified' for a DOR QUAL_CD."""
    if qual_cd in QUALIFIED_CODES:
        return "qualified"
    if qual_cd in PENDING_CODES:
        return "pending"
    return "unqualified"


def _appraiser_months(session: Session, county_id: str) -> set[tuple[int, date]]:
    """(property_id, sale-month) pairs the appraiser connector already staged."""
    rows = session.execute(
        text(
            "SELECT property_id, date_trunc('month', event_date)::date "
            "FROM outcome_candidates "
            "WHERE source_type = 'appraiser_sale_outcomes' AND county_id = :cid"
        ),
        {"cid": county_id},
    ).fetchall()
    return {(r[0], r[1]) for r in rows}


def stage_outcomes(session: Session, county_id: str) -> ConnectorRunResult:
    result = ConnectorRunResult()

    unresolved = session.execute(
        text("SELECT COUNT(*) FROM dor_sales WHERE county_id = :cid AND property_id IS NULL"),
        {"cid": county_id},
    ).scalar()

    rows = session.execute(
        text(
            "SELECT id, property_id, qual_cd, sale_yr, sale_mo, sale_price, match_method "
            "FROM dor_sales WHERE county_id = :cid AND property_id IS NOT NULL"
        ),
        {"cid": county_id},
    ).fetchall()
    result.total_read = len(rows)

    already_staged = _appraiser_months(session, county_id)

    candidates: list[OutcomeCandidateData] = []
    unqualified = pending = overlap = 0
    for row in rows:
        kind = classify(row.qual_cd)
        if kind == "unqualified":
            unqualified += 1
            result.skipped += 1
            continue
        if kind == "pending":
            pending += 1
            result.skipped += 1
            continue
        if not (1 <= row.sale_mo <= 12):
            result.errors += 1
            logger.error("dor_sales.id=%s has invalid sale_mo=%s — not staged", row.id, row.sale_mo)
            continue
        event_date = date(row.sale_yr, row.sale_mo, 1)
        if (row.property_id, event_date) in already_staged:
            overlap += 1
            result.skipped += 1
            continue
        candidates.append(OutcomeCandidateData(
            property_id=row.property_id,
            county_id=county_id,
            source_type=SOURCE_TYPE,
            source_table="dor_sales",
            source_id=row.id,
            event_type=EVENT_TYPE_QUALIFIED_SALE,
            event_date=event_date,
            amount=row.sale_price,
            raw_status=row.qual_cd,
            match_confidence=1.0,               # deterministic join at ingestion, not a fuzzy match
            match_method=row.match_method,
        ))

    result.staged = upsert_outcome_candidates_bulk(session, candidates)

    logger.info(
        "[%s] county=%s read=%d staged=%d unqualified=%d pending=%d "
        "appraiser_overlap=%d unresolved_in_dor_sales=%d",
        SOURCE_TYPE, county_id, result.total_read, result.staged,
        unqualified, pending, overlap, unresolved,
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage DOR qualified sales as OutcomeCandidate rows")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true", help="Roll back after running — no rows persisted")
    args = parser.parse_args()

    return run_connector(SOURCE_TYPE, args.county_id, stage_outcomes, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
